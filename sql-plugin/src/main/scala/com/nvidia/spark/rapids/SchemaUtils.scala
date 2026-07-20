/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.util.Optional

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import ai.rapids.cudf._
import ai.rapids.cudf.ColumnWriterOptions._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import org.apache.orc.TypeDescription

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object SchemaUtils {
  // Side-channel keys for nested element field ids. Spark's StructField metadata only attaches to
  // top-level fields, so to round-trip ids for list elements / map keys / map values we store
  // them on the parent StructField under these keys (with `_NESTED_IDS` carrying further-nested
  // ids as a JSON-serialized Metadata).
  val LIST_ELEMENT_FIELD_ID_METADATA_KEY =
    "rapids.parquet.list.element.field.id"
  val LIST_ELEMENT_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.list.element.nested.ids"
  val MAP_KEY_FIELD_ID_METADATA_KEY =
    "rapids.parquet.map.key.field.id"
  val MAP_KEY_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.map.key.nested.ids"
  val MAP_VALUE_FIELD_ID_METADATA_KEY =
    "rapids.parquet.map.value.field.id"
  val MAP_VALUE_NESTED_IDS_METADATA_KEY =
    "rapids.parquet.map.value.nested.ids"

  /**
   * Convert a TypeDescription to a Catalyst StructType.
   */
  implicit def toCatalystSchema(schema: TypeDescription): StructType = {
    // Here just follows the implementation of Spark3.0.x, so it does not replace the
    // CharType/VarcharType with StringType. It is OK because GPU does not support
    // these two char types yet.
    CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
  }

  private def getPrecisionsList(dt: DataType): Seq[Int] = dt match {
    case ArrayType(et, _) => getPrecisionsList(et)
    case MapType(kt, vt, _) => getPrecisionsList(kt) ++ getPrecisionsList(vt)
    case StructType(fields) => fields.flatMap(f => getPrecisionsList(f.dataType))
    case d: DecimalType => Seq(d.precision)
    case _ => Seq.empty[Int]
  }

  private def buildTypeIdMapFromSchema(schema: StructType,
      isCaseSensitive: Boolean): Map[String, (DataType, Int)] = {
    val typeIdSeq = schema.map(_.dataType).zipWithIndex
    val name2TypeIdSensitiveMap = schema.map(_.name).zip(typeIdSeq).toMap
    if (isCaseSensitive) {
      name2TypeIdSensitiveMap
    } else {
      CaseInsensitiveMap[(DataType, Int)](name2TypeIdSensitiveMap)
    }
  }

  /**
   * Execute the schema evolution, which includes
   *  1) Casting decimal columns with precision that can be stored in an int to cuDF DECIMAL32.
   *     The reason to do this is the plugin requires decimals being stored as DECIMAL32 if the
   *     precision is small enough to fit in an int. And getting this wrong may lead to a number
   *     of problems later on. For example, the cuDF ORC reader always read decimals as DECIMAL64.
   *  2) Adding columns filled with nulls for names are in the "readSchema"
   *     but not in the "tableSchema",
   *  3) Re-ordering columns according to the `readSchema`,
   *  4) Removing columns not being required by the `readSchema`,
   *  5) Running type casting when the required type is not equal to the column type.
   *     "castFunc" is required in this case, otherwise it will blow up.
   *
   * @param table The input table, will be closed after returning
   * @param tableSchema The schema of the table
   * @param readSchema  The read schema from Spark
   * @param isCaseSensitive Whether the name check should be case sensitive or not
   * @param castFunc optional, function to cast the input column to the required type
   * @param needCast if true, table columns will always be traversed to look for needed casts
   * @param allowNonStructWrapping if true, allow wrapping a non-STRUCT column into a struct
   *   view when the expected Catalyst type is a single-field StructType. This is only needed
   *   for the Parquet legacy 2-level LIST case where cuDF returns an unwrapped child column.
   *   Should NOT be enabled for ORC or other formats.
   * @return a new table mapping to the "readSchema". Users should close it if no longer needed.
   */
  private[rapids] def evolveSchemaIfNeededAndClose(
      table: Table,
      tableSchema: StructType,
      readSchema: StructType,
      isCaseSensitive: Boolean,
      castFunc: Option[(ColumnView, DataType, DataType) => ColumnView] = None,
      needCast: Boolean = false,
      allowNonStructWrapping: Boolean = false): Table = {
    // Schema evolution is needed when
    //   1) there are columns with precision can be stored in an int, or
    //   2) "readSchema" is not equal to "tableSchema".
    val isSchemaEvolutionNeeded = closeOnExcept(table) { _ =>
      assert(table.getNumberOfColumns == tableSchema.length)
      needCast ||
          getPrecisionsList(tableSchema).exists(p => p <= Decimal.MAX_INT_DIGITS) ||
          !TrampolineUtil.sameType(readSchema, tableSchema)
    }
    if (isSchemaEvolutionNeeded) {
      withResource(table) { _ =>
        val name2TypeIdMap = buildTypeIdMapFromSchema(tableSchema, isCaseSensitive)
        val newColumns = readSchema.safeMap { rf =>
          if (name2TypeIdMap.contains(rf.name)) {
            // Found the column in the table, so start the column evolution.
            val typeAndId = name2TypeIdMap(rf.name)
            val cv = table.getColumn(typeAndId._2)
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val newCol = evolveColumnRecursively(cv, typeAndId._1, rf.dataType, isCaseSensitive,
                toClose, castFunc, needCast, allowNonStructWrapping)
              if (newCol == cv) {
                cv.incRefCount()
              } else {
                toClose += newCol
                newCol.copyToColumnVector()
              }
            }
          } else {
            // Return a null column if the name is not found in the table.
            GpuColumnVector.columnVectorFromNull(table.getRowCount.toInt, rf.dataType)
          }
        }
        withResource(newColumns) { newCols =>
          new Table(newCols: _*)
        }
      }
    } else {
      table
    }
  }

  private def evolveColumnRecursively(
      col: ColumnView, colType: DataType, targetType: DataType,
      isCaseSensitive: Boolean, toClose: ArrayBuffer[ColumnView],
      castFunc: Option[(ColumnView, DataType, DataType) => ColumnView],
      needCast: Boolean,
      allowNonStructWrapping: Boolean): ColumnView = {
    // An util function to add a view to the buffer "toClose".
    val addToClose = (v: ColumnView) => {
      toClose += v
      v
    }

    (colType, targetType) match {
      case (colSt: StructType, toSt: StructType) =>
        // This is for the case of nested columns.
        val needSchemaEvo = needCast ||
            !TrampolineUtil.sameType(colSt, toSt) ||
            getPrecisionsList(colSt).exists(p => p <= Decimal.MAX_INT_DIGITS)
        if (needSchemaEvo) {
          // cuDF's Parquet reader may not follow the backward-compatibility rules
          // for legacy 2-level LIST types (e.g. repeated groups named "array" with
          // a single field). In this case cuDF returns the unwrapped child column
          // instead of a STRUCT column. Detect this and wrap it back into a struct
          // so schema evolution can proceed normally. See NVIDIA/spark-rapids#11454.
          // Gated by allowNonStructWrapping so this only applies to Parquet paths,
          // not ORC or other formats that share this method.
          val actualCol = if (allowNonStructWrapping &&
              col.getType.getTypeId != DType.DTypeEnum.STRUCT &&
              colSt.length == 1) {
            addToClose(ColumnView.makeStructView(col))
          } else {
            col
          }
          val typeIdMap = buildTypeIdMapFromSchema(colSt, isCaseSensitive)
          val newViews = toSt.safeMap { f =>
            if (typeIdMap.contains(f.name)) {
              val typeAndId = typeIdMap(f.name)
              val cv = addToClose(actualCol.getChildColumnView(typeAndId._2))
              val newChild = evolveColumnRecursively(cv, typeAndId._1, f.dataType,
                isCaseSensitive, toClose, castFunc, needCast, allowNonStructWrapping)
              if (newChild != cv) {
                addToClose(newChild)
              }
              newChild
            } else {
              // Return a null column if the name is not found in the table.
              addToClose(GpuColumnVector.columnVectorFromNull(
                actualCol.getRowCount.toInt, f.dataType))
            }
          }
          val opNullCount =
            Optional.of(actualCol.getNullCount.asInstanceOf[java.lang.Long])
          new ColumnView(actualCol.getType, actualCol.getRowCount, opNullCount,
            actualCol.getValid, actualCol.getOffsets, newViews.toArray)
        } else {
          col
        }
      case (colAt: ArrayType, toAt: ArrayType) =>
        val child = addToClose(col.getChildColumnView(0))
        val newChild = evolveColumnRecursively(child, colAt.elementType, toAt.elementType,
          isCaseSensitive, toClose, castFunc, needCast, allowNonStructWrapping)
        if (child == newChild) {
          col
        } else {
          col.replaceListChild(addToClose(newChild))
        }
      case (colMt: MapType, toMt: MapType) =>
        val listChild = addToClose(col.getChildColumnView(0))
        // listChild is struct with two fields: key and value.
        val newStructChildren = new ArrayBuffer[ColumnView](2)
        val newStructIndices = new ArrayBuffer[Int](2)

        // An until function to handle key and value view
        val processView = (id: Int, srcType: DataType, distType: DataType) => {
          val view = addToClose(listChild.getChildColumnView(id))
          val newView = evolveColumnRecursively(view, srcType, distType, isCaseSensitive,
            toClose, castFunc, needCast, allowNonStructWrapping)
          if (newView != view) {
            newStructChildren += addToClose(newView)
            newStructIndices += id
          }
        }
        // key and value
        processView(0, colMt.keyType, toMt.keyType)
        processView(1, colMt.valueType, toMt.valueType)

        if (newStructChildren.nonEmpty) {
          // Have new key or value, or both
          col.replaceListChild(
            addToClose(listChild.replaceChildrenWithViews(newStructIndices.toArray,
              newStructChildren.toArray))
          )
        } else {
          col
        }
      case (fromDec: DecimalType, toDec: DecimalType) if fromDec == toDec &&
          !GpuColumnVector.getNonNestedRapidsType(fromDec).equals(col.getType) =>
        col.castTo(DecimalUtil.createCudfDecimal(fromDec))
      case (fromChar: CharType, StringType) =>
        castFunc.map(f => f(col, StringType, fromChar))
          .getOrElse(throw new QueryExecutionException("Casting function is missing for " +
            s"type conversion from $colType to $targetType"))
      case _ if !GpuColumnVector.getNonNestedRapidsType(targetType).equals(col.getType) =>
        castFunc.map(f => f(col, targetType, colType))
          .getOrElse(throw new QueryExecutionException("Casting function is missing for " +
            s"type conversion from $colType to $targetType"))
      case _ => col
    }
  }

  private def writerOptionsFromField[T <: NestedBuilder[T, V], V <: ColumnWriterOptions](
      builder: NestedBuilder[T, V],
      dataType: DataType,
      name: String,
      nullable: Boolean,
      writeInt96: Boolean,
      fieldMeta: Metadata,
      parquetFieldIdWriteEnabled: Boolean): T = {

    // Parquet specific field id
    val parquetFieldId: Option[Int] = if (fieldMeta.contains(FIELD_ID_METADATA_KEY)) {
      Option(Math.toIntExact(fieldMeta.getLong(FIELD_ID_METADATA_KEY)))
    } else {
      Option.empty
    }

    def nestedFieldMetadata(fieldIdKey: String, nestedIdsKey: String): Metadata = {
      val nestedBuilder = new MetadataBuilder()
      if (fieldMeta.contains(fieldIdKey)) {
        nestedBuilder.putLong(FIELD_ID_METADATA_KEY, fieldMeta.getLong(fieldIdKey))
      }
      if (fieldMeta.contains(nestedIdsKey)) {
        nestedBuilder.withMetadata(Metadata.fromJson(fieldMeta.getString(nestedIdsKey)))
      }
      nestedBuilder.build()
    }

    dataType match {
      case dt: DecimalType =>
        if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withDecimalColumn(name, dt.precision, nullable, parquetFieldId.get)
        } else {
          builder.withDecimalColumn(name, dt.precision, nullable)
        }
      case TimestampType =>
        if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withTimestampColumn(name, writeInt96, nullable, parquetFieldId.get)
        } else {
          builder.withTimestampColumn(name, writeInt96, nullable)
        }
      case s: StructType =>
        val structB = if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          structBuilder(name, nullable, parquetFieldId.get)
        } else {
          structBuilder(name, nullable)
        }
        builder.withStructColumn(writerOptionsFromSchema(
          structB,
          s,
          nullable = nullable,
          writeInt96,
          parquetFieldIdWriteEnabled).build())
      case a: ArrayType =>
        val elementFieldMetadata = nestedFieldMetadata(
          LIST_ELEMENT_FIELD_ID_METADATA_KEY,
          LIST_ELEMENT_NESTED_IDS_METADATA_KEY)
        val outerListBuilder = if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          listBuilder(name, nullable, parquetFieldId.get)
        } else {
          listBuilder(name, nullable)
        }
        val listOptions = a.elementType match {
          case BinaryType =>
            val elementParquetFieldId = if (elementFieldMetadata.contains(FIELD_ID_METADATA_KEY)) {
              Option(Math.toIntExact(elementFieldMetadata.getLong(FIELD_ID_METADATA_KEY)))
            } else {
              Option.empty
            }
            if (parquetFieldIdWriteEnabled && elementParquetFieldId.nonEmpty) {
              outerListBuilder
                .withBinaryColumn("element", a.containsNull, elementParquetFieldId.get)
                .build()
            } else {
              outerListBuilder.withBinaryColumn("element", a.containsNull).build()
            }
          case _ =>
            writerOptionsFromField(
              outerListBuilder,
              a.elementType,
              "element",
              a.containsNull,
              writeInt96,
              elementFieldMetadata,
              parquetFieldIdWriteEnabled).build()
        }
        builder.withListColumn(listOptions)
      case m: MapType =>
        // It is ok to use `StructBuilder` here for key and value, since either
        // `OrcWriterOptions.Builder` or `ParquetWriterOptions.Builder` is actually an
        // `AbstractStructBuilder`, and here only handles the common column metadata things.
        val keyOptions = writerOptionsFromField(
          // This nullable is useless because we use the child of struct column
          structBuilder(name, nullable),
          m.keyType,
          "key",
          nullable = false,
          writeInt96,
          nestedFieldMetadata(
            MAP_KEY_FIELD_ID_METADATA_KEY,
            MAP_KEY_NESTED_IDS_METADATA_KEY),
          parquetFieldIdWriteEnabled).build().getChildColumnOptions()(0)
        val valueOptions = writerOptionsFromField(
          structBuilder(name, nullable),
          m.valueType,
          "value",
          m.valueContainsNull,
          writeInt96,
          nestedFieldMetadata(
            MAP_VALUE_FIELD_ID_METADATA_KEY,
            MAP_VALUE_NESTED_IDS_METADATA_KEY),
          parquetFieldIdWriteEnabled).build().getChildColumnOptions()(0)
        // set the nullable for this map
        // if `m` is a key of another map, this `nullable` should be false
        // e.g.: map1(map2(int,int), int), the map2 is the map
        // key of map1, map2 should be non-nullable
        val mapOptions = if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          mapColumn(name, keyOptions, valueOptions, nullable, parquetFieldId.get)
        } else {
          mapColumn(name, keyOptions, valueOptions, nullable)
        }
        builder.withMapColumn(mapOptions)
      case BinaryType =>
        if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withBinaryColumn(name, nullable, parquetFieldId.get)
        } else {
          builder.withBinaryColumn(name, nullable)
        }
      case _ =>
        if (parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withColumn(nullable, name, parquetFieldId.get)
        } else {
          builder.withColumns(nullable, name)
        }
    }
    builder.asInstanceOf[T]
  }

  /**
   * Build writer options from schema for both ORC and Parquet writers.
   *
   * (There is an open issue "https://github.com/rapidsai/cudf/issues/7654" for Parquet writer,
   * but it is circumvented by https://github.com/rapidsai/cudf/pull/9061, so the nullable can
   * go back to the actual setting, instead of the hard-coded nullable=true before.)
   */
  def writerOptionsFromSchema[T <: NestedBuilder[T, V], V <: ColumnWriterOptions](
      builder: NestedBuilder[T, V],
      schema: StructType,
      nullable: Boolean,
      writeInt96: Boolean = false,
      parquetFieldIdEnabled: Boolean = false): T = {
    schema.foreach(field =>
      // CUDF has issues if the child of a struct is not-nullable, but the struct itself is
      // So we have to work around it and tell CUDF what it expects.
      writerOptionsFromField(builder, field.dataType, field.name, nullable || field.nullable,
        writeInt96, field.metadata, parquetFieldIdWriteEnabled = parquetFieldIdEnabled)
    )
    builder.asInstanceOf[T]
  }
}
