/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet

import java.util.{Locale, Optional}

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnView, DType, Table}
import com.nvidia.spark.rapids.{CastOptions, GpuCast, GpuColumnVector, SchemaUtils}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.parquet.ParquetSchemaClipShims
import org.apache.parquet.schema._
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._

object ParquetSchemaUtils {
  // Copied from Spark
  private val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"
  // Copied from Spark
  val EMPTY_MESSAGE: MessageType = Types.buildMessage().named(SPARK_PARQUET_SCHEMA_NAME)

  /**
   * Similar to Spark's ParquetReadSupport.clipParquetSchema but does NOT add fields that only
   * exist in `catalystSchema` to the resulting Parquet schema. This only removes column paths
   * that do not exist in `catalystSchema`.
   */
  def clipParquetSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): MessageType = {
    val clippedParquetFields = clipParquetGroupFields(
      parquetSchema.asGroupType(), catalystSchema, caseSensitive, useFieldId)
    if (clippedParquetFields.isEmpty) {
      EMPTY_MESSAGE
    } else {
      Types
          .buildMessage()
          .addFields(clippedParquetFields: _*)
          .named(SPARK_PARQUET_SCHEMA_NAME)
    }
  }

  private def clipParquetType(
      parquetType: Type,
      catalystType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Type = {
    val newParquetType = catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        clipParquetListType(parquetType.asGroupType(), t.elementType, caseSensitive, useFieldId)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
            !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        clipParquetMapType(
          parquetType.asGroupType(), t.keyType, t.valueType, caseSensitive, useFieldId)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t, caseSensitive, useFieldId)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        parquetType
    }

    if (useFieldId && parquetType.getId != null) {
      newParquetType.withId(parquetType.getId.intValue())
    } else {
      newParquetType
    }
  }

  /**
   * Whether a Catalyst DataType is primitive.  Primitive DataType is not equivalent to
   * AtomicType.  For example, CalendarIntervalType is primitive, but it's not an
   * AtomicType.
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet GroupType which corresponds to a Catalyst ArrayType.  The element type
   * of the ArrayType should also be a nested type, namely an ArrayType, a MapType, or a
   * StructType.
   */
  @scala.annotation.nowarn("msg=method as in class Builder is deprecated")
  private def clipParquetListType(
      parquetList: GroupType,
      elementType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Type = {
    // Precondition of this method, should only be called for lists with nested element types.
    assert(!isPrimitiveCatalystType(elementType))

    // Unannotated repeated group should be interpreted as required list of required element, so
    // list element type is just the group itself.  Clip it.
    // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
    //if (parquetList.getLogicalTypeAnnotation == null &&
    if (parquetList.getOriginalType == null &&
        parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType, caseSensitive, useFieldId)
    } else {
      assert(
        // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
        //parquetList.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation],
        parquetList.getOriginalType == OriginalType.LIST,
        "Invalid Parquet schema. " +
            "Logical type annotation of annotated Parquet lists must be " +
            "ListLogicalTypeAnnotation: " + parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
            "LIST-annotated group should only have exactly one repeated field: " +
            parquetList)

      // Precondition of this method, should only be called for lists with nested element types.
      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()

      // If the repeated field is a group with multiple fields, or the repeated field is a group
      // with one field and is named either "array" or uses the LIST-annotated group's name with
      // "_tuple" appended then the repeated type is the element type and elements are required.
      // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
      // only field.
      if (
        repeatedGroup.getFieldCount > 1 ||
            repeatedGroup.getName == "array" ||
            repeatedGroup.getName == parquetList.getName + "_tuple"
      ) {
        Types
            .buildGroup(parquetList.getRepetition)
            // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
            //.as(LogicalTypeAnnotation.listType())
            .as(OriginalType.LIST)
            .addField(clipParquetType(repeatedGroup, elementType, caseSensitive, useFieldId))
            .named(parquetList.getName)
      } else {
        val newRepeatedGroup = Types
            .repeatedGroup()
            .addField(
              clipParquetType(repeatedGroup.getType(0), elementType, caseSensitive, useFieldId))
            .named(repeatedGroup.getName)

        val newElementType = if (useFieldId && repeatedGroup.getId != null) {
          newRepeatedGroup.withId(repeatedGroup.getId.intValue())
        } else {
          newRepeatedGroup
        }

        // Otherwise, the repeated field's type is the element type with the repeated field's
        // repetition.
        Types
            .buildGroup(parquetList.getRepetition)
            // When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
            //.as(LogicalTypeAnnotation.listType())
            .as(OriginalType.LIST)
            .addField(newElementType)
            .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet GroupType which corresponds to a Catalyst MapType.  Either key type or
   * value type of the MapType must be a nested type, namely an ArrayType, a MapType, or
   * a StructType.
   */
  @scala.annotation.nowarn("msg=method as in class Builder is deprecated")
  private def clipParquetMapType(
      parquetMap: GroupType,
      keyType: DataType,
      valueType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): GroupType = {
    // Precondition of this method, only handles maps with nested key types or value types.
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup = {
      val newRepeatedGroup = Types
          .repeatedGroup()
          // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
          //.as(repeatedGroup.getLogicalTypeAnnotation)
          .as(repeatedGroup.getOriginalType)
          .addField(clipParquetType(parquetKeyType, keyType, caseSensitive, useFieldId))
          .addField(clipParquetType(parquetValueType, valueType, caseSensitive, useFieldId))
          .named(repeatedGroup.getName)
      if (useFieldId && repeatedGroup.getId != null) {
        newRepeatedGroup.withId(repeatedGroup.getId.intValue())
      } else {
        newRepeatedGroup
      }
    }

    Types
        .buildGroup(parquetMap.getRepetition)
        // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
        //.as(parquetMap.getLogicalTypeAnnotation)
        .as(parquetMap.getOriginalType)
        .addField(clippedRepeatedGroup)
        .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet GroupType which corresponds to a Catalyst StructType.
   *
   * @return A clipped GroupType, which has at least one field.
   * @note Parquet doesn't allow creating empty GroupType instances except for empty
   *       MessageType.  Because it's legal to construct an empty requested schema for column
   *       pruning.
   */
  @scala.annotation.nowarn("msg=method as in class Builder is deprecated")
  private def clipParquetGroup(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): GroupType = {
    val clippedParquetFields =
      clipParquetGroupFields(parquetRecord, structType, caseSensitive, useFieldId)
    Types
        .buildGroup(parquetRecord.getRepetition)
        // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
        //.as(parquetRecord.getLogicalTypeAnnotation)
        .as(parquetRecord.getOriginalType)
        .addFields(clippedParquetFields: _*)
        .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet GroupType which corresponds to a Catalyst StructType.
   *
   * @return A list of clipped GroupType fields, which can be empty.
   */
  private def clipParquetGroupFields(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Seq[Type] = {
    lazy val caseSensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    lazy val caseInsensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
    lazy val idToParquetFieldMap =
      parquetRecord.getFields.asScala.filter(_.getId != null).groupBy(f => f.getId.intValue())

    def matchCaseSensitiveField(f: StructField): Option[Type] = {
      caseSensitiveParquetFieldMap
          .get(f.name)
          .map(clipParquetType(_, f.dataType, caseSensitive, useFieldId))
    }

    def matchCaseInsensitiveField(f: StructField): Option[Type] = {
      // Do case-insensitive resolution only if in case-insensitive mode
      caseInsensitiveParquetFieldMap
          .get(f.name.toLowerCase(Locale.ROOT))
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw RapidsErrorUtils.foundDuplicateFieldInCaseInsensitiveModeError(
                f.name, parquetTypesString)
            } else {
              clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
            }
          }
    }

    def matchIdField(f: StructField): Option[Type] = {
      val fieldId = ParquetSchemaClipShims.getFieldId(f)
      idToParquetFieldMap
          .get(fieldId)
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw new RuntimeException(
                s"""
                   |Found duplicate field(s) "$fieldId": $parquetTypesString
                   |in case-insensitive mode
                 """.stripMargin.replaceAll("\n", " "))
            } else {
              clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
            }
          }
    }

    val shouldMatchById = useFieldId && ParquetSchemaClipShims.hasFieldIds(structType)
    structType.flatMap { f =>
      if (shouldMatchById && ParquetSchemaClipShims.hasFieldId(f)) {
        matchIdField(f)
      } else if (caseSensitive) {
        matchCaseSensitiveField(f)
      } else {
        matchCaseInsensitiveField(f)
      }
    }
  }

  /**
   * Trims the Spark schema to the corresponding fields found in the Parquet schema.
   * The Spark schema must be a superset of the Parquet schema.
   */
  def clipSparkSchema(
      sparkSchema: StructType,
      parquetSchema: MessageType,
      caseSensitive: Boolean,
      useFieldId: Boolean): StructType = {
    clipSparkStructType(sparkSchema, parquetSchema.asGroupType(), caseSensitive, useFieldId)
  }

  private def clipSparkType(
      sparkType: DataType,
      parquetType: Type,
      caseSensitive: Boolean,
      useFieldId: Boolean): DataType = {
    sparkType match {
      case t: ArrayType =>
        // Only clips array types with nested type as element type.
        clipSparkArrayType(t, parquetType, caseSensitive, useFieldId)

      case t: MapType =>
        clipSparkMapType(t, parquetType.asGroupType(), caseSensitive, useFieldId)

      case t: StructType =>
        clipSparkStructType(t, parquetType.asGroupType(), caseSensitive, useFieldId)

      case _ =>
        ParquetSchemaClipShims.convertPrimitiveField(parquetType.asPrimitiveType())
    }
  }

  private def clipSparkArrayType(
      sparkType: ArrayType,
      parquetList: Type,
      caseSensitive: Boolean,
      useFieldId: Boolean): DataType = {
    val elementType = sparkType.elementType
    // Per the Parquet backward-compatibility rules, a REPEATED field is the legacy 1-level list
    // unless it is LIST- or MAP-annotated. A LIST-annotated REPEATED group must use the wrapper
    // branch below; routing it through the legacy path throws ClassCastException (#11589, #11592).
    val newSparkType = if (parquetList.isRepetition(Repetition.REPEATED) &&
        parquetList.getOriginalType != OriginalType.LIST &&
        parquetList.getOriginalType != OriginalType.MAP) {
      clipSparkType(elementType, parquetList, caseSensitive, useFieldId)
    } else {
      val parquetListGroup = parquetList.asGroupType()
      assert(
        // TODO: When we drop Spark 3.1.x, this should use Parquet's LogicalTypeAnnotation
        //parquetList.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation],
        parquetList.getOriginalType == OriginalType.LIST,
        "Invalid Parquet schema. " +
            "Logical type annotation of annotated Parquet lists must be " +
            "ListLogicalTypeAnnotation: " + parquetList.toString)

      assert(
        parquetListGroup.getFieldCount == 1 &&
            parquetListGroup.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
            "LIST-annotated group should only have exactly one repeated field: " +
            parquetList)

      val repeated = parquetListGroup.getType(0)
      if (repeated.isPrimitive) {
        clipSparkType(elementType, parquetListGroup.getType(0), caseSensitive, useFieldId)
      } else {
        val repeatedGroup = repeated.asGroupType()

        // If the repeated field is a group with multiple fields, or the repeated field is a group
        // with one field and is named either "array" or uses the LIST-annotated group's name with
        // "_tuple" appended then the repeated type is the element type and elements are required.
        // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
        // only field.
        val parquetElementType = if (
          repeatedGroup.getFieldCount > 1 ||
              repeatedGroup.getName == "array" ||
              repeatedGroup.getName == parquetList.getName + "_tuple"
        ) {
          repeatedGroup
        } else {
          repeatedGroup.getType(0)
        }
        clipSparkType(elementType, parquetElementType, caseSensitive, useFieldId)
      }
    }

    sparkType.copy(elementType = newSparkType)
  }

  private def clipSparkMapType(
      sparkType: MapType,
      parquetMap: GroupType,
      caseSensitive: Boolean,
      useFieldId: Boolean): MapType = {
    val keyType = sparkType.keyType
    val valueType = sparkType.valueType
    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val newKeyType = clipSparkType(keyType, parquetKeyType, caseSensitive, useFieldId)
    val newValueType = clipSparkType(valueType, parquetValueType, caseSensitive, useFieldId)
    sparkType.copy(keyType = newKeyType, valueType = newValueType)
  }

  private def clipSparkStructType(
      sparkType: StructType,
      parquetType: GroupType,
      caseSensitive: Boolean,
      useFieldId: Boolean): StructType = {
    lazy val caseSensitiveParquetFieldMap =
      parquetType.getFields.asScala.map(f => f.getName -> f).toMap
    lazy val caseInsensitiveParquetFieldMap =
      parquetType.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
    lazy val idToParquetFieldMap =
      parquetType.getFields.asScala.filter(_.getId != null).groupBy(f => f.getId.intValue())

    def updateField(oldField: StructField, p: Type): StructField = {
      val newSparkType = clipSparkType(oldField.dataType, p, caseSensitive, useFieldId)
      oldField.copy(dataType = newSparkType)
    }

    def matchCaseSensitiveField(f: StructField): Option[StructField] = {
      caseSensitiveParquetFieldMap
          .get(f.name)
          .map(updateField(f, _))
    }

    def matchCaseInsensitiveField(f: StructField): Option[StructField] = {
      // Do case-insensitive resolution only if in case-insensitive mode
      caseInsensitiveParquetFieldMap
          .get(f.name.toLowerCase(Locale.ROOT))
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw RapidsErrorUtils.foundDuplicateFieldInCaseInsensitiveModeError(
                f.name, parquetTypesString)
            } else {
              updateField(f, parquetTypes.head)
            }
          }
    }

    def matchIdField(f: StructField): Option[StructField] = {
      val fieldId = ParquetSchemaClipShims.getFieldId(f)
      idToParquetFieldMap
          .get(fieldId)
          .map { parquetTypes =>
            if (parquetTypes.size > 1) {
              // Need to fail if there is ambiguity, i.e. more than one field is matched
              val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
              throw new RuntimeException(
                s"""
                   |Found duplicate field(s) "$fieldId": $parquetTypesString
                   |in case-insensitive mode
                 """.stripMargin.replaceAll("\n", " "))
            } else {
              updateField(f, parquetTypes.head)
            }
          }
    }

    val shouldMatchById = useFieldId && ParquetSchemaClipShims.hasFieldIds(sparkType)
    val updatedFields = sparkType.flatMap { f =>
      if (shouldMatchById && ParquetSchemaClipShims.hasFieldId(f)) {
        matchIdField(f)
      } else if (caseSensitive) {
        matchCaseSensitiveField(f)
      } else {
        matchCaseInsensitiveField(f)
      }
    }
    StructType(updatedFields)
  }

  def evolveSchemaIfNeededAndClose(
      table: Table,
      fileSchema: MessageType,
      sparkSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Table = {
    val fileSparkSchema = closeOnExcept(table) { _ =>
      clipSparkSchema(sparkSchema, fileSchema, caseSensitive, useFieldId)
    }
    SchemaUtils.evolveSchemaIfNeededAndClose(table, fileSparkSchema, sparkSchema,
      caseSensitive, Some(evolveSchemaCasts),
      existsUnsignedType(fileSchema.asGroupType()) ||
          TrampolineUtil.dataTypeExistsRecursively(sparkSchema, _.isInstanceOf[BinaryType]),
      allowNonStructWrapping = true)
  }

  /**
   * Need to convert cudf unsigned integer to wider signed integer that Spark expects
   * After Spark 3.2.0, Spark reads uint8 as int16, uint16 as int32, uint32 as int64
   * TODO uint64 -> Decimal(20,0) depends CUDF, see issue #3475
   *
   * @param group the schema
   * @return if has unsigned integer
   */
  private def existsUnsignedType(group: GroupType): Boolean = {
    group.getFields.asScala.exists(
      field => {
        if (field.isPrimitive) {
          val t = field.getOriginalType
          (t == OriginalType.UINT_8) || (t == OriginalType.UINT_16) ||
              (t == OriginalType.UINT_32) || (t == OriginalType.UINT_64)
        } else {
          existsUnsignedType(field.asGroupType)
        }
      }
    )
  }

  private def needDecimalCast(cv: ColumnView, dt: DataType): Boolean = {
    // UINT64 is casted to Decimal(20,0) by Spark to accommodate
    // the largest possible values this type can take. Other Unsigned data types are converted to
    // basic types like LongType, this is analogous to that except we spill over to large
    // decimal/ints.
    cv.getType.isDecimalType && !GpuColumnVector.getNonNestedRapidsType(dt).equals(cv.getType()) ||
        cv.getType.equals(DType.UINT64)
  }

  // INT32 is special because smaller INT types are stored as it in parquet
  // So to be cautious we support down casting to these types.
  private def needInt32Downcast(cv: ColumnView, dt: DataType): Boolean = {
    cv.getType.equals(DType.INT32) && Seq(ByteType, ShortType).contains(dt)
  }

  private def needUpcast(cv: ColumnView, dt: DataType): Boolean = {
    cv.getType match {
      case DType.INT8 | DType.UINT8 =>
        dt == ShortType || dt == IntegerType || dt == LongType || dt == DoubleType
      case DType.INT16 | DType.UINT16 =>
        dt == IntegerType || dt == LongType || dt == DoubleType
      case DType.INT32 =>
        dt == LongType || dt == DoubleType || dt == DateType
      case DType.UINT32 =>
        dt == LongType || dt == DoubleType
      case DType.FLOAT32 =>
        dt == DoubleType
      case _ => false
    }
  }

  // Wrap up all required casts for Parquet schema evolution
  //
  // Note: The behavior of unsigned to signed is decided by the Spark,
  // this means the parameter dt is from Spark meta module.
  // This implements the requested type behavior accordingly for GPU.
  // This is suitable for all Spark versions, no need to add to shim layer.
  private def evolveSchemaCasts(cv: ColumnView, dt: DataType,
                                originalFromDt: DataType): ColumnView = {
    // This code needs to be kept in sync with
    // GpuParquetFileFilterHandler.checkSchemaCompat or more specifically
    // GpuParquetFileFilterHandler.checkPrimitiveCompat
    if (needDecimalCast(cv, dt)) {
      val fromDecimal = originalFromDt.asInstanceOf[DecimalType]
      val toDecimal = dt.asInstanceOf[DecimalType]
      val ansiMode = CastOptions.DEFAULT_CAST_OPTIONS.isAnsiMode
      GpuCast.castDecimalToDecimal(cv, fromDecimal, toDecimal, ansiMode)
    } else if (needInt32Downcast(cv, dt) ||
      needUpcast(cv, dt)) {
      cv.castTo(GpuColumnVector.getNonNestedRapidsType(dt))
    } else if (DType.STRING.equals(cv.getType) && dt == BinaryType) {
      // Ideally we would bitCast the STRING to a LIST, but that does not work.
      // Instead, we are going to have to pull apart the string and put it back together
      // as a list.

      val dataBuf = Option(cv.getData)
      withResource(new ColumnView(DType.UINT8, dataBuf.map(_.getLength).getOrElse(0),
        Optional.of(0L),
        dataBuf.orNull, null)) { data =>
        withResource(new ColumnView(DType.LIST, cv.getRowCount,
          Optional.of[java.lang.Long](cv.getNullCount),
          cv.getValid, cv.getOffsets, Array(data))) { everything =>
          everything.copyToColumnVector()
        }
      }
    } else {
      throw new IllegalStateException("Logical error: no valid casts are found " +
          s"${cv.getType} to $dt")
    }
  }
}
