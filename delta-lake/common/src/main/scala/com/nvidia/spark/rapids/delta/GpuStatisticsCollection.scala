/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
 *
 * This file was derived from StatisticsCollection.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.nvidia.spark.rapids.delta

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnView, DType}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.delta.shims.{ShimDeltaColumnMapping, ShimDeltaUDF, ShimUsesMetadataFields}

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, count, lit, max, min, struct, sum, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/** GPU version of Delta Lake's StatisticsCollection. */
trait GpuStatisticsCollection extends ShimUsesMetadataFields {
  protected def spark: SparkSession
  def tableDataSchema: StructType
  def dataSchema: StructType
  val numIndexedCols: Int
  val stringPrefixLength: Int

  protected def deletionVectorsSupported: Boolean

  // Build a mapping of a field path to a field index within the parent struct
  lazy val explodedDataSchema: Map[Seq[String], Int] =
    GpuStatisticsCollection.explode(dataSchema).toMap

  /**
   * statCollectionSchema is the schema that is composed of all the columns that have the stats
   * collected with our current table configuration.
   */
  lazy val statCollectionSchema: StructType = {
    if (numIndexedCols >= 0) {
      truncateSchema(tableDataSchema, numIndexedCols)._1
    } else {
      tableDataSchema
    }
  }

  /**
   * Returns a struct column that can be used to collect statistics for the current
   * schema of the table.
   * The types we keep stats on must be consistent with DataSkippingReader.SkippingEligibleLiteral.
   * If a column is missing from dataSchema (which will be filled with nulls), we will only
   * collect the NULL_COUNT stats for it as the number of rows.
   *
   * @note The stats collected here must be kept in sync with how stats are gathered by the GPU
   *       in batchStatsToRow.
   */
  lazy val statsCollector: Column = {
    val prefixLength = stringPrefixLength

    // On file initialization/stat recomputation TIGHT_BOUNDS is always set to true
    val tightBoundsColOpt = if (deletionVectorsSupported &&
        !RapidsDeltaUtils.getTightBoundColumnOnFileInitDisabled(spark)) {
      Some(lit(true).as("tightBounds"))
    } else {
      None
    }

    val statCols = Seq(
      count(new Column("*")) as NUM_RECORDS,
      collectStats(MIN, statCollectionSchema) {
        // Truncate string min values as necessary
        case (c, GpuSkippingEligibleDataType(StringType), true) =>
          val udfTruncateMin = ShimDeltaUDF.stringStringUdf(
            GpuStatisticsCollection.truncateMinStringAgg(prefixLength)_)
          udfTruncateMin(min(c))

        // Collect all numeric min values
        case (c, GpuSkippingEligibleDataType(_), true) =>
          min(c)
      },
      collectStats(MAX, statCollectionSchema) {
        // Truncate and pad string max values as necessary
        case (c, GpuSkippingEligibleDataType(StringType), true) =>
          val udfTruncateMax = ShimDeltaUDF.stringStringUdf(
            GpuStatisticsCollection.truncateMaxStringAgg(prefixLength)_)
          udfTruncateMax(max(c))

        // Collect all numeric max values
        case (c, GpuSkippingEligibleDataType(_), true) =>
          max(c)
      },
      collectStats(NULL_COUNT, statCollectionSchema) {
        case (c, _, true) => sum(when(c.isNull, 1).otherwise(0))
        case (_, _, false) => count(new Column("*"))
      }) ++ tightBoundsColOpt

    struct(statCols: _*).as("stats")
  }

  /**
   * Generate a truncated data schema for stats collection
   * @param schema the original data schema
   * @param indexedCols the maximum number of leaf columns to collect stats on
   * @return truncated schema and the number of leaf columns in this schema
   */
  private def truncateSchema(schema: StructType, indexedCols: Int): (StructType, Int) = {
    var accCnt = 0
    var i = 0
    val fields = ArrayBuffer[StructField]()
    while (i < schema.length && accCnt < indexedCols) {
      val field = schema.fields(i)
      val newField = field match {
        case StructField(name, st: StructType, nullable, metadata) =>
          val (newSt, cnt) = truncateSchema(st, indexedCols - accCnt)
          accCnt += cnt
          StructField(name, newSt, nullable, metadata)
        case f =>
          accCnt += 1
          f
      }
      i += 1
      fields += newField
    }
    (StructType(fields.toSeq), accCnt)
  }

  /**
   * Recursively walks the given schema, constructing an expression to calculate
   * multiple statistics that mirrors structure of the data. When `function` is
   * defined for a given column, it return value is added to statistics structure.
   * When `function` is not defined, that column is skipped.
   *
   * @param name     The name of the top level column for this statistic (i.e. minValues).
   * @param schema The schema of the data to collect statistics from.
   * @param function A partial function that is passed a tuple of (column, metadata about that
   *                 column, a flag that indicates whether the column is in the data schema). Based
   *                 on the metadata and flag, the function can decide if the given statistic should
   *                 be collected on the column by returning the correct aggregate expression.
   */
  private def collectStats(
      name: String,
      schema: StructType)(
      function: PartialFunction[(Column, StructField, Boolean), Column]): Column = {

    def collectStats(
        schema: StructType,
        parent: Option[Column],
        parentFields: Seq[String],
        function: PartialFunction[(Column, StructField, Boolean), Column]): Seq[Column] = {
      schema.flatMap {
        case f @ StructField(name, s: StructType, _, _) =>
          val column = parent.map(_.getItem(name))
              .getOrElse(col(name))
          val stats = collectStats(s, Some(column), parentFields :+ name, function)
          if (stats.nonEmpty) {
            Some(struct(stats: _*) as ShimDeltaColumnMapping.getPhysicalName(f))
          } else {
            None
          }
        case f @ StructField(name, _, _, _) =>
          val fieldPath = parentFields :+ name
          val column = parent.map(_.getItem(name))
              .getOrElse(col(name))
          // Note: explodedDataSchema comes from dataSchema. In the read path, dataSchema comes
          // from the table's metadata.dataSchema, which is the same as tableDataSchema. In the
          // write path, dataSchema comes from the DataFrame schema. We then assume
          // TransactionWrite.writeFiles has normalized dataSchema, and
          // TransactionWrite.getStatsSchema has done the column mapping for tableDataSchema and
          // dropped the partition columns for both dataSchema and tableDataSchema.
          function.lift((column, f, explodedDataSchema.contains(fieldPath))).
              map(_.as(ShimDeltaColumnMapping.getPhysicalName(f)))
      }
    }

    val allStats = collectStats(schema, None, Nil, function)
    val stats = if (numIndexedCols > 0) {
      allStats.take(numIndexedCols)
    } else {
      allStats
    }

    if (stats.nonEmpty) {
      struct(stats: _*).as(name)
    } else {
      lit(null).as(name)
    }
  }
}

object GpuStatisticsCollection {
  val ASCII_MAX_CHARACTER = '\u007F'

  val UTF8_MAX_CHARACTER = new String(Character.toChars(Character.MAX_CODE_POINT))

  def truncateMinStringAgg(prefixLen: Int)(input: String): String = {
    if (input == null || input.length <= prefixLen) {
      return input
    }
    if (prefixLen <= 0) {
      return null
    }
    if (Character.isHighSurrogate(input.charAt(prefixLen - 1)) &&
        Character.isLowSurrogate(input.charAt(prefixLen))) {
      // If the character at prefixLen - 1 is a high surrogate and the next character is a low
      // surrogate, we need to include the next character in the prefix to ensure that we don't
      // truncate the string in the middle of a surrogate pair.
      input.take(prefixLen + 1)
    } else {
      input.take(prefixLen)
    }
  }

  /**
   * Helper method to truncate the input string `input` to the given `prefixLen` length, while also
   * ensuring the any value in this column is less than or equal to the truncated max in UTF-8
   * encoding.
   */
  def truncateMaxStringAgg(prefixLen: Int)(originalMax: String): String = {
    // scalastyle:off nonascii
    if (originalMax == null || originalMax.length <= prefixLen) {
      return originalMax
    }
    if (prefixLen <= 0) {
      return null
    }

    // Grab the prefix. We want to append max Unicode code point `\uDBFF\uDFFF` as a tie-breaker,
    // but that is only safe if the character we truncated was smaller in UTF-8 encoded binary
    // comparison. Keep extending the prefix until that condition holds, or we run off the end of
    // the string.
    // We also try to use the ASCII max character `\u007F` as a tie-breaker if possible.
    val maxLen = getExpansionLimit(prefixLen)
    // Start with a valid prefix
    var currLen = truncateMinStringAgg(prefixLen)(originalMax).length
    while (currLen <= maxLen) {
      if (currLen >= originalMax.length) {
        // Return originalMax if we have reached the end of the string
        return originalMax
      } else if (currLen + 1 < originalMax.length &&
        originalMax.substring(currLen, currLen + 2) == UTF8_MAX_CHARACTER) {
        // Skip the UTF-8 max character. It occupies two characters in a Scala string.
        currLen += 2
      } else if (originalMax.charAt(currLen) < ASCII_MAX_CHARACTER) {
        return originalMax.take(currLen) + ASCII_MAX_CHARACTER
      } else {
        return originalMax.take(currLen) + UTF8_MAX_CHARACTER
      }
    }

    // Return null when the input string is too long to truncate.
    null
    // scalastyle:on nonascii
  }

  /**
   * Calculates the upper character limit when constructing a maximum is not possible with only
   * prefixLen chars.
   */
  private def getExpansionLimit(prefixLen: Int): Int = 2 * prefixLen

  def batchStatsToRow(
      schema: StructType,
      explodedDataSchema: Map[Seq[String], Int],
      batch: ColumnarBatch,
      row: InternalRow): Unit = {
    var nextSlot = 0
    // NUM_RECORDS
    row.setLong(nextSlot, batch.numRows)
    nextSlot += 1

    val views = GpuColumnVector.extractBases(batch).asInstanceOf[Array[ColumnView]]

    // MIN
    nextSlot = batchStatsToRow(schema, explodedDataSchema, batch.numRows, views, row,
      Nil, nextSlot, GpuStatisticsCollection.computeMin, isForAllTypes = false)

    // MAX
    nextSlot = batchStatsToRow(schema, explodedDataSchema, batch.numRows, views, row,
      Nil, nextSlot, GpuStatisticsCollection.computeMax, isForAllTypes = false)

    // NULL_COUNT
    nextSlot = batchStatsToRow(schema, explodedDataSchema, batch.numRows, views, row,
      Nil, nextSlot, GpuStatisticsCollection.computeNullCount, isForAllTypes = true)

    require(nextSlot == row.numFields, s"Expected ${row.numFields} stats, only wrote $nextSlot")
  }

  private def batchStatsToRow(
      schema: StructType,
      explodedDataSchema: Map[Seq[String], Int],
      batchNumRows: Int,
      views: Array[ColumnView],
      row: InternalRow,
      parentFields: Seq[String],
      slot: Int,
      statFunc: (ColumnView, DataType, InternalRow, Int) => Unit,
      isForAllTypes: Boolean): Int = {
    withResource(ColumnView.makeStructView(views: _*)) { structView =>
      structStatsToRow(explodedDataSchema, structView, schema, batchNumRows, row,
        parentFields, slot, statFunc, isForAllTypes)
    }
  }

  private def structStatsToRow(
      explodedDataSchema: Map[Seq[String], Int],
      struct: ColumnView,
      structSchema: StructType,
      batchNumRows: Int,
      row: InternalRow,
      parentFields: Seq[String],
      slot: Int,
      statFunc: (ColumnView, DataType, InternalRow, Int) => Unit,
      isForAllTypes: Boolean): Int = {
    require(struct.getType.equals(DType.STRUCT), s"Expected struct column, found ${struct.getType}")
    var nextSlot = slot
    structSchema.foreach { field =>
      val fieldPath = parentFields :+ field.name
      val dataFieldIndex = explodedDataSchema.get(fieldPath)
      field.dataType match {
        case s: StructType =>
          // If the struct is not in the field index then there's no struct column view
          // to use. Send down null as a stand-in, since we may need to generate null count
          // metrics for struct field members that are missing.
          withResource(dataFieldIndex.map(struct.getChildColumnView).orNull) { c =>
            nextSlot = structStatsToRow(explodedDataSchema, c, s, batchNumRows, row,
              parentFields :+ field.name, nextSlot, statFunc, isForAllTypes)
          }
        case dt if isForAllTypes || GpuSkippingEligibleDataType(dt) =>
          if (dataFieldIndex.isDefined) {
            withResource(struct.getChildColumnView(dataFieldIndex.get)) { c =>
              statFunc(c, dt, row, nextSlot)
            }
            nextSlot += 1
          } else if (isForAllTypes) {
            // doing a top-level null count for a field not in the data schema
            row.setLong(nextSlot, batchNumRows)
            nextSlot += 1
          }
        case _ =>
      }
    }
    nextSlot
  }

  private def writeValueToRow(value: Any, dt: DataType, row: InternalRow, slot: Int): Unit = {
    val valueType = if (value == null) NullType else dt
    val writer = InternalRow.getWriter(slot, valueType)
    writer(row, value)
  }

  private def computeMin(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    val minValue = dt match {
      // cudf min with NaNs does not match Spark behavior, so work around it here
      case FloatType | DoubleType =>
        val (isAllNan, isAnyNan) = withResource(c.isNan) { isNan =>
          val isAllNan = withResource(isNan.all()) { allNans =>
            allNans.isValid && allNans.getBoolean
          }
          if (isAllNan) {
            (isAllNan, true)
          } else {
            val isAnyNan = withResource(isNan.any()) { anyNans =>
              anyNans.isValid && anyNans.getBoolean
            }
            (isAllNan, isAnyNan)
          }
        }
        if (isAllNan) {
          if (dt == FloatType) Float.NaN else Double.NaN
        } else if (isAnyNan) {
          // Need to replace NaNs with nulls before calling cudf min
          withResource(c.nansToNulls()) { noNans =>
            withResource(noNans.min()) { s =>
              GpuScalar.extract(s)
            }
          }
        } else {
          withResource(c.min()) { s =>
            GpuScalar.extract(s)
          }
        }
      case _ =>
        withResource(c.min()) { s =>
          GpuScalar.extract(s)
        }
    }
    writeValueToRow(minValue, dt, row, slot)
  }

  private def computeMax(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    def hasNans(c: ColumnView): Boolean = {
      withResource(c.isNan) { isNan =>
        withResource(isNan.any()) { anyNan =>
          anyNan.isValid && anyNan.getBoolean
        }
      }
    }

    // cudf max with NaNs does not match Spark behavior, so work around it here
    val maxValue = dt match {
      case FloatType if hasNans(c) => Float.NaN
      case DoubleType if hasNans(c) => Double.NaN
      case _ =>
        withResource(c.max()) { s =>
          GpuScalar.extract(s)
        }
    }
    writeValueToRow(maxValue, dt, row, slot)
  }

  private def computeNullCount(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    row.setLong(slot, c.getNullCount)
  }

  /**
   * Similar to Delta Lake's SchemaMergingUtils.explode except instead of mapping a field
   * path to a field it maps a field paths to a field ordinal.
   *
   * Returns pairs of (full column name path, field index) in this schema as a list.
   * For example, a schema like:
   * <field a>          | - a
   * <field 1>          | | - 1
   * <field 2>          | | - 2
   * <field b>          | - b
   * <field c>          | - c
   * <field `foo.bar`>  | | - `foo.bar`
   * <field 3>          |   | - 3
   * will return [
   * ([a], 0), ([a, 1], 0), ([a, 2], 1), ([b], 1),
   * ([c], 2), ([c, foo.bar], 0), ([c, foo.bar, 3], 0)
   * ]
   */
  def explode(schema: StructType): Seq[(Seq[String], Int)] = {
    def recurseIntoComplexTypes(complexType: DataType): Seq[(Seq[String], Int)] = {
      complexType match {
        case s: StructType => explode(s)
        case a: ArrayType => recurseIntoComplexTypes(a.elementType)
            .map { case (path, i) => (Seq("element") ++ path, i) }
        case m: MapType =>
          recurseIntoComplexTypes(m.keyType)
              .map { case (path, i) => (Seq("key") ++ path, i) } ++
              recurseIntoComplexTypes(m.valueType)
                  .map { case (path, i) => (Seq("value") ++ path, i) }
        case _ => Nil
      }
    }

    schema.zipWithIndex.flatMap {
      case (StructField(name, s: StructType, _, _), i) =>
        Seq((Seq(name), i)) ++
            explode(s).map { case (path, i) => (Seq(name) ++ path, i) }
      case (StructField(name, a: ArrayType, _, _), i) =>
        Seq((Seq(name), i)) ++
            recurseIntoComplexTypes(a).map { case (path, i) => (Seq(name) ++ path, i) }
      case (StructField(name, m: MapType, _, _), i) =>
        Seq((Seq(name), i)) ++
            recurseIntoComplexTypes(m).map { case (path, i) => (Seq(name) ++ path, i) }
      case (f, i) => (Seq(f.name), i) :: Nil
    }
  }
}

/**
 * Based on Delta's SkippingEligibleDataType. Copied here to keep consistent with
 * statistics generating code and avoid surprises if provided Delta code changes.
 */
object GpuSkippingEligibleDataType {
  // Call this directly, e.g. `GpuSkippingEligibleDataType(dataType)`
  def apply(dataType: DataType): Boolean = dataType match {
    case _: NumericType | DateType | TimestampType | StringType => true
    case _ => false
  }

  // Use these in `match` statements
  def unapply(dataType: DataType): Option[DataType] = {
    if (GpuSkippingEligibleDataType(dataType)) Some(dataType) else None
  }

  def unapply(f: StructField): Option[DataType] = unapply(f.dataType)
}

