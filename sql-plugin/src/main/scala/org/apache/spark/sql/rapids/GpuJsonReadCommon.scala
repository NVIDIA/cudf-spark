/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.Locale

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Schema, Table}
import com.fasterxml.jackson.core.JsonParser
import com.nvidia.spark.rapids.{ColumnCastUtil, GpuColumnVector, GpuScalar, GpuTextBasedPartitionReader, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.jni.JSONUtils

import org.apache.spark.sql.catalyst.json.{GpuJsonUtils, JSONOptions}
import org.apache.spark.sql.rapids.shims.GpuJsonToStructsShim
import org.apache.spark.sql.types.{DataType, _}

/**
 * This is a utility method intended to provide common functionality between JsonToStructs and
 * ScanJson
 */
object GpuJsonReadCommon {
  private def populateSchema(dt: DataType,
      name: String, builder: Schema.Builder): Unit = dt match {
    case at: ArrayType =>
      val child = builder.addColumn(DType.LIST, name)
      populateSchema(at.elementType, "element", child)
    case st: StructType =>
      val child = builder.addColumn(DType.STRUCT, name)
      for (sf <- st.fields) {
        populateSchema(sf.dataType, sf.name, child)
      }
    case _: MapType =>
      throw new IllegalArgumentException("MapType is not supported yet for schema conversion")
    case dt: DecimalType =>
      builder.addColumn(GpuColumnVector.getNonNestedRapidsType(dt), name, dt.precision)
    case _ =>
      builder.addColumn(GpuColumnVector.getNonNestedRapidsType(dt), name)
  }

  /**
   * Make a read schema given an input data type
   * @param input the input Spark schema to convert
   * @return the schema to use when reading Spark data.
   */
  def makeSchema(input: StructType): Schema = {
    val builder = Schema.builder
    input.foreach(f => populateSchema(f.dataType, f.name, builder))
    builder.build
  }

  private def dateFormat(options: JSONOptions): Option[String] =
    GpuJsonUtils.optionalDateFormatInRead(options)

  private def timestampFormat(options: JSONOptions): String =
    GpuJsonUtils.timestampFormatInRead(options)

  private def throwMismatchException(cv: ColumnView,
      dt: DataType): (Option[ColumnView], Seq[AutoCloseable]) = {
    throw new IllegalStateException(s"Don't know how to transform $cv to $dt for JSON")
  }

  private def nestedColumnViewMismatchTransform(cv: ColumnView,
    dt: DataType): (Option[ColumnView], Seq[AutoCloseable]) = {
    // In the future we should be able to convert strings to maps/etc, but for
    // now we are working around issues where CUDF is not returning a STRING for nested
    // types when asked for it.
    cv.getType match {
      case DType.LIST =>
        dt match {
          case ByteType | ShortType | IntegerType | LongType |
               BooleanType | FloatType | DoubleType |
               _: DecimalType | _: StructType =>
            // This is all nulls
            val rows = cv.getRowCount().toInt
            val ret = withResource(GpuScalar.from(null, dt)) { nullScalar =>
              ColumnVector.fromScalar(nullScalar, rows)
            }
            (Some(ret.asInstanceOf[ColumnView]), Seq(ret))
          case _ =>
            throwMismatchException(cv, dt)
        }
      case DType.STRUCT =>
        dt match {
          case _: ArrayType =>
            // This is all nulls
            val rows = cv.getRowCount().toInt
            val ret = withResource(GpuScalar.from(null, dt)) { nullScalar =>
              ColumnVector.fromScalar(nullScalar, rows)
            }
            (Some(ret.asInstanceOf[ColumnView]), Seq(ret))
          case _ =>
            throwMismatchException(cv, dt)
        }
      case _ =>
        throwMismatchException(cv, dt)
    }
  }

  private def convertStringToDate(input: ColumnView, options: JSONOptions): ColumnVector = {
    withResource(JSONUtils.removeQuotes(input, /*nullifyIfNotQuoted*/ true)) { removedQuotes =>
      GpuJsonToStructsShim.castJsonStringToDateFromScan(removedQuotes, DType.TIMESTAMP_DAYS,
        dateFormat(options))
    }
  }

  private def convertStringToTimestamp(input: ColumnView, options: JSONOptions): ColumnVector = {
    withResource(JSONUtils.removeQuotes(input, /*nullifyIfNotQuoted*/ true)) { removedQuotes =>
      GpuTextBasedPartitionReader.castStringToTimestamp(removedQuotes, timestampFormat(options),
        DType.TIMESTAMP_MICROSECONDS)
    }
  }

  private def convertToDesiredType(inputCv: ColumnVector,
      topLevelType: DataType,
      options: JSONOptions): ColumnVector = {
    ColumnCastUtil.deepTransform(inputCv, Some(topLevelType),
      Some(nestedColumnViewMismatchTransform)) {
      case (cv, Some(DateType)) if cv.getType == DType.STRING =>
        convertStringToDate(cv, options)
      case (cv, Some(TimestampType)) if cv.getType == DType.STRING =>
        convertStringToTimestamp(cv, options)
      case (cv, Some(dt)) if cv.getType == DType.STRING =>
        // There is an issue with the Schema implementation such that the schema's top level
        // is never used when passing down data schema from Java to C++.
        // As such, we have to wrap the current column schema `dt` in a struct schema.
        val builder = Schema.builder // This is created as a struct schema
        populateSchema(dt, "", builder)
        JSONUtils.convertFromStrings(cv, builder.build, options.allowNonNumericNumbers,
          options.locale == Locale.US)
    }
  }

  /**
   * Convert the parsed input table to the desired output types
   * @param table the table to start with
   * @param desired the desired output data types
   * @param options the options the user provided
   * @return an array of converted column vectors in the same order as the input table.
   */
  def convertTableToDesiredType(table: Table,
      desired: StructType,
      options: JSONOptions): Array[ColumnVector] = {
    NvtxRegistry.JSON_CONVERT_TABLE {
      val dataTypes = desired.fields.map(_.dataType)
      dataTypes.zipWithIndex.safeMap {
        case (dt, i) =>
          convertToDesiredType(table.getColumn(i), dt, options)
      }
    }
  }

  /**
   * Convert a strings column into date/time types.
   * @param inputCv The input column vector
   * @param topLevelType The desired output data type
   * @param options JSON options for the conversion
   * @return The converted column vector
   */
  def convertDateTimeType(inputCv: ColumnVector,
      topLevelType: DataType,
      options: JSONOptions): ColumnVector = {
    NvtxRegistry.JSON_CONVERT_DATETIME {
      ColumnCastUtil.deepTransform(inputCv, Some(topLevelType),
        Some(nestedColumnViewMismatchTransform)) {
        case (cv, Some(DateType)) if cv.getType == DType.STRING =>
          convertStringToDate(cv, options)
        case (cv, Some(TimestampType)) if cv.getType == DType.STRING =>
          convertStringToTimestamp(cv, options)
      }
    }
  }

  def cudfJsonOptions(options: JSONOptions): ai.rapids.cudf.JSONOptions = {
    // This is really ugly, but options.allowUnquotedControlChars is marked as private
    // and this is the only way I know to get it without even uglier tricks
    @scala.annotation.nowarn("msg=Java enum ALLOW_UNQUOTED_CONTROL_CHARS in " +
      "Java enum Feature is deprecated")
    val allowUnquotedControlChars = options.buildJsonFactory()
      .isEnabled(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)

    baseCudfJsonOptionsBuilder()
      .withNormalizeSingleQuotes(options.allowSingleQuotes)
      .withLeadingZeros(options.allowNumericLeadingZeros)
      .withNonNumericNumbers(options.allowNonNumericNumbers)
      .withUnquotedControlChars(allowUnquotedControlChars)
      .build()
  }

  def baseCudfJsonOptionsBuilder(): ai.rapids.cudf.JSONOptions.Builder = {
    ai.rapids.cudf.JSONOptions.builder()
      .withRecoverWithNull(true)
      .withMixedTypesAsStrings(true)
      .withNormalizeWhitespace(true)
      .withKeepQuotes(true)
      .withStrictValidation(true)
      .withCudfPruneSchema(true)
      .withExperimental(true)
  }
}
