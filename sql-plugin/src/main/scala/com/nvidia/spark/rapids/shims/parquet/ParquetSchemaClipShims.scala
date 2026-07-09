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

package com.nvidia.spark.rapids.shims.parquet

import scala.collection.JavaConverters._

import org.apache.parquet.schema._
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport.containsFieldIds
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.RapidsAnalysisException
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._

object ParquetSchemaClipShims {

  def useFieldId(conf: SQLConf): Boolean = conf.parquetFieldIdReadEnabled

  def ignoreMissingIds(conf: SQLConf): Boolean = conf.ignoreMissingParquetFieldId

  def checkIgnoreMissingIds(ignoreMissingIds: Boolean, parquetFileSchema: MessageType,
      catalystRequestedSchema: StructType): Unit = {
    if (!ignoreMissingIds &&
        !containsFieldIds(parquetFileSchema) &&
        ParquetUtils.hasFieldIds(catalystRequestedSchema)) {
      throw new RuntimeException(
        "Spark read schema expects field Ids, " +
            "but Parquet file schema doesn't contain any field Ids.\n" +
            "Please remove the field ids from Spark schema or ignore missing ids by " +
            s"setting `${SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key} = true`\n" +
            s"""
               |Spark read schema:
               |${catalystRequestedSchema.prettyJson}
               |
               |Parquet file schema:
               |${parquetFileSchema.toString}
               |""".stripMargin)
    }
  }

  def hasFieldId(field: StructField): Boolean = ParquetUtils.hasFieldId(field)

  def hasFieldIds(schema: StructType): Boolean = ParquetUtils.hasFieldIds(schema)

  def getFieldId(field: StructField): Int = ParquetUtils.getFieldId(field)

  def fieldIdToFieldMap(useFieldId: Boolean, fileType: Type): Map[Int, Type] = {
    if (useFieldId) {
      fileType.asGroupType().getFields.asScala.filter(_.getId != null)
          .map(f => f.getId.intValue() -> f).toMap
    } else {
      Map.empty[Int, Type]
    }
  }

  def fieldIdToNameMap(useFieldId: Boolean, fileType: Type): Map[Int, String] = {
    if (useFieldId) {
      fileType.asGroupType().getFields.asScala.filter(_.getId != null)
          .map(f => f.getId.intValue() -> f.getName).toMap
    } else {
      Map.empty[Int, String]
    }
  }

  /**
   * Convert a Parquet primitive type to a Spark type.
   * Based on Spark's ParquetSchemaConverter.convertPrimitiveField
   */
  def convertPrimitiveField(parquetType: PrimitiveType): DataType = {
    val typeAnnotation = parquetType.getLogicalTypeAnnotation
    val typeName = parquetType.getPrimitiveTypeName

    def typeString =
      if (typeAnnotation == null) s"$typeName" else s"$typeName ($typeAnnotation)"

    def typeNotImplemented() =
      throw RapidsErrorUtils.parquetTypeUnsupportedYetError(typeString)

    def illegalType() =
      throw RapidsErrorUtils.illegalParquetTypeError(typeString)


    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val decimalLogicalTypeAnnotation = typeAnnotation
          .asInstanceOf[DecimalLogicalTypeAnnotation]
      val precision = decimalLogicalTypeAnnotation.getPrecision
      val scale = decimalLogicalTypeAnnotation.getScale

      if (!(maxPrecision == -1 || 1 <= precision && precision <= maxPrecision)) {
        throw new RapidsAnalysisException(s"Invalid decimal precision: $typeName " +
            s"cannot store $precision digits (max $maxPrecision)")
      }

      DecimalType(precision, scale)
    }

    typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ByteType
              case 16 => ShortType
              case 32 => IntegerType
              case _ => illegalType()
            }
          case null => IntegerType
          case _: DateLogicalTypeAnnotation => DateType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 8 => ShortType
              case 16 => IntegerType
              case 32 => LongType
              case _ => illegalType()
            }
          case t: TimestampLogicalTypeAnnotation if t.getUnit == TimeUnit.MILLIS =>
            typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        typeAnnotation match {
          case intTypeAnnotation: IntLogicalTypeAnnotation if intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              case 64 => LongType
              case _ => illegalType()
            }
          case null => LongType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case intTypeAnnotation: IntLogicalTypeAnnotation if !intTypeAnnotation.isSigned =>
            intTypeAnnotation.getBitWidth match {
              // The precision to hold the largest unsigned long is:
              // `java.lang.Long.toUnsignedString(-1).length` = 20
              case 64 => DecimalType(20, 0)
              case _ => illegalType()
            }
          case timestamp: TimestampLogicalTypeAnnotation
            if timestamp.getUnit == TimeUnit.MICROS || timestamp.getUnit == TimeUnit.MILLIS =>
              ParquetTimestampAnnotationShims.timestampTypeForMillisOrMicros(timestamp)
          case timestamp: TimestampLogicalTypeAnnotation if timestamp.getUnit == TimeUnit.NANOS &&
              ParquetLegacyNanoAsLongShims.legacyParquetNanosAsLong =>
            throw new RapidsAnalysisException(
              "GPU does not support spark.sql.legacy.parquet.nanosAsLong")
          case _ => illegalType()
        }

      case INT96 =>
        if (!SQLConf.get.isParquetINT96AsTimestamp) {
          throw new RapidsAnalysisException(
            "INT96 is not supported unless it's interpreted as timestamp. " +
              s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true.")
        }
        TimestampType

      case BINARY =>
        typeAnnotation match {
          case _: StringLogicalTypeAnnotation | _: EnumLogicalTypeAnnotation |
               _: JsonLogicalTypeAnnotation => StringType
          case null if SQLConf.get.isParquetBinaryAsString => StringType
          case null => BinaryType
          case _: BsonLogicalTypeAnnotation => BinaryType
          case _: DecimalLogicalTypeAnnotation => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        typeAnnotation match {
          case _: DecimalLogicalTypeAnnotation =>
            makeDecimalType(Decimal.maxPrecisionForBytes(parquetType.getTypeLength))
          case null => BinaryType
          case _: IntervalLogicalTypeAnnotation => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
    }
  }
}
