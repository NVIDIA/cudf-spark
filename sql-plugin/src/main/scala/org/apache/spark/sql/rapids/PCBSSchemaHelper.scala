/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.GpuTypeShims

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{ArrayType, AtomicType, ByteType, CalendarIntervalType, DataType, DataTypes, Decimal, DecimalType, IntegerType, LongType, MapType, NullType, StructField, StructType, UserDefinedType}

object PCBSSchemaHelper {
  val calendarIntervalStructType = new StructType()
      .add("_days", IntegerType)
      .add("_months", IntegerType)
      .add("_ms", LongType)

  /**
   * This method checks if the datatype passed is officially supported by parquet.
   *
   * Please refer to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md to see
   * the what types are supported by parquet
   */
  def isTypeSupportedByParquet(dataType: DataType): Boolean = {
    dataType match {
      case CalendarIntervalType | NullType => false
      case s: StructType => s.forall(field => isTypeSupportedByParquet(field.dataType))
      case ArrayType(elementType, _) => isTypeSupportedByParquet(elementType)
      case MapType(keyType, valueType, _) => isTypeSupportedByParquet(keyType) &&
          isTypeSupportedByParquet(valueType)
      case d: DecimalType if d.scale < 0 => false
      //Atomic Types
      case _: AtomicType => true
      case _ => false
    }
  }

  def isTypeSupportedByColumnarSparkParquetWriter(dataType: DataType): Boolean = {
    // Columnar writer in Spark only supports AtomicTypes ATM
    dataType match {
      case _: AtomicType => true
      case other if GpuTypeShims.isParquetColumnarWriterSupportedForType(other) => true
      case _ => false
    }
  }

  /**
   * This method converts types that parquet doesn't recognize to types that Parquet understands.
   * e.g. CalendarIntervalType is converted to a struct with a struct of two integer types and a
   * long type.
   */
  def getSupportedDataType(dataType: DataType): DataType = {
    var curId = 0
    dataType match {
      case CalendarIntervalType =>
        calendarIntervalStructType
      case NullType =>
        ByteType
      case s: StructType =>
        val newStructType = StructType(
          s.indices.map { index =>
            val field = StructField(s"_col$curId",
              getSupportedDataType(s.fields(index).dataType),
              s.fields(index).nullable, s.fields(index).metadata)
            curId += 1
            field
          })
        newStructType
      case _@ArrayType(elementType, nullable) =>
        val newArrayType =
          ArrayType(getSupportedDataType(elementType), nullable)
        newArrayType
      case _@MapType(keyType, valueType, nullable) =>
        val newKeyType = getSupportedDataType(keyType)
        val newValueType = getSupportedDataType(valueType)
        val mapType = MapType(newKeyType, newValueType, nullable)
        mapType
      case d: DecimalType if d.scale < 0 =>
        val newType = if (d.precision <= Decimal.MAX_INT_DIGITS) {
          IntegerType
        } else {
          LongType
        }
        newType
      case _: AtomicType => dataType
      case o =>
        throw new IllegalArgumentException(s"We don't support ${o.typeName}")
    }
  }

  /**
   * There are certain types that are not supported by Parquet. This method converts the schema
   * of those types to something parquet understands e.g. CalendarIntervalType will be converted
   * to an attribute with {@link calendarIntervalStructType} as type
   */
  def getSupportedSchemaFromUnsupported(cachedAttributes: Seq[Attribute]): Seq[Attribute] = {
    // We convert CalendarIntervalType, UDT and NullType ATM convert it to a supported type
    var curId = 0
    cachedAttributes.map {
      attribute =>
        val name = s"_col$curId"
        curId += 1
        attribute.dataType match {
          case CalendarIntervalType =>
            AttributeReference(name, calendarIntervalStructType,
              attribute.nullable, metadata = attribute.metadata)(attribute.exprId)
                .asInstanceOf[Attribute]
          case NullType =>
            AttributeReference(name, DataTypes.ByteType,
              nullable = true, metadata =
                attribute.metadata)(attribute.exprId).asInstanceOf[Attribute]
          case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | DecimalType() =>
            AttributeReference(name,
              getSupportedDataType(attribute.dataType),
              attribute.nullable, attribute.metadata)(attribute.exprId)
          case udt: UserDefinedType[_] =>
            AttributeReference(name,
              getSupportedDataType(udt.sqlType),
              attribute.nullable, attribute.metadata)(attribute.exprId)
          case _ =>
            attribute.withName(name)
        }
    }
  }

}
