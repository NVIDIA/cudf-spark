/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{RapidsHostColumnBuilder, TypeConverter, TypeSig}

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types.{DataType, VariantType}

object VariantTypeShims {
  private val OFFSET = Integer.BYTES
  private val VALIDITY = 0.125
  private val CHILD_NULL_SIZE = OFFSET + VALIDITY
  private val NULL_SIZE = 2 * CHILD_NULL_SIZE + VALIDITY

  def isVariantType(dataType: DataType): Boolean = dataType == VariantType
  def additionalParquetReadSupportedTypes: TypeSig = TypeSig.VARIANT
  def supportsVariantType: Boolean = true
  def additionalCommonOperatorSupportedTypes: TypeSig = TypeSig.VARIANT

  private def appendVariant(
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
    val variant = row.getVariant(column)
    val value = variant.getValue
    val metadata = variant.getMetadata
    builder.getChild(0).appendByteList(value)
    builder.getChild(1).appendByteList(metadata)
    builder.endStruct()
    value.length + metadata.length + 2 * CHILD_NULL_SIZE
  }

  private object VariantConverter extends TypeConverter {
    override def append(
        row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        NULL_SIZE
      } else {
        appendVariant(row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = NULL_SIZE
  }

  private object NotNullVariantConverter extends TypeConverter {
    override def append(
        row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double =
      appendVariant(row, column, builder)

    // A null parent struct still cascades nulls to its two list children.
    override def getNullSize: Double = NULL_SIZE
  }

  def getRowToColumnConverter(nullable: Boolean): TypeConverter =
    if (nullable) VariantConverter else NotNullVariantConverter
}
