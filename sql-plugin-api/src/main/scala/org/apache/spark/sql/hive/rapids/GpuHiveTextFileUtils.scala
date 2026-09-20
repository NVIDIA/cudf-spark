/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
package org.apache.spark.sql.hive.rapids

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType, StructField, StructType}

object GpuHiveTextFileUtils {
  val textInputFormat      = "org.apache.hadoop.mapred.TextInputFormat"
  val textOutputFormat     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  val lazySimpleSerDe      = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
  val serializationKey     = "serialization.format"
  val ctrlASeparatedFormat = "1" // Implying '^A' field delimiter.
  val lineDelimiterKey     = "line.delim"
  val escapeDelimiterKey   = "escape.delim"
  val newLine              = "\n"

  /**
   * Checks whether the specified data type is supported for Hive delimited text tables.
   *
   * Note: This is used to check the schema of a Hive *Table*, either when reading
   * delimited text tables (i.e. [[GpuHiveTableScanExec]]), or writing them
   * (i.e. GpuHiveTextFileFormat).
   *
   * This is separate from the checks in [[com.nvidia.spark.rapids.FileFormatChecks]],
   * which check the data types in the read/write *schemas*, per file format (including
   * for Hive delimited text).
   *
   * When the supported types are modified here, they must similarly be modified
   * in [[com.nvidia.spark.rapids.GpuOverrides.fileFormats]].
   *
   * @param dataType The data-type being checked for compatibility in Hive text.
   * @return true if the type is supported for Hive text, false otherwise
   */
  def isSupportedType(dataType: DataType): Boolean = dataType match {
    case ArrayType(_,_) => false
    case StructType(_)  => false
    case MapType(_,_,_) => false
    case BinaryType     => false
    case _              => true
  }

  def hasUnsupportedType(column: StructField): Boolean = !isSupportedType(column.dataType)
  def hasUnsupportedType(column: AttributeReference): Boolean = !isSupportedType(column.dataType)
}
