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

package com.nvidia.spark.rapids

sealed trait FileFormatType
object CsvFormatType extends FileFormatType {
  override def toString = "CSV"
}
object HiveDelimitedTextFormatType extends FileFormatType {
  override def toString = "HiveText"
}
object ParquetFormatType extends FileFormatType {
  override def toString = "Parquet"
}
object OrcFormatType extends FileFormatType {
  override def toString = "ORC"
}
object JsonFormatType extends FileFormatType {
  override def toString = "JSON"
}
object AvroFormatType extends FileFormatType {
  override def toString = "Avro"
}
object IcebergFormatType extends FileFormatType {
  override def toString = "Iceberg"
}
object DeltaFormatType extends FileFormatType {
  override def toString = "Delta"
}

sealed trait FileFormatOp
object ReadFileOp extends FileFormatOp {
  override def toString = "read"
}
object WriteFileOp extends FileFormatOp {
  override def toString = "write"
}
