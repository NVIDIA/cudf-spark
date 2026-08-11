/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution.datasources.parquet.rapids

import java.time.ZoneId

import com.nvidia.spark.rapids.shims.LegacyBehaviorPolicyShim
import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.{NoopUpdater, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.execution.datasources.parquet.rapids.shims.ShimParquetRowConverter
import org.apache.spark.sql.types.StructType

/**
 * This class exposes the ParquetRecordMaterializer
 */
class ParquetRecordMaterializer(
   parquetSchema: MessageType,
   catalystSchema: StructType,
   schemaConverter: ParquetToSparkSchemaConverter,
   convertTz: Option[ZoneId],
   datetimeRebaseMode: String) extends RecordMaterializer[InternalRow] {

  private val rootConverter = new ShimParquetRowConverter(
    schemaConverter,
    parquetSchema,
    catalystSchema,
    convertTz,
    datetimeRebaseMode, // always LegacyBehaviorPolicy.CORRECTED
    LegacyBehaviorPolicyShim.EXCEPTION_STR,
    false,
    NoopUpdater)

  override def getCurrentRecord: InternalRow = rootConverter.currentRecord

  override def getRootConverter: GroupConverter = rootConverter
}
