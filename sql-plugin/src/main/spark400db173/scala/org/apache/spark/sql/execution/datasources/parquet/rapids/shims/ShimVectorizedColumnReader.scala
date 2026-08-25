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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet.rapids.shims

import java.time.ZoneId
import java.util.TimeZone

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{GroupType, Type}

import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.parquet.{ParentContainerUpdater,
  ParquetIdExternalMapping, ParquetRowConverter, ParquetToSparkSchemaConverter,
  VectorizedColumnReader}
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.types.StructType

/**
 * Identity mapping implementation for ParquetIdExternalMapping in Databricks 17.3.
 * Based on the interface methods: getRootId(), getChild(name), getFieldId(name)
 */
object IdentityParquetIdMapping extends ParquetIdExternalMapping {
  override def getRootId: Option[Int] = None
  override def getChild(name: String): ParquetIdExternalMapping = this
  override def getFieldId(name: String): Option[Int] = None
}

/**
 * Databricks 17.3 version where ParquetRowConverter requires
 * 8 parameters including externalIdMapping.
 */
class ShimParquetRowConverter(
    schemaConverter: ParquetToSparkSchemaConverter,
    parquetType: GroupType,
    catalystType: StructType,
    convertTz: Option[ZoneId],
    datetimeRebaseMode: String,
    int96RebaseMode: String,
    int96CDPHive3Compatibility: Boolean,
    updater: ParentContainerUpdater
) extends ParquetRowConverter(
      schemaConverter,
      parquetType,
      catalystType,
      convertTz,
      RebaseSpec(LegacyBehaviorPolicy.withName(datetimeRebaseMode)),
      RebaseSpec(LegacyBehaviorPolicy.withName(int96RebaseMode)),
      updater,
      IdentityParquetIdMapping)

class ShimVectorizedColumnReader(
    index: Int,
    columns: java.util.List[ColumnDescriptor],
    types: java.util.List[Type],
    pageReadStore: PageReadStore,
    convertTz: ZoneId,
    datetimeRebaseMode: String,
    int96RebaseMode: String,
    int96CDPHive3Compatibility: Boolean,
    writerVersion: ParsedVersion
) extends VectorizedColumnReader(
      columns.get(index),
      true,
      false,
      pageReadStore,
      convertTz,
      datetimeRebaseMode,
      TimeZone.getDefault.getID,
      int96RebaseMode,
      TimeZone.getDefault.getID,
      writerVersion)
