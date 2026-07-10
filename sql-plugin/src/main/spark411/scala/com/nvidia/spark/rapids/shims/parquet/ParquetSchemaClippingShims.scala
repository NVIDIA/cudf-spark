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
{"spark": "411"}
{"spark": "412"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ParquetSchemaClippingShims {
  def clipParquetSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      conf: Configuration,
      returnNullStructIfAllFieldsMissing: Boolean,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      ignoreMissingFieldId: Boolean): MessageType = {
    val clippingConf = new Configuration(conf)
    clippingConf.setBoolean(SQLConf.CASE_SENSITIVE.key, caseSensitive)
    clippingConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, true)
    clippingConf.setBoolean(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key, useFieldId)
    clippingConf.setBoolean(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key, ignoreMissingFieldId)
    clippingConf.setBoolean(
      SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key,
      returnNullStructIfAllFieldsMissing)

    // A non-vectorized request intersects Spark's clipped schema with the file schema when nested
    // pruning is enabled. Force that internal intersection so this method stays physical-only even
    // when the user has disabled nested schema pruning.
    ParquetReadSupport.getRequestedSchema(
      parquetSchema, catalystSchema, clippingConf, enableVectorizedReader = false)
  }

  def returnNullStructIfAllFieldsMissing(sqlConf: SQLConf): Boolean =
    sqlConf.getConf(SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING)

  def nativeFooterCanPreserveMissingStructNullability(
      returnNullStructIfAllFieldsMissing: Boolean,
      readSchemaContainsStruct: Boolean): Boolean = {
    returnNullStructIfAllFieldsMissing || !readSchemaContainsStruct
  }
}
