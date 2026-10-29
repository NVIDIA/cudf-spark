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
{"spark":["330","350"],"carryForward":"all"}
{"spark":["330db","332db","350db143"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Shim for Parquet variant-related configurations.
 * In Spark <= 4.0.x, PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE doesn't exist.
 * In Spark 4.1.0+, we need to set this configuration for ParquetWriteSupport.
 */
object ParquetVariantShims {
  def setupParquetVariantConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // No-op for Spark versions before 4.1.0
    // PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE doesn't exist
  }

  def supportsV2VariantPushdown: Boolean = false

  def isPushedVariantStruct(_dataType: DataType): Boolean = false

  def isPotentiallyShreddedVariant(_dataType: DataType, _sqlConf: SQLConf): Boolean = false
}
