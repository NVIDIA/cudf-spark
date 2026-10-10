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
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import scala.util.Try

import com.nvidia.spark.rapids.ShimReflectionUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Shim for Parquet variant-related configurations in Spark 4.0.x.
 */
object ParquetVariantShims {
  def setupParquetVariantConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // No-op because PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE does not exist in Spark 4.0.x.
  }

  // OSS Spark 4.0.x only pushes Variant extraction into V1 scans. Detect distributions that
  // backport V2 pushdown by checking whether their Parquet V2 scan builder implements the API.
  def supportsV2VariantPushdown: Boolean = Try {
    val pushdownInterface = ShimReflectionUtils.loadClass(
      "org.apache.spark.sql.connector.read.SupportsPushDownVariantExtractions")
    val parquetScanBuilder = ShimReflectionUtils.loadClass(
      "org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder")
    pushdownInterface.isAssignableFrom(parquetScanBuilder)
  }.getOrElse(false)

  def isPushedVariantStruct(dataType: DataType): Boolean =
    VariantMetadata.isVariantStruct(dataType)

  def isPotentiallyShreddedVariant(_dataType: DataType, _sqlConf: SQLConf): Boolean = false
}
