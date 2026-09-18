/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.parquet

import com.nvidia.spark.rapids.internal.config.CudfConfKeys
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

object ParquetFieldIdShims {
  private val fieldIdOverrideKey: String = "spark.cudf.sql.parquet.writeFieldIds"
  private val legacyFieldIdOverrideKey: String = CudfConfKeys.legacyKey(fieldIdOverrideKey)

  /** Updates the Hadoop configuration with the Parquet field ID write setting from SQLConf */
  def setupParquetFieldIdWriteConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    conf.set(
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sqlConf.parquetFieldIdWriteEnabled.toString)
  }

  /** Get Parquet field ID write enabled configuration value */
  def getParquetIdWriteEnabled(conf: Configuration, sqlConf: SQLConf): Boolean = {
    Option(conf.get(fieldIdOverrideKey))
      .orElse(Option(conf.get(legacyFieldIdOverrideKey)))
      .exists(_.toBoolean) || sqlConf.parquetFieldIdWriteEnabled
  }

  /** Set the Parquet field ID write enable override */
  def setWriteIdOverride(conf: Configuration, enabled: Boolean): Unit = {
    conf.set(fieldIdOverrideKey, enabled.toString)
  }
}
