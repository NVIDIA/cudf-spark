/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.parquet

import com.nvidia.spark.rapids.shims.FileWriteOptionsShims
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

object ParquetFieldIdShims {
  /**
   * Applies the SQLConf field ID setting without replacing a Spark 4.2+ per-write option.
   * Older shims retain the original unconditional SQLConf behavior.
   */
  def setupParquetFieldIdWriteConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    FileWriteOptionsShims.setConfWithWriteOptionPrecedence(
      conf,
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sqlConf.parquetFieldIdWriteEnabled.toString)
  }

  /** Gets the field ID setting resolved in the Hadoop configuration. */
  def getParquetIdWriteEnabled(conf: Configuration, sqlConf: SQLConf): Boolean = {
    conf.getBoolean(
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sqlConf.parquetFieldIdWriteEnabled)
  }
}
