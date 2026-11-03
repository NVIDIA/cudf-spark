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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * Spark 4.2+ adapters for the per-write option precedence introduced by SPARK-56414.
 */
object FileWriteOptionsShims {
  def mergeWriteOptionsIntoHadoopConf(
      options: Map[String, String],
      conf: Configuration): Unit = {
    DataSourceUtils.mergeWriteOptionsIntoHadoopConf(options, conf)
  }

  def setConfWithWriteOptionPrecedence(
      conf: Configuration,
      key: String,
      value: => String): Unit = {
    DataSourceUtils.setConfIfAbsent(conf, key, value)
  }

  def getEffectiveOption(
      options: Map[String, String],
      conf: Configuration,
      key: String,
      defaultValue: String): String = {
    // Resolve tagging decisions against a copy so the shared Hadoop configuration is not mutated.
    val effectiveConf = new Configuration(conf)
    DataSourceUtils.mergeWriteOptionsIntoHadoopConf(options, effectiveConf)
    Option(effectiveConf.get(key)).getOrElse(defaultValue)
  }

  def setupLegacyParquetNanosAsLong(conf: Configuration, sqlConf: SQLConf): Unit = {
    DataSourceUtils.setConfIfAbsent(
      conf,
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sqlConf.legacyParquetNanosAsLong.toString)
  }
}
