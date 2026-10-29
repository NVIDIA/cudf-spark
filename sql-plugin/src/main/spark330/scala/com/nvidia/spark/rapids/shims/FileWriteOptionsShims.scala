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
{"spark":["330","350","400"],"carryForward":"all"}
{"spark":["330db","332db","350db143","400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

/**
 * Compatibility implementation for Spark versions before 4.2.
 *
 * Shared writer code calls these helpers, but these versions must keep the original behavior:
 * write options are not merged here and prepareWrite applies SQLConf values unconditionally.
 */
object FileWriteOptionsShims {
  def mergeWriteOptionsIntoHadoopConf(
      options: Map[String, String],
      conf: Configuration): Unit = {}

  def setConfWithWriteOptionPrecedence(
      conf: Configuration,
      key: String,
      value: => String): Unit = {
    conf.set(key, value)
  }

  def getEffectiveOption(
      options: Map[String, String],
      conf: Configuration,
      key: String,
      defaultValue: String): String = {
    defaultValue
  }

  def setupLegacyParquetNanosAsLong(conf: Configuration, sqlConf: SQLConf): Unit = {}
}
