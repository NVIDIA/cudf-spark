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

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

object RowBasedShuffleChecksumConf {
  val ChecksumEnabledKey = "spark.sql.shuffle.orderIndependentChecksum.enabled"
  val ChecksumMismatchFullRetryKey =
    "spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch"

  // SQLConf takes priority over SparkConf when explicitly set (e.g. SET command mid-session).
  // SparkConf is checked next for values set at session start or via --conf.
  // If neither is explicitly set, we fall back to Spark's registered config default via
  // SQLConf.getConfString: on Spark 4.2+ both keys default to true (checksums on by
  // default), so RAPIDS will fall back to SortShuffleManager unless the user explicitly
  // disables them (set both keys to false). On Spark < 4.2 these keys default to false
  // or are not registered in SQLConf, so GPU shuffle proceeds normally.
  def isEnabled(sqlConf: SQLConf, sparkConf: SparkConf): Boolean = {
    getBoolean(sqlConf, sparkConf, ChecksumEnabledKey) ||
      getBoolean(sqlConf, sparkConf, ChecksumMismatchFullRetryKey)
  }

  private def getBoolean(sqlConf: SQLConf, sparkConf: SparkConf, key: String): Boolean = {
    if (sqlConf.contains(key)) {
      sqlConf.getConfString(key).toBoolean
    } else if (sparkConf.contains(key)) {
      sparkConf.getBoolean(key, false)
    } else {
      Try(sqlConf.getConfString(key)).map(_.toBoolean).getOrElse(false)
    }
  }
}
