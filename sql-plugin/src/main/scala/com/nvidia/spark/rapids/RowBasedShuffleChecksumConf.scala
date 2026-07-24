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

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

object RowBasedShuffleChecksumConf {
  val ChecksumEnabledKey = "spark.sql.shuffle.orderIndependentChecksum.enabled"
  val ChecksumMismatchFullRetryKey =
    "spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch"

  def isEnabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean(ChecksumEnabledKey, false) ||
      sparkConf.getBoolean(ChecksumMismatchFullRetryKey, false)
  }

  def isEnabled(
      sqlConf: SQLConf,
      sparkConf: SparkConf,
      checksumEnabledDefault: Boolean,
      checksumMismatchFullRetryDefault: Boolean): Boolean = {
    getBoolean(sqlConf, sparkConf, ChecksumEnabledKey, checksumEnabledDefault) ||
      getBoolean(sqlConf, sparkConf, ChecksumMismatchFullRetryKey,
        checksumMismatchFullRetryDefault)
  }

  private def getBoolean(
      sqlConf: SQLConf,
      sparkConf: SparkConf,
      key: String,
      defaultValue: Boolean): Boolean = {
    if (sqlConf.contains(key)) {
      sqlConf.getConfString(key).toBoolean
    } else {
      sparkConf.getBoolean(key, defaultValue)
    }
  }
}
