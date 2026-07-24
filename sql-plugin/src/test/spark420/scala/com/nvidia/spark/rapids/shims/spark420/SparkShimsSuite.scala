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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.spark420

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

class SparkShimsSuite extends AnyFunSuite with FQSuiteName {
  private val checksumEnabledKey = RowBasedShuffleChecksumConf.ChecksumEnabledKey
  private val fullRetryKey = RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey

  test("spark shims version") {
    assert(ShimLoader.getShimVersion === SparkShimVersion(4, 2, 0))
  }

  test("shuffle manager class") {
    assert(ShimLoader.getRapidsShuffleManagerClass ===
      classOf[com.nvidia.spark.rapids.spark420.RapidsShuffleManager].getCanonicalName)
  }

  test("row-based shuffle checksum defaults to enabled") {
    val sqlConf = new SQLConf()
    assert(sqlConf.shuffleOrderIndependentChecksumEnabled)
    assert(sqlConf.shuffleChecksumMismatchFullRetryEnabled)
    assert(SparkShimImpl.isRowBasedShuffleChecksumEnabled(sqlConf, new SparkConf(false)))
  }

  test("row-based shuffle checksum uses SparkConf when SQLConf is unset") {
    val sparkConf = new SparkConf(false)
      .set(checksumEnabledKey, "false")
      .set(fullRetryKey, "false")

    assert(!SparkShimImpl.isRowBasedShuffleChecksumEnabled(new SQLConf(), sparkConf))
  }

  test("row-based shuffle checksum uses SQLConf when present") {
    val sqlConf = new SQLConf()
    sqlConf.setConfString(checksumEnabledKey, "false")
    sqlConf.setConfString(fullRetryKey, "false")
    val sparkConf = new SparkConf(false)
      .set(checksumEnabledKey, "true")
      .set(fullRetryKey, "true")

    assert(!SparkShimImpl.isRowBasedShuffleChecksumEnabled(sqlConf, sparkConf))
  }

}
