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
package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{FQSuiteName, RowBasedShuffleChecksumConf}
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.BlockManager

class RapidsShuffleManagerChecksumSuite extends AnyFunSuite with FQSuiteName {
  private val checksumEnabledKey = RowBasedShuffleChecksumConf.ChecksumEnabledKey
  private val fullRetryKey = RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey

  test("rapids shuffle manager fallback follows row-based checksum sources") {
    Seq(
      ("checksum enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = true, fullRetry = false),
        sparkConfWithChecksums(checksumEnabled = false, fullRetry = false), true),
      ("full retry enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = false, fullRetry = true),
        sparkConfWithChecksums(checksumEnabled = false, fullRetry = false), true),
      ("both checksum flags enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = true, fullRetry = true),
        sparkConfWithChecksums(checksumEnabled = false, fullRetry = false), true),
      ("checksum enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        sparkConfWithChecksums(checksumEnabled = true, fullRetry = false), true),
      ("full retry enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        sparkConfWithChecksums(checksumEnabled = false, fullRetry = true), true),
      ("both checksum flags enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        sparkConfWithChecksums(checksumEnabled = true, fullRetry = true), true),
      ("disabled in SQLConf", sqlConfWithChecksums(checksumEnabled = false, fullRetry = false),
        sparkConfWithChecksums(checksumEnabled = true, fullRetry = true), false),
      ("disabled in SparkConf", sqlConfWithoutChecksumEntries(),
        sparkConfWithChecksums(checksumEnabled = false, fullRetry = false), false)
    ).foreach { case (label, sqlConf, sparkConf, shouldFallback) =>
      // The fallback decision is private; resolver selection is its simplest observable effect.
      // Checksum fallback uses the wrapped Spark resolver, while the GPU path uses RAPIDS' resolver.
      val resolver = shuffleBlockResolver(sqlConf, sparkConf)
      if (shouldFallback) {
        assert(resolver.isInstanceOf[IndexShuffleBlockResolver], label)
      } else {
        assert(resolver.isInstanceOf[GpuShuffleBlockResolverBase], label)
      }
    }
  }

  private def sqlConfWithChecksums(checksumEnabled: Boolean, fullRetry: Boolean): SQLConf = {
    val sqlConf = new SQLConf()
    sqlConf.setConfString(checksumEnabledKey, checksumEnabled.toString)
    sqlConf.setConfString(fullRetryKey, fullRetry.toString)
    sqlConf
  }

  private def sqlConfWithoutChecksumEntries(): SQLConf = {
    val sqlConf = new SQLConf()
    assert(!sqlConf.contains(checksumEnabledKey))
    assert(!sqlConf.contains(fullRetryKey))
    sqlConf
  }

  private def sparkConfWithChecksums(checksumEnabled: Boolean, fullRetry: Boolean): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set("spark.app.id", "shuffle-checksum-test")
      .set("spark.rapids.shuffle.mode", "MULTITHREADED")
      .set("spark.rapids.shuffle.multiThreaded.writer.threads", "0")
      .set("spark.rapids.shuffle.multiThreaded.reader.threads", "0")
      .set(checksumEnabledKey, checksumEnabled.toString)
      .set(fullRetryKey, fullRetry.toString)
  }

  private def shuffleBlockResolver(sqlConf: SQLConf, sparkConf: SparkConf) = {
    withTestSparkEnv(sparkConf) {
      SQLConf.withExistingConf(sqlConf) {
        val manager = new com.nvidia.spark.rapids.spark420.RapidsShuffleManager(
          sparkConf,
          isDriver = true)
        try {
          manager.shuffleBlockResolver
        } finally {
          manager.stop()
        }
      }
    }
  }

  private def withTestSparkEnv[T](conf: SparkConf)(f: => T): T = {
    val previousEnv = SparkEnv.get
    val blockManager = mock[BlockManager]
    when(blockManager.externalShuffleServiceEnabled).thenReturn(false)
    val env = mock[SparkEnv]
    when(env.conf).thenReturn(conf)
    when(env.blockManager).thenReturn(blockManager)
    SparkEnv.set(env)
    try {
      f
    } finally {
      SparkEnv.set(previousEnv)
    }
  }
}
