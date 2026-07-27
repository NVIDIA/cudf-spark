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

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManager

class RapidsShuffleManagerChecksumSuite extends AnyFunSuite with FQSuiteName {
  private val checksumEnabledKey = RowBasedShuffleChecksumConf.ChecksumEnabledKey
  private val fullRetryKey = RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey

  test("rapids shuffle manager handle selection follows row-based checksum sources") {
    Seq(
      ("checksum enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = true, fullRetry = false),
        rapidsShuffleConf(checksumEnabled = false, fullRetry = false), true),
      ("full retry enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = false, fullRetry = true),
        rapidsShuffleConf(checksumEnabled = false, fullRetry = false), true),
      ("both checksum flags enabled in SQLConf",
        sqlConfWithChecksums(checksumEnabled = true, fullRetry = true),
        rapidsShuffleConf(checksumEnabled = false, fullRetry = false), true),
      ("checksum enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        rapidsShuffleConf(checksumEnabled = true, fullRetry = false), true),
      ("full retry enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        rapidsShuffleConf(checksumEnabled = false, fullRetry = true), true),
      ("both checksum flags enabled in SparkConf", sqlConfWithoutChecksumEntries(),
        rapidsShuffleConf(checksumEnabled = true, fullRetry = true), true),
      ("disabled in SQLConf", sqlConfWithChecksums(checksumEnabled = false, fullRetry = false),
        rapidsShuffleConf(checksumEnabled = true, fullRetry = true), false),
      ("disabled in SparkConf", sqlConfWithoutChecksumEntries(),
        rapidsShuffleConf(checksumEnabled = false, fullRetry = false), false)
    ).zipWithIndex.foreach { case ((label, sqlConf, sparkConf, shouldFallback), idx) =>
      // The fallback is observable at shuffle registration: when checksum support
      // forces the Spark path, the manager returns the wrapped Spark handle.
      val handle = registeredShuffleHandle(sqlConf, sparkConf, idx)
      if (shouldFallback) {
        assert(!handle.isInstanceOf[GpuShuffleHandle[_, _]], label)
      } else {
        assert(handle.isInstanceOf[GpuShuffleHandle[_, _]], label)
      }
    }
  }

  test("rapids shuffle manager checksum fallback follows dynamic SQLConf") {
    val sparkConf = rapidsShuffleConf(checksumEnabled = false, fullRetry = false)
    withTestSparkEnv(sparkConf) {
      val manager = new RapidsShuffleInternalManagerBase(sparkConf, isDriver = true)
      try {
        val gpuHandle = SQLConf.withExistingConf(
          sqlConfWithChecksums(checksumEnabled = false, fullRetry = false)) {
          manager.registerShuffle(100, gpuShuffleDependency(shuffleId = 100))
        }
        assert(gpuHandle.isInstanceOf[GpuShuffleHandle[_, _]])

        val fallbackHandle = SQLConf.withExistingConf(
          sqlConfWithChecksums(checksumEnabled = true, fullRetry = true)) {
          manager.registerShuffle(101, gpuShuffleDependency(shuffleId = 101))
        }
        assert(!fallbackHandle.isInstanceOf[GpuShuffleHandle[_, _]])

        val gpuHandleAgain = SQLConf.withExistingConf(
          sqlConfWithChecksums(checksumEnabled = false, fullRetry = false)) {
          manager.registerShuffle(102, gpuShuffleDependency(shuffleId = 102))
        }
        assert(gpuHandleAgain.isInstanceOf[GpuShuffleHandle[_, _]])
      } finally {
        manager.stop()
      }
    }
  }

  private def registeredShuffleHandle(
      sqlConf: SQLConf,
      sparkConf: SparkConf,
      shuffleId: Int) = {
    withTestSparkEnv(sparkConf) {
      SQLConf.withExistingConf(sqlConf) {
        val manager = new RapidsShuffleInternalManagerBase(sparkConf, isDriver = true)
        try {
          manager.registerShuffle(shuffleId, gpuShuffleDependency(shuffleId))
        } finally {
          manager.stop()
        }
      }
    }
  }

  private def gpuShuffleDependency(shuffleId: Int) = {
    val sc = mock[SparkContext]
    when(sc.newShuffleId()).thenReturn(shuffleId)
    when(sc.cleaner).thenReturn(None)
    when(sc.conf).thenReturn(new SparkConf(loadDefaults = false))
    when(sc.env).thenReturn(SparkEnv.get)
    when(sc.shuffleDriverComponents).thenReturn(mock[ShuffleDriverComponents])
    val rdd = new RDD[(Int, ColumnarBatch)](sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[(Int, ColumnarBatch)] = ???
      override protected def getPartitions: Array[Partition] = Array.empty
    }

    new GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rdd,
      new HashPartitioner(1),
      Array.empty[DataType],
      mock[Serializer],
      useGPUShuffle = true,
      useMultiThreadedShuffle = false)
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

  private def rapidsShuffleConf(checksumEnabled: Boolean, fullRetry: Boolean): SparkConf = {
    new SparkConf(loadDefaults = false)
      .set("spark.app.id", "shuffle-checksum-test")
      .set("spark.rapids.shuffle.mode", "CACHE_ONLY")
      .set(checksumEnabledKey, checksumEnabled.toString)
      .set(fullRetryKey, fullRetry.toString)
  }

  private def withTestSparkEnv[T](conf: SparkConf)(f: => T): T = {
    val previousEnv = SparkEnv.get
    val blockManager = mock[BlockManager]
    when(blockManager.externalShuffleServiceEnabled).thenReturn(false)
    val env = mock[SparkEnv]
    when(env.conf).thenReturn(conf)
    when(env.blockManager).thenReturn(blockManager)
    when(env.shuffleManager).thenReturn(mock[ShuffleManager])
    SparkEnv.set(env)
    try {
      f
    } finally {
      SparkEnv.set(previousEnv)
    }
  }
}
