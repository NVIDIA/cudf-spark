/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class KudoGpuSerializerRetrySuite extends RmmSparkRetrySuiteBase {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(9, null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
      .column("nine", "eight", null, null, "six", "five", "four", "three", "two", "one")
      .column(Long.box(9L), Long.box(8L), Long.box(7L), Long.box(6L), Long.box(5L),
        Long.box(4L), Long.box(3L), Long.box(2L), Long.box(1L), Long.box(0L))
      .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, LongType))
    }
  }

  test("kudo gpu serializer retries on gpu oom") {
    val conf = new SparkConf(false)
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")

    val sqlConf = new SQLConf()
    conf.getAll.foreach { case (k, v) => sqlConf.setConfString(k, v) }

    SQLConf.withExistingConf(sqlConf) {
      val partitionIndices = Array(0, 3, 7)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }

      withResource(buildBatch()) { batch =>
        val numRows = batch.numRows
        GpuColumnVector.incRefCounts(batch)
        val columns = GpuColumnVector.extractColumns(batch)

        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)

        var results: Array[(ColumnarBatch, Int)] = null
        try {
          results = gp.sliceInternalGpuOrCpuAndClose(numRows, partitionIndices, columns)
          assertResult(gp.numPartitions)(results.length)
          results.foreach { case (cb, _) =>
            if (cb != null) {
              assert(cb.numRows() >= 0)
            }
          }
          val retryCount = RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
          assert(retryCount > 0, s"expected at least one retry but saw $retryCount")
        } finally {
          if (results != null) {
            results.foreach { case (cb, _) =>
              if (cb != null) {
                cb.close()
              }
            }
          }
        }
      }
    }
  }
}
