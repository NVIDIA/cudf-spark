/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import java.math.RoundingMode

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSinglePartitioningSuite extends AnyFunSuite {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .decimal64Column(-3, RoundingMode.UNNECESSARY ,
          5.1, null, 3.3, 4.4e2, 0, -2.1e-1, 1.111, 2.345, null, 1.23e3)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType,
        DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 3)))
    }
  }

  test("generates contiguous split uncompressed") {
    val conf = new SparkConf()
      .set("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
      // set up as UCX because that's what triggers nvcomp
      .set("spark.rapids.shuffle.mode", RapidsConf.RapidsShuffleManagerMode.UCX.toString)
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitioner = GpuSinglePartitioning
      withResource(buildBatch()) { batch =>
        withResource(GpuColumnVector.from(batch)) { table =>
          withResource(table.contiguousSplit()) { contigTables =>
            val expected = contigTables.head
            // partition will consume batch, so increment refcounts enabling withResource to close
            GpuColumnVector.extractBases(batch).foreach(_.incRefCount())
            val result =
              partitioner.columnarEvalAny(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
            try {
              assertResult(1)(result.length)
              assertResult(0)(result.head._2)
              val resultBatch = result.head._1
              // verify this is a contiguous split table
              assert(GpuPackedTableColumn.isBatchPacked(resultBatch))
              val packedColumn = resultBatch.column(0).asInstanceOf[GpuPackedTableColumn]
              val actual = packedColumn.getContiguousTable
              assertResult(expected.getBuffer.getLength)(actual.getBuffer.getLength)
              assertResult(expected.getMetadataDirectBuffer)(actual.getMetadataDirectBuffer)
              TestUtils.compareTables(expected.getTable, actual.getTable)
            } finally {
              result.foreach(_._1.close())
            }
          }
        }
      }
    }
  }
}
