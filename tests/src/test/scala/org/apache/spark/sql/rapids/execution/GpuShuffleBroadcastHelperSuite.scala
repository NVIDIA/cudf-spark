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
package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ByteType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuShuffleBroadcastHelperSuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("value", ByteType)))
  private val output = Seq(AttributeReference("value", ByteType)())
  private val metrics: Map[String, GpuMetric] = Map(
    GpuMetric.CONCAT_TIME -> NoopMetric, GpuMetric.OP_TIME_LEGACY -> NoopMetric)

  private def batch(column: ColumnVector): ColumnarBatch =
    new ColumnarBatch(Array(new GpuColumnVector(ByteType, column.incRefCount())),
      column.getRowCount.toInt)

  test("accept an actual build exactly at the byte ceiling") {
    withResource(ColumnVector.fromBytes(1.toByte, 2.toByte)) { column =>
      val input = batch(column)
      val bytes = GpuColumnVector.getTotalDeviceMemoryUsed(input)
      withResource(GpuShuffleBroadcastHelper.assemble(
          Iterator(input), schema, output, metrics, bytes)) { result =>
        assert(result.numRows() == 2)
        assert(GpuColumnVector.getTotalDeviceMemoryUsed(result) == bytes)
        withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
          assert(host.getByte(0) == 1 && host.getByte(1) == 2)
        }
      }
      assert(column.getRefCount == 1)
    }
  }

  test("reject received bytes before reading the next batch and release accepted inputs") {
    val allocated = Rmm.getTotalBytesAllocated
    withResource(ColumnVector.fromBytes(1.toByte, 2.toByte)) { first =>
      withResource(ColumnVector.fromBytes(3.toByte, 4.toByte)) { second =>
        val baselineHandles = SpillFramework.stores.deviceStore.numHandles
        var reads = 0
        val input = Iterator(first, second, first).map { column =>
          reads += 1
          batch(column)
        }
        val error = intercept[IllegalStateException] {
          GpuShuffleBroadcastHelper.assemble(input, schema, output, metrics,
            first.getDeviceMemorySize)
        }
        assert(error.getMessage.contains("received build"))
        assert(reads == 2)
        assert(first.getRefCount == 1 && second.getRefCount == 1)
        assert(SpillFramework.stores.deviceStore.numHandles == baselineHandles)
      }
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("reject concatenation growth before cache publication and permit a fresh build") {
    val allocated = Rmm.getTotalBytesAllocated
    withResource(ColumnVector.fromBytes(Array.fill[Byte](32768)(1): _*)) { nonNull =>
      withResource(ColumnVector.fromBoxedBytes(null.asInstanceOf[java.lang.Byte])) { nullable =>
        def inputs: Iterator[ColumnarBatch] = Iterator(nonNull, nullable).map(batch)
        val inputBytes = nonNull.getDeviceMemorySize + nullable.getDeviceMemorySize
        val assembledBytes = withResource(GpuShuffleBroadcastHelper.assemble(
            inputs, schema, output, metrics, Long.MaxValue)) { result =>
          assert(result.numRows() == 32769)
          GpuColumnVector.getTotalDeviceMemoryUsed(result)
        }
        assert(assembledBytes > inputBytes, "The test must exercise concatenation growth")
        val handles = SpillFramework.stores.deviceStore.numHandles
        val before = Rmm.getTotalBytesAllocated
        withResource(new ExecutorBroadcastCache[SpillableColumnarBatch](_.close())) { cache =>
          var builds = 0
          def read(limit: Long): ColumnarBatch = cache.getOrBuild(7, () => {
            builds += 1
            val result = GpuShuffleBroadcastHelper.assemble(inputs, schema, output, metrics, limit)
            SpillableColumnarBatch(result, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          }, () => (), () => ())(_.getColumnarBatch())
          val error = intercept[IllegalStateException](read(inputBytes))
          assert(error.getMessage.contains("assembled build"))
          assert(nonNull.getRefCount == 1 && nullable.getRefCount == 1)
          assert(SpillFramework.stores.deviceStore.numHandles == handles)
          assert(Rmm.getTotalBytesAllocated == before)
          withResource(read(assembledBytes)) { result =>
            assert(result.numRows() == 32769)
          }
          assert(builds == 2, "The failed build must not publish a cache entry")
        }
      }
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("an input iterator failure releases earlier accepted batches") {
    withResource(ColumnVector.fromBytes(1.toByte)) { column =>
      val handles = SpillFramework.stores.deviceStore.numHandles
      var reads = 0
      val input = Iterator.continually {
        reads += 1
        if (reads == 2) throw new IllegalArgumentException("injected input failure")
        batch(column)
      }
      val error = intercept[IllegalArgumentException] {
        GpuShuffleBroadcastHelper.assemble(input, schema, output, metrics, Long.MaxValue)
      }
      assert(error.getMessage == "injected input failure")
      assert(column.getRefCount == 1)
      assert(SpillFramework.stores.deviceStore.numHandles == handles)
    }
  }
}
