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
import org.apache.spark.sql.types.{ByteType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuDriverlessBroadcastHelperSuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("value", ByteType)))
  private val output = Seq(AttributeReference("value", ByteType)())
  private val metrics: Map[String, GpuMetric] = Map(
    GpuMetric.CONCAT_TIME -> NoopMetric, GpuMetric.OP_TIME_LEGACY -> NoopMetric)

  private def batch(column: ColumnVector): ColumnarBatch =
    new ColumnarBatch(Array(GpuColumnVector.from(column.incRefCount(), ByteType)),
      column.getRowCount.toInt)

  test("a single input keeps its data and releases borrowed references") {
    withResource(ColumnVector.fromBytes(1.toByte, 2.toByte)) { column =>
      withResource(GpuDriverlessBroadcastHelper.assemble(
          Iterator(batch(column)), schema, output, metrics)) { result =>
        assert(result.numRows() == 2)
        withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
          assert(host.getByte(0) == 1 && host.getByte(1) == 2)
        }
      }
      assert(column.getRefCount == 1)
    }
  }

  test("the driver bound covers real null-mask growth at allocation boundaries") {
    val allocated = Rmm.getTotalBytesAllocated
    Seq(1, 511, 512, 513, 32768).foreach { rows =>
      withResource(ColumnVector.fromBytes(Array.fill[Byte](rows)(1): _*)) { nonNull =>
        withResource(ColumnVector.fromBoxedBytes(null.asInstanceOf[java.lang.Byte])) { nullable =>
          val inputBytes = nonNull.getDeviceMemorySize + nullable.getDeviceMemorySize
          val bound = DriverlessBroadcastBuildAdmission.upperBound(
            Array(inputBytes), inputBytes, rows + 1, schema)
          withResource(GpuDriverlessBroadcastHelper.assemble(
              Iterator(nonNull, nullable).map(batch), schema, output, metrics)) { result =>
            val actualBytes = GpuColumnVector.getTotalDeviceMemoryUsed(result)
            assert(result.numRows() == rows + 1)
            assert(bound.exists(_ >= actualBytes))
            if (rows == 32768) {
              info(s"received bytes=$inputBytes, assembled bytes=$actualBytes, bound=$bound")
              assert(actualBytes > inputBytes, "This case must exercise concatenation growth")
              assert(!DriverlessBroadcastBuildAdmission.accepts(bound, inputBytes))
              assert(DriverlessBroadcastBuildAdmission.accepts(bound, bound.get.toLong))
            }
          }
          assert(nonNull.getRefCount == 1 && nullable.getRefCount == 1)
        }
      }
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("string offsets and null masks fit the driver bound") {
    val allocated = Rmm.getTotalBytesAllocated
    val strings = StructType(Seq(StructField("value", StringType)))
    val attributes = Seq(AttributeReference("value", StringType)())
    withResource(ColumnVector.fromStrings("abc", "long string", "")) { first =>
      withResource(ColumnVector.fromStrings(null.asInstanceOf[String])) { second =>
        val inputBytes = first.getDeviceMemorySize + second.getDeviceMemorySize
        val bound = DriverlessBroadcastBuildAdmission.upperBound(
          Array(inputBytes), inputBytes, 4, strings)
        val input = Iterator(first, second).map { column =>
          new ColumnarBatch(Array(GpuColumnVector.from(column.incRefCount(), StringType)),
            column.getRowCount.toInt)
        }
        withResource(GpuDriverlessBroadcastHelper.assemble(input, strings, attributes, metrics)) {
          result =>
            assert(result.numRows() == 4)
            assert(bound.exists(_ >= GpuColumnVector.getTotalDeviceMemoryUsed(result)))
            withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
              assert(host.getJavaString(0) == "abc" && host.isNull(3))
            }
        }
        assert(first.getRefCount == 1 && second.getRefCount == 1)
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
        GpuDriverlessBroadcastHelper.assemble(input, schema, output, metrics)
      }
      assert(error.getMessage == "injected input failure")
      assert(column.getRefCount == 1)
      assert(SpillFramework.stores.deviceStore.numHandles == handles)
    }
  }
}
