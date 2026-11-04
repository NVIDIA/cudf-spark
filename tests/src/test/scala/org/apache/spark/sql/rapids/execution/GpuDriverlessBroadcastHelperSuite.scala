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

import ai.rapids.cudf.{ColumnVector, Cuda, Rmm, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ByteType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuDriverlessBroadcastHelperSuite extends RmmSparkRetrySuiteBase {
  private val codecConfigs = TableCompressionCodec.makeCodecConfig(
    new RapidsConf(Map.empty[String, String]))
  private val schema = StructType(Seq(StructField("value", ByteType)))
  private val output = Seq(AttributeReference("value", ByteType)())
  private val metrics: Map[String, GpuMetric] = Map(
    GpuMetric.CONCAT_TIME -> NoopMetric, GpuMetric.OP_TIME_LEGACY -> NoopMetric)

  private def batch(column: ColumnVector): ColumnarBatch =
    new ColumnarBatch(Array(GpuColumnVector.from(column.incRefCount(), ByteType)),
      column.getRowCount.toInt)

  test("a spilled cached build retries GPU allocation without rebuilding its input") {
    val allocated = Rmm.getTotalBytesAllocated
    withResource(new ExecutorBroadcastCache[SpillableColumnarBatch](_.close())) { cache =>
      var builds = 0
      def read(): ColumnarBatch = cache.getOrBuild[ColumnarBatch](42, () => {
        builds += 1
        withResource(ColumnVector.fromBytes(1.toByte, 2.toByte)) { column =>
          SpillableColumnarBatch(batch(column), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
      }, () => (), () => ())(
        GpuDriverlessBroadcastHelper.borrow, result => withResource(result)(_ => ()))
      withResource(read()) { result => assert(result.numRows() == 2) }
      assert(SpillFramework.stores.deviceStore.spill(Long.MaxValue) > 0)
      RmmSpark.getAndResetNumRetryThrow(1)
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 2,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(read()) { result =>
        assert(result.numRows() == 2)
        withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
          assert(host.getByte(0) == 1 && host.getByte(1) == 2)
        }
      }
      assert(RmmSpark.getAndResetNumRetryThrow(1) >= 2)
      assert(builds == 1)
      cache.invalidate(42)
      assert(cache.activeEntryCount == 0)
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("a single input keeps its data and releases borrowed references") {
    withResource(ColumnVector.fromBytes(1.toByte, 2.toByte)) { column =>
      withResource(GpuDriverlessBroadcastHelper.assemble(
          Iterator(batch(column)), schema, output, metrics, codecConfigs,
          Long.MaxValue)) { result =>
        assert(result.numRows() == 2)
        withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
          assert(host.getByte(0) == 1 && host.getByte(1) == 2)
        }
      }
      assert(column.getRefCount == 1)
    }
  }

  test("concatenation preserves nulls when its layout grows beyond input bytes") {
    val allocated = Rmm.getTotalBytesAllocated
    Seq(1, 511, 512, 513, 32768).foreach { rows =>
      withResource(ColumnVector.fromBytes(Array.fill[Byte](rows)(1): _*)) { nonNull =>
        withResource(ColumnVector.fromBoxedBytes(null.asInstanceOf[java.lang.Byte])) { nullable =>
          val inputBytes = nonNull.getDeviceMemorySize + nullable.getDeviceMemorySize
          withResource(GpuDriverlessBroadcastHelper.assemble(
              Iterator(nonNull, nullable).map(batch), schema, output, metrics,
              codecConfigs, Long.MaxValue)) { result =>
            val actualBytes = GpuColumnVector.getTotalDeviceMemoryUsed(result)
            assert(result.numRows() == rows + 1)
            if (rows == 32768) {
              info(s"received bytes=$inputBytes, assembled bytes=$actualBytes")
              assert(actualBytes > inputBytes, "This case must exercise concatenation growth")
              withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
                assert(host.getByte(0) == 1 && host.isNull(rows))
              }
            }
          }
          assert(nonNull.getRefCount == 1 && nullable.getRefCount == 1)
        }
      }
    }
    assert(Rmm.getTotalBytesAllocated == allocated)
  }

  test("string concatenation preserves offsets and nulls") {
    val allocated = Rmm.getTotalBytesAllocated
    val strings = StructType(Seq(StructField("value", StringType)))
    val attributes = Seq(AttributeReference("value", StringType)())
    withResource(ColumnVector.fromStrings("abc", "long string", "")) { first =>
      withResource(ColumnVector.fromStrings(null.asInstanceOf[String])) { second =>
        val input = Iterator(first, second).map { column =>
          new ColumnarBatch(Array(GpuColumnVector.from(column.incRefCount(), StringType)),
            column.getRowCount.toInt)
        }
        withResource(GpuDriverlessBroadcastHelper.assemble(
          input, strings, attributes, metrics, codecConfigs, Long.MaxValue)) {
          result =>
            assert(result.numRows() == 4)
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
        GpuDriverlessBroadcastHelper.assemble(
          input, schema, output, metrics, codecConfigs, Long.MaxValue)
      }
      assert(error.getMessage == "injected input failure")
      assert(column.getRefCount == 1)
      assert(SpillFramework.stores.deviceStore.numHandles == handles)
    }
  }

  private def compressedBatch(value: Byte, rows: Int, codecName: String): ColumnarBatch = {
    val codec = TableCompressionCodec.getCodec(codecName, codecConfigs)
    withResource(ColumnVector.fromBytes(Array.fill[Byte](rows)(value): _*)) { column =>
      withResource(new Table(column)) { table =>
        withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
          compressor.addTableToCompress(table.contiguousSplit()(0))
          withResource(compressor.finish()) { tables =>
            GpuCompressedColumnVector.from(tables.head)
          }
        }
      }
    }
  }

  Seq("copy", "lz4").foreach { codec =>
    test(s"$codec build batches are decoded before concatenation") {
      val allocated = Rmm.getTotalBytesAllocated
      val rows = 100000
      val input = Iterator[Byte](7, 9).map { value =>
        val cb = compressedBatch(value, rows, codec)
        val cv = cb.column(0).asInstanceOf[GpuCompressedColumnVector]
        assert(cv.getTableMeta.bufferMeta().uncompressedSize() > 0)
        cb
      }
      withResource(GpuDriverlessBroadcastHelper.assemble(
          input, schema, output, metrics, codecConfigs, 1)) { result =>
        assert(result.numRows() == 2 * rows)
        assert(!GpuCompressedColumnVector.isBatchCompressed(result))
        withResource(GpuColumnVector.extractBases(result).head.copyToHost()) { host =>
          (0 until 2 * rows).foreach { i =>
            assert(host.getByte(i) == (if (i < rows) 7.toByte else 9.toByte))
          }
        }
      }
      assert(Rmm.getTotalBytesAllocated == allocated)
    }
  }

}
