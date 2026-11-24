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

import ai.rapids.cudf.Cuda
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType

class ShuffleReceivedBufferCatalogSuite extends AnyFunSuite with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    try {
      SpillFramework.shutdown()
    } finally {
      super.afterEach()
    }
  }

  private def verifyReceivedCompressedBatch(codecName: String, spillFirst: Boolean): Unit = {
    val codec = TableCompressionCodec.getCodec(codecName,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    val catalog = new ShuffleReceivedBufferCatalog
    withResource(codec.createBatchCompressor(16 * 1024 * 1024, Cuda.DEFAULT_STREAM)) {
        compressor =>
      compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(1000))
      withResource(compressor.finish()) { compressed =>
        val source = compressed.head
        val copied = source.buffer.sliceWithCopy(0, source.buffer.getLength)
        val handle = catalog.addBuffer(copied, source.meta, -1)
        if (spillFirst) {
          assertResult(copied.getLength)(
            SpillFramework.stores.deviceStore.spill(copied.getLength))
        }
        val (received, bytesUsed) =
          catalog.getColumnarBatchAndRemove(handle, Array(IntegerType))
        withResource(received) { batch =>
          assert(GpuCompressedColumnVector.isBatchCompressed(batch))
          assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
          assertResult(1000)(batch.numRows())
          assertResult(source.buffer.getLength)(bytesUsed)
          val column = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
          assertResult(1)(column.getTableBuffer.getRefCount)
          // Decode with the received metadata, as the post-shuffle coalesce does.
          val meta = column.getTableMeta
          assertResult(codec.codecId)(meta.bufferMeta.codecBufferDescrs(0).codec)
          withResource(codec.createBatchDecompressor(1024 * 1024, Cuda.DEFAULT_STREAM)) {
              decompressor =>
            column.getTableBuffer.incRefCount()
            decompressor.addBufferToDecompress(
              column.getTableBuffer, meta.bufferMeta())
            withResource(decompressor.finishAsync()) { output =>
              withResource(MetaUtils.getTableFromMeta(output.head, meta)) { table =>
                withResource(table.getColumn(0).copyToHost()) { actual =>
                  assertResult(1000)(actual.getRowCount)
                  assert((0 until 1000).forall(index => actual.getInt(index) == index))
                }
              }
            }
          }
        }
      }
    }
  }

  for (codecName <- Seq("copy", "lz4", "zstd"); spillFirst <- Seq(false, true)) {
    test(s"received $codecName payload stays compressed, spilled first: $spillFirst") {
      verifyReceivedCompressedBatch(codecName, spillFirst)
    }
  }

  test("received uncompressed payload remains uncompressed") {
    val catalog = new ShuffleReceivedBufferCatalog
    withResource(RapidsShuffleTestHelper.buildContiguousTable(1000)) { source =>
      val meta = MetaUtils.buildTableMeta(0, source)
      val copied = source.getBuffer.sliceWithCopy(0, source.getBuffer.getLength)
      val handle = catalog.addBuffer(copied, meta, -1)
      val (received, bytesUsed) =
        catalog.getColumnarBatchAndRemove(handle, Array(IntegerType))
      withResource(received) { batch =>
        assert(!GpuCompressedColumnVector.isBatchCompressed(batch))
        assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
        assertResult(1000)(batch.numRows())
        assertResult(source.getBuffer.getLength)(bytesUsed)
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { actual =>
          assert((0 until 1000).forall(index => actual.getInt(index) == index))
        }
      }
      assertResult(0)(copied.getRefCount)
    }
  }
}
