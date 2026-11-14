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

import scala.util.Try

import ai.rapids.cudf.{Cuda, Rmm}
import ai.rapids.cudf.nvcomp.BatchedDecompressor
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.CodecType
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper

import org.apache.spark.SparkConf

class BatchedNvcompDecompressorSuite extends RmmSparkRetrySuiteBase {
  private val clue = "(what finishAsync did, references each input gained, device bytes left)"

  /**
   * The class of a failed finishAsync's error, then of each exception suppressed in it, such as
   * a second close of a buffer, and "in cuDF" when cuDF's decompress threw it.
   */
  private def describe(error: Throwable): String = {
    val classes = (error +: error.getSuppressed).map(_.getClass.getSimpleName).mkString("+")
    val inCudf = error.getStackTrace.exists(
      _.getClassName == classOf[BatchedDecompressor].getName)
    if (inCudf) s"$classes in cuDF" else classes
  }

  /**
   * Decompresses `numInputs` buffers compressed with `codecId`, in one batch. With `oomSkip`, a
   * retry OOM fails the device allocation that follows the first `oomSkip` of them. Returns what
   * finishAsync did, the references each input gained, and the device bytes left allocated.
   */
  private def decompress(
      codecId: Byte,
      numInputs: Int,
      oomSkip: Option[Int]): (String, List[Int], Long) = {
    val codec = TableCompressionCodec.getCodec(codecId,
      TableCompressionCodec.makeCodecConfig(new RapidsConf(new SparkConf)))
    withResource(codec.createBatchCompressor(0, Cuda.DEFAULT_STREAM)) { compressor =>
      (0 until numInputs).foreach { _ =>
        compressor.addTableToCompress(RapidsShuffleTestHelper.buildContiguousTable(1000))
      }
      withResource(compressor.finish()) { tables =>
        val inputs = tables.map(_.buffer)
        val refsBefore = inputs.map(_.getRefCount)
        val bytesBefore = Rmm.getTotalBytesAllocated
        val outcome = withResource(codec.createBatchDecompressor(Long.MaxValue,
            Cuda.DEFAULT_STREAM)) { decompressor =>
          tables.foreach { table =>
            // the decompressor closes the buffer it is given
            table.buffer.incRefCount()
            decompressor.addBufferToDecompress(table.buffer, table.meta.bufferMeta)
          }
          oomSkip.foreach { skip =>
            RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
              RmmSpark.OomInjectionType.GPU.ordinal, skip)
          }
          Try(decompressor.finishAsync()).fold(describe,
            outputs => withResource(outputs)(_ => "ok"))
        }
        val bytesLeft = Rmm.getTotalBytesAllocated - bytesBefore
        val gained = inputs.zip(refsBefore).map { case (input, before) =>
          input.getRefCount - before
        }
        // A reference that a leak left has no owner to release it. Releasing it here makes a
        // test that finds a leak fail on its assertion, and lets RMM shut down after it.
        inputs.zip(gained).flatMap { case (input, n) => Seq.fill(n)(input) }.safeClose()
        (outcome, gained.toList, bytesLeft)
      }
    }
  }

  for ((name, codecId) <- Seq("LZ4" -> CodecType.NVCOMP_LZ4, "ZSTD" -> CodecType.NVCOMP_ZSTD)) {
    test(s"the $name decompressor leaves its input's count unchanged when nothing fails") {
      assertResult(("ok", List(0), 0L), clue)(decompress(codecId, numInputs = 1, oomSkip = None))
    }

    test(s"an OOM in the $name decompressor's output allocation leaves its input's count " +
        "unchanged") {
      assertResult(("GpuRetryOOM", List(0), 0L), clue)(
        decompress(codecId, numInputs = 1, oomSkip = Some(0)))
    }

    test(s"an OOM in cuDF's decompress frees the $name decompressor's output and leaves its " +
        "input's count unchanged") {
      // one input's output is the decompressor's only allocation, so the next one is cuDF's
      assertResult(("GpuRetryOOM in cuDF", List(0), 0L), clue)(
        decompress(codecId, numInputs = 1, oomSkip = Some(1)))
    }
  }

  test("an OOM in the LZ4 decompressor's second output allocation frees the first output and " +
      "leaves each input's count unchanged") {
    // the LZ4 decompressor allocates an output per input, where ZSTD slices one allocation
    assertResult(("GpuRetryOOM", List(0, 0), 0L), clue)(
      decompress(CodecType.NVCOMP_LZ4, numInputs = 2, oomSkip = Some(1)))
  }

  test("an OOM in cuDF's decompress frees both of the ZSTD decompressor's outputs and leaves " +
      "each input's count unchanged") {
    // the two outputs are slices of one allocation, so the next allocation is cuDF's
    assertResult(("GpuRetryOOM in cuDF", List(0, 0), 0L), clue)(
      decompress(CodecType.NVCOMP_ZSTD, numInputs = 2, oomSkip = Some(1)))
  }

  test("an OOM in cuDF's decompress frees both of the LZ4 decompressor's outputs and leaves " +
      "each input's count unchanged") {
    // the two outputs take the first two allocations, so the third is cuDF's
    assertResult(("GpuRetryOOM in cuDF", List(0, 0), 0L), clue)(
      decompress(CodecType.NVCOMP_LZ4, numInputs = 2, oomSkip = Some(2)))
  }
}
