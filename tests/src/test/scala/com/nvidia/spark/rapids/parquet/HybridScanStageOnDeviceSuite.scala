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

package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmSparkRetrySuiteBase
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

class HybridScanStageOnDeviceSuite extends RmmSparkRetrySuiteBase {
  // Each buffer is its own host allocation, so a pack of CHUNK bytes holds exactly one of them.
  private val CHUNK = 64L

  private def filledChunks(count: Int): Array[HostMemoryBuffer] = {
    (0 until count).toArray.safeMap { c =>
      val buffer = HostMemoryBuffer.allocate(CHUNK)
      (0L until CHUNK).foreach(i => buffer.setByte(i, (c * CHUNK + i).toByte))
      buffer
    }
  }

  private def injectSplitOnNextGpuAlloc(): Unit =
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)

  private def assertStaged(chunks: Array[HostMemoryBuffer],
      device: Array[DeviceMemoryBuffer]): Unit = {
    assert(device.length == chunks.length)
    chunks.zip(device).foreach { case (expected, actual) =>
      assert(actual.getLength == CHUNK)
      withResource(HostMemoryBuffer.allocate(CHUNK)) { back =>
        back.copyFromDeviceBuffer(actual)
        (0L until CHUNK).foreach(i => assert(back.getByte(i) == expected.getByte(i)))
      }
    }
  }

  test("a split OOM while staging retries with half the pack size") {
    withResource(new Cuda.Event()) { event =>
      withResource(filledChunks(2)) { chunks =>
        RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1)
        injectSplitOnNextGpuAlloc()
        HybridScanPipeline.withStagedOnDevice(chunks, 2 * CHUNK, CHUNK, event) { device =>
          assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
          assertStaged(chunks, device)
        }
      }
    }
  }

  test("a split OOM while staging retries at the minimum when half would be below it") {
    withResource(new Cuda.Event()) { event =>
      withResource(filledChunks(3)) { chunks =>
        RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1)
        injectSplitOnNextGpuAlloc()
        // Half of the 3 * CHUNK bytes is below the 2 * CHUNK floor, so the retry uses the floor.
        HybridScanPipeline.withStagedOnDevice(chunks, 8 * CHUNK, 2 * CHUNK, event) { device =>
          assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
          assertStaged(chunks, device)
        }
      }
    }
  }

  test("a split OOM while staging at the minimum pack size fails") {
    withResource(new Cuda.Event()) { event =>
      withResource(filledChunks(2)) { chunks =>
        injectSplitOnNextGpuAlloc()
        assertThrows[GpuSplitAndRetryOOM] {
          HybridScanPipeline.withStagedOnDevice(chunks, CHUNK, CHUNK, event)(_ => ())
        }
      }
    }
  }

  test("an injected OOM reaches the work on the staged buffers") {
    withResource(new Cuda.Event()) { event =>
      withResource(filledChunks(2)) { chunks =>
        var calls = 0
        // Skip the one pack allocation, so the OOM hits the allocation in the work.
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 1)
        HybridScanPipeline.withStagedOnDevice(chunks, 2 * CHUNK, CHUNK, event) { _ =>
          calls += 1
          withResource(DeviceMemoryBuffer.allocate(1))(_ => ())
        }
        assert(calls == 2)
      }
    }
  }

  test("a split OOM in the work on the staged buffers fails without shrinking the pack") {
    withResource(new Cuda.Event()) { event =>
      withResource(filledChunks(2)) { chunks =>
        var calls = 0
        assertThrows[GpuSplitAndRetryOOM] {
          HybridScanPipeline.withStagedOnDevice(chunks, 2 * CHUNK, CHUNK, event) { _ =>
            calls += 1
            injectSplitOnNextGpuAlloc()
            withResource(DeviceMemoryBuffer.allocate(1))(_ => ())
          }
        }
        assert(calls == 1)
      }
    }
  }
}
