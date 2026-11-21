/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, Cuda, HostMemoryBuffer, PageableMemoryPool}

/** Selects staging and allocation policy without exposing it to copy callers. */
private[rapids] object CopyPipeline {
  def allocateHostDestination(bytes: Long): HostMemoryBuffer = {
    if (PageableMemoryPool.isDeviceToHostStagingEnabled) {
      HostAlloc.allocPageableForDeviceToHost(bytes)
    } else {
      HostMemoryBuffer.allocate(bytes)
    }
  }

  def tryAllocateHostDestination(bytes: Long): Option[HostMemoryBuffer] = {
    if (PageableMemoryPool.isDeviceToHostStagingEnabled) {
      HostAlloc.tryAllocPageableForDeviceToHost(bytes)
    } else {
      HostAlloc.tryAlloc(bytes)
    }
  }

  def allocateHostSource(bytes: Long): HostMemoryBuffer = {
    if (PageableMemoryPool.isHostToDeviceStagingEnabled && bytes > 0) {
      HostAlloc.alloc(bytes, preferPinned = false)
    } else {
      HostMemoryBuffer.allocate(bytes)
    }
  }

  private def stageToHost(dst: HostMemoryBuffer, dstOffset: Long,
      src: BaseDeviceMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Boolean = {
    if (PageableMemoryPool.isDeviceToHostStagingEnabled) {
      PageableMemoryPool.copyDeviceToPageableHost(dst, dstOffset, src, srcOffset, length, stream)
      true
    } else {
      false
    }
  }

  private def stageToDevice(dst: BaseDeviceMemoryBuffer, dstOffset: Long,
      src: HostMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Boolean = {
    if (PageableMemoryPool.isHostToDeviceStagingEnabled &&
        PageableMemoryPool.isPageablePoolBuffer(src)) {
      PageableMemoryPool.copyPageableHostToDevice(dst, dstOffset, src, srcOffset, length, stream)
      true
    } else {
      false
    }
  }

  /** Completes the copy before returning. */
  def copyToHost(dst: HostMemoryBuffer, dstOffset: Long,
      src: BaseDeviceMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Unit = {
    if (!stageToHost(dst, dstOffset, src, srcOffset, length, stream)) {
      dst.copyFromMemoryBuffer(dstOffset, src, srcOffset, length, stream)
    }
  }

  /**
   * Enqueues a direct copy on stream, or completes a staged copy before returning.
   * May block. Retain both buffers until stream synchronization, which is sufficient
   * for both paths: staged CPU work is already complete when this method returns.
   */
  def copyToHostAsync(dst: HostMemoryBuffer, dstOffset: Long,
      src: BaseDeviceMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Unit = {
    if (!stageToHost(dst, dstOffset, src, srcOffset, length, stream)) {
      dst.copyFromMemoryBufferAsync(dstOffset, src, srcOffset, length, stream)
    }
  }

  /** Completes the copy before returning. */
  def copyToDevice(dst: BaseDeviceMemoryBuffer, dstOffset: Long,
      src: HostMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Unit = {
    if (!stageToDevice(dst, dstOffset, src, srcOffset, length, stream)) {
      dst.copyFromHostBuffer(dstOffset, src, srcOffset, length, stream)
    }
  }

  /** Same completion and lifetime contract as copyToHostAsync. */
  def copyToDeviceAsync(dst: BaseDeviceMemoryBuffer, dstOffset: Long,
      src: HostMemoryBuffer, srcOffset: Long, length: Long, stream: Cuda.Stream): Unit = {
    if (!stageToDevice(dst, dstOffset, src, srcOffset, length, stream)) {
      dst.copyFromHostBufferAsync(dstOffset, src, srcOffset, length, stream)
    }
  }

  /** Retain the source and result until stream synchronization; staging may complete inline. */
  def copyToHostAsync(column: GpuColumnVector, stream: Cuda.Stream): RapidsHostColumnVector = {
    if (PageableMemoryPool.isDeviceToHostStagingEnabled) {
      new RapidsHostColumnVector(column.dataType, StagedHostCopy.copyToHost(column.getBase, stream))
    } else {
      column.copyToHostAsync(stream)
    }
  }
}
