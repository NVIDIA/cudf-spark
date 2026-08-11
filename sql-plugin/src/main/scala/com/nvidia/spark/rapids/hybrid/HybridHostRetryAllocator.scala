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
package com.nvidia.spark.rapids.hybrid

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.hybrid.{HostMemoryAllocator => HybridMemoryAllocator}

/**
 * Allocator that has the memory allocation protected by the retry framework to handle
 * OOM errors. And it will always try pinned memory first by default.
 * Setting "tryPinned" to false to disable this behavior.
 *
 * This is designed to support retry things for the host allocation in hybrid scans.
 */
class HybridHostRetryAllocator(tryPinned: Boolean = true)
  extends HybridMemoryAllocator[HostBufferInfo] with HostRetryAllocator {

  override def allocate(bytesSizes: Seq[Long]): Seq[HostBufferInfo] = {
    closeOnExcept(allocWithRetry(bytesSizes, tryPinned))(_.map { ar =>
      HostBufferInfo(ar.buffer, ar.isPinned)
    })
  }
}
