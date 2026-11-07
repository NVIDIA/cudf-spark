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

/**
 * Memory allocation kind for retry coverage tracking.
 */
object AllocationKind extends Enumeration {
  type AllocationKind = Value
  val HOST, DEVICE = Value
}

/**
 * This is a wrapper that turns a target size into an autocloseable to allow it to be used
 * in withRetry blocks.  It is intended to be used to help with cases where the split calculation
 * happens inside the retry block, and depends on the target size.  On a `GpuSplitAndRetryOOM` or
 * `CpuSplitAndRetryOOM`, a split policy like `splitTargetSizeInHalfGpu` or
 * `splitTargetSizeInHalfCpu` can be used to retry the block with a smaller target size.
 */
case class AutoCloseableTargetSize(targetSize: Long, minSize: Long,
    dataSize: Long = 0) extends AutoCloseable {
  def this(targetSize: Long, minSize: Long) = this(targetSize, minSize, 0)
  override def close(): Unit = ()
}

object SplitReason extends Enumeration {
  type SplitReason = Value
  val GPU_OOM, CPU_OOM, CUDF_OVERFLOW, NONE = Value
}
