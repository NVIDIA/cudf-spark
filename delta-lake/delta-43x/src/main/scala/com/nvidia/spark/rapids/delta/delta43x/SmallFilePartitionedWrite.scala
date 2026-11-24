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

package com.nvidia.spark.rapids.delta.delta43x

import com.nvidia.spark.rapids.RapidsMeta

/**
 * Shared fallback for writes that would put many small files in each task. Without optimized
 * write each task writes a file for every partition it holds, and the GPU Parquet writer's
 * per-file cost makes that slower than the CPU writer.
 */
private[delta43x] object SmallFilePartitionedWrite {
  def tagUnlessOptimizeWrite(
      meta: RapidsMeta[_, _, _],
      operation: String,
      optimizeWrite: Boolean): Unit = {
    if (!optimizeWrite) {
      meta.willNotWorkOnGpu(s"Delta 4.3 $operation: without optimized write each task writes " +
        "many small files, which is slower on GPU than on CPU. Enable optimized write " +
        "(spark.databricks.delta.optimizeWrite.enabled or the delta.autoOptimize.optimizeWrite " +
        "table property) to run on GPU")
    }
  }
}
