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
 * Soft limit on the internal ("pass") memory of the chunked Parquet and ORC readers.
 *
 * The pass memory holds the compressed and decompressed pages of the row groups that are
 * decoded together. It can be neither spilled nor split, so a reader that runs out of memory
 * while decoding fails the task once retries are exhausted. It only needs to be about the size
 * of one output chunk (the target batch size): a smaller limit cuts the output into smaller
 * batches, while a larger one uses more GPU memory without making decode faster. On small GPU
 * pools the limit is further capped to a fraction of the pool so that several tasks can decode
 * at the same time.
 */
object ChunkedReaderMemoryLimit {
  /** Smallest limit handed to cudf. Below one row group cudf ignores the limit anyway. */
  val MIN_LIMIT_BYTES: Long = 256L * 1024 * 1024

  /** A single reader may use at most 1 / POOL_DIVISOR of the GPU pool for pass memory. */
  val POOL_DIVISOR: Long = 8

  /**
   * The limit requested for a reader, computed where the reader factory is built (usually the
   * driver, where the GPU pool size is not known). Returns 0 when the limit is disabled.
   */
  def requested(rapidsConf: RapidsConf, targetBatchSizeBytes: Long): Long = {
    if (rapidsConf.limitChunkedReaderMemoryUsage) targetBatchSizeBytes else 0L
  }

  /**
   * The limit to pass to cudf when a chunked reader is created on an executor: the requested
   * limit capped by the GPU pool. 0 (no limit) is passed through unchanged.
   */
  def forReader(requested: Long): Long = forReader(requested, GpuDeviceManager.getMemorySize)

  def forReader(requested: Long, poolSizeBytes: Long): Long = {
    if (requested <= 0) {
      0L
    } else if (poolSizeBytes <= 0) {
      math.max(MIN_LIMIT_BYTES, requested)
    } else {
      math.max(MIN_LIMIT_BYTES, math.min(requested, poolSizeBytes / POOL_DIVISOR))
    }
  }
}
