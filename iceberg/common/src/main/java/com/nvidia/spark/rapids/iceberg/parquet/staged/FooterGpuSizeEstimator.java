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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

/**
 * Estimates decoded output bytes for a filtered footer and row count.
 *
 * <p>The physical Parquet read schema can be smaller than the format's final output schema. For
 * example, Iceberg post-processing may synthesize evolved null columns, partition constants, or
 * metadata columns. Keeping this estimate at the adapter boundary lets planning enforce GPU batch
 * limits against the actual post-processed output while remaining reusable by raw Parquet and
 * Delta adapters.</p>
 *
 * @param <C> format-specific footer context
 */
@FunctionalInterface
public interface FooterGpuSizeEstimator<C> {
  /** Return a non-negative GPU-memory estimate in bytes. */
  long estimate(FooterResult<C> footer, long rowCount);
}
