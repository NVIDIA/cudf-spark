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

import java.util.Objects;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * Borrowed, fully assembled synthetic Parquet input backed by one reusable pool slot.
 *
 * <p>Remote data is already committed to the local file cache before a shared worker fills this
 * buffer. The Spark task receives the input only after the header, selected encoded chunks, and
 * relocated footer are complete. Each RMM retry obtains a fresh owning slice, while the pool slot
 * itself remains leased until the eagerly drained cuDF producer no longer references the input.</p>
 */
public final class StagedParquetInput implements AutoCloseable {
  private final long length;
  private AssemblyBufferPool.Lease lease;

  StagedParquetInput(long length, AssemblyBufferPool.Lease lease) {
    if (length <= 0) {
      throw new IllegalArgumentException("length must be positive: " + length);
    }
    this.length = length;
    this.lease = Objects.requireNonNull(lease, "lease");
  }

  /** Return one fresh owning buffer array for a cuDF retry attempt. */
  public synchronized HostMemoryBuffer[] materialize() {
    if (lease == null) {
      throw new IllegalStateException("staged Parquet input is closed");
    }
    return new HostMemoryBuffer[] {lease.materialize(length)};
  }

  @Override
  public synchronized void close() {
    if (lease != null) {
      lease.close();
      lease = null;
    }
  }
}
