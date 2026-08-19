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

package com.nvidia.spark.rapids.iceberg.parquet.async;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * Owned framed Parquet footer and the statistics collected while loading it.
 *
 * <p>The buffer layout is {@code MAGIC + footer + footerLength + MAGIC}. The asynchronous
 * planner owns this result and must close it after the shaded Parquet reader has parsed the
 * footer.</p>
 */
public final class FooterBufferResult implements AutoCloseable {
  private HostMemoryBuffer buffer;
  private final boolean cacheHit;
  private final long loadNanos;
  private final long remoteReadNanos;
  private final long requestCount;
  private final long requestedBytes;

  FooterBufferResult(
      HostMemoryBuffer buffer,
      boolean cacheHit,
      long loadNanos,
      long remoteReadNanos,
      long requestCount,
      long requestedBytes) {
    this.buffer = buffer;
    this.cacheHit = cacheHit;
    this.loadNanos = loadNanos;
    this.remoteReadNanos = remoteReadNanos;
    this.requestCount = requestCount;
    this.requestedBytes = requestedBytes;
  }

  public HostMemoryBuffer getBuffer() {
    if (buffer == null) {
      throw new IllegalStateException("footer buffer is closed");
    }
    return buffer;
  }

  public boolean isCacheHit() {
    return cacheHit;
  }

  public long getBufferSize() {
    return getBuffer().getLength();
  }

  public long getLoadNanos() {
    return loadNanos;
  }

  public long getRemoteReadNanos() {
    return remoteReadNanos;
  }

  public long getRequestCount() {
    return requestCount;
  }

  public long getRequestedBytes() {
    return requestedBytes;
  }

  @Override
  public void close() {
    if (buffer != null) {
      buffer.close();
      buffer = null;
    }
  }
}
