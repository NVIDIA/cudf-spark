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

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Host-memory implementation of {@link StagedParquetOutput}.
 *
 * <p>The factory transfers ownership of {@code buffer} to this object. The buffer stays a plain
 * non-pinned allocation for its whole staged lifetime: the blocking-worker execution model keeps
 * the number of sealed-but-unclaimed outputs bounded by the shared pool size, and the decode path
 * re-wraps the materialized reference as spillable for the retry framework. A materialized buffer
 * is a separate, caller-owned reference over the same allocation.</p>
 */
final class MemoryStagedParquetOutput extends StagedParquetOutput {
  private HostMemoryBuffer buffer;

  MemoryStagedParquetOutput(HostMemoryBuffer buffer, long capacityBytes) {
    super(capacityBytes, false);
    this.buffer = Objects.requireNonNull(buffer, "buffer");
    long bufferLength = buffer.getLength();
    if (bufferLength < capacityBytes) {
      buffer.close();
      this.buffer = null;
      throw new IllegalArgumentException(
          "host buffer length " + bufferLength +
              " is less than capacity " + capacityBytes);
    }
  }

  @Override
  CompletableFuture<Void> submitVectoredRead(
      RapidsInputFile input,
      List<RapidsInputFile.CopyRange> copies) {
    return input.readVectoredAsync(buffer, copies);
  }

  @Override
  void copyCachedRangeStorage(
      SeekableByteChannel source,
      long outputOffset,
      long length) throws IOException {
    long copied = 0L;
    while (copied < length) {
      int amount = (int) Math.min(length - copied, Integer.MAX_VALUE);
      ByteBuffer destination = buffer.asByteBuffer(outputOffset + copied, amount);
      while (destination.hasRemaining()) {
        int read = source.read(destination);
        if (read < 0) {
          throw new EOFException(
              "cached data range ended with " + destination.remaining() +
                  " bytes remaining");
        }
        if (read == 0) {
          Thread.yield();
        }
      }
      copied += amount;
    }
  }

  @Override
  HostMemoryBuffer sliceForCacheStorage(long outputOffset, long length) {
    return buffer.slice(outputOffset, length);
  }

  @Override
  void writeBytesStorage(long outputOffset, byte[] source, int sourceOffset, int length) {
    buffer.setBytes(outputOffset, source, sourceOffset, length);
  }

  @Override
  void sealStorage() {
    // Sealing is purely a lifecycle transition: the exact-sized buffer already holds the final
    // synthetic file, and the parked worker hands it over unchanged at claim time.
  }

  @Override
  HostMemoryBuffer materializeStorage() {
    return buffer.slice(0L, exactSizeBytes());
  }

  @Override
  void closeStorage() {
    if (buffer != null) {
      buffer.close();
      buffer = null;
    }
  }
}
