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
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableHostBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.spill.SpillFramework$;

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
 * <p>The factory transfers ownership of {@code buffer} to this object. The buffer is a plain
 * non-pinned allocation while it is written; sealing transfers it to a {@link SpillableHostBuffer}
 * so completed results waiting in the completion queue can participate in normal host spilling.
 * A materialized buffer is a separate, caller-owned reference.</p>
 */
final class MemoryStagedParquetOutput extends StagedParquetOutput {
  /** Owned only before seal; set to null when ownership moves to {@code sealedBuffer}. */
  private HostMemoryBuffer buffer;

  /** Owned only after seal and responsible for spill-framework registration. */
  private SpillableHostBuffer sealedBuffer;

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
  void copyFromHostBufferStorage(
      long outputOffset,
      HostMemoryBuffer source,
      long sourceOffset,
      long length) {
    buffer.copyFromHostBuffer(outputOffset, source, sourceOffset, length);
  }

  @Override
  void sealStorage() throws IOException {
    if (SpillFramework$.MODULE$.storesInternal() == null) {
      // Executors always initialize the spill framework before any scan runs; only
      // spill-framework-less unit tests reach this branch, where the plain buffer stays owned
      // directly and materialize hands out slices of it.
      return;
    }
    // SpillableHostBuffer.apply takes ownership even when construction throws. Clear the
    // writable reference before transfer so close() can never release the same buffer twice.
    HostMemoryBuffer toTransfer = buffer;
    buffer = null;
    try {
      sealedBuffer = SpillableHostBuffer.apply(
          toTransfer, exactSizeBytes(), SpillPriorities.ACTIVE_BATCHING_PRIORITY());
    } catch (RuntimeException e) {
      throw new IOException("failed to register staged Parquet buffer for spilling", e);
    }
  }

  @Override
  HostMemoryBuffer materializeStorage() throws IOException {
    if (sealedBuffer == null) {
      return buffer.slice(0L, exactSizeBytes());
    }
    try {
      return sealedBuffer.getDataHostBuffer();
    } catch (RuntimeException e) {
      throw new IOException("failed to materialize staged Parquet host buffer", e);
    }
  }

  @Override
  void closeStorage() {
    if (sealedBuffer != null) {
      sealedBuffer.close();
      sealedBuffer = null;
    }
    if (buffer != null) {
      buffer.close();
      buffer = null;
    }
  }
}
