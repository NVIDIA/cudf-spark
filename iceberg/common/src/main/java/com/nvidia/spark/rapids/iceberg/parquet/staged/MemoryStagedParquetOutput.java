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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Host-memory implementation of {@link StagedParquetOutput}.
 *
 * <p>The factory transfers ownership of {@code writableBuffer} to this object. The buffer is
 * writable only during I/O and combination. Sealing transfers it to a {@link SpillableHostBuffer}
 * so completed results waiting in the completion queue can participate in normal host spilling.
 * A materialized buffer is a separate, caller-owned reference.</p>
 */
final class MemoryStagedParquetOutput extends AbstractStagedParquetOutput {
  /** Owned only before seal; set to null when ownership moves to {@code sealedBuffer}. */
  private HostMemoryBuffer writableBuffer;

  /** Owned only after seal and responsible for spill-framework registration. */
  private SpillableHostBuffer sealedBuffer;

  MemoryStagedParquetOutput(HostMemoryBuffer writableBuffer, long capacityBytes) {
    super(capacityBytes);
    this.writableBuffer = Objects.requireNonNull(writableBuffer, "writableBuffer");
    long bufferLength = writableBuffer.getLength();
    if (bufferLength < capacityBytes) {
      writableBuffer.close();
      this.writableBuffer = null;
      throw new IllegalArgumentException(
          "host buffer length " + bufferLength +
              " is less than capacity " + capacityBytes);
    }
  }

  @Override
  public BackingStore backingStore() {
    return BackingStore.HOST_MEMORY;
  }

  @Override
  public void copyRanges(
      RapidsInputFile input,
      List<PlannedReadRange> ranges,
      int scratchBytes,
      RangeCopyObserver observer) throws IOException {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(ranges, "ranges");
    Objects.requireNonNull(observer, "observer");
    if (ranges.isEmpty()) {
      return;
    }

    beginConcurrentWrite();
    try {
      List<RapidsInputFile.CopyRange> copies = new ArrayList<>(ranges.size());
      for (PlannedReadRange range : ranges) {
        Objects.requireNonNull(range, "range");
        checkWriteBounds(range.getOutputOffset(), range.getLength());
        if (range.getInputOffset() < 0) {
          throw new IllegalArgumentException(
              "source offset must be non-negative: " + range.getInputOffset());
        }
        // Keep every column chunk distinct. PerfIO uses this list to issue the range GETs
        // concurrently and is responsible for any backend-specific request optimization.
        copies.add(new RapidsInputFile.CopyRange(
            range.getInputOffset(), range.getLength(), range.getOutputOffset()));
      }
      input.readVectored(writableBuffer, copies);
      for (PlannedReadRange range : ranges) {
        observer.rangeCopied(
            range, writableBuffer.slice(range.getOutputOffset(), range.getLength()));
      }
    } finally {
      endConcurrentWrite();
    }
  }

  @Override
  public void copyCachedRange(
      SeekableByteChannel channel,
      long outputOffset,
      long length,
      int scratchBytes) throws IOException {
    Objects.requireNonNull(channel, "channel");
    beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      long copied = 0L;
      while (copied < length) {
        int amount = (int) Math.min(length - copied, Integer.MAX_VALUE);
        ByteBuffer destination = writableBuffer.asByteBuffer(outputOffset + copied, amount);
        while (destination.hasRemaining()) {
          int read = channel.read(destination);
          if (read < 0) {
            throw new EOFException(
                "cached data range ended with " + destination.remaining() + " bytes remaining");
          }
          if (read == 0) {
            Thread.yield();
          }
        }
        copied += amount;
      }
    } finally {
      endConcurrentWrite();
    }
  }

  @Override
  public void writeBytes(
      long outputOffset,
      byte[] source,
      int sourceOffset,
      int length) throws IOException {
    Objects.requireNonNull(source, "source");
    if (sourceOffset < 0 || length < 0 || sourceOffset > source.length - length) {
      throw new IndexOutOfBoundsException(
          "source range [" + sourceOffset + ", " + (sourceOffset + length) +
              ") exceeds array length " + source.length);
    }
    beginExclusiveWrite();
    try {
      checkWriteBounds(outputOffset, length);
      writableBuffer.setBytes(outputOffset, source, sourceOffset, length);
    } finally {
      endExclusiveWrite();
    }
  }

  @Override
  public void seal(long actualSizeBytes) throws IOException {
    beginExclusiveWrite();
    try {
      beginSeal(actualSizeBytes);

      // SpillableHostBuffer.apply takes ownership even when construction throws. Clear the
      // writable reference before transfer so close() can never release the same buffer twice.
      HostMemoryBuffer toTransfer = writableBuffer;
      writableBuffer = null;
      try {
        sealedBuffer = SpillableHostBuffer.apply(
            toTransfer, actualSizeBytes, SpillPriorities.ACTIVE_BATCHING_PRIORITY());
        finishSeal();
      } catch (RuntimeException e) {
        throw asIOException("failed to register staged Parquet buffer for spilling", e);
      }
    } finally {
      endExclusiveWrite();
    }
  }

  @Override
  public HostMemoryBuffer materialize() throws IOException {
    beginSealedRead();
    try {
      return sealedBuffer.getDataHostBuffer();
    } catch (RuntimeException e) {
      throw asIOException("failed to materialize staged Parquet host buffer", e);
    } finally {
      endSealedRead();
    }
  }

  @Override
  public void close() {
    beginExclusiveClose();
    try {
      if (!beginClose()) {
        return;
      }
      if (sealedBuffer != null) {
        sealedBuffer.close();
        sealedBuffer = null;
      }
      if (writableBuffer != null) {
        writableBuffer.close();
        writableBuffer = null;
      }
    } finally {
      endExclusiveClose();
    }
  }
}
