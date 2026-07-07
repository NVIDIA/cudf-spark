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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Executor-local-file implementation of {@link StagedParquetOutput}.
 *
 * <p>This is selected only when the bounded exact host allocation cannot make progress. The local
 * file is exposed as a writable memory-mapped {@link HostMemoryBuffer}, which lets one vectored
 * call retain every Parquet column chunk as a distinct request and write directly to its final
 * file offset. The mapping is pageable local-file storage rather than a full pinned/pageable
 * HostAlloc allocation. This object owns the mapping, random-access handle, and path.</p>
 */
final class FileStagedParquetOutput extends AbstractStagedParquetOutput {
  private final Path path;
  private RandomAccessFile file;
  private FileChannel channel;
  private HostMemoryBuffer mappedBuffer;

  FileStagedParquetOutput(Path path, long capacityBytes) throws IOException {
    super(capacityBytes);
    this.path = Objects.requireNonNull(path, "path");
    boolean succeeded = false;
    try {
      file = new RandomAccessFile(path.toFile(), "rw");
      file.setLength(capacityBytes);
      channel = file.getChannel();
      mappedBuffer = HostMemoryBuffer.mapFile(
          path.toFile(), FileChannel.MapMode.READ_WRITE, 0L, capacityBytes);
      succeeded = true;
    } finally {
      if (!succeeded) {
        if (file != null) {
          file.close();
          file = null;
        }
        Files.deleteIfExists(path);
      }
    }
  }

  @Override
  public BackingStore backingStore() {
    return BackingStore.LOCAL_FILE;
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
    if (scratchBytes <= 0) {
      throw new IllegalArgumentException(
          "positive scratchBytes are required for file-backed staged I/O");
    }

    beginConcurrentWrite();
    try {
      List<RapidsInputFile.CopyRange> copies = new java.util.ArrayList<>(ranges.size());
      for (PlannedReadRange range : ranges) {
        Objects.requireNonNull(range, "range");
        checkWriteBounds(range.outputOffset(), range.length());
        if (range.sourceOffset() < 0) {
          throw new IllegalArgumentException(
              "source offset must be non-negative: " + range.sourceOffset());
        }
        copies.add(new RapidsInputFile.CopyRange(
            range.sourceOffset(), range.length(), range.outputOffset()));
      }

      // PerfIO schedules the CopyRanges concurrently. Keep one range per column chunk and make a
      // single call for this source; the mapped output avoids needing an aggregate scratch HMB.
      input.readVectored(mappedBuffer, copies);
      for (PlannedReadRange range : ranges) {
        observer.rangeCopied(
            range, mappedBuffer.slice(range.outputOffset(), range.length()));
      }
    } finally {
      endConcurrentWrite();
    }
  }

  @Override
  public void copyCachedRange(
      SeekableByteChannel source,
      long outputOffset,
      long length,
      int scratchBytes) throws IOException {
    Objects.requireNonNull(source, "source");
    if (scratchBytes <= 0) {
      throw new IllegalArgumentException("scratchBytes must be positive: " + scratchBytes);
    }
    beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      long copied = 0L;
      while (copied < length) {
        int amount = (int) Math.min(length - copied, Integer.MAX_VALUE);
        java.nio.ByteBuffer destination =
            mappedBuffer.asByteBuffer(outputOffset + copied, amount);
        while (destination.hasRemaining()) {
          int read = source.read(destination);
          if (read < 0) {
            throw new EOFException(
                "cached data range ended with " + (length - copied) + " bytes remaining");
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
      mappedBuffer.setBytes(outputOffset, source, sourceOffset, length);
    } finally {
      endExclusiveWrite();
    }
  }

  @Override
  public void seal(long actualSizeBytes) throws IOException {
    beginExclusiveWrite();
    try {
      beginSeal(actualSizeBytes);
      // GPU decode consumes an owning slice of this live mapping; the task-owned file is never a
      // durable result, so forcing dirty pages to disk here would only add a full-subtask barrier.
      finishSeal();
    } finally {
      endExclusiveWrite();
    }
  }

  @Override
  public HostMemoryBuffer materialize() throws IOException {
    beginSealedRead();
    try {
      // A slice retains the mapping independently. EMR runs on Linux, where the task-owned path can
      // be unlinked by result.close while this reference remains valid through GPU decode.
      return mappedBuffer.slice(0L, sealedSizeBytes());
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
      Throwable failure = null;
      try {
        if (mappedBuffer != null) {
          mappedBuffer.close();
        }
      } catch (Throwable error) {
        failure = error;
      }
      try {
        if (channel != null) {
          channel.close();
        }
      } catch (IOException ignored) {
        // Best effort; RandomAccessFile.close below gets another chance to release the descriptor.
      }
      try {
        if (file != null) {
          file.close();
        }
      } catch (IOException ignored) {
        // Cleanup is best effort and close must remain safe from task-completion listeners.
      }
      mappedBuffer = null;
      channel = null;
      file = null;
      try {
        Files.deleteIfExists(path);
      } catch (IOException ignored) {
        // Spark executor cleanup will remove its local directory if immediate deletion fails.
      }
      if (failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
    } finally {
      endExclusiveClose();
    }
  }
}
