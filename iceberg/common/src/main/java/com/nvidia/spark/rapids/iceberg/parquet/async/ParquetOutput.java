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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.HostMemoryOutputStream;
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableHostBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import com.nvidia.spark.rapids.spill.SpillFramework$;
import org.apache.iceberg.aws.s3.IcebergS3InputFile;

/**
 * Exact-sized writable storage for one file's packed Parquet column chunks.
 *
 * <p>The output has a strict lifecycle: {@code WRITABLE -> SEALED -> CLOSED}. Cache copies run on
 * workers, while Iceberg S3 responses write directly into this buffer. After every response is
 * terminal, a worker seals the output. The Spark task thread then obtains an independent
 * host-buffer reference with {@link #materialize()} before closing this output.</p>
 *
 * <p>Storage uses the same pinned-preferred host allocation policy as the base multithreaded
 * Parquet reader. HostAlloc falls back to bounded non-pinned memory when the pinned pool cannot
 * satisfy an allocation. The blocking-worker execution model bounds how many outputs are being
 * written at once, and zero-copy decode avoids retaining a second task-sized assembly buffer.
 * The planned fragment size is exact, so sealing never needs a second size argument.</p>
 */
final class ParquetOutput implements AutoCloseable {
  private final long exactSizeBytes;
  // A read stamp is held around every write so seal and close (exclusive stamps) stay behind
  // all writers even if a caller ever writes concurrently from another thread.
  private final StampedLock operationLock = new StampedLock();
  /** Owned only before seal; set to null when ownership moves to {@code sealedBuffer}. */
  private HostMemoryBuffer buffer;
  /** Owned only after seal and responsible for spill-framework registration. */
  private SpillableHostBuffer sealedBuffer;
  private boolean sealed;
  private boolean closed;

  private ParquetOutput(HostMemoryBuffer buffer, long exactSizeBytes) {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }
    this.exactSizeBytes = exactSizeBytes;
    this.buffer = Objects.requireNonNull(buffer, "buffer");
    long bufferLength = buffer.getLength();
    if (bufferLength < exactSizeBytes) {
      buffer.close();
      this.buffer = null;
      throw new IllegalArgumentException(
          "host buffer length " + bufferLength +
              " is less than capacity " + exactSizeBytes);
    }
  }

  /**
   * Create one owned output for an exact packed-fragment size.
   *
   * <p>The allocation is pinned-preferred and blocking, matching the base reader's destination.
   * It falls back to bounded non-pinned storage and participates in the normal host retry/spill
 * protocol. Waiting for this allocation on a compute worker provides memory backpressure.</p>
   */
  static ParquetOutput create(long exactSizeBytes) throws IOException {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }
    HostMemoryBuffer allocation = HostAlloc$.MODULE$.alloc(exactSizeBytes, true);
    return new ParquetOutput(allocation, exactSizeBytes);
  }

  /**
   * Asynchronously copy all remote ranges into this output.
   *
   * <p>The Iceberg S3 path submits directly to the AWS async client and does not occupy
   * {@code fallbackExecutor}. Other input-file implementations retain the old blocking behavior
   * on that executor so local tests and non-S3 fallbacks continue to work without widening the
   * JNI {@link RapidsInputFile} API.</p>
   *
   * <p>A shared write stamp remains held until the returned future is terminal. Therefore
   * {@link #seal()} and {@link #close()} cannot race an S3 response that is still writing into the
   * host buffer.</p>
   */
  final CompletableFuture<Long> copyRangesAsync(
      RapidsInputFile input,
      List<RapidsInputFile.CopyRange> ranges,
      Executor fallbackExecutor,
      Consumer<RapidsInputFile.CopyRange> requestSucceeded) throws IOException {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(ranges, "ranges");
    Objects.requireNonNull(fallbackExecutor, "fallbackExecutor");
    Objects.requireNonNull(requestSucceeded, "requestSucceeded");
    if (ranges.isEmpty()) {
      return CompletableFuture.completedFuture(0L);
    }
    long expectedBytes = 0L;
    for (RapidsInputFile.CopyRange range : ranges) {
      Objects.requireNonNull(range, "range");
      checkWriteBounds(range.getOutputOffset(), range.getLength());
      expectedBytes = Math.addExact(expectedBytes, range.getLength());
    }

    long writeStamp = beginConcurrentWrite();
    CompletableFuture<Long> read;
    try {
      if (input instanceof IcebergS3InputFile) {
        read = ((IcebergS3InputFile) input).readVectoredAsync(
            buffer, ranges, requestSucceeded);
      } else {
        final long bytes = expectedBytes;
        read = CompletableFuture.supplyAsync(() -> {
          try {
            input.readVectored(buffer, ranges);
            ranges.forEach(requestSucceeded);
            return bytes;
          } catch (IOException error) {
            throw new CompletionException(error);
          }
        }, fallbackExecutor);
      }
    } catch (RuntimeException | Error error) {
      endConcurrentWrite(writeStamp);
      throw error;
    }
    return read.whenComplete((ignored, error) -> endConcurrentWrite(writeStamp));
  }

  /**
   * Copy one file's cached column chunks into the synthetic output.
   *
   * <p>The cache I/O pool submits one job per file, and that job deliberately reuses one
   * {@link HostMemoryOutputStream}. This matches the base reader's bulk-copy primitive without
   * constructing a stream or entering the output lifecycle lock once per column chunk.</p>
   */
  final void copyCachedRanges(
      List<ParquetDataReader.CachedRange> ranges) throws IOException {
    Objects.requireNonNull(ranges, "ranges");
    if (ranges.isEmpty()) {
      return;
    }
    long writeStamp = beginConcurrentWrite();
    try {
      // Use the base Parquet reader's cache-copy primitive verbatim. Besides avoiding a second
      // implementation of the channel-to-host loop, this keeps both readers on the same JITted
      // copy path and gives them identical EOF and large-range behavior.
      HostMemoryOutputStream output = new HostMemoryOutputStream(buffer);
      for (ParquetDataReader.CachedRange range : ranges) {
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(range.channel, "range.channel");
        checkWriteBounds(range.fragmentOffset, range.length);
        output.seek(range.fragmentOffset);
        output.copyFromChannel(range.channel, range.length);
      }
    } finally {
      endConcurrentWrite(writeStamp);
    }
  }

  /**
   * Return an owning view of a completed writable range for transfer to the data cache.
   *
   * <p>The returned slice retains its backing allocation independently. The caller owns it and
   * must close it or transfer ownership, even after this output is sealed or closed.</p>
   */
  final HostMemoryBuffer sliceForCache(long outputOffset, long length) throws IOException {
    long writeStamp = beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      return buffer.slice(outputOffset, length);
    } finally {
      endConcurrentWrite(writeStamp);
    }
  }

  /** Write combine-stage bytes, such as the synthetic Parquet header or footer. */
  final void writeBytes(
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
    long writeStamp = beginExclusiveWrite();
    try {
      checkWriteBounds(outputOffset, length);
      buffer.setBytes(outputOffset, source, sourceOffset, length);
    } finally {
      endExclusiveWrite(writeStamp);
    }
  }

  /** Write an entire byte array at an absolute synthetic-file offset. */
  final void writeBytes(long outputOffset, byte[] source) throws IOException {
    writeBytes(outputOffset, source, 0, source.length);
  }

  /** Seal the exact-sized output after every source writer is terminal. */
  final void seal() throws IOException {
    long writeStamp = beginExclusiveWrite();
    try {
      if (SpillFramework$.MODULE$.storesInternal() != null) {
        // SpillableHostBuffer transfers ownership only after it creates the spill handle. Keep
        // our reference until registration succeeds so close() still owns it on failure.
        HostMemoryBuffer toTransfer = buffer;
        try {
          sealedBuffer = SpillableHostBuffer.apply(
              toTransfer, exactSizeBytes, SpillPriorities.ACTIVE_BATCHING_PRIORITY());
          buffer = null;
        } catch (RuntimeException e) {
          throw new IOException("failed to register Parquet buffer for spilling", e);
        }
      }
      sealed = true;
    } finally {
      endExclusiveWrite(writeStamp);
    }
  }

  /**
   * Return a caller-owned host-buffer reference containing the sealed synthetic file.
   * The reference remains valid independently of this output's subsequent close.
   */
  final HostMemoryBuffer materialize() throws IOException {
    long stamp = operationLock.readLock();
    try {
      ensureSealed();
      if (sealedBuffer == null) {
        return buffer.slice(0L, exactSizeBytes);
      }
      try {
        return sealedBuffer.getDataHostBuffer();
      } catch (RuntimeException e) {
        throw new IOException("failed to materialize Parquet host buffer", e);
      }
    } finally {
      operationLock.unlockRead(stamp);
    }
  }

  /** Release the host allocation or local file. Closing is idempotent. */
  @Override
  public final void close() {
    long stamp = operationLock.writeLock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (sealedBuffer != null) {
        sealedBuffer.close();
        sealedBuffer = null;
      }
      if (buffer != null) {
        buffer.close();
        buffer = null;
      }
    } finally {
      operationLock.unlockWrite(stamp);
    }
  }

  /** Verify that an operation is permitted only before sealing. */
  private void ensureWritable() {
    ensureOpen();
    if (sealed) {
      throw new IllegalStateException("Parquet output is already sealed");
    }
  }

  /** Verify that an operation is permitted only after sealing. */
  private void ensureSealed() {
    ensureOpen();
    if (!sealed) {
      throw new IllegalStateException("Parquet output has not been sealed");
    }
  }

  /** Validate a write range against the exact synthetic-file size. */
  private void checkWriteBounds(long offset, long length) {
    if (offset < 0 || length < 0 || offset > exactSizeBytes - length) {
      throw new IndexOutOfBoundsException(
          "output range [" + offset + ", " + (offset + length) +
              ") exceeds size " + exactSizeBytes);
    }
  }

  /** Enter a data-copy operation; disjoint ranges may be written concurrently. */
  private long beginConcurrentWrite() {
    long stamp = operationLock.readLock();
    boolean succeeded = false;
    try {
      ensureWritable();
      succeeded = true;
      return stamp;
    } finally {
      if (!succeeded) {
        operationLock.unlockRead(stamp);
      }
    }
  }

  private void endConcurrentWrite(long stamp) {
    operationLock.unlockRead(stamp);
  }

  /** Enter a header/footer write or seal operation after all concurrent data writes finish. */
  private long beginExclusiveWrite() {
    long stamp = operationLock.writeLock();
    boolean succeeded = false;
    try {
      ensureWritable();
      succeeded = true;
      return stamp;
    } finally {
      if (!succeeded) {
        operationLock.unlockWrite(stamp);
      }
    }
  }

  private void endExclusiveWrite(long stamp) {
    operationLock.unlockWrite(stamp);
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Parquet output is closed");
    }
  }
}
