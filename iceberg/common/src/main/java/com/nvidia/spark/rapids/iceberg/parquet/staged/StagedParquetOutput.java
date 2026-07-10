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

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

/**
 * Exact-sized writable storage for one synthetic Parquet file.
 *
 * <p>The output has a strict lifecycle: {@code WRITABLE -> SEALED -> CLOSED}. Async source
 * writers may concurrently copy disjoint ranges while it is writable. After the owning worker's
 * read barrier, it writes the synthetic header and footer and seals the output. The Spark task
 * thread then obtains an independent host-buffer reference with {@link #materialize()} before
 * closing this output.</p>
 *
 * <p>Storage is non-pinned host memory obtained with one blocking allocation that participates
 * in normal host spilling. The blocking-worker execution model bounds how many outputs exist at
 * once, and keeping staged bytes out of the pinned pool leaves it free for decode transfers and
 * device spill. The planned synthetic size is exact, so sealing never needs a second size
 * argument.</p>
 */
abstract class StagedParquetOutput implements AutoCloseable {
  private final long exactSizeBytes;
  private final boolean diskBacked;
  // A read stamp is held for the lifetime of every asynchronous source write. Unlike a
  // ReentrantReadWriteLock, StampedLock does not require the thread that acquired a stamp to
  // release it. That matters because a vectored-read future normally completes on an S3 client
  // thread rather than on the staged worker that submitted it.
  private final StampedLock operationLock = new StampedLock();
  private boolean sealed;
  private boolean closed;

  StagedParquetOutput(long exactSizeBytes, boolean diskBacked) {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }
    this.exactSizeBytes = exactSizeBytes;
    this.diskBacked = diskBacked;
  }

  /**
   * Create one owned output for an exact synthetic-file size.
   *
   * <p>The allocation is non-pinned and blocking: it spills and waits through the normal host
   * retry protocol instead of failing over to another backend. The caller is a dedicated subtask
   * worker whose whole pipeline is blocking, so waiting here is the intended backpressure.</p>
   */
  static StagedParquetOutput create(long exactSizeBytes) throws IOException {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }
    HostMemoryBuffer allocation = HostAlloc$.MODULE$.alloc(exactSizeBytes, false);
    return new MemoryStagedParquetOutput(allocation, exactSizeBytes);
  }

  /** Return the exact allocation and final sealed size to subclasses. */
  final long exactSizeBytes() {
    return exactSizeBytes;
  }

  /** Return whether this output uses an executor-local file. */
  final boolean isDiskBacked() {
    return diskBacked;
  }

  /**
   * Copy all cache-miss column chunks for one source with one vectored read.
   *
   * <p>Every planned range remains a distinct {@link RapidsInputFile.CopyRange}, preserving
   * PerfIO's per-column-chunk concurrency. Different source futures may write concurrently because
   * their output ranges are disjoint.</p>
   */
  final CompletableFuture<Void> copyRangesAsync(
      RapidsInputFile input,
      List<PlannedReadRange> ranges) {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(ranges, "ranges");
    if (ranges.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    try {
      List<RapidsInputFile.CopyRange> copies = new ArrayList<>(ranges.size());
      for (PlannedReadRange range : ranges) {
        Objects.requireNonNull(range, "range");
        checkWriteBounds(range.getOutputOffset(), range.getLength());
        copies.add(new RapidsInputFile.CopyRange(
            range.getInputOffset(), range.getLength(), range.getOutputOffset()));
      }
      long writeStamp = beginConcurrentWrite();
      CompletableFuture<Void> readFuture;
      try {
        readFuture = Objects.requireNonNull(
            submitVectoredRead(input, copies),
            "readVectoredAsync returned null");
      } catch (Throwable submissionError) {
        endConcurrentWrite(writeStamp);
        return failedFuture(submissionError);
      }

      CompletableFuture<Void> completion = new CompletableFuture<>();
      readFuture.whenComplete((ignored, readError) -> {
        Throwable failure = readError;
        try {
          endConcurrentWrite(writeStamp);
        } catch (Throwable closeError) {
          if (failure == null) {
            failure = closeError;
          } else if (failure != closeError) {
            failure.addSuppressed(closeError);
          }
        }
        if (failure == null) {
          completion.complete(null);
        } else {
          completion.completeExceptionally(failure);
        }
      });
      return completion;
    } catch (Throwable validationError) {
      return failedFuture(validationError);
    }
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  /** Copy one cached column chunk from its positioned channel into the synthetic output. */
  final void copyCachedRange(
      SeekableByteChannel source,
      long outputOffset,
      long length) throws IOException {
    Objects.requireNonNull(source, "source");
    long writeStamp = beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      copyCachedRangeStorage(source, outputOffset, length);
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
      return sliceForCacheStorage(outputOffset, length);
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
      writeBytesStorage(outputOffset, source, sourceOffset, length);
    } finally {
      endExclusiveWrite(writeStamp);
    }
  }

  /** Write an entire byte array at an absolute synthetic-file offset. */
  final void writeBytes(long outputOffset, byte[] source) throws IOException {
    writeBytes(outputOffset, source, 0, source.length);
  }

  /**
   * Copy bytes from a host buffer into a bounds-checked output range. Used by the owning worker
   * to route the useful segments of a gap-merged read from its scratch buffer into the packed
   * output; disjoint ranges may be written concurrently with other writers.
   */
  final void copyFromHostBuffer(
      long outputOffset,
      HostMemoryBuffer source,
      long sourceOffset,
      long length) throws IOException {
    Objects.requireNonNull(source, "source");
    long writeStamp = beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      copyFromHostBufferStorage(outputOffset, source, sourceOffset, length);
    } finally {
      endConcurrentWrite(writeStamp);
    }
  }

  /** Seal the exact-sized output after every source writer is terminal. */
  final void seal() throws IOException {
    long writeStamp = beginExclusiveWrite();
    try {
      sealStorage();
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
      return materializeStorage();
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
      closeStorage();
    } finally {
      operationLock.unlockWrite(stamp);
    }
  }

  /**
   * Submit one vectored read whose destination is this output's backing store. Called while a
   * concurrent-write stamp is held; the stamp is released when the returned future is terminal.
   */
  abstract CompletableFuture<Void> submitVectoredRead(
      RapidsInputFile input,
      List<RapidsInputFile.CopyRange> copies) throws IOException;

  /** Copy one bounds-checked cached range into the backing store under a write stamp. */
  abstract void copyCachedRangeStorage(
      SeekableByteChannel source,
      long outputOffset,
      long length) throws IOException;

  /** Create an owning bounds-checked view of a completed range for the data cache. */
  abstract HostMemoryBuffer sliceForCacheStorage(
      long outputOffset,
      long length) throws IOException;

  /** Write bounds-checked combine-stage bytes while the exclusive write stamp is held. */
  abstract void writeBytesStorage(
      long outputOffset,
      byte[] source,
      int sourceOffset,
      int length) throws IOException;

  /** Copy one bounds-checked host-buffer range into the backing store under a write stamp. */
  abstract void copyFromHostBufferStorage(
      long outputOffset,
      HostMemoryBuffer source,
      long sourceOffset,
      long length) throws IOException;

  /** Perform the backing-store transition while the exclusive lifecycle lock is held. */
  abstract void sealStorage() throws IOException;

  /** Create an owning sealed-buffer reference while close is excluded. */
  abstract HostMemoryBuffer materializeStorage() throws IOException;

  /** Release backing-store resources while the exclusive lifecycle lock is held. */
  abstract void closeStorage();

  /** Verify that an operation is permitted only before sealing. */
  private void ensureWritable() {
    ensureOpen();
    if (sealed) {
      throw new IllegalStateException("staged Parquet output is already sealed");
    }
  }

  /** Verify that an operation is permitted only after sealing. */
  private void ensureSealed() {
    ensureOpen();
    if (!sealed) {
      throw new IllegalStateException("staged Parquet output has not been sealed");
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
      throw new IllegalStateException("staged Parquet output is closed");
    }
  }
}
