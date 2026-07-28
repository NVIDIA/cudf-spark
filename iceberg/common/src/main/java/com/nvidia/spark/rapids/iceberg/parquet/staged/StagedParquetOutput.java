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
import java.util.concurrent.locks.StampedLock;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

/**
 * Exact-sized writable storage for one file's packed Parquet column chunks.
 *
 * <p>The output has a strict lifecycle: {@code WRITABLE -> SEALED -> CLOSED}. The owning worker
 * writes it with blocking vectored reads, cached-range copies, and scratch segment routing;
 * disjoint ranges may also be written concurrently. After its reads return, the worker seals the
 * output. The Spark task thread then obtains an independent host-buffer reference with
 * {@link #materialize()} before closing this output.</p>
 *
 * <p>Storage uses the same pinned-preferred host allocation policy as the base multithreaded
 * Parquet reader. HostAlloc falls back to bounded non-pinned memory when the pinned pool cannot
 * satisfy an allocation. The blocking-worker execution model bounds how many outputs are being
 * written at once, and zero-copy decode avoids retaining a second task-sized assembly buffer.
 * The planned fragment size is exact, so sealing never needs a second size argument.</p>
 */
abstract class StagedParquetOutput implements AutoCloseable {
  private final long exactSizeBytes;
  private final boolean diskBacked;
  // A read stamp is held around every write so seal and close (exclusive stamps) stay behind
  // all writers even if a caller ever writes concurrently from another thread.
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
   * Create one owned output for an exact packed-fragment size.
   *
   * <p>The allocation is pinned-preferred and blocking, matching the base reader's destination.
   * It falls back to bounded non-pinned storage and participates in the normal host retry/spill
   * protocol. The caller is a dedicated download worker, so waiting here is backpressure.</p>
   */
  static StagedParquetOutput create(long exactSizeBytes) throws IOException {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }
    HostMemoryBuffer allocation = HostAlloc$.MODULE$.alloc(exactSizeBytes, true);
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
   * Copy all cache-miss ranges for one file with one blocking vectored read, mirroring the base
   * multithreaded reader's synchronous {@code readVectored} call on its pool workers. The call
   * returns only when every byte has landed, so close() never races an in-flight writer on this
   * path.
   */
  final void copyRanges(
      RapidsInputFile input,
      List<PlannedReadRange> ranges) throws IOException {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(ranges, "ranges");
    if (ranges.isEmpty()) {
      return;
    }
    List<RapidsInputFile.CopyRange> copies = new ArrayList<>(ranges.size());
    for (PlannedReadRange range : ranges) {
      Objects.requireNonNull(range, "range");
      checkWriteBounds(range.getOutputOffset(), range.getLength());
      copies.add(new RapidsInputFile.CopyRange(
          range.getInputOffset(), range.getLength(), range.getOutputOffset()));
    }
    long writeStamp = beginConcurrentWrite();
    try {
      readVectoredStorage(input, copies);
    } finally {
      endConcurrentWrite(writeStamp);
    }
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
   * Perform one blocking vectored read whose destination is this output's backing store. Called
   * while a concurrent-write stamp is held; the read is terminal when this returns.
   */
  abstract void readVectoredStorage(
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
