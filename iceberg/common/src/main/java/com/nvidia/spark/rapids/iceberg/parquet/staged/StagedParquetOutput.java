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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.spark.SparkEnv;
import scala.Option;

/**
 * Exact-sized writable storage for one synthetic Parquet file.
 *
 * <p>The output has a strict lifecycle: {@code WRITABLE -> SEALED -> CLOSED}. Source workers may
 * concurrently copy disjoint ranges while it is writable. The last source worker writes the
 * synthetic header and footer and seals the output. The Spark task thread then obtains an
 * independent host-buffer reference with {@link #materialize()} before closing this output.</p>
 *
 * <p>This class also owns storage selection. It uses host memory when one non-blocking allocation
 * cycle succeeds and otherwise creates a memory-mapped file in an executor-local directory. The
 * planned synthetic size is exact, so sealing never needs a second size argument.</p>
 */
abstract class StagedParquetOutput implements AutoCloseable {
  private static final String SPARK_LOCAL_DIR = "spark.local.dir";
  private static final String SPARK_LOCAL_DIRS_ENV = "SPARK_LOCAL_DIRS";
  private static final AtomicInteger NEXT_LOCAL_DIRECTORY = new AtomicInteger();

  private final long exactSizeBytes;
  private final boolean diskBacked;
  private final ReentrantReadWriteLock operationLock = new ReentrantReadWriteLock();
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

  /** Create one owned memory- or file-backed output for an exact synthetic-file size. */
  static StagedParquetOutput create(
      long exactSizeBytes,
      long taskAttemptId,
      long subtaskId) throws IOException {
    if (exactSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "exactSizeBytes must be positive: " + exactSizeBytes);
    }

    Option<HostMemoryBuffer> allocation = HostAlloc$.MODULE$.tryAlloc2(exactSizeBytes, true);
    if (allocation.isDefined()) {
      return new MemoryStagedParquetOutput(allocation.get(), exactSizeBytes);
    }
    return new FileStagedParquetOutput(
        createLocalFile(taskAttemptId, subtaskId), exactSizeBytes);
  }

  /** Return the exact allocation and final sealed size to subclasses. */
  final long exactSizeBytes() {
    return exactSizeBytes;
  }

  /** Return whether this output uses an executor-local memory-mapped file. */
  final boolean isDiskBacked() {
    return diskBacked;
  }

  /** Return the live writable buffer. This is called only during a locked writable operation. */
  abstract HostMemoryBuffer writableBuffer();

  /**
   * Copy all cache-miss column chunks for one source with one vectored read.
   *
   * <p>Every planned range remains a distinct {@link RapidsInputFile.CopyRange}, preserving
   * PerfIO's per-column-chunk concurrency. Different source workers may call this concurrently
   * because their output ranges are disjoint.</p>
   */
  final void copyRanges(
      RapidsInputFile input,
      List<PlannedReadRange> ranges) throws IOException {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(ranges, "ranges");
    if (ranges.isEmpty()) {
      return;
    }

    beginConcurrentWrite();
    try {
      List<RapidsInputFile.CopyRange> copies = new ArrayList<>(ranges.size());
      for (PlannedReadRange range : ranges) {
        Objects.requireNonNull(range, "range");
        checkWriteBounds(range.getOutputOffset(), range.getLength());
        copies.add(new RapidsInputFile.CopyRange(
            range.getInputOffset(), range.getLength(), range.getOutputOffset()));
      }
      input.readVectored(writableBuffer(), copies);
    } finally {
      endConcurrentWrite();
    }
  }

  /** Copy one cached column chunk from its positioned channel into the synthetic output. */
  final void copyCachedRange(
      SeekableByteChannel source,
      long outputOffset,
      long length) throws IOException {
    Objects.requireNonNull(source, "source");
    beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      long copied = 0L;
      while (copied < length) {
        int amount = (int) Math.min(length - copied, Integer.MAX_VALUE);
        ByteBuffer destination = writableBuffer().asByteBuffer(outputOffset + copied, amount);
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
    } finally {
      endConcurrentWrite();
    }
  }

  /**
   * Return an owning view of a completed writable range for transfer to the data cache.
   *
   * <p>The returned slice retains its backing allocation independently. The caller owns it and
   * must close it or transfer ownership, even after this output is sealed or closed.</p>
   */
  final HostMemoryBuffer sliceForCache(long outputOffset, long length) {
    beginConcurrentWrite();
    try {
      checkWriteBounds(outputOffset, length);
      return writableBuffer().slice(outputOffset, length);
    } finally {
      endConcurrentWrite();
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
    beginExclusiveWrite();
    try {
      checkWriteBounds(outputOffset, length);
      writableBuffer().setBytes(outputOffset, source, sourceOffset, length);
    } finally {
      endExclusiveWrite();
    }
  }

  /** Write an entire byte array at an absolute synthetic-file offset. */
  final void writeBytes(long outputOffset, byte[] source) throws IOException {
    writeBytes(outputOffset, source, 0, source.length);
  }

  /** Seal the exact-sized output after every source writer is terminal. */
  final void seal() throws IOException {
    beginExclusiveWrite();
    try {
      sealStorage();
      sealed = true;
    } finally {
      endExclusiveWrite();
    }
  }

  /**
   * Return a caller-owned host-buffer reference containing the sealed synthetic file.
   * The reference remains valid independently of this output's subsequent close.
   */
  final HostMemoryBuffer materialize() throws IOException {
    Lock lock = operationLock.readLock();
    lock.lock();
    try {
      ensureSealed();
      return materializeStorage();
    } finally {
      lock.unlock();
    }
  }

  /** Release the host allocation or local file. Closing is idempotent. */
  @Override
  public final void close() {
    Lock lock = operationLock.writeLock();
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      closeStorage();
    } finally {
      lock.unlock();
    }
  }

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
  private void beginConcurrentWrite() {
    Lock lock = operationLock.readLock();
    lock.lock();
    boolean succeeded = false;
    try {
      ensureWritable();
      succeeded = true;
    } finally {
      if (!succeeded) {
        lock.unlock();
      }
    }
  }

  private void endConcurrentWrite() {
    operationLock.readLock().unlock();
  }

  /** Enter a header/footer write or seal operation after all concurrent data writes finish. */
  private void beginExclusiveWrite() {
    Lock lock = operationLock.writeLock();
    lock.lock();
    boolean succeeded = false;
    try {
      ensureWritable();
      succeeded = true;
    } finally {
      if (!succeeded) {
        lock.unlock();
      }
    }
  }

  private void endExclusiveWrite() {
    operationLock.writeLock().unlock();
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("staged Parquet output is closed");
    }
  }

  private static Path createLocalFile(long taskAttemptId, long subtaskId) throws IOException {
    String prefix = "rapids-iceberg-staged-" + taskAttemptId + "-" + subtaskId + "-";
    IOException failure = null;
    List<Path> directories = candidateDirectories();
    if (directories.isEmpty()) {
      throw new IOException("no Spark or JVM local directory is configured");
    }
    int start = Math.floorMod(NEXT_LOCAL_DIRECTORY.getAndIncrement(), directories.size());
    for (int index = 0; index < directories.size(); index++) {
      Path directory = directories.get((start + index) % directories.size());
      try {
        Files.createDirectories(directory);
        return Files.createTempFile(directory, prefix, ".parquet");
      } catch (IOException | RuntimeException e) {
        IOException current = e instanceof IOException
            ? (IOException) e
            : new IOException("invalid Spark local directory " + directory, e);
        if (failure == null) {
          failure = current;
        } else {
          failure.addSuppressed(current);
        }
      }
    }
    throw new IOException("unable to create a local staged Parquet file", failure);
  }

  private static List<Path> candidateDirectories() {
    Set<String> candidates = new LinkedHashSet<>();
    addDirectories(candidates, System.getenv(SPARK_LOCAL_DIRS_ENV));

    SparkEnv sparkEnv = SparkEnv.get();
    if (sparkEnv != null) {
      addDirectories(candidates,
          sparkEnv.conf().get(SPARK_LOCAL_DIR, System.getProperty("java.io.tmpdir")));
    }
    addDirectories(candidates, System.getProperty("java.io.tmpdir"));

    List<Path> paths = new ArrayList<>(candidates.size());
    for (String candidate : candidates) {
      paths.add(Paths.get(candidate));
    }
    return paths;
  }

  private static void addDirectories(Set<String> candidates, String directories) {
    if (directories == null) {
      return;
    }
    for (String candidate : directories.split(",")) {
      String trimmed = candidate.trim();
      if (!trimmed.isEmpty()) {
        candidates.add(trimmed);
      }
    }
  }
}
