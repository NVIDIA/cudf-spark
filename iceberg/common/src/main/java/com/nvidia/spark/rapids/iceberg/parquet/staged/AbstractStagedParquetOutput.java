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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Shared lifecycle and bounds validation for staged Parquet outputs. */
abstract class AbstractStagedParquetOutput implements StagedParquetOutput {
  private final long capacityBytes;
  private final ReentrantReadWriteLock operationLock = new ReentrantReadWriteLock();
  private long actualSizeBytes;
  private boolean sealed;
  private boolean closed;

  AbstractStagedParquetOutput(long capacityBytes) {
    if (capacityBytes <= 0) {
      throw new IllegalArgumentException("capacityBytes must be positive: " + capacityBytes);
    }
    this.capacityBytes = capacityBytes;
    this.actualSizeBytes = capacityBytes;
  }

  @Override
  public final synchronized long capacityBytes() {
    return capacityBytes;
  }

  @Override
  public final synchronized long sizeBytes() {
    ensureOpen();
    return actualSizeBytes;
  }

  @Override
  public final synchronized boolean isSealed() {
    return sealed;
  }

  /** Verify that an operation is permitted only before sealing. */
  final synchronized void ensureWritable() {
    ensureOpen();
    if (sealed) {
      throw new IllegalStateException("staged Parquet output is already sealed");
    }
  }

  /** Verify that an operation is permitted only after sealing. */
  final synchronized void ensureSealed() {
    ensureOpen();
    if (!sealed) {
      throw new IllegalStateException("staged Parquet output has not been sealed");
    }
  }

  /** Validate a write range against the reserved synthetic-file capacity. */
  final synchronized void checkWriteBounds(long offset, long length) {
    ensureWritable();
    if (offset < 0 || length < 0 || offset > capacityBytes - length) {
      throw new IndexOutOfBoundsException(
          "output range [" + offset + ", " + (offset + length) +
              ") exceeds capacity " + capacityBytes);
    }
  }

  /** Validate and record the final byte count before implementation-specific sealing. */
  final synchronized void beginSeal(long finalSizeBytes) {
    ensureWritable();
    if (finalSizeBytes <= 0 || finalSizeBytes > capacityBytes) {
      throw new IllegalArgumentException(
          "actualSizeBytes must be in (0, " + capacityBytes + "]: " + finalSizeBytes);
    }
    actualSizeBytes = finalSizeBytes;
  }

  /** Mark implementation-specific sealing as successfully completed. */
  final synchronized void finishSeal() {
    sealed = true;
  }

  /** Return the final size to an implementation after lifecycle validation. */
  final synchronized long sealedSizeBytes() {
    ensureSealed();
    return actualSizeBytes;
  }

  /** Return true when an idempotent close has already released this output. */
  final synchronized boolean beginClose() {
    if (closed) {
      return false;
    }
    closed = true;
    return true;
  }

  /**
   * Enter a data-copy operation. Disjoint column ranges may be copied concurrently, while sealing
   * and close take the exclusive side of this lock and therefore wait for every copy to finish.
   */
  final void beginConcurrentWrite() {
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

  /** Leave a data-copy operation entered by {@link #beginConcurrentWrite()}. */
  final void endConcurrentWrite() {
    operationLock.readLock().unlock();
  }

  /** Enter a header/footer write or seal operation after all concurrent data copies finish. */
  final void beginExclusiveWrite() {
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

  /** Leave an operation entered by {@link #beginExclusiveWrite()}. */
  final void endExclusiveWrite() {
    operationLock.writeLock().unlock();
  }

  /** Enter a sealed read operation, excluding close until the returned view has been created. */
  final void beginSealedRead() {
    Lock lock = operationLock.readLock();
    lock.lock();
    boolean succeeded = false;
    try {
      ensureSealed();
      succeeded = true;
    } finally {
      if (!succeeded) {
        lock.unlock();
      }
    }
  }

  /** Leave an operation entered by {@link #beginSealedRead()}. */
  final void endSealedRead() {
    operationLock.readLock().unlock();
  }

  /** Serialize idempotent close with every data-copy, seal, and materialize operation. */
  final void beginExclusiveClose() {
    operationLock.writeLock().lock();
  }

  /** Leave an operation entered by {@link #beginExclusiveClose()}. */
  final void endExclusiveClose() {
    operationLock.writeLock().unlock();
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("staged Parquet output is closed");
    }
  }

  /** Convert a lifecycle failure into an {@link IOException} where an I/O signature requires it. */
  static IOException asIOException(String message, RuntimeException cause) {
    return new IOException(message, cause);
  }
}
