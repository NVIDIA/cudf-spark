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

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;

/**
 * Partition-reader-owned bounded pool of reusable host buffers that are fed to cuDF.
 *
 * <p>The pool pre-creates a fixed number of slots, not fixed-sized allocations. A slot lazily
 * allocates or grows its pinned-preferred buffer on the shared staged worker that fills it. This
 * keeps allocation out of synchronized planner callbacks while making the number of large host
 * allocations independent of the footer/download worker count. Closing the owning partition
 * reader closes this pool and all buffers allocated by that Spark task.</p>
 *
 * <p>Acquisition is asynchronous and FIFO. A shared worker must never block waiting for a slot:
 * doing so could occupy every worker while the Spark task is waiting for an assembly job that
 * cannot start. The lease is returned only after cuDF has eagerly consumed the assembled input.</p>
 */
final class AssemblyBufferPool implements AutoCloseable {
  private final ArrayDeque<Slot> freeSlots = new ArrayDeque<>();
  private final ArrayDeque<CompletableFuture<Lease>> waiters = new ArrayDeque<>();
  // These values describe retained HMB capacity, not bytes currently occupied by encoded input.
  // They are guarded by this pool's monitor so readers can publish a consistent pair.
  private long currentCapacityBytes;
  private long peakCapacityBytes;
  private boolean closed;

  AssemblyBufferPool(int slotCount) {
    if (slotCount <= 0) {
      throw new IllegalArgumentException("slotCount must be positive: " + slotCount);
    }
    for (int index = 0; index < slotCount; index++) {
      freeSlots.addLast(new Slot());
    }
  }

  /** Return a future completed in fair FIFO order when a slot is available. */
  synchronized CompletableFuture<Lease> acquire() {
    if (closed) {
      CompletableFuture<Lease> failed = new CompletableFuture<>();
      failed.completeExceptionally(
          new IllegalStateException("assembly buffer pool is closed"));
      return failed;
    }
    Slot slot = freeSlots.pollFirst();
    if (slot != null) {
      return CompletableFuture.completedFuture(new Lease(this, slot));
    }
    CompletableFuture<Lease> waiter = new CompletableFuture<>();
    waiters.addLast(waiter);
    return waiter;
  }

  /** Return one atomic partition-reader-pool capacity observation for scan metrics. */
  synchronized CapacitySnapshot capacitySnapshot() {
    return new CapacitySnapshot(currentCapacityBytes, peakCapacityBytes);
  }

  private synchronized void addCapacity(long bytes) {
    currentCapacityBytes = Math.addExact(currentCapacityBytes, bytes);
    peakCapacityBytes = Math.max(peakCapacityBytes, currentCapacityBytes);
  }

  private synchronized void removeCapacity(long bytes) {
    if (bytes > currentCapacityBytes) {
      throw new IllegalStateException(
          "assembly capacity underflow: removing " + bytes + " from " +
              currentCapacityBytes);
    }
    currentCapacityBytes -= bytes;
  }

  private void closeSlot(Slot slot) {
    HostMemoryBuffer buffer = slot.detachBuffer();
    if (buffer == null) {
      return;
    }
    long capacity = buffer.getLength();
    try {
      buffer.close();
    } finally {
      removeCapacity(capacity);
    }
  }

  private void release(Slot slot) {
    CompletableFuture<Lease> waiter = null;
    boolean closeSlot = false;
    synchronized (this) {
      if (closed) {
        closeSlot = true;
      } else {
        while (!waiters.isEmpty() && waiter == null) {
          CompletableFuture<Lease> candidate = waiters.pollFirst();
          if (!candidate.isCancelled()) {
            waiter = candidate;
          }
        }
        if (waiter == null) {
          freeSlots.addLast(slot);
        }
      }
    }

    if (closeSlot) {
      closeSlot(slot);
      return;
    }
    if (waiter == null) {
      return;
    }

    // Complete outside the pool monitor. The dependent planner callback may immediately submit
    // work or release the lease, neither of which should run while the pool is locked.
    if (!waiter.complete(new Lease(this, slot))) {
      release(slot);
    }
  }

  @Override
  public void close() {
    ArrayDeque<Slot> slots;
    ArrayDeque<CompletableFuture<Lease>> pending;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      slots = new ArrayDeque<>(freeSlots);
      freeSlots.clear();
      pending = new ArrayDeque<>(waiters);
      waiters.clear();
    }
    IllegalStateException failure =
        new IllegalStateException("assembly buffer pool is closed");
    for (CompletableFuture<Lease> waiter : pending) {
      waiter.completeExceptionally(failure);
    }
    Throwable closeFailure = null;
    for (Slot slot : slots) {
      try {
        closeSlot(slot);
      } catch (Throwable error) {
        if (closeFailure == null) {
          closeFailure = error;
        } else if (closeFailure != error) {
          closeFailure.addSuppressed(error);
        }
      }
    }
    if (closeFailure instanceof Error) {
      throw (Error) closeFailure;
    }
    if (closeFailure instanceof RuntimeException) {
      throw (RuntimeException) closeFailure;
    }
    if (closeFailure != null) {
      throw new RuntimeException(closeFailure);
    }
  }

  /** Immutable, internally consistent observation of retained partition-reader capacity. */
  static final class CapacitySnapshot {
    private final long currentCapacityBytes;
    private final long peakCapacityBytes;

    private CapacitySnapshot(long currentCapacityBytes, long peakCapacityBytes) {
      this.currentCapacityBytes = currentCapacityBytes;
      this.peakCapacityBytes = peakCapacityBytes;
    }

    long getCurrentCapacityBytes() {
      return currentCapacityBytes;
    }

    long getPeakCapacityBytes() {
      return peakCapacityBytes;
    }
  }

  /** Exclusive ownership of one reusable slot. */
  static final class Lease implements AutoCloseable {
    private final AssemblyBufferPool owner;
    private final Slot slot;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Lease(AssemblyBufferPool owner, Slot slot) {
      this.owner = owner;
      this.slot = slot;
    }

    /**
     * Ensure this slot can hold {@code requiredBytes}; returns time spent allocating in nanos.
     * Must be called only by the assembly worker that owns this lease.
     */
    long ensureCapacity(long requiredBytes) {
      if (requiredBytes <= 0) {
        throw new IllegalArgumentException(
            "requiredBytes must be positive: " + requiredBytes);
      }
      ensureOpen();
      if (slot.buffer != null && slot.buffer.getLength() >= requiredBytes) {
        return 0L;
      }
      if (slot.buffer != null) {
        HostMemoryBuffer oldBuffer = slot.detachBuffer();
        long oldCapacity = oldBuffer.getLength();
        try {
          oldBuffer.close();
        } finally {
          owner.removeCapacity(oldCapacity);
        }
      }
      long start = System.nanoTime();
      HostMemoryBuffer newBuffer = HostAlloc$.MODULE$.alloc(requiredBytes, true);
      slot.buffer = newBuffer;
      try {
        owner.addCapacity(newBuffer.getLength());
      } catch (RuntimeException | Error registrationError) {
        slot.buffer = null;
        try {
          newBuffer.close();
        } catch (Throwable closeError) {
          registrationError.addSuppressed(closeError);
        }
        throw registrationError;
      }
      return System.nanoTime() - start;
    }

    HostMemoryBuffer buffer() {
      ensureOpen();
      if (slot.buffer == null) {
        throw new IllegalStateException("assembly slot has no allocated buffer");
      }
      return slot.buffer;
    }

    /** Return a fresh owning input reference for one cuDF retry attempt. */
    HostMemoryBuffer materialize(long length) {
      HostMemoryBuffer buffer = buffer();
      if (length < 0 || length > buffer.getLength()) {
        throw new IndexOutOfBoundsException(
            "materialized length " + length + " exceeds capacity " + buffer.getLength());
      }
      return buffer.slice(0L, length);
    }

    private void ensureOpen() {
      if (closed.get()) {
        throw new IllegalStateException("assembly buffer lease is closed");
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        owner.release(slot);
      }
    }
  }

  /** One reusable allocation retained only while the owning partition reader is alive. */
  private static final class Slot {
    private HostMemoryBuffer buffer;

    private HostMemoryBuffer detachBuffer() {
      HostMemoryBuffer detached = buffer;
      buffer = null;
      return detached;
    }
  }
}
