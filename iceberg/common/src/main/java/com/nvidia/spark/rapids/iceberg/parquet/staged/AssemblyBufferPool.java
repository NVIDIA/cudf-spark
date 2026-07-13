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
 * Executor-wide bounded pool of reusable host buffers that are fed to cuDF.
 *
 * <p>The pool pre-creates a fixed number of slots, not fixed-sized allocations. A slot lazily
 * allocates or grows its pinned-preferred buffer on the shared staged worker that fills it. This
 * keeps allocation out of synchronized planner callbacks while making the number of large host
 * allocations independent of the footer/download worker count.</p>
 *
 * <p>Acquisition is asynchronous and FIFO. A shared worker must never block waiting for a slot:
 * doing so could occupy every worker while the Spark task is waiting for an assembly job that
 * cannot start. The lease is returned only after cuDF has eagerly consumed the assembled input.</p>
 */
final class AssemblyBufferPool implements AutoCloseable {
  private final ArrayDeque<Slot> freeSlots = new ArrayDeque<>();
  private final ArrayDeque<CompletableFuture<Lease>> waiters = new ArrayDeque<>();
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

  private void release(Slot slot) {
    CompletableFuture<Lease> waiter = null;
    synchronized (this) {
      if (closed) {
        slot.close();
        return;
      }
      while (!waiters.isEmpty() && waiter == null) {
        CompletableFuture<Lease> candidate = waiters.pollFirst();
        if (!candidate.isCancelled()) {
          waiter = candidate;
        }
      }
      if (waiter == null) {
        freeSlots.addLast(slot);
        return;
      }
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
    for (Slot slot : slots) {
      slot.close();
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
        slot.buffer.close();
        slot.buffer = null;
      }
      long start = System.nanoTime();
      slot.buffer = HostAlloc$.MODULE$.alloc(requiredBytes, true);
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

  /** One reusable allocation retained only while the executor-wide pool is alive. */
  private static final class Slot implements AutoCloseable {
    private HostMemoryBuffer buffer;

    @Override
    public void close() {
      if (buffer != null) {
        buffer.close();
        buffer = null;
      }
    }
  }
}
