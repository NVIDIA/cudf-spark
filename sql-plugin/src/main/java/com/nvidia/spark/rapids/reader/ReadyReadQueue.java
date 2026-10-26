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

package com.nvidia.spark.rapids.reader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
 * Bounded ready-handle delivery for a scheduler's logical reads.
 *
 * <p>The limit bounds admitted logical reads, including ready and continuation states.
 * Byte reservations remain with ReaderTaskScope results and the existing resource pool:
 * closing an admission never releases a buffer's reservation. A scheduler closes this
 * queue to wake consumers, then closes its task scope to stop and join IO operations.
 */
public final class ReadyReadQueue<H extends ReadHandle<?>>
    implements Iterator<H>, AutoCloseable {
  private final int maxOutstanding;
  private final Set<Admission> active = new LinkedHashSet<>();
  private final Queue<Admission> ready = new ArrayDeque<>();
  private boolean admissionFinished;
  private boolean closed;
  private Throwable failure;

  public ReadyReadQueue(int maxOutstanding) {
    if (maxOutstanding <= 0) {
      throw new IllegalArgumentException("Outstanding read limit must be positive");
    }
    this.maxOutstanding = maxOutstanding;
  }

  /**
   * Non-blocking admission. An empty result requires the scheduler to drain ready work
   * or stop on cancellation, not wait on the task thread while holding all ready inputs.
   */
  public synchronized Optional<Admission> tryAdmit() {
    if (closed) {
      return Optional.empty();
    }
    if (admissionFinished) {
      throw new IllegalStateException("Initial read admission is complete");
    }
    if (active.size() >= maxOutstanding) {
      return Optional.empty();
    }
    Admission admission = new Admission();
    active.add(admission);
    return Optional.of(admission);
  }

  /** Mark that all initial sources have been admitted. Existing reads may still continue. */
  public synchronized void finishAdmission() {
    admissionFinished = true;
    notifyAll();
  }

  @Override
  public boolean hasNext() {
    try {
      synchronized (this) {
        while (ready.isEmpty() && !closed && failure == null &&
            !(admissionFinished && active.isEmpty())) {
          wait();
        }
        if (failure != null) {
          throw ReaderResources.propagate(failure);
        }
        return !ready.isEmpty();
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      fail(error);
      throw ReaderResources.propagate(error);
    }
  }

  @Override
  public H next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No ready read handle");
    }
    synchronized (this) {
      Admission admission = ready.poll();
      if (admission == null) {
        throw new NoSuchElementException("Ready reads were cancelled");
      }
      H handle = admission.handle;
      admission.handle = null;
      return handle;
    }
  }

  /** Latch an IO/planning failure, wake waiters, and release all unyielded handles. */
  public void fail(Throwable error) {
    Objects.requireNonNull(error, "error");
    List<H> cleanup;
    synchronized (this) {
      if (closed) {
        return;
      }
      failure = error;
      cleanup = drain();
    }
    ReaderResources.closeAll(error, cleanup.toArray(new AutoCloseable[0]));
  }

  @Override
  public void close() {
    List<H> cleanup;
    synchronized (this) {
      cleanup = drain();
    }
    ReaderResources.closeAll(null, cleanup.toArray(new AutoCloseable[0]));
  }

  private List<H> drain() {
    closed = true;
    List<H> cleanup = new ArrayList<>();
    for (Admission admission : active) {
      admission.closed = true;
      if (admission.handle != null) {
        cleanup.add(admission.handle);
        admission.handle = null;
      }
    }
    active.clear();
    ready.clear();
    notifyAll();
    return cleanup;
  }

  /** One logical read, kept live across filter and complete handoffs. */
  public final class Admission implements AutoCloseable {
    private H handle;
    private boolean closed;

    private Admission() {
    }

    /**
     * Consume one fresh, fully materialized handle. Return false and close it if cancelled.
     * A logical read has at most one queued handoff; publish its continuation only after yield.
     */
    public boolean publish(H value) {
      Objects.requireNonNull(value, "handle");
      boolean duplicate = false;
      try {
        synchronized (ReadyReadQueue.this) {
          if (!closed && !ReadyReadQueue.this.closed) {
            duplicate = handle == value;
            if (handle != null) {
              throw new IllegalStateException("A handoff is already ready for this read");
            }
            handle = value;
            ready.add(this);
            ReadyReadQueue.this.notifyAll();
            return true;
          }
        }
      } catch (Throwable error) {
        if (!duplicate) {
          ReaderResources.closeAll(error, value);
        }
        throw ReaderResources.propagate(error);
      }
      ReaderResources.closeAll(null, value);
      return false;
    }

    /** Complete or cancel this logical read. Yielded handles remain owned by their consumer. */
    @Override
    public void close() {
      H cleanup;
      synchronized (ReadyReadQueue.this) {
        if (closed) {
          return;
        }
        closed = true;
        cleanup = handle;
        handle = null;
        ready.remove(this);
        active.remove(this);
        ReadyReadQueue.this.notifyAll();
      }
      ReaderResources.closeAll(null, cleanup);
    }
  }
}
