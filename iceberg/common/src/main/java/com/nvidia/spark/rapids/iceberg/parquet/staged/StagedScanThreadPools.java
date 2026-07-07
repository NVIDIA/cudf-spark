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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor-wide worker pool for the Iceberg staged reader.
 *
 * <p>Footer loading/filtering, source-file I/O, and synthetic Parquet finalization all use this
 * pool. Sharing one concurrency budget avoids reserving workers for a stage that is temporarily
 * idle. Data work also passes through a small executor-wide subtask admission queue. Once a
 * subtask is admitted, all of its source jobs run concurrently and PerfIO can issue all of one
 * source's column-chunk requests concurrently. Later subtasks do not submit data requests until an
 * admitted subtask is terminal. Footer jobs bypass the subtask limit, although like all jobs they
 * can still wait for a worker in this shared FIFO pool.</p>
 *
 * <p>Pool submission does not transfer Spark task context automatically. Each staged callable is
 * responsible for installing and removing its captured {@code TaskContext}, as well as the RAPIDS
 * pool-thread marker, in a {@code finally} block.</p>
 */
public final class StagedScanThreadPools {
  private static final Logger LOG = LoggerFactory.getLogger(StagedScanThreadPools.class);
  private static final long KEEP_ALIVE_SECONDS = 60L;

  private static StagedScanThreadPools singleton;

  private final int threads;
  private final int maxConcurrentSubtasks;
  private final ExecutorService executor;
  private final Object admissionLock = new Object();
  private final ArrayDeque<SubtaskAdmission> waitingSubtasks = new ArrayDeque<>();
  // Guarded by admissionLock.
  private int activeSubtasks;

  private StagedScanThreadPools(int threads, int maxConcurrentSubtasks) {
    this.threads = threads;
    this.maxConcurrentSubtasks = maxConcurrentSubtasks;
    this.executor = newPool("iceberg-staged-worker", threads);
  }

  /**
   * Return the executor-wide pool, creating it on the first request.
   *
   * <p>The size must be positive. A later request with a different size reuses the initialized
   * pool and logs a warning because replacing a pool while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized StagedScanThreadPools getOrCreate(
      int threads,
      int maxConcurrentSubtasks) {
    checkPositive("threads", threads);
    checkPositive("maxConcurrentSubtasks", maxConcurrentSubtasks);
    if (singleton == null) {
      singleton = new StagedScanThreadPools(threads, maxConcurrentSubtasks);
    } else if (singleton.threads != threads ||
        singleton.maxConcurrentSubtasks != maxConcurrentSubtasks) {
      LOG.warn("Reusing initialized Iceberg staged-read pool (threads={}, subtasks={}) " +
              "instead of requested settings (threads={}, subtasks={})",
          singleton.threads, singleton.maxConcurrentSubtasks, threads, maxConcurrentSubtasks);
    }
    return singleton;
  }

  /** Return the shared pool used by every asynchronous staged-reader operation. */
  public ExecutorService executor() {
    return executor;
  }

  /**
   * Queue one whole data-read subtask without blocking the caller or a worker thread.
   *
   * <p>The starter is invoked immediately when a slot is free, or by the thread that releases a
   * previous slot. It should only construct and submit asynchronous work. That work must call
   * {@link SubtaskAdmission#complete()} after every source and its finalizer are terminal.</p>
   */
  SubtaskAdmission admitSubtask(SubtaskStarter starter) {
    SubtaskAdmission admission = new SubtaskAdmission(this, starter);
    boolean startNow = false;
    synchronized (admissionLock) {
      if (activeSubtasks < maxConcurrentSubtasks) {
        admission.state = AdmissionState.ACTIVE;
        activeSubtasks += 1;
        startNow = true;
      } else {
        waitingSubtasks.addLast(admission);
      }
    }
    if (startNow) {
      startAdmission(admission);
    }
    return admission;
  }

  private void startAdmission(SubtaskAdmission admission) {
    try {
      admission.starter.start(admission);
    } catch (Throwable error) {
      LOG.error("Uncaught failure while starting an Iceberg staged subtask", error);
      admission.complete();
    }
  }

  private void completeAdmission(SubtaskAdmission admission) {
    SubtaskAdmission next = null;
    synchronized (admissionLock) {
      if (admission.state != AdmissionState.ACTIVE) {
        return;
      }
      admission.state = AdmissionState.FINISHED;
      activeSubtasks -= 1;
      while (!waitingSubtasks.isEmpty() && next == null) {
        SubtaskAdmission candidate = waitingSubtasks.removeFirst();
        if (candidate.state == AdmissionState.WAITING) {
          candidate.state = AdmissionState.ACTIVE;
          activeSubtasks += 1;
          next = candidate;
        }
      }
    }
    if (next != null) {
      startAdmission(next);
    }
  }

  private boolean cancelAdmission(SubtaskAdmission admission) {
    synchronized (admissionLock) {
      if (admission.state != AdmissionState.WAITING) {
        return false;
      }
      admission.state = AdmissionState.FINISHED;
      waitingSubtasks.remove(admission);
      return true;
    }
  }

  /** Stop and forget the singleton so a same-JVM test can select deterministic pool widths. */
  static synchronized void resetForTesting() {
    if (singleton != null) {
      singleton.executor.shutdownNow();
      synchronized (singleton.admissionLock) {
        for (SubtaskAdmission admission : singleton.waitingSubtasks) {
          admission.state = AdmissionState.FINISHED;
        }
        singleton.waitingSubtasks.clear();
      }
      singleton = null;
    }
  }

  private static void checkPositive(String name, int value) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive: " + value);
    }
  }

  @FunctionalInterface
  interface SubtaskStarter {
    void start(SubtaskAdmission admission) throws Exception;
  }

  /**
   * One executor-wide data-I/O slot.
   *
   * <p>A waiting admission can be cancelled without starting any reads. An active admission is
   * released only by {@link #complete()}, after its asynchronous source barrier is terminal; this
   * prevents cancellation from admitting another subtask while writers still use the old one.</p>
   */
  static final class SubtaskAdmission {
    private final StagedScanThreadPools owner;
    private final SubtaskStarter starter;
    // Guarded by owner.admissionLock.
    private AdmissionState state = AdmissionState.WAITING;

    private SubtaskAdmission(
        StagedScanThreadPools owner,
        SubtaskStarter starter) {
      this.owner = java.util.Objects.requireNonNull(owner, "owner");
      this.starter = java.util.Objects.requireNonNull(starter, "starter");
    }

    void complete() {
      owner.completeAdmission(this);
    }

    boolean cancel() {
      return owner.cancelAdmission(this);
    }
  }

  private enum AdmissionState {
    WAITING,
    ACTIVE,
    FINISHED
  }

  private static ExecutorService newPool(String name, int threads) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threads,
        threads,
        KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedDaemonThreadFactory(name));
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /** Named daemon factory used only by this executor-wide singleton. */
  private static final class NamedDaemonThreadFactory implements ThreadFactory {
    private final String name;
    private final AtomicInteger nextId = new AtomicInteger();

    private NamedDaemonThreadFactory(String name) {
      this.name = name;
    }

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, name + "-" + nextId.incrementAndGet());
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler((failedThread, error) ->
          LOG.error("Uncaught exception on " + failedThread.getName(), error));
      return thread;
    }
  }
}
