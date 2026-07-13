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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executor-wide worker pool for the Iceberg staged reader.
 *
 * <p>Footer loading/filtering, source-file I/O, and synthetic Parquet finalization all use this
 * pool. Sharing one concurrency budget avoids reserving workers for a stage that is temporarily
 * idle. One fused job holds one worker from footer loading through the blocking direct-to-cache
 * S3 read and cache publication. The fixed pool width therefore bounds concurrent whole-file
 * pipelines. Cache-to-host assembly uses the same workers but is prioritized ahead of queued
 * file jobs so prepared GPU input is not stranded behind speculative downloads.</p>
 *
 * <p>Pool submission does not transfer Spark task context automatically. Each staged callable is
 * responsible for installing its captured {@code TaskContext} and RAPIDS pool-thread marker once
 * around each blocking file or assembly job, and removing both in a {@code finally} block.</p>
 */
public final class StagedScanThreadPools {
  private static final Logger LOG = LoggerFactory.getLogger(StagedScanThreadPools.class);
  private static final long KEEP_ALIVE_SECONDS = 60L;
  private static final int ASSEMBLY_PRIORITY = 0;
  private static final int FILE_PRIORITY = 1;

  private static StagedScanThreadPools singleton;

  private final int threads;
  private final int assemblyBuffers;
  private final ThreadPoolExecutor executor;
  private final ScheduledExecutorService timer;
  private final AssemblyBufferPool assemblyBufferPool;
  private final AtomicLong nextSequence = new AtomicLong();

  private StagedScanThreadPools(int threads, int assemblyBuffers) {
    this.threads = threads;
    this.assemblyBuffers = assemblyBuffers;
    this.executor = newPool("iceberg-staged-worker", threads);
    this.timer = Executors.newSingleThreadScheduledExecutor(
        new NamedDaemonThreadFactory("iceberg-staged-timer"));
    this.assemblyBufferPool = new AssemblyBufferPool(assemblyBuffers);
  }

  /**
   * Return the executor-wide pool, creating it on the first request.
   *
   * <p>Both sizes must be positive. A later request with different sizes reuses the initialized
   * pools and logs a warning because replacing them while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized StagedScanThreadPools getOrCreate(
      int threads,
      int assemblyBuffers) {
    checkPositive("threads", threads);
    checkPositive("assemblyBuffers", assemblyBuffers);
    if (singleton == null) {
      singleton = new StagedScanThreadPools(threads, assemblyBuffers);
    } else if (singleton.threads != threads || singleton.assemblyBuffers != assemblyBuffers) {
      LOG.warn("Reusing initialized Iceberg staged-read pool with {} workers and {} assembly " +
          "buffers instead of requested {} workers and {} assembly buffers",
          singleton.threads, singleton.assemblyBuffers, threads, assemblyBuffers);
    }
    return singleton;
  }

  /** Submit one footer/download file pipeline in FIFO order behind decode-critical assembly. */
  <T> CompletableFuture<T> submitFile(Callable<T> callable) {
    return submit(FILE_PRIORITY, callable);
  }

  /** Submit cache-to-host assembly ahead of queued speculative file pipelines. */
  <T> CompletableFuture<T> submitAssembly(Callable<T> callable) {
    return submit(ASSEMBLY_PRIORITY, callable);
  }

  /** Schedule a tiny non-blocking planner timeout callback. */
  void schedule(Runnable callback, long delay, TimeUnit unit) {
    timer.schedule(callback, delay, unit);
  }

  AssemblyBufferPool assemblyBuffers() {
    return assemblyBufferPool;
  }

  private <T> CompletableFuture<T> submit(int priority, Callable<T> callable) {
    CompletableFuture<T> result = new CompletableFuture<>();
    PrioritizedRunnable task = new PrioritizedRunnable(
        priority, nextSequence.getAndIncrement(), () -> {
          try {
            result.complete(callable.call());
          } catch (Throwable error) {
            result.completeExceptionally(error);
          }
        });
    try {
      executor.execute(task);
    } catch (Throwable error) {
      result.completeExceptionally(error);
    }
    return result;
  }

  /** Stop and forget the singleton so a same-JVM test can select deterministic pool widths. */
  static synchronized void resetForTesting() {
    if (singleton != null) {
      singleton.executor.shutdownNow();
      singleton.timer.shutdownNow();
      singleton.assemblyBufferPool.close();
      singleton = null;
    }
  }

  private static void checkPositive(String name, int value) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive: " + value);
    }
  }

  private static ThreadPoolExecutor newPool(String name, int threads) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threads,
        threads,
        KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        new PriorityBlockingQueue<Runnable>(),
        new NamedDaemonThreadFactory(name));
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /** Priority first, then global submission order for deterministic FIFO within a stage. */
  private static final class PrioritizedRunnable
      implements Runnable, Comparable<PrioritizedRunnable> {
    private final int priority;
    private final long sequence;
    private final Runnable delegate;

    private PrioritizedRunnable(int priority, long sequence, Runnable delegate) {
      this.priority = priority;
      this.sequence = sequence;
      this.delegate = delegate;
    }

    @Override
    public void run() {
      delegate.run();
    }

    @Override
    public int compareTo(PrioritizedRunnable other) {
      int priorityOrder = Integer.compare(priority, other.priority);
      return priorityOrder != 0 ? priorityOrder : Long.compare(sequence, other.sequence);
    }
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
