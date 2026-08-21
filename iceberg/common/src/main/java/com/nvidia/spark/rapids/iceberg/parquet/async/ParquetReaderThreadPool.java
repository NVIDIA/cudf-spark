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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor-wide worker pool for the Iceberg asynchronous reader.
 *
 * <p>Only short CPU or blocking-memory stages use this pool: footer parsing/filtering, read
 * preparation/finalization, and combining. Iceberg S3 requests and file-cache I/O use their own
 * asynchronous executors and do not retain these workers while in flight.</p>
 *
 * <p>The old reader's worker width also happened to cap the number of live whole-file reads.
 * Decoupling S3 waits from workers must not remove that memory bound, so this singleton also owns
 * an asynchronous executor-wide file-pipeline admission queue. A {@link FilePermit} is acquired
 * only after footer filtering and spans output allocation through data-read finalization. Waiting
 * for a permit never occupies a worker.</p>
 *
 * <p>Pool submission does not transfer Spark task context automatically. Each asynchronous callable is
 * responsible for installing its captured {@code TaskContext} and RAPIDS pool-thread marker once
 * around the fused file job, and removing both in a {@code finally} block.</p>
 */
public final class ParquetReaderThreadPool {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderThreadPool.class);
  private static final long KEEP_ALIVE_SECONDS = 60L;

  private static ParquetReaderThreadPool singleton;

  private final int threads;
  private final int maxInFlightFiles;
  private final ExecutorService executor;
  private final ArrayDeque<CompletableFuture<FilePermit>> fileWaiters = new ArrayDeque<>();
  private int inFlightFiles;

  private ParquetReaderThreadPool(int threads, int maxInFlightFiles) {
    this.threads = threads;
    this.maxInFlightFiles = maxInFlightFiles;
    this.executor = newPool("iceberg-async-worker", threads);
  }

  /**
   * Return the executor-wide pool, creating it on the first request.
   *
   * <p>The size must be positive. A later request with a different size reuses the initialized
   * pool and logs a warning because replacing a pool while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized ParquetReaderThreadPool getOrCreate(
      int threads,
      int maxInFlightFiles) {
    checkPositive("threads", threads);
    checkPositive("maxInFlightFiles", maxInFlightFiles);
    if (singleton == null) {
      singleton = new ParquetReaderThreadPool(threads, maxInFlightFiles);
    } else if (singleton.threads != threads ||
        singleton.maxInFlightFiles != maxInFlightFiles) {
      LOG.warn("Reusing initialized Iceberg asynchronous-read pool with {} threads and {} " +
              "in-flight files instead of requested {} threads and {} in-flight files",
          singleton.threads, singleton.maxInFlightFiles, threads, maxInFlightFiles);
    }
    return singleton;
  }

  /** Compatibility overload for tests that only care about the executor. */
  public static synchronized ParquetReaderThreadPool getOrCreate(int threads) {
    return getOrCreate(threads, threads);
  }

  /** Return the shared pool used by every asynchronous file job. */
  public ExecutorService executor() {
    return executor;
  }

  /**
   * Asynchronously acquire one executor-wide whole-file pipeline slot.
   *
   * <p>The future is completed in FIFO order. Its continuation may submit short work to
   * {@link #executor()}, but no executor worker is consumed while this future is queued.</p>
   */
  public CompletableFuture<FilePermit> acquireFilePermit() {
    synchronized (this) {
      if (inFlightFiles < maxInFlightFiles) {
        inFlightFiles += 1;
        return CompletableFuture.completedFuture(new FilePermit(this));
      }
      CompletableFuture<FilePermit> waiter = new CompletableFuture<>();
      fileWaiters.addLast(waiter);
      return waiter;
    }
  }

  private void releaseFilePermit() {
    CompletableFuture<FilePermit> next;
    synchronized (this) {
      next = fileWaiters.pollFirst();
      if (next == null) {
        inFlightFiles -= 1;
        if (inFlightFiles < 0) {
          inFlightFiles = 0;
          throw new IllegalStateException("Iceberg file-pipeline permit released twice");
        }
        return;
      }
      // Keep inFlightFiles unchanged: ownership transfers directly to the next waiter.
    }
    next.complete(new FilePermit(this));
  }

  /** One idempotently releasable executor-wide file-pipeline admission slot. */
  public static final class FilePermit implements AutoCloseable {
    private final ParquetReaderThreadPool owner;
    private boolean closed;

    private FilePermit(ParquetReaderThreadPool owner) {
      this.owner = owner;
    }

    @Override
    public void close() {
      synchronized (this) {
        if (closed) {
          return;
        }
        closed = true;
      }
      owner.releaseFilePermit();
    }
  }

  /** Stop and forget the singleton so a same-JVM test can select deterministic pool widths. */
  static synchronized void resetForTesting() {
    if (singleton != null) {
      singleton.failWaitersForTesting();
      singleton.executor.shutdownNow();
      singleton = null;
    }
  }

  private void failWaitersForTesting() {
    synchronized (this) {
      IllegalStateException failure = new IllegalStateException("reader pool reset");
      while (!fileWaiters.isEmpty()) {
        fileWaiters.removeFirst().completeExceptionally(failure);
      }
    }
  }

  private static void checkPositive(String name, int value) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive: " + value);
    }
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
