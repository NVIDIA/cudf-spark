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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor-wide worker pool for the Iceberg asynchronous reader.
 *
 * <p>Footer loading/filtering, source-file I/O, and synthetic Parquet finalization all use this
 * pool. Sharing one concurrency budget avoids reserving workers for a stage that is temporarily
 * idle. One fused job holds one worker from footer loading through the blocking vectored read,
 * cache publication, and fragment sealing. The fixed pool width therefore bounds concurrent
 * whole-file pipelines; a worker returns to the shared FIFO queue only after its file is complete
 * or has failed.</p>
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
  private final ExecutorService executor;

  private ParquetReaderThreadPool(int threads) {
    this.threads = threads;
    this.executor = newPool("iceberg-async-worker", threads);
  }

  /**
   * Return the executor-wide pool, creating it on the first request.
   *
   * <p>The size must be positive. A later request with a different size reuses the initialized
   * pool and logs a warning because replacing a pool while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized ParquetReaderThreadPool getOrCreate(int threads) {
    checkPositive("threads", threads);
    if (singleton == null) {
      singleton = new ParquetReaderThreadPool(threads);
    } else if (singleton.threads != threads) {
      LOG.warn("Reusing initialized Iceberg asynchronous-read pool with {} threads instead of " +
          "requested {} threads", singleton.threads, threads);
    }
    return singleton;
  }

  /** Return the shared pool used by every asynchronous file job. */
  public ExecutorService executor() {
    return executor;
  }

  /** Stop and forget the singleton so a same-JVM test can select deterministic pool widths. */
  static synchronized void resetForTesting() {
    if (singleton != null) {
      singleton.executor.shutdownNow();
      singleton = null;
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
