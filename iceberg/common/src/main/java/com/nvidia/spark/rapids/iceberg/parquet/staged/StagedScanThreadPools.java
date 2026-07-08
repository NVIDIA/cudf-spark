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
 * idle. Each Spark task builds its own ordered data-I/O submission chain. Once a vectored read is
 * submitted, its asynchronous engine owns the request until the returned future is terminal; the
 * shared workers are free to perform footer, allocation, cache, and Parquet-finalization work.</p>
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
  private final ExecutorService executor;

  private StagedScanThreadPools(int threads) {
    this.threads = threads;
    this.executor = newPool("iceberg-staged-worker", threads);
  }

  /**
   * Return the executor-wide pool, creating it on the first request.
   *
   * <p>The size must be positive. A later request with a different size reuses the initialized
   * pool and logs a warning because replacing a pool while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized StagedScanThreadPools getOrCreate(int threads) {
    checkPositive("threads", threads);
    if (singleton == null) {
      singleton = new StagedScanThreadPools(threads);
    } else if (singleton.threads != threads) {
      LOG.warn("Reusing initialized Iceberg staged-read pool with {} threads instead of " +
          "requested {} threads", singleton.threads, threads);
    }
    return singleton;
  }

  /** Return the shared pool used by every asynchronous staged-reader operation. */
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
