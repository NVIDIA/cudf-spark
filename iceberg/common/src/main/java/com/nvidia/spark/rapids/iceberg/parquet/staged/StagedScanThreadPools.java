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
 * Executor-wide I/O and CPU pools for the Iceberg staged reader.
 *
 * <p>The CPU pool fetches and filters footers and finalizes already-fetched synthetic Parquet
 * files. The I/O pool owns independently scheduled source-file reads; its width is the only bound
 * on active source jobs. Keeping footer jobs out of the I/O queue prevents already-submitted data
 * reads from delaying another Spark task's footer barrier. The first task on an executor fixes the
 * configured sizes, and later tasks reuse those pools. All workers are daemon threads and idle
 * core threads time out, so the singleton does not delay executor shutdown.</p>
 *
 * <p>Pool submission does not transfer Spark task context automatically. Each staged callable is
 * responsible for installing and removing its captured {@code TaskContext}, as well as the RAPIDS
 * pool-thread marker, in a {@code finally} block.</p>
 */
public final class StagedScanThreadPools {
  private static final Logger LOG = LoggerFactory.getLogger(StagedScanThreadPools.class);
  private static final long KEEP_ALIVE_SECONDS = 60L;

  private static StagedScanThreadPools singleton;

  private final int cpuThreads;
  private final int ioThreads;
  private final ExecutorService cpuExecutor;
  private final ExecutorService ioExecutor;

  private StagedScanThreadPools(int cpuThreads, int ioThreads) {
    this.cpuThreads = cpuThreads;
    this.ioThreads = ioThreads;
    this.cpuExecutor = newPool("iceberg-staged-cpu", cpuThreads);
    this.ioExecutor = newPool("iceberg-staged-io", ioThreads);
  }

  /**
   * Return the executor-wide pools, creating them on the first request.
   *
   * <p>Every size must be positive. A later request with different sizes reuses the initialized
   * pools and logs a warning because replacing a pool while tasks own futures would violate
   * cancellation and ownership guarantees.</p>
   */
  public static synchronized StagedScanThreadPools getOrCreate(
      int cpuThreads,
      int ioThreads) {
    checkThreadCount("cpuThreads", cpuThreads);
    checkThreadCount("ioThreads", ioThreads);
    if (singleton == null) {
      singleton = new StagedScanThreadPools(cpuThreads, ioThreads);
    } else if (singleton.cpuThreads != cpuThreads || singleton.ioThreads != ioThreads) {
      LOG.warn("Reusing initialized Iceberg staged-read pools ({}/{}) instead of " +
              "requested sizes ({}/{})",
          singleton.cpuThreads, singleton.ioThreads, cpuThreads, ioThreads);
    }
    return singleton;
  }

  /** Return the pool that fetches/filters footers and finalizes synthetic Parquet outputs. */
  public ExecutorService cpuExecutor() {
    return cpuExecutor;
  }

  /** Return the pool that fetches planned Parquet data ranges. */
  public ExecutorService ioExecutor() {
    return ioExecutor;
  }

  /** Stop and forget the singleton so a same-JVM test can select deterministic pool widths. */
  static synchronized void resetForTesting() {
    if (singleton != null) {
      singleton.cpuExecutor.shutdownNow();
      singleton.ioExecutor.shutdownNow();
      singleton = null;
    }
  }

  private static void checkThreadCount(String name, int value) {
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
