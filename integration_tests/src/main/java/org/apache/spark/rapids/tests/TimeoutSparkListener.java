/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
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

package org.apache.spark.rapids.tests;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.status.api.v1.ThreadStackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * This code helps accelerate root cause investigations for pipeline hangs
 * and prevents other failures from being masked by the hangs in the meantime.
 *
 * This class implements a SparkListener to keep track of Spark Actions (Jobs)
 * taking longer than expected / hanging perpetually leading to the CI pipeline
 * holding on to expensive Compute Cloud resources. Stuck Spark actions are killed
 * with the diagnostic message and a thread dump.
 *
 * Without this class, the pipeline is eventually killed by CI without sufficient diagnostics.
 * One needs to parse the log to keep track which
 * tests are scheduled to run but do not have the corresponding FAILED/SUCCEEDED
 * messages. Out of these tests most pytest items might be false positives because
 * xdist assigns them in batches to workers that become blocked by only one of them
 */
public class TimeoutSparkListener extends SparkListener {
  private static final Logger LOG = LoggerFactory.getLogger(TimeoutSparkListener.class);
  private static JavaSparkContext sparkContext;
  private static int timeoutSeconds;
  private static boolean shouldDumpThreads;
  private static final ScheduledExecutorService runner = Executors.newScheduledThreadPool(1,
    runnable -> {
      final Thread t = new Thread(runnable);
      t.setDaemon(true);
      t.setName("spark-job-timeout-thread-" + t.hashCode());
      return t;
    }
  );
  private static final Map<Integer,ScheduledFuture<?>> cancelJobMap = new ConcurrentHashMap<>();
  private static final TimeoutSparkListener SINGLETON = new TimeoutSparkListener();

  public TimeoutSparkListener() {
    super();
  }

  public static synchronized void init(JavaSparkContext sc) {
    if (sparkContext == null) {
      sparkContext = sc;
      sparkContext.sc().addSparkListener(SINGLETON);
    }
  }

  private static synchronized void unregister() {
    if (sparkContext != null) {
      sparkContext.sc().removeSparkListener(SINGLETON);
      sparkContext = null;
    }
  }

  private static synchronized void cancelJob(int jobId, String message) {
    if (sparkContext != null) {
      sparkContext.sc().cancelJob(jobId, message);
    }
    LOG.error(message + ". Shutting down the Driver JVM; xdist worker will stop as well per " +
"https://github.com/NVIDIA/spark-rapids/pull/12455. Pending tests will be re-executed " +
"in the replacement worker");
    System.exit(timeoutSeconds);
  }

  public void onJobStart(SparkListenerJobStart jobStart) {
    final int jobId = jobStart.jobId();
    LOG.debug("JobStart: registering timeout for Job {}", jobId);
    // create a task config snapshot
    final boolean taskShouldDumpThreads = shouldDumpThreads;
    final int taskTimeout = timeoutSeconds;
    final ScheduledFuture<?> scheduledFuture = runner.schedule(() -> {
      final String message = "RAPIDS Integration Test Job " + jobId + " exceeded the timeout of " +
        timeoutSeconds + " seconds, cancelling. " +
        "Look into fixing the test or reducing its execution time. " +
        "If necessary, adjust the timeout using the marker " +
        "pytest.mark.spark_job_timeout(seconds,dump_threads)";
      if (taskShouldDumpThreads) {
        LOG.error(message + " Driver thread dump follows");
        dumpThreads();
        dumpExecutorThreads();
      }
      cancelJob(jobId, message);
    }, taskTimeout, TimeUnit.SECONDS);
    cancelJobMap.put(jobId, scheduledFuture);
  }

  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    final int jobId = jobEnd.jobId();
    LOG.debug("JobEnd: cancelling timeout for Job {}", jobId);
    final ScheduledFuture<?> cancelFuture = cancelJobMap.remove(jobId);
    if (cancelFuture != null) {
      cancelFuture.cancel(false);
    } else {
      LOG.debug("Timeout task for Job {} not found", jobId);
    }
  }

  public static void setSparkJobTimeout(int ts, boolean dumpThreads) {
    timeoutSeconds = ts;
    shouldDumpThreads = dumpThreads;
  }

  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    unregister();
    // no new work
    runner.shutdownNow();
    cancelJobMap.clear();
  }

  private static void dumpThreads() {
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo threadInfo : threadMXBean.dumpAllThreads(true, true)) {
      LOG.warn(threadInfo.toString());
    }
  }

  // Total wall-clock budget for collecting all executor thread dumps. A stuck
  // executor's RPC blocks for spark.rpc.askTimeout (default 120s); without a
  // shared deadline, N stuck executors would extend the driver's lifetime by
  // 120s * N past the original job timeout. 30s is enough for healthy
  // executors to reply while bounding the worst case.
  private static final long EXECUTOR_DUMP_BUDGET_SECONDS = 30;

  private static final class ExecutorDumpResult {
    private final String execId;
    private final Option<ThreadStackTrace[]> dumpOpt;
    private final Throwable error;

    private ExecutorDumpResult(
        String execId,
        Option<ThreadStackTrace[]> dumpOpt,
        Throwable error) {
      this.execId = execId;
      this.dumpOpt = dumpOpt;
      this.error = error;
    }
  }

  /**
   * Best-effort dump of every live executor's JVM threads via Spark's
   * built-in {@link SparkContext#getExecutorThreadDump} RPC. A driver-only
   * thread dump is rarely enough to diagnose a hung Spark job because the
   * stuck thread is almost always on an executor; this fills the gap on
   * runtimes where the Spark UI / REST API is unreachable (e.g. Dataproc
   * Serverless without component gateway).
   *
   * RPCs are issued concurrently and share a single overall deadline
   * ({@link #EXECUTOR_DUMP_BUDGET_SECONDS}) so an unresponsive executor
   * cannot block the surrounding {@code cancelJob} / {@code System.exit}
   * path for more than that budget regardless of executor count.
   */
  private static void dumpExecutorThreads() {
    final JavaSparkContext jsc;
    synchronized (TimeoutSparkListener.class) {
      jsc = sparkContext;
    }
    if (jsc == null) {
      LOG.warn("SparkContext not initialized; skipping executor thread dump");
      return;
    }
    final SparkContext sc = jsc.sc();
    final Seq<String> execIdsSeq;
    try {
      execIdsSeq = sc.getExecutorIds();
    } catch (Throwable t) {
      LOG.warn("Failed to enumerate executors for thread dump", t);
      return;
    }
    final int numExecs = execIdsSeq.size();
    LOG.error("Executor thread dump follows ({} executor(s), {}s shared budget)",
      numExecs, EXECUTOR_DUMP_BUDGET_SECONDS);
    if (numExecs == 0) {
      return;
    }

    // Submit all RPCs in parallel. A dedicated daemon pool is used so that
    // an in-flight RPC blocked on a stuck executor cannot delay shutdown.
    final ExecutorService rpcPool = Executors.newFixedThreadPool(
      Math.min(numExecs, 16),
      runnable -> {
        final Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.setName("timeout-listener-executor-dump-" + t.hashCode());
        return t;
      });
    try {
      final CompletionService<ExecutorDumpResult> completionService =
        new ExecutorCompletionService<>(rpcPool);
      final List<Future<ExecutorDumpResult>> futures = new ArrayList<>(numExecs);
      // Iterate the Scala Seq directly via its Iterator to avoid the
      // scala.collection.JavaConverters deprecation in Scala 2.13.
      final Iterator<String> it = execIdsSeq.iterator();
      while (it.hasNext()) {
        final String execId = it.next();
        futures.add(completionService.submit(() -> {
          try {
            return new ExecutorDumpResult(execId, sc.getExecutorThreadDump(execId), null);
          } catch (Throwable t) {
            return new ExecutorDumpResult(execId, null, t);
          }
        }));
      }

      final long deadlineNanos = System.nanoTime()
        + TimeUnit.SECONDS.toNanos(EXECUTOR_DUMP_BUDGET_SECONDS);
      int remaining = numExecs;
      while (remaining > 0) {
        final long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          LOG.warn("Executor dump budget exhausted before receiving {} executor dump(s)",
            remaining);
          break;
        }
        try {
          final Future<ExecutorDumpResult> future =
            completionService.poll(remainingNanos, TimeUnit.NANOSECONDS);
          if (future == null) {
            LOG.warn("Timed out fetching thread dumps from {} executor(s)", remaining);
            break;
          }
          remaining--;
          final ExecutorDumpResult result = future.get();
          if (result.error != null) {
            LOG.warn("Failed to fetch thread dump for executor " + result.execId, result.error);
            continue;
          }
          if (result.dumpOpt.isEmpty()) {
            LOG.warn("No thread dump returned for executor {}", result.execId);
            continue;
          }
          final ThreadStackTrace[] stacks = result.dumpOpt.get();
          LOG.error("Executor {} thread dump ({} threads):", result.execId, stacks.length);
          for (ThreadStackTrace stack : stacks) {
            LOG.warn("Executor {}: {}", result.execId, stack);
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted fetching executor thread dumps", ie);
          break;
        } catch (ExecutionException ee) {
          LOG.warn("Failed to fetch an executor thread dump", ee.getCause());
        } catch (Throwable t) {
          LOG.warn("Failed to fetch an executor thread dump", t);
        }
      }
      for (Future<ExecutorDumpResult> future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    } finally {
      // shutdownNow interrupts the RPC threads so any still-blocking dumps
      // are abandoned rather than holding the driver JVM alive.
      rpcPool.shutdownNow();
    }
  }
}
