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

package com.nvidia.spark.rapids

import java.util.{Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Manager for coordinating shuffle cleanup between driver and executors.
 *
 * When a shuffle is unregistered on the driver, this manager:
 * 1. Records the shuffle ID for cleanup
 * 2. Responds to executor polls with the list of shuffles to clean up
 * 3. Posts SparkRapidsShuffleDiskSavingsEvent immediately when executor reports stats
 *
 * This solves the problem where shuffle data is stored in executor-side catalogs,
 * but unregisterShuffle is called on the driver. By using a polling mechanism,
 * executors can learn about shuffles that need cleanup and report their statistics.
 *
 * For information on aggregating SparkRapidsShuffleDiskSavingsEvent from eventlogs,
 * see docs/dev/shuffle-metrics.md.
 *
 * == Design Rationale: Why Pull (Polling) Instead of Push ==
 *
 * This implementation uses a pull model (executors poll driver) rather than a push model
 * (driver pushes to executors). Here's why:
 *
 * '''1. Spark Plugin API Limitation'''
 *
 * The Spark Plugin API only supports executor-to-driver communication:
 * {{{
 * // PluginContext (executor side)
 * def send(message: Any): Unit      // executor -> driver (fire-and-forget)
 * def ask(message: Any): AnyRef     // executor -> driver (request-response)
 *
 * // DriverPlugin (driver side)
 * def receive(message: Any): AnyRef // can only respond to executor requests
 * }}}
 *
 * There is NO `driver.sendToExecutor(executorId, message)` API available.
 *
 * '''2. Alternative: Spark's RemoveShuffle Mechanism'''
 *
 * Spark has a built-in RemoveShuffle mechanism triggered by GC:
 * {{{
 * ContextCleaner.doCleanupShuffle(shuffleId)
 *   -> mapOutputTrackerMaster.unregisterShuffle(shuffleId)  // clears map output info
 *   -> shuffleDriverComponents.removeShuffle(shuffleId)     // sends RemoveShuffle to executors
 * }}}
 *
 * This calls `shuffleManager.unregisterShuffle(shuffleId)` on each executor. However:
 * - It's GC-triggered, so timing is unpredictable (often too late for short jobs)
 * - Calling it manually risks clearing map output info too early, causing FetchFailedException
 * - It's one-way (no return value), so we'd still need Plugin RPC to report stats back
 *
 * '''3. Benefits of Pull Model'''
 *
 * - Uses standard Plugin API, no dependency on Spark internals
 * - Safe timing: we control when cleanup happens (after SQL execution ends)
 * - Naturally handles executor failures (failed executors just stop polling)
 * - No need to track executor list on driver side
 * - 1-second polling overhead is negligible
 *
 * @param sc SparkContext for posting events
 * @param staleEntryMaxAgeMs maximum age for pending entries before they are removed
 * @param cleanupIntervalMs interval for running stale entry cleanup
 */
class ShuffleCleanupManager(
    sc: SparkContext,
    staleEntryMaxAgeMs: Long = 300000,  // 5 minutes
    cleanupIntervalMs: Long = 60000     // 1 minute
) extends Logging {

  /**
   * Shuffles pending cleanup. Maps shuffleId -> timestamp when unregister was called.
   */
  private val pendingCleanup = new ConcurrentHashMap[Int, Long]()

  /**
   * Track which executors have reported stats for each shuffle.
   * Maps shuffleId -> Set of executorIds that have reported.
   * Used to avoid sending cleanup requests multiple times to the same executor.
   */
  private val reportedExecutors =
    new ConcurrentHashMap[Int, JSet[String]]()

  /**
   * Scheduled executor for periodic stale entry cleanup.
   */
  private val cleanupExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("shuffle-cleanup-manager")
        .setDaemon(true)
        .build())

  // Start periodic cleanup
  cleanupExecutor.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        try {
          cleanupStaleEntries(staleEntryMaxAgeMs)
        } catch {
          case e: Exception =>
            logWarning("Error during stale entry cleanup", e)
        }
      }
    },
    cleanupIntervalMs,
    cleanupIntervalMs,
    TimeUnit.MILLISECONDS)

  /**
   * Register a shuffle for cleanup. Called from driver when unregisterShuffle is invoked.
   *
   * @param shuffleId the shuffle ID to clean up
   */
  def registerForCleanup(shuffleId: Int): Unit = {
    logDebug(s"Registering shuffle $shuffleId for cleanup")
    pendingCleanup.put(shuffleId, System.currentTimeMillis())
    reportedExecutors.put(shuffleId, ConcurrentHashMap.newKeySet[String]())
  }

  /**
   * Handle poll from executor asking for shuffles to clean up.
   *
   * @param executorId the executor polling for work
   * @return list of shuffle IDs that need cleanup
   */
  def handlePoll(executorId: String): RapidsShuffleCleanupResponseMsg = {
    val shuffleIds = new ArrayBuffer[Int]()
    val iter = pendingCleanup.keySet().iterator()
    while (iter.hasNext) {
      val shuffleId = iter.next()
      val reported = reportedExecutors.get(shuffleId)
      // Only send to executors that haven't reported yet
      if (reported != null && !reported.contains(executorId)) {
        shuffleIds += shuffleId
      }
    }
    if (shuffleIds.nonEmpty) {
      logDebug(s"Sending ${shuffleIds.size} shuffles to executor $executorId for cleanup: " +
        s"${shuffleIds.mkString(", ")}")
    }
    RapidsShuffleCleanupResponseMsg(shuffleIds.toArray)
  }

  /**
   * Handle cleanup statistics from executor.
   * Immediately posts a SparkRapidsShuffleDiskSavingsEvent for each non-zero stat.
   *
   * @param executorId the executor reporting stats
   * @param stats cleanup statistics
   */
  def handleStats(executorId: String, stats: Array[ShuffleCleanupStats]): Unit = {
    stats.foreach { stat =>
      val shuffleId = stat.shuffleId
      logDebug(s"Received cleanup stats from executor $executorId for shuffle $shuffleId: " +
        s"bytesFromMemory=${stat.bytesFromMemory}, bytesFromDisk=${stat.bytesFromDisk}")

      // Mark this executor as having reported for this shuffle
      val reported = reportedExecutors.get(shuffleId)
      if (reported != null) {
        reported.add(executorId)
      }

      // Immediately emit event if there are non-zero bytes
      if (stat.bytesFromMemory > 0 || stat.bytesFromDisk > 0) {
        logDebug(s"Emitting SparkRapidsShuffleDiskSavingsEvent for shuffle $shuffleId " +
          s"from executor $executorId: " +
          s"bytesFromMemory=${stat.bytesFromMemory}, bytesFromDisk=${stat.bytesFromDisk}")

        try {
          TrampolineUtil.postEvent(sc,
            SparkRapidsShuffleDiskSavingsEvent(shuffleId, stat.bytesFromMemory, stat.bytesFromDisk,
              stat.numExpansions, stat.numSpills, stat.numForcedFileOnly))
        } catch {
          case e: Exception =>
            logWarning(s"Failed to post shuffle disk savings event for shuffle $shuffleId", e)
        }
      }
    }
  }

  /**
   * Cleanup stale entries that haven't been processed.
   * Called periodically to prevent memory leaks.
   *
   * @param maxAgeMs maximum age in milliseconds before an entry is considered stale
   */
  def cleanupStaleEntries(maxAgeMs: Long = 300000): Unit = {
    val now = System.currentTimeMillis()
    val iter = pendingCleanup.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val shuffleId = entry.getKey
      val timestamp = entry.getValue
      if (now - timestamp > maxAgeMs) {
        val reported = reportedExecutors.get(shuffleId)
        val numReported = if (reported != null) reported.size() else 0
        if (numReported > 0) {
          // At least some executors reported cleanup - this is normal, just metadata cleanup
          logDebug(s"Removing stale shuffle cleanup entry for shuffle $shuffleId " +
            s"(registered ${now - timestamp}ms ago, $numReported executor(s) reported cleanup)")
        } else {
          // No executor ever reported cleanup - potential resource leak
          logWarning(s"Removing stale shuffle cleanup entry for shuffle $shuffleId " +
            s"with NO executor cleanup reported (registered ${now - timestamp}ms ago). " +
            s"Shuffle resources may not have been cleaned up properly.")
        }
        iter.remove()
        reportedExecutors.remove(shuffleId)
      }
    }
  }

  /**
   * Shutdown the manager.
   */
  def shutdown(): Unit = {
    logInfo("Shutting down ShuffleCleanupManager")

    // Stop the cleanup executor
    cleanupExecutor.shutdown()
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        cleanupExecutor.shutdownNow()
    }

    pendingCleanup.clear()
    reportedExecutors.clear()
  }
}

object ShuffleCleanupManager {
  @volatile private var instance: ShuffleCleanupManager = _

  def init(sc: SparkContext): Unit = synchronized {
    if (instance == null) {
      instance = new ShuffleCleanupManager(sc)
    }
  }

  def get: ShuffleCleanupManager = instance

  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
    }
  }
}
