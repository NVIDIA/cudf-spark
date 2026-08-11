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

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuShuffleEnv

/**
 * Executor-side endpoint for shuffle cleanup coordination.
 *
 * This endpoint periodically polls the driver for shuffles that need cleanup,
 * performs the cleanup on the executor's MultithreadedShuffleBufferCatalog,
 * and reports statistics back to the driver.
 *
 * @param pluginContext the plugin context for RPC communication
 * @param pollIntervalMs interval between polls to driver (in milliseconds)
 */
class ShuffleCleanupEndpoint(
    pluginContext: PluginContext,
    pollIntervalMs: Long = 1000) extends Logging with AutoCloseable {

  private val executorId: String = pluginContext.executorID()

  private val executorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(
      GpuDeviceManager.wrapThreadFactory(
        new ThreadFactoryBuilder()
          .setNameFormat("rapids-shuffle-cleanup")
          .setDaemon(true)
          .build(),
        null,
        () => RmmSpark.removeAllCurrentThreadAssociation()))

  private val pollTask = new Runnable {
    override def run(): Unit = {
      try {
        pollAndCleanup()
      } catch {
        case e: Exception =>
          logWarning("Error during shuffle cleanup poll", e)
      }
    }
  }

  @volatile private var shutdownHookAdded = false
  @volatile private var closed = false

  /**
   * Start the periodic polling for shuffle cleanup.
   */
  def start(): Unit = {
    logInfo(s"Starting ShuffleCleanupEndpoint on executor $executorId " +
      s"with poll interval ${pollIntervalMs}ms")
    executorService.scheduleWithFixedDelay(
      pollTask,
      pollIntervalMs, // initial delay
      pollIntervalMs,
      TimeUnit.MILLISECONDS)
    
    // Add shutdown hook to ensure finalCleanup is called on JVM termination
    synchronized {
    if (!shutdownHookAdded) {
      shutdownHookAdded = true
      Runtime.getRuntime.addShutdownHook(new Thread("rapids-shuffle-cleanup-shutdown") {
        override def run(): Unit = {
          if (!closed) {
            logInfo("Shutdown hook triggered, performing final cleanup")
            try {
              finalCleanup()
            } catch {
              case e: Exception =>
                logWarning("Error during shutdown hook cleanup", e)
            }
          }
        }
      })
      }
    }
  }

  /**
   * Poll driver for shuffles to clean up and process them.
   */
  private def pollAndCleanup(): Unit = {
    // Ask driver for shuffles that need cleanup
    val response = pluginContext.ask(RapidsShuffleCleanupPollMsg(executorId))

    response match {
      case RapidsShuffleCleanupResponseMsg(shuffleIds) if shuffleIds.nonEmpty =>
        logDebug(s"Received ${shuffleIds.length} shuffles to clean up: " +
          s"${shuffleIds.mkString(", ")}")
        processCleanup(shuffleIds)

      case RapidsShuffleCleanupResponseMsg(_) =>
        // No shuffles to clean up
        logTrace("No shuffles to clean up")

      case other =>
        logWarning(s"Unexpected response from driver: $other")
    }
  }

  /**
   * Process cleanup for the given shuffle IDs and report statistics.
   */
  private def processCleanup(shuffleIds: Array[Int]): Unit = {
    val catalog = GpuShuffleEnv.getMultithreadedCatalog
    if (catalog.isEmpty) {
      logDebug("MultithreadedShuffleBufferCatalog not available, skipping cleanup")
      // Still report empty stats so driver knows we processed the request
      val emptyStats = shuffleIds.map(id => ShuffleCleanupStats(id, 0, 0))
      reportStats(emptyStats)
      return
    }

    val mtCatalog = catalog.get
    val statsBuffer = new ArrayBuffer[ShuffleCleanupStats]()

    shuffleIds.foreach { shuffleId =>
      try {
        val stats = mtCatalog.unregisterShuffle(shuffleId)
        stats match {
          case Some(s) =>
            logDebug(s"Cleaned up shuffle $shuffleId: " +
              s"bytesFromMemory=${s.bytesFromMemory}, bytesFromDisk=${s.bytesFromDisk}")
            statsBuffer += s
          case None =>
            // This executor didn't have data for this shuffle
            logDebug(s"No data for shuffle $shuffleId on this executor")
            // Report zero stats so driver knows we processed it
            statsBuffer += ShuffleCleanupStats(shuffleId, 0, 0)
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to clean up shuffle $shuffleId", e)
          // Report zero stats on error
          statsBuffer += ShuffleCleanupStats(shuffleId, 0, 0)
      }
    }

    if (statsBuffer.nonEmpty) {
      reportStats(statsBuffer.toArray)
    }
  }

  /**
   * Report cleanup statistics to driver.
   */
  private def reportStats(stats: Array[ShuffleCleanupStats]): Unit = {
    try {
      val nonZeroStats = stats.filter(s => s.bytesFromMemory > 0 || s.bytesFromDisk > 0)
      if (nonZeroStats.nonEmpty) {
        logDebug(s"Reporting cleanup stats to driver for ${nonZeroStats.length} shuffle(s)")
      }
      pluginContext.send(RapidsShuffleCleanupStatsMsg(executorId, stats))
    } catch {
      case e: Exception =>
        logWarning("Failed to report cleanup stats to driver", e)
    }
  }

  override def close(): Unit = {
    if (closed) {
      return
    }
    closed = true
    logInfo(s"Shutting down ShuffleCleanupEndpoint on executor $executorId")

    // Stop the scheduled polling first
    executorService.shutdown()

    // Perform a final cleanup of any remaining data in the catalog
    // This ensures no data is left behind when executor shuts down
    try {
      finalCleanup()
    } catch {
      case e: Exception =>
        logWarning("Error during final shuffle cleanup", e)
    }

    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        executorService.shutdownNow()
    }
  }

  /**
   * Perform final cleanup of all shuffles in the catalog.
   * Called during executor shutdown to ensure no data is left behind.
   */
  private def finalCleanup(): Unit = {
    val catalog = GpuShuffleEnv.getMultithreadedCatalog
    if (catalog.isEmpty) {
      return
    }

    val mtCatalog = catalog.get
    // Get all active shuffle IDs and clean them up
    val activeShuffleIds = mtCatalog.getActiveShuffleIds
    if (activeShuffleIds.isEmpty) {
      logDebug("No active shuffles to clean up during shutdown")
      return
    }

    logInfo(s"Final cleanup: cleaning ${activeShuffleIds.size} active shuffle(s)")
    val statsBuffer = new ArrayBuffer[ShuffleCleanupStats]()

    activeShuffleIds.foreach { shuffleId =>
      try {
        val stats = mtCatalog.unregisterShuffle(shuffleId)
        stats.foreach { s =>
          logDebug(s"Final cleanup: shuffle $shuffleId - " +
            s"bytesFromMemory=${s.bytesFromMemory}, bytesFromDisk=${s.bytesFromDisk}")
          statsBuffer += s
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to clean up shuffle $shuffleId during shutdown", e)
      }
    }

    // Report final stats to driver (best effort)
    if (statsBuffer.nonEmpty) {
      val nonZeroStats = statsBuffer.filter(s => s.bytesFromMemory > 0 || s.bytesFromDisk > 0)
      if (nonZeroStats.nonEmpty) {
        try {
          logInfo(s"Reporting final cleanup stats for ${nonZeroStats.size} shuffle(s)")
          pluginContext.send(RapidsShuffleCleanupStatsMsg(executorId, nonZeroStats.toArray))
        } catch {
          case e: Exception =>
            logWarning("Failed to report final cleanup stats to driver", e)
        }
      }
    }
  }
}

