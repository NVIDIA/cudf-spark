/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.zip.GZIPOutputStream

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.Arm.withResource
import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

/**
 * Unlike ProfilerOnExecutor, which is used to create Nsys reports, this class is used to create
 * flame graph to understand where time is spent for each stage. Typically, we can use metrics to
 * achieve the same goal, but metrics cannot guarantee to capture all the time spent in a stage,
 * and it may not be detailed enough because it lacks the stacktraces information.
 *
 * From the perspective of profiling, we know that different stages may exhibit different patterns.
 * Since the output is a FUSED flame graph, it is not possible to distinguish between different
 * stages or threads, so the profiling is done per stage, and the output file is named
 * after each stage.
 *
 * It is strongly recommended to set 'spark.scheduler.mode' to 'FIFO' for better stage
 * separation. However, even with FIFO scheduling, we cannot guarantee perfect stage boundaries
 * due to various factors like out-of-order submissions, task failures, stragglers, etc.
 * To handle these cases, this implementation uses a task-count-based approach to determine stage
 * epochs. We periodically check which stage has the most running tasks (and more than 50% of total
 * tasks) to determine the current profiling epoch. This approach helps avoid profiling
 * anomalies when multiple stages have concurrent tasks, ensuring more accurate per-stage
 * flame graphs.
 *
 */

object AsyncProfilerOnExecutor extends Logging {

  private var asyncProfilerPrefix: Option[String] = None
  private var asyncProfiler: Option[AsyncProfiler] = None
  private var pluginCtx: PluginContext = _
  private var profileOptions: String = _
  private var currentProfilingStage = -1
  private var jfrCompressionEnabled: Boolean = false
  private var stageEpochInterval: Int = 5 // seconds

  private var needMoveFile: Boolean = false // true when `asyncProfilerPathPrefix` is non-local
  private var tempFilePath: java.nio.file.Path = _

  // Task tracking for stage epoch determination
  private val runningTasks = new ConcurrentHashMap[Long, Int]() // taskId -> stageId
  private var epochScheduler: Option[ScheduledExecutorService] = None
  private var epochTask: Option[ScheduledFuture[_]] = None
  private var isShutdown = false
  
  // Epoch tracking for same stage multiple executions
  private val stageEpochCounters = new ConcurrentHashMap[Int, Int]() // stageId -> epochCount

  def init(ctx: PluginContext, conf: RapidsConf): Unit = {
    pluginCtx = ctx
    asyncProfilerPrefix = conf.asyncProfilerPathPrefix
    // Only initialize the profiler if the prefix is non-empty
    asyncProfilerPrefix.foreach(prefix => {

      val executorId = pluginCtx.executorID()
      asyncProfiler = if (shouldProfile(executorId, conf) && AsyncProfilerLoader.isSupported) {
        Some(AsyncProfilerLoader.load())
      } else {
        None
      }

      asyncProfiler.foreach(_ => {
        if (prefix.startsWith("/")) {
          // like /tmp
          needMoveFile = false
        } else {
          // like hdfs://namenode:port/tmp or file:///tmp
          needMoveFile = true
        }

        this.profileOptions = conf.asyncProfilerProfileOptions
        this.jfrCompressionEnabled = conf.asyncProfilerJfrCompression
        this.stageEpochInterval = conf.asyncProfilerStageEpochInterval

        // Start the epoch scheduler
        startEpochScheduler()
      })
    })
  }


  def onTaskStart(): Unit = {
    asyncProfiler.foreach(_ => {
      val taskContext = TaskContext.get
      val stageId = taskContext.stageId()
      val taskId = taskContext.taskAttemptId()
      
      // Track this task
      runningTasks.put(taskId, stageId)
      
      // Add task completion listener to clean up tracking
      taskContext.addTaskCompletionListener[Unit] { _ =>
        runningTasks.remove(taskId)
      }
      
      log.debug(s"Task $taskId from stage $stageId started, " +
        s"total running tasks: ${runningTasks.size()}")
    })
  }

  def shutdown(): Unit = {
    isShutdown = true
    stopEpochScheduler()
    closeLastProfiler()
  }


  private def getAppId: String = {
    val appId = pluginCtx.conf.get("spark.app.id", "")
    if (appId.isEmpty) {
      java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    } else {
      appId
    }
  }

  /**
   * Starts the epoch scheduler that periodically determines the current stage epoch
   * based on running task counts.
   */
  private def startEpochScheduler(): Unit = {
    if (epochScheduler.isEmpty && stageEpochInterval > 0) {
      val scheduler = Executors.newSingleThreadScheduledExecutor(r => {
        val thread = new Thread(r, "AsyncProfiler-EpochScheduler")
        thread.setDaemon(true)
        thread
      })
      epochScheduler = Some(scheduler)
      
      val task = scheduler.scheduleAtFixedRate(
        () => {
          try {
            determineAndSwitchStageEpoch()
          } catch {
            case e: Exception =>
              log.error("Error in epoch scheduler", e)
          }
        },
        stageEpochInterval, // initial delay
        stageEpochInterval, // period
        TimeUnit.SECONDS
      )
      epochTask = Some(task)
      
      log.info(s"Started stage epoch scheduler with interval ${stageEpochInterval}s")
    }
  }
  
  /**
   * Stops the epoch scheduler.
   */
  private def stopEpochScheduler(): Unit = {
    epochTask.foreach(_.cancel(false))
    epochTask = None
    
    epochScheduler.foreach { scheduler =>
      scheduler.shutdown()
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          scheduler.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
    epochScheduler = None
  }
  
  /**
   * Determines which stage should be the current epoch based on running task counts
   * and switches profiling if necessary. The dominant stage must have more than
   * half of all running tasks to trigger a switch.
   */
  private def determineAndSwitchStageEpoch(): Unit = {
    if (isShutdown || asyncProfiler.isEmpty) {
      return
    }
    
    // Count running tasks per stage
    val stageTaskCounts = mutable.HashMap[Int, Int]()
    runningTasks.values().asScala.foreach { stageId =>
      stageTaskCounts(stageId) = stageTaskCounts.getOrElse(stageId, 0) + 1
    }
    
    if (stageTaskCounts.isEmpty) {
      log.debug("No running tasks, keeping current profiling stage")
      return
    }
    
    val totalTasks = stageTaskCounts.values.sum
    val (dominantStage, taskCount) = stageTaskCounts.maxBy(_._2)
    val dominantRatio = taskCount.toDouble / totalTasks
    
    log.debug(s"Stage task counts: ${stageTaskCounts.toMap}, " +
      s"dominant stage: $dominantStage with $taskCount/$totalTasks tasks " +
      s"(${(dominantRatio * 100).toInt}%)")
    
    // Only switch if the dominant stage has more than half of all running tasks
    if (dominantRatio > 0.5) {
      // Switch to the dominant stage if it's different from current
      if (dominantStage != currentProfilingStage) {
        asyncProfiler.synchronized {
          if (dominantStage != currentProfilingStage && !isShutdown) {
            log.info(s"Switching profiling from stage $currentProfilingStage " +
              s"to stage $dominantStage (has $taskCount/$totalTasks tasks, " +
              s"${(dominantRatio * 100).toInt}%)")
            
            closeLastProfiler()
            currentProfilingStage = dominantStage
            startProfilingForStage(dominantStage)
          }
        }
      }
    } else {
      log.debug(s"Dominant stage $dominantStage has only " +
        s"${(dominantRatio * 100).toInt}% of tasks, not switching " +
        s"(requires >50%)")
    }
  }
  
  /**
   * Starts profiling for the specified stage.
   */
  private def startProfilingForStage(stageId: Int): Unit = {
    asyncProfiler.foreach(profiler => {
      try {
        // Get current epoch (starting from 0) and increment for next time
        val currentEpoch = stageEpochCounters.compute(stageId, (_, currentCount) => {
          currentCount + 1
        }) - 1
        
        val filePath = {
          if (needMoveFile) {
            // if the asyncProfilerPathPrefix is non-local, we first write to a temp file
            // then move it to the final location
            tempFilePath = Files.createTempFile("async-profiler-temp-jfr", null)
            tempFilePath.toString
          } else {
            // if the path is local, we can just use it directly
            val parentPath: java.nio.file.Path = Paths.get(asyncProfilerPrefix.get)
            parentPath.resolve(
              s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                s"-stage-$stageId-epoch-$currentEpoch.jfr").toString
          }
        }
        profiler.execute(s"start,$profileOptions,file=$filePath")
        log.info(s"Successfully started profiling for stage $stageId epoch $currentEpoch")
      } catch {
        case e: Exception =>
          log.error(s"Error starting profiling for stage $stageId", e)
      }
    })
  }

  private def shouldProfile(executorId: String, conf: RapidsConf): Boolean = {
    // special support for "*", which means profile all executors
    if ("*".equals(conf.asyncProfilerExecutors)) {
      return true
    }

    val matcher = new RangeConfMatcher(conf, RapidsConf.ASYNC_PROFILER_EXECUTORS)
    matcher.contains(executorId)
  }

  /**
   * Compresses a JFR file using GZIP compression.
   *
   * @param inputPath  Path to the input JFR file
   * @param outputPath Path to the compressed output file (will have .gz extension)
   * @return true if compression was successful, false otherwise
   */
  private def compressJfrFile(
      inputPath: java.nio.file.Path, outputPath: java.nio.file.Path): Boolean = {
    try {
      val buffer = new Array[Byte](8192)
      withResource(new BufferedInputStream(new FileInputStream(inputPath.toFile))) { in =>
        withResource(new BufferedOutputStream(new GZIPOutputStream(
          new FileOutputStream(outputPath.toFile)))) { out =>
          var bytesRead = in.read(buffer)
          while (bytesRead != -1) {
            out.write(buffer, 0, bytesRead)
            bytesRead = in.read(buffer)
          }
        }
      }
      log.debug(s"Successfully compressed JFR file from $inputPath to $outputPath")
      true
    } catch {
      case e: Exception =>
        log.error(s"Failed to compress JFR file from $inputPath to $outputPath", e)
        false
    }
  }

  private def closeLastProfiler(): Unit = {
    asyncProfiler.foreach(profiler => {
      asyncProfiler.synchronized {
        if (currentProfilingStage != -1) { // for the first stage, skip stopping
          try {
            log.info(s"stop profiling for stage $currentProfilingStage")
            profiler.execute("stop")
            if (needMoveFile) {
              val executorId = pluginCtx.executorID()
              val currentEpoch = Option(stageEpochCounters.get(currentProfilingStage))
                .map(_ - 1)
                .getOrElse {
                  log.warn(s"Stage $currentProfilingStage not found in epoch counters, " +
                    s"using epoch 0")
                  0
                }

              val baseFileName = s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                s"-stage-$currentProfilingStage-epoch-$currentEpoch.jfr"
              val outPath = new Path(asyncProfilerPrefix.get, 
                if (jfrCompressionEnabled) baseFileName + ".gz" else baseFileName)
              
              val hadoopConf = pluginCtx.ask(ProfileInitMsg(executorId, outPath.toString))
                .asInstanceOf[SerializableConfiguration].value
              val fs = outPath.getFileSystem(hadoopConf)

              if (jfrCompressionEnabled) {
                // Compress the temp file first
                val compressedTempPath = Files.createTempFile("compressed-jfr", ".gz")
                if (compressJfrFile(tempFilePath, compressedTempPath)) {
                  withResource(fs.create(outPath, false)) { out =>
                    Files.copy(compressedTempPath, out)
                  }
                  compressedTempPath.toFile.delete() // delete compressed temp file
                } else {
                  // If compression fails, copy the original file
                  log.warn("JFR compression failed, copying uncompressed file")
                  withResource(
                    fs.create(new Path(asyncProfilerPrefix.get, baseFileName), false)) { out =>
                    Files.copy(tempFilePath, out)
                  }
                }
              } else {
                withResource(fs.create(outPath, false)) { out =>
                  Files.copy(tempFilePath, out)
                }
              }
              tempFilePath.toFile.delete() // delete the original temp file after moving
            } else {
              // For local files, compress in place if enabled
              if (jfrCompressionEnabled) {
                val currentEpoch = Option(stageEpochCounters.get(currentProfilingStage))
                  .map(_ - 1)
                  .getOrElse {
                    log.warn(s"Stage $currentProfilingStage not found in epoch counters, " +
                      s"using epoch 0")
                    0
                  }
                val originalPath = Paths.get(asyncProfilerPrefix.get).resolve(
                  s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                    s"-stage-$currentProfilingStage-epoch-$currentEpoch.jfr")
                val compressedPath = Paths.get(originalPath.toString + ".gz")
                
                if (compressJfrFile(originalPath, compressedPath)) {
                  originalPath.toFile.delete() // delete original file after successful compression
                } else {
                  log.warn("JFR compression failed, keeping uncompressed file")
                }
              }
            }
            log.info(s"successfully stopped profiling stage $currentProfilingStage")
          } catch {
            case e: Exception =>
              log.error(s"error stopping profiling for stage $currentProfilingStage", e)
          }
        }
      }
    })
  }
}



