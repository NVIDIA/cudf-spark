/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.concurrent.{ConcurrentHashMap, Future, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import com.nvidia.spark.rapids.jni.Profiler
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.SerializableConfiguration

/**
 * For profiling with com.nvidia.spark.rapids.jni.Profiler
 */

object ProfilerOnExecutor extends Logging {
  private val jobPattern = raw"SPARK_.*_JId_([0-9]+).*".r
  private var writer: Option[ProfileWriter] = None
  private var timeRanges: Option[Seq[(Long, Long)]] = None
  private var jobRanges: RangeConfMatcher = null
  private var stageRanges: RangeConfMatcher = null
  // NOTE: Active sets are updated asynchronously, synchronize on ProfilerOnExecutor to access
  private val activeJobs = mutable.HashSet[Int]()
  private val activeStages = mutable.HashSet[Int]()
  private var timer: Option[ScheduledExecutorService] = None
  private var timerFuture: Option[Future[_]] = None
  private var driverPollMillis = 0
  private val startTimestamp = System.nanoTime()
  private var isProfileActive = false
  private var currentContextMethod: Method = null
  private var getContextMethod: Method = null
  private val stageTaskCount = mutable.HashMap[Int, Int]()
  private var stageTaskLimit = 0

  def init(pluginCtx: PluginContext, conf: RapidsConf): Unit = {
    require(writer.isEmpty, "Already initialized")
    timeRanges = conf.profileTimeRangesSeconds.map(parseTimeRanges)
    stageTaskLimit = conf.profileTaskLimitPerStage
    jobRanges = new RangeConfMatcher(conf, RapidsConf.PROFILE_JOBS)
    stageRanges = new RangeConfMatcher(conf, RapidsConf.PROFILE_STAGES)
    driverPollMillis = conf.profileDriverPollMillis
    if (timeRanges.isDefined && (stageRanges.nonEmpty || jobRanges.nonEmpty)) {
      throw new UnsupportedOperationException(
        "Profiling with time ranges and stage or job ranges simultaneously is not supported")
    }
    if (jobRanges.nonEmpty) {
      // Hadoop's CallerContext is used to identify the job ID of a task on the executor.
      val callerContextClass = TrampolineUtil.classForName("org.apache.hadoop.ipc.CallerContext")
      currentContextMethod = callerContextClass.getMethod("getCurrent")
      getContextMethod = callerContextClass.getMethod("getContext")
    }
    writer = conf.profilePath.flatMap { pathPrefix =>
      val executorId = pluginCtx.executorID()
      var profileWriter: ProfileWriter = null
      if (shouldProfile(executorId, conf)) {
        try {
          logInfo("Initializing profiler")
          if (jobRanges.nonEmpty) {
            // Need caller context enabled to get the job ID of a task on the executor
            TrampolineUtil.getSparkHadoopUtilConf.setBoolean("hadoop.caller.context.enabled", true)
          }
          val codec = conf.profileCompression match {
            case "none" => None
            case c => Some(TrampolineUtil.createCodec(pluginCtx.conf(), c))
          }
          profileWriter = new ProfileWriter(pluginCtx, pathPrefix, codec)
          val profilerConf = new Profiler.Config.Builder()
            .withWriteBufferSize(conf.profileWriteBufferSize)
            .withFlushPeriodMillis(conf.profileFlushPeriodMillis)
            .withAllocAsyncCapturing(conf.profileAsyncAllocCapture)
            .build()
          Profiler.init(profileWriter, profilerConf)
          Some(profileWriter)
        } catch {
          case l: Exception =>
            logWarning("Unable to launch profiler, we will abort profiling session.",l)
            pluginCtx.send(ProfileErrorMsg(executorId, s"error launching profiler: $l"))
            // failed to initialize, lets close the writer, and try to shutdown.
            if (profileWriter != null) {
              Profiler.shutdown()
              profileWriter.close()
            }
            None
        }
      } else {
        None
      }
    }
    writer.foreach { _ =>
      updateAndSchedule()
    }
  }

  def onTaskStart(): Unit = {
    if (jobRanges.nonEmpty) {
      val callerCtx = currentContextMethod.invoke(null)
      if (callerCtx != null) {
        getContextMethod.invoke(callerCtx).asInstanceOf[String] match {
          case jobPattern(jid) =>
            val jobId = jid.toInt
            if (jobRanges.contains(jobId)) {
              synchronized {
                activeJobs.add(jobId)
                enable()
                startPollingDriver()
              }
            }
          case _ =>
        }
      }
    }
    if (stageRanges.nonEmpty) {
      val taskCtx = TaskContext.get
      val stageId = taskCtx.stageId
      if (stageRanges.contains(stageId)) {
        if (stageTaskLimit <= 0) {
          // Unlimited tasks per stage
          synchronized {
            activeStages.add(taskCtx.stageId)
            enable()
            startPollingDriver()
          }
        } else {
          // Limited tasks per stage
          val currentCount = synchronized(stageTaskCount.getOrElseUpdate(stageId, 0))
          if (currentCount < stageTaskLimit) {
            synchronized {
              activeStages.add(taskCtx.stageId)
              enable()
              startPollingDriver()
            }
            taskCtx.addTaskCompletionListener[Unit] { _ =>
              synchronized {
                stageTaskCount.get(stageId) match {
                  case Some(count) if (count < stageTaskLimit) => {
                    stageTaskCount(stageId) = count + 1
                  }
                  case Some(_) => {
                    activeStages.remove(stageId)
                  }
                  case None =>
                }
              }
            }
          }
        }
      }
    }
  }

  def shutdown(): Unit = {
    writer.foreach { w =>
      timerFuture.foreach(_.cancel(false))
      timerFuture = None
      Profiler.shutdown()
      w.close()
    }
    writer = None
  }

  private def enable(): Unit = {
    writer.foreach { w =>
      if (!isProfileActive) {
        Profiler.start()
        isProfileActive = true
        w.pluginCtx.send(ProfileStatusMsg(w.executorId, "profile started"))
      }
    }
  }

  private def disable(): Unit = {
    writer.foreach { w =>
      if (isProfileActive) {
        Profiler.stop()
        isProfileActive = false
        w.pluginCtx.send(ProfileStatusMsg(w.executorId, "profile stopped"))
      }
    }
  }

  private def shouldProfile(executorId: String, conf: RapidsConf): Boolean = {
    val matcher = new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
    matcher.contains(executorId)
  }

  private def parseTimeRanges(confVal: String): Seq[(Long, Long)] = {
    val ranges = try {
      confVal.split(',').map(RangeConfMatcher.parseRange).map {
        case (start, end) =>
          // convert relative time in seconds to absolute time in nanoseconds
          (startTimestamp + TimeUnit.SECONDS.toNanos(start),
            startTimestamp + TimeUnit.SECONDS.toNanos(end))
      }
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid range settings for ${RapidsConf.PROFILE_TIME_RANGES_SECONDS}: $confVal", e)
    }
    ranges.sorted.toIndexedSeq
  }

  private def updateAndSchedule(): Unit = {
    if (timeRanges.isDefined) {
      if (timer.isEmpty) {
        timer = Some(TrampolineUtil.newDaemonSingleThreadScheduledExecutor("profiler timer"))
      }
      val now = System.nanoTime()
      // skip time ranges that have already passed
      val currentRanges = timeRanges.get.dropWhile {
        case (_, end) => end <= now
      }
      timeRanges = Some(currentRanges)
      if (currentRanges.isEmpty) {
        logWarning("No further time ranges to profile, shutting down")
        shutdown()
      } else {
        currentRanges.headOption.foreach {
          case (start, end) =>
            val delay = if (start <= now) {
              enable()
              end - now
            } else {
              disable()
              start - now
            }
            timerFuture = Some(timer.get.schedule(new Runnable {
              override def run(): Unit = try {
                updateAndSchedule()
              } catch {
                case e: Exception =>
                  logError(s"Error in profiler timer task", e)
              }
            }, delay, TimeUnit.NANOSECONDS))
        }
      }
    } else if (jobRanges.nonEmpty || stageRanges.nonEmpty) {
      // nothing to do yet, profiling will start when tasks for targeted job/stage are seen
    } else {
      enable()
    }
  }

  private def startPollingDriver(): Unit = {
    if (timerFuture.isEmpty) {
      if (timer.isEmpty) {
        timer = Some(TrampolineUtil.newDaemonSingleThreadScheduledExecutor("profiler timer"))
      }
      timerFuture = Some(timer.get.scheduleWithFixedDelay(() => try {
        updateActiveFromDriver()
      } catch {
        case e: Exception =>
          logError("Profiler timer task error: ", e)
      }, driverPollMillis, driverPollMillis, TimeUnit.MILLISECONDS))
    }
  }

  private def stopPollingDriver(): Unit = {
    timerFuture.foreach(_.cancel(false))
    timerFuture = None
  }

  private def updateActiveFromDriver(): Unit = {
    writer.foreach { w =>
      val (jobs, stages) = synchronized {
        (activeJobs.toArray, (activeStages ++ stageTaskCount.keys).toArray)
      }
      val (completedJobs, completedStages, allDone) =
        w.pluginCtx.ask(ProfileJobStageQueryMsg(jobs, stages))
          .asInstanceOf[(Array[Int], Array[Int], Boolean)]
      synchronized {
        completedJobs.foreach(activeJobs.remove)
        completedStages.foreach(activeStages.remove)
        completedStages.foreach(stageTaskCount.remove)
        if (activeJobs.isEmpty && activeStages.isEmpty) {
          disable()
          if (stageTaskCount.isEmpty) {
            stopPollingDriver()
          }
        }
      }
      if (allDone) {
        logWarning("No further jobs or stages to profile, shutting down")
        shutdown()
      }
    }
  }
}

class ProfileWriter(
    val pluginCtx: PluginContext,
    profilePathPrefix: String,
    codec: Option[CompressionCodec]) extends Profiler.DataWriter with Logging {
  val executorId: String = pluginCtx.executorID()
  private val outPath = getOutputPath(profilePathPrefix, codec)
  private val out = openOutput(codec)
  private var isClosed = false

  override def write(data: ByteBuffer): Unit = {
    if (!isClosed) {
      while (data.hasRemaining) {
        out.write(data)
      }
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      isClosed = true
      out.close()
      logWarning(s"Profiling completed, output written to $outPath")
      pluginCtx.send(ProfileEndMsg(executorId, outPath.toString))
    }
  }

  private def getAppId: String = {
    val appId = pluginCtx.conf.get("spark.app.id", "")
    if (appId.isEmpty) {
      java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    } else {
      appId
    }
  }

  private def getOutputPath(prefix: String, codec: Option[CompressionCodec]): Path = {
    val parentDir = new Path(prefix)
    val suffix = codec.map(c => "." + TrampolineUtil.getCodecShortName(c.getClass.getName))
      .getOrElse("")
    new Path(parentDir, s"rapids-profile-$getAppId-$executorId.bin$suffix")
  }

  private def openOutput(codec: Option[CompressionCodec]): WritableByteChannel = {
    logWarning(s"Profiler initialized, output will be written to $outPath")
    val hadoopConf = pluginCtx.ask(ProfileInitMsg(executorId, outPath.toString))
      .asInstanceOf[SerializableConfiguration].value
    val fs = outPath.getFileSystem(hadoopConf)
    val fsStream = fs.create(outPath, false)
    val outStream = codec.map(_.compressedOutputStream(fsStream)).getOrElse(fsStream)
    Channels.newChannel(outStream)
  }
}

object ProfilerOnDriver extends Logging {
  private var hadoopConf: SerializableConfiguration = null
  private var jobRanges: RangeConfMatcher = null
  private var numJobsToProfile: Long = 0L
  private var stageRanges: RangeConfMatcher = null
  private var numStagesToProfile: Long = 0L
  private val completedJobs = new ConcurrentHashMap[Int, Unit]()
  private val completedStages = new ConcurrentHashMap[Int, Unit]()
  private var isJobsStageProfilingComplete = false
  // did an executor fail to profile, if so we will log once
  private var profilerErrored: Boolean = false

  def init(sc: SparkContext, conf: RapidsConf): Unit = {
    // Even if conf.profilePath is not set, we still might need it in AsyncProfiler
    hadoopConf = new SerializableConfiguration(sc.hadoopConfiguration)

    // if no profile path, profiling is disabled and nothing to do
    conf.profilePath.foreach { _ =>
      jobRanges = new RangeConfMatcher(conf, RapidsConf.PROFILE_JOBS)
      stageRanges = new RangeConfMatcher(conf, RapidsConf.PROFILE_STAGES)
      if (jobRanges.nonEmpty || stageRanges.nonEmpty) {
        numJobsToProfile = jobRanges.size
        numStagesToProfile = stageRanges.size
        if (jobRanges.nonEmpty) {
          // Need caller context enabled to get the job ID of a task on the executor
          try {
            TrampolineUtil.classForName("org.apache.hadoop.ipc.CallerContext")
          } catch {
            case _: ClassNotFoundException =>
              throw new UnsupportedOperationException(s"${RapidsConf.PROFILE_JOBS} requires " +
                "Hadoop CallerContext which is unavailable.")
          }
          sc.getConf.set("hadoop.caller.context.enabled", "true")
        }
        sc.addSparkListener(Listener)
      }
    }
  }

  def handleMsg(m: ProfileMsg): AnyRef = m match {
    case ProfileInitMsg(executorId, path) =>
      logWarning(s"Profiling: Executor $executorId initialized profiler, writing to $path")
      if (hadoopConf == null) {
        throw new IllegalStateException("Hadoop configuration not set")
      }
      hadoopConf
    case ProfileErrorMsg(executorId, msg) =>
      if (profilerErrored) {
        logDebug(s"Profiling: Error starting profiler from $executorId: $msg")
      } else {
        logError(s"Profiling: Error starting profiler from $executorId: $msg. Suppressing others.")
      }
      profilerErrored = true
      null
    case ProfileStatusMsg(executorId, msg) =>
      logWarning(s"Profiling: Executor $executorId: $msg")
      null
    case ProfileJobStageQueryMsg(activeJobs, activeStages) =>
      val filteredJobs = activeJobs.filter(j => completedJobs.containsKey(j))
      val filteredStages = activeStages.filter(s => completedStages.containsKey(s))
      (filteredJobs, filteredStages, isJobsStageProfilingComplete)
    case ProfileEndMsg(executorId, path) =>
      logWarning(s"Profiling: Executor $executorId ended profiling, profile written to $path")
      null
    case _ =>
      throw new IllegalStateException(s"Unexpected profile msg: $m")
  }

  private object Listener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      val jobId = jobEnd.jobId
      if (jobRanges.contains(jobId)) {
        completedJobs.putIfAbsent(jobId, ())
        isJobsStageProfilingComplete = completedJobs.size == numJobsToProfile &&
          completedStages.size == numStagesToProfile
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      if (stageRanges.contains(stageId)) {
        completedStages.putIfAbsent(stageId, ())
        isJobsStageProfilingComplete = completedJobs.size == numJobsToProfile &&
          completedStages.size == numStagesToProfile
      }
    }
  }
}

trait ProfileMsg

case class ProfileInitMsg(executorId: String, path: String) extends ProfileMsg
case class ProfileStatusMsg(executorId: String, msg: String) extends ProfileMsg
case class ProfileErrorMsg(executorId: String, msg: String) extends ProfileMsg
case class ProfileEndMsg(executorId: String, path: String) extends ProfileMsg

// Reply is a tuple of:
// - array of jobs that have completed
// - array of stages that have completed
// - boolean if there are no further jobs/stages to profile
case class ProfileJobStageQueryMsg(
    activeJobs: Array[Int],
    activeStages: Array[Int]) extends ProfileMsg
