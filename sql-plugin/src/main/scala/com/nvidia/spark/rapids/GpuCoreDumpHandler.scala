/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.io.{File, PrintWriter}
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.NullOutputStreamShim
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.StringBuilderWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.SerializableConfiguration

object GpuCoreDumpHandler extends Logging {
  private var executor: Option[ExecutorService] = None
  private var dumpedPath: Option[String] = None
  private var namedPipeFile: File = _
  private var isDumping: Boolean = false

  /**
   * Configures the executor launch environment for GPU core dumps, if applicable.
   * Should only be called from the driver on driver startup.
   */
  def driverInit(sc: SparkContext, conf: RapidsConf): Unit = {
    // This only works in practice on Spark standalone clusters. It's too late to influence the
    // executor environment for Spark-on-YARN or Spark-on-k8s.
    // TODO: Leverage CUDA 12.1 core dump APIs in the executor to programmatically set this up
    //       on executor startup. https://github.com/NVIDIA/spark-rapids/issues/9370
    conf.gpuCoreDumpDir.foreach { _ =>
      TrampolineUtil.setExecutorEnv(sc, "CUDA_ENABLE_COREDUMP_ON_EXCEPTION", "1")
      TrampolineUtil.setExecutorEnv(sc, "CUDA_ENABLE_CPU_COREDUMP_ON_EXCEPTION", "0")
      TrampolineUtil.setExecutorEnv(sc, "CUDA_ENABLE_LIGHTWEIGHT_COREDUMP",
        if (conf.isGpuCoreDumpFull) "0" else "1")
      TrampolineUtil.setExecutorEnv(sc, "CUDA_COREDUMP_FILE", conf.gpuCoreDumpPipePattern)
      TrampolineUtil.setExecutorEnv(sc, "CUDA_COREDUMP_SHOW_PROGRESS", "1")
    }
  }

  /**
   * Sets up the GPU core dump background copy thread, if applicable.
   * Should only be called from the executor on executor startup.
   */
  def executorInit(rapidsConf: RapidsConf, pluginCtx: PluginContext): Unit = {
    rapidsConf.gpuCoreDumpDir.foreach { dumpDir =>
      namedPipeFile = createNamedPipe(rapidsConf)
      executor = Some(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("gpu-core-copier")
        .setDaemon(true)
        .build()))
      executor.foreach { exec =>
        val codec = if (rapidsConf.isGpuCoreDumpCompressed) {
          Some(TrampolineUtil.createCodec(pluginCtx.conf(),
            rapidsConf.gpuCoreDumpCompressionCodec))
        } else {
          None
        }
        val suffix = codec.map { c =>
          "." + TrampolineUtil.getCodecShortName(c.getClass.getName)
        }.getOrElse("")
        exec.submit(new Runnable {
          override def run(): Unit = {
            try {
              copyLoop(pluginCtx, namedPipeFile, new Path(dumpDir), codec, suffix)
            } catch {
              case _: InterruptedException => logInfo("Stopping GPU core dump copy thread")
              case t: Throwable => logWarning("Error in GPU core dump copy thread", t)
            }
          }
        })
      }
    }
  }

  /**
   * Wait for a GPU dump in progress, if any, to complete
   * @param timeoutSecs maximum amount of time to wait before returning
   * @return true if the wait timedout, false otherwise
   */
  def waitForDump(timeoutSecs: Int): Boolean = {
    val endTime = System.nanoTime + TimeUnit.SECONDS.toNanos(timeoutSecs)
    while (isDumping && System.nanoTime < endTime) {
      Thread.sleep(10)
    }
    System.nanoTime < endTime
  }

  def shutdown(): Unit = {
    executor.foreach { exec =>
      exec.shutdownNow()
      executor = None
      namedPipeFile.delete()
      namedPipeFile = null
    }
  }

  def handleMsg(msg: GpuCoreDumpMsg): AnyRef = msg match {
    case GpuCoreDumpMsgStart(executorId, dumpPath) =>
      logError(s"Executor $executorId starting a GPU core dump to $dumpPath")
      val spark = SparkSession.active
      new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    case GpuCoreDumpMsgCompleted(executorId, dumpPath) =>
      logError(s"Executor $executorId wrote a GPU core dump to $dumpPath")
      null
    case GpuCoreDumpMsgFailed(executorId, error) =>
      logError(s"Executor $executorId failed to write a GPU core dump: $error")
      null
    case m =>
      throw new IllegalStateException(s"Unexpected GPU core dump msg: $m")
  }

  // visible for testing
  def getNamedPipeFile: File = namedPipeFile

  private def createNamedPipe(conf: RapidsConf): File = {
    val processName = ManagementFactory.getRuntimeMXBean.getName
    val pidstr = processName.substring(0, processName.indexOf("@"))
    val pipePath = conf.gpuCoreDumpPipePattern.replace("%p", pidstr)
    val pipeFile = new File(pipePath)
    val mkFifoProcess = Runtime.getRuntime.exec(Array("mkfifo", "-m", "600", pipeFile.toString))
    require(mkFifoProcess.waitFor(10, TimeUnit.SECONDS), "mkfifo timed out")
    pipeFile.deleteOnExit()
    pipeFile
  }

  private def copyLoop(
      pluginCtx: PluginContext,
      namedPipe: File,
      dumpDirPath: Path,
      codec: Option[CompressionCodec],
      suffix: String): Unit = {
    val executorId = pluginCtx.executorID()
    try {
      logInfo(s"Monitoring ${namedPipe.getAbsolutePath} for GPU core dumps")
      withResource(new java.io.FileInputStream(namedPipe)) { in =>
        isDumping = true
        val appId = pluginCtx.conf.get("spark.app.id")
        val dumpPath = new Path(dumpDirPath,
          s"gpucore-$appId-$executorId.nvcudmp$suffix")
        logError(s"Generating GPU core dump at $dumpPath")
        val hadoopConf = pluginCtx.ask(GpuCoreDumpMsgStart(executorId, dumpPath.toString))
          .asInstanceOf[SerializableConfiguration].value
        val dumpFs = dumpPath.getFileSystem(hadoopConf)
        val bufferSize = hadoopConf.getInt("io.file.buffer.size", 4096)
        val perms = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE)
        val fsOut = dumpFs.create(dumpPath, perms, false, bufferSize,
          dumpFs.getDefaultReplication(dumpPath), dumpFs.getDefaultBlockSize(dumpPath), null)
        val out = closeOnExcept(fsOut) { _ =>
          codec.map(_.compressedOutputStream(fsOut)).getOrElse(fsOut)
        }
        withResource(out) { _ =>
          IOUtils.copy(in, out)
        }
        dumpedPath = Some(dumpPath.toString)
        pluginCtx.send(GpuCoreDumpMsgCompleted(executorId, dumpedPath.get))
      }
    } catch {
      case e: Exception =>
        logError("Error copying GPU dump", e)
        val writer = new StringBuilderWriter()
        e.printStackTrace(new PrintWriter(writer))
        pluginCtx.send(GpuCoreDumpMsgFailed(executorId, s"$e\n${writer.toString}"))
    } finally {
      isDumping = false
    }
    // Always drain the pipe to avoid blocking the thread that triggers the coredump
    while (namedPipe.exists()) {
      Files.copy(namedPipe.toPath, NullOutputStreamShim.INSTANCE)
    }
  }
}
