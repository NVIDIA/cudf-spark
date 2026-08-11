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

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream, InputStream, IOException}
import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{timeout, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.SerializableConfiguration

class GpuCoreDumpHandlerSuite extends AnyFunSuite {
  private val WAIT_MSECS = TimeUnit.SECONDS.toMillis(10)
  private val APP_ID = "app-1234-567"
  private val EXECUTOR_ID = "0"
  private val FAKE_DUMP_SIZE = 1024*1024L
  private val FAKE_DUMP_BYTES = "abc123".getBytes
  private var pipeNum = 0

  test("test no dump handler by default") {
    val sparkConf = new SparkConf(false)
    val mockCtx = buildMockCtx(sparkConf)
    val rapidsConf = new RapidsConf(sparkConf)
    GpuCoreDumpHandler.executorInit(rapidsConf, mockCtx)
    try {
      assertResult(null)(GpuCoreDumpHandler.getNamedPipeFile)
    } finally {
      GpuCoreDumpHandler.shutdown()
    }
  }

  test("test dump handler enabled no dump") {
    val sparkConf = buildSparkConf()
      .set(RapidsConf.GPU_COREDUMP_DIR.key, "/tmp")
    val mockCtx = buildMockCtx(sparkConf)
    val rapidsConf = new RapidsConf(sparkConf)
    GpuCoreDumpHandler.executorInit(rapidsConf, mockCtx)
    val namedPipeFile = GpuCoreDumpHandler.getNamedPipeFile
    try {
      assert(waitForIt(WAIT_MSECS)(() => namedPipeFile.exists()))
    } finally {
      GpuCoreDumpHandler.shutdown()
    }
    assert(waitForIt(WAIT_MSECS)(() => !namedPipeFile.exists()))
  }

  test("test dump handler enabled dump") {
    val sparkConf = buildSparkConf()
      .set(RapidsConf.GPU_COREDUMP_DIR.key, "/tmp")
    val mockCtx = buildMockCtx(sparkConf)
    val rapidsConf = new RapidsConf(sparkConf)
    GpuCoreDumpHandler.executorInit(rapidsConf, mockCtx)
    val namedPipeFile = GpuCoreDumpHandler.getNamedPipeFile
    try {
      assert(waitForIt(WAIT_MSECS)(() => namedPipeFile.exists()))
      fakeDump(namedPipeFile)
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).ask(any[GpuCoreDumpMsgStart]())
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).send(any[GpuCoreDumpMsgCompleted]())
      val dumpFile = new File(s"/tmp/gpucore-$APP_ID-$EXECUTOR_ID.nvcudmp.zstd")
      assert(dumpFile.exists())
      try {
        val codec = TrampolineUtil.createCodec(sparkConf, "zstd")
        verifyDump(codec.compressedInputStream(new FileInputStream(dumpFile)))
      } finally {
        dumpFile.delete()
      }
    } finally {
      GpuCoreDumpHandler.shutdown()
    }
    assert(waitForIt(WAIT_MSECS)(() => !namedPipeFile.exists()))
  }

  test("test dump handler enabled dump no codec") {
    val sparkConf = buildSparkConf()
      .set(RapidsConf.GPU_COREDUMP_DIR.key, "file:/tmp")
      .set(RapidsConf.GPU_COREDUMP_COMPRESS.key, "false")
    val mockCtx = buildMockCtx(sparkConf)
    val rapidsConf = new RapidsConf(sparkConf)
    GpuCoreDumpHandler.executorInit(rapidsConf, mockCtx)
    val namedPipeFile = GpuCoreDumpHandler.getNamedPipeFile
    try {
      assert(waitForIt(WAIT_MSECS)(() => namedPipeFile.exists()))
      fakeDump(namedPipeFile)
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).ask(any[GpuCoreDumpMsgStart]())
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).send(any[GpuCoreDumpMsgCompleted]())
      val dumpFile = new File(s"/tmp/gpucore-$APP_ID-$EXECUTOR_ID.nvcudmp")
      assert(dumpFile.exists())
      try {
        verifyDump(new FileInputStream(dumpFile))
      } finally {
        dumpFile.delete()
      }
    } finally {
      GpuCoreDumpHandler.shutdown()
    }
    assert(waitForIt(WAIT_MSECS)(() => !namedPipeFile.exists()))
  }

  test("test dump handler failure") {
    val sparkConf = buildSparkConf()
      .set(RapidsConf.GPU_COREDUMP_DIR.key, "thiswillfail://foo/bar")
      .set(RapidsConf.GPU_COREDUMP_COMPRESS.key, "false")
    val mockCtx = buildMockCtx(sparkConf)
    val rapidsConf = new RapidsConf(sparkConf)
    GpuCoreDumpHandler.executorInit(rapidsConf, mockCtx)
    val namedPipeFile = GpuCoreDumpHandler.getNamedPipeFile
    try {
      assert(waitForIt(WAIT_MSECS)(() => namedPipeFile.exists()))
      try {
        fakeDump(namedPipeFile)
      } catch {
        case e: IOException if e.getMessage.contains("Broken pipe") =>
          // broken pipe is expected when reader crashes
      }
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).ask(any[GpuCoreDumpMsgStart]())
      verify(mockCtx, timeout(WAIT_MSECS).times(1)).send(any[GpuCoreDumpMsgFailed]())
    } finally {
      GpuCoreDumpHandler.shutdown()
    }
    assert(waitForIt(WAIT_MSECS)(() => !namedPipeFile.exists()))
  }

  private def buildMockCtx(conf: SparkConf): PluginContext = {
    val ctxConf = conf.clone()
    ctxConf.set("spark.app.id", APP_ID)
    pipeNum += 1
    val mockCtx = mock[PluginContext]
    when(mockCtx.conf).thenReturn(ctxConf)
    when(mockCtx.executorID()).thenReturn(EXECUTOR_ID)
    when(mockCtx.ask(any[GpuCoreDumpMsgStart]())).thenAnswer(_ =>
      new SerializableConfiguration(new Configuration(false)))
    mockCtx
  }

  private def waitForIt(millis: Long)(block: () => Boolean): Boolean = {
    val end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis)
    while (!block() && System.nanoTime() < end) {
      Thread.sleep(10)
    }
    block()
  }

  private def fakeDump(pipeFile: File): Unit = {
    val out = new BufferedOutputStream(new FileOutputStream(pipeFile))
    withResource(out) { _ =>
      var bytesWritten = 0L
      while (bytesWritten < FAKE_DUMP_SIZE) {
        out.write(FAKE_DUMP_BYTES)
        bytesWritten += FAKE_DUMP_BYTES.length
      }
    }
  }

  private def verifyDump(in: InputStream): Unit = {
    withResource(in) { _ =>
      val dumpBytesStr = new String(FAKE_DUMP_BYTES)
      var numBytes = 0L
      val data = new Array[Byte](FAKE_DUMP_BYTES.length)
      while (numBytes < FAKE_DUMP_SIZE) {
        var offset = 0
        while (offset < data.length) {
          val bytesRead = in.read(data, offset, data.length - offset)
          assert(bytesRead > 0)
          numBytes += bytesRead
          offset += bytesRead
        }
        assertResult(dumpBytesStr)(new String(data))
      }
      assertResult(-1)(in.read())
    }
  }

  private def buildSparkConf(): SparkConf = {
    // Use a different named pipe for each test, as background threads can linger on old pipes
    // and conflict with other coredump tests.
    new SparkConf(false)
      .set(RapidsConf.GPU_COREDUMP_PIPE_PATTERN.key, "gpucore.test" + pipeNum)
  }
}
