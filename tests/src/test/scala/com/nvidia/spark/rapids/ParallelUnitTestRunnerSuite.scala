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

import java.io.{
  BufferedWriter, ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream, Writer}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.hadoop.fs.FileUtil
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ParallelUnitTestRunnerSuite extends AnyFunSuite {
  test("JUnit XML reports are scoped by test wave") {
    val reportsDir = Files.createTempDirectory("parallel-unit-test-reports")
    try {
      val wave1Args = ParallelUnitTestRunner.scalaTestArgs(
        "example.Suite", 1, 1, reportsDir, reportsDir, Seq.empty, Seq.empty)
      val wave2Args = ParallelUnitTestRunner.scalaTestArgs(
        "example.Suite", 1, 2, reportsDir, reportsDir, Seq.empty, Seq.empty)

      val wave1Reports = Paths.get(wave1Args(wave1Args.indexOf("-u") + 1))
      val wave2Reports = Paths.get(wave2Args(wave2Args.indexOf("-u") + 1))
      assert(wave1Reports === reportsDir.resolve("wave-1"))
      assert(wave2Reports === reportsDir.resolve("wave-2"))
      assert(Files.isDirectory(wave1Reports))
      assert(Files.isDirectory(wave2Reports))
    } finally {
      FileUtil.fullyDelete(reportsDir.toFile)
    }
  }

  test("historical timings are loaded from wave report directories") {
    val reportsDir = Files.createTempDirectory("parallel-unit-test-timings")
    val wave1Reports = Files.createDirectories(reportsDir.resolve("wave-1"))
    val wave2Reports = Files.createDirectories(reportsDir.resolve("wave-2"))
    try {
      Files.write(
        wave1Reports.resolve("TEST-example.SuiteOne.xml"),
        "<testsuite name=\"example.SuiteOne\" time=\"12.5\"/>"
            .getBytes(StandardCharsets.UTF_8))
      Files.write(
        wave2Reports.resolve("TEST-example.SuiteTwo.xml"),
        "<testsuite name=\"example.SuiteTwo\" time=\"7.25\"/>"
            .getBytes(StandardCharsets.UTF_8))

      assert(ParallelUnitTestRunner.loadTimings(reportsDir) === Map(
        "example.SuiteOne" -> 12.5,
        "example.SuiteTwo" -> 7.25))
    } finally {
      FileUtil.fullyDelete(reportsDir.toFile)
    }
  }

  test("a forcibly terminated worker does not count as a test failure") {
    class TimedOutProcess extends Process {
      private var alive = true
      var forciblyDestroyed = false

      override def getOutputStream: OutputStream = new ByteArrayOutputStream()

      override def getInputStream: InputStream = new ByteArrayInputStream(Array.empty[Byte])

      override def getErrorStream: InputStream = new ByteArrayInputStream(Array.empty[Byte])

      override def waitFor(): Int = throw new UnsupportedOperationException()

      override def waitFor(timeout: Long, unit: TimeUnit): Boolean = !alive

      override def exitValue(): Int = if (alive) throw new IllegalThreadStateException() else 137

      override def destroy(): Unit = {}

      override def destroyForcibly(): Process = {
        forciblyDestroyed = true
        alive = false
        this
      }

      override def isAlive: Boolean = alive
    }
    val process = new TimedOutProcess

    val (exited, terminated) = ParallelUnitTestRunner.stopWorkerProcess(
      process, 1, 1, exitTimeoutSeconds = 0, destroyTimeoutSeconds = 0)

    assert(!exited)
    assert(terminated)
    assert(process.forciblyDestroyed)
    assert(!process.isAlive)
  }

  test("worker stop request does not block on the command pipe") {
    val writeStarted = new CountDownLatch(1)
    val releaseWrite = new CountDownLatch(1)
    val writer = new BufferedWriter(new Writer {
      override def write(chars: Array[Char], offset: Int, length: Int): Unit = {
        writeStarted.countDown()
        releaseWrite.await()
      }

      override def flush(): Unit = {}

      override def close(): Unit = {}
    })

    val stopThread = ParallelUnitTestRunner.requestWorkerStop(writer, 1, 1)
    try {
      assert(writeStarted.await(5, TimeUnit.SECONDS))
      assert(stopThread.isAlive)
    } finally {
      releaseWrite.countDown()
      stopThread.join(TimeUnit.SECONDS.toMillis(5))
    }
    assert(!stopThread.isAlive)
  }

  test("cleanup worker state stops Spark sessions and contexts") {
    val tmpDir = Files.createTempDirectory("parallel-unit-test-runner")
    val warehouseDir = tmpDir.resolve("spark-warehouse")
    val sparkConf = new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local[1]")
        .set("spark.driver.host", "localhost")
        .set("spark.sql.warehouse.dir", warehouseDir.toString)
        .set("spark.ui.enabled", "false")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    try {
      assert(!spark.sparkContext.isStopped)
      assert(SparkSession.getActiveSession.contains(spark))
      assert(SparkSession.getDefaultSession.contains(spark))

      ParallelUnitTestRunner.cleanupWorkerState(tmpDir)

      assert(spark.sparkContext.isStopped)
      assert(SparkSession.getActiveSession.isEmpty)
      assert(SparkSession.getDefaultSession.isEmpty)
    } finally {
      if (!spark.sparkContext.isStopped) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      FileUtil.fullyDelete(tmpDir.toFile)
    }
  }
}
