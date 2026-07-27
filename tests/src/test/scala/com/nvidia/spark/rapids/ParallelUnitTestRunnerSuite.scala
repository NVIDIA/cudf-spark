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
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.FileUtil
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ParallelUnitTestRunnerSuite extends AnyFunSuite {
  private val fixtureSuiteName = classOf[ParallelUnitTestRunnerFixtureSuite].getName

  private def fixtureRunnerArgs(reportsDir: Path, failFixture: Boolean): Array[String] = {
    val testClasses = Paths.get(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
    val fixtureJvmArgs = if (failFixture) {
      s"-D${ParallelUnitTestRunnerFixtureSuite.failProperty}=true"
    } else {
      ""
    }
    Array(
      s"testClasses=$testClasses",
      s"reportsDir=$reportsDir",
      "forkCount=2",
      s"wildcardSuites=$fixtureSuiteName",
      "tagsToInclude=",
      "tagsToExclude=",
      "suffixes=",
      "testsFilter=",
      "argLine=",
      s"testJvmArgs=$fixtureJvmArgs",
      "shuffleManagerOverride=true",
      "allocationFraction=1.0",
      "maxAllocationFraction=1.0",
      "minAllocationFraction=0.25",
      "testFailureIgnore=false",
      "sparkConfs=",
      "suiteTimeoutSeconds=30")
  }

  test("special suites are submitted as serial worker batches") {
    val parquetSuite = "com.nvidia.spark.rapids.ParquetWriterSuite"
    val dppOff =
      "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOff"
    val dppOn =
      "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOn"
    val tasks = Seq(
      ParallelUnitTestRunner.SuiteTask(1, "example.SuiteOne"),
      ParallelUnitTestRunner.SuiteTask(2, dppOn),
      ParallelUnitTestRunner.SuiteTask(3, parquetSuite),
      ParallelUnitTestRunner.SuiteTask(4, dppOff),
      ParallelUnitTestRunner.SuiteTask(5, "example.SuiteTwo"))

    val batches = ParallelUnitTestRunner.createSuiteBatches(tasks)

    assert(batches.map(_.tasks.map(_.suite)) === Seq(
      Seq(parquetSuite),
      Seq(dppOff, dppOn),
      Seq("example.SuiteOne"),
      Seq("example.SuiteTwo")))
  }

  test("worker count is capped by suite batches") {
    val tasks = Seq(
      ParallelUnitTestRunner.SuiteTask(
        1,
        "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOff"),
      ParallelUnitTestRunner.SuiteTask(
        2,
        "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOn"))
    val batches = ParallelUnitTestRunner.createSuiteBatches(tasks)

    assert(batches.size === 1)
    val workerCount = ParallelUnitTestRunner.effectiveWorkerCount(4, batches)
    assert(workerCount === 1)
    val (allocation, maximum, minimum) =
      ParallelUnitTestRunner.perWorkerGpuAllocations(workerCount, 1.0, 1.0, 0.25)
    assert(allocation === 0.8)
    assert(maximum === 0.8)
    assert(minimum === 0.25)
  }

  test("suites are ordered by fully qualified name") {
    val suites = Seq("example.SuiteZ", "another.SuiteB", "another.SuiteA")

    assert(ParallelUnitTestRunner.orderSuites(suites) ===
        Seq("another.SuiteA", "another.SuiteB", "example.SuiteZ"))
  }

  test("wildcardSuites match by fully qualified name prefix, like ScalaTest -w") {
    val suite = "com.nvidia.spark.rapids.ParquetWriterSuite"

    // No wildcards selects everything.
    assert(ParallelUnitTestRunner.matchesWildcard(suite, Seq.empty))
    // A package prefix selects suites under it.
    assert(ParallelUnitTestRunner.matchesWildcard(suite, Seq("com.nvidia.spark.rapids")))
    // An exact name matches itself.
    assert(ParallelUnitTestRunner.matchesWildcard(suite, Seq(suite)))
    // Any of several prefixes matching is enough.
    assert(ParallelUnitTestRunner.matchesWildcard(suite, Seq("org.apache.spark", "com.nvidia")))
    // A substring that is not a prefix must not match (unlike the previous `contains` behavior).
    assert(!ParallelUnitTestRunner.matchesWildcard(suite, Seq("rapids.ParquetWriterSuite")))
    assert(!ParallelUnitTestRunner.matchesWildcard(suite, Seq("ParquetWriterSuite")))
    // An unrelated prefix must not match.
    assert(!ParallelUnitTestRunner.matchesWildcard(suite, Seq("org.apache.spark")))
  }

  test("JUnit XML reports are scoped by test wave") {
    val reportsDir = Files.createTempDirectory("parallel-unit-test-reports")
    try {
      val wave1Args = ParallelUnitTestRunner.scalaTestArgs(
        "example.Suite", 1, 1, reportsDir, reportsDir, Seq.empty, Seq.empty)
      val wave2Args = ParallelUnitTestRunner.scalaTestArgs(
        "example.Suite", 1, 2, reportsDir, reportsDir, Seq.empty, Seq.empty)

      assert(!wave1Args.contains("-R"))
      assert(!wave2Args.contains("-R"))
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

  test("main runs a successful suite in a child JVM and writes its JUnit report") {
    val reportsDir = Files.createTempDirectory("parallel-unit-test-success")
    try {
      ParallelUnitTestRunner.main(fixtureRunnerArgs(reportsDir, failFixture = false))

      assert(Files.isRegularFile(
        reportsDir.resolve("wave-1").resolve(s"TEST-$fixtureSuiteName.xml")))
    } finally {
      FileUtil.fullyDelete(reportsDir.toFile)
    }
  }

  test("main propagates a child JVM suite failure") {
    val reportsDir = Files.createTempDirectory("parallel-unit-test-failure")
    try {
      val error = intercept[IllegalStateException] {
        ParallelUnitTestRunner.main(fixtureRunnerArgs(reportsDir, failFixture = true))
      }

      assert(error.getMessage.contains(fixtureSuiteName))
      assert(Files.isRegularFile(
        reportsDir.resolve("wave-1").resolve(s"TEST-$fixtureSuiteName.xml")))
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

  test("watchdog records a timed-out suite and forcibly terminates its worker") {
    class TimedOutProcess extends Process {
      @volatile private var alive = true
      @volatile var forciblyDestroyed = false

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
    val failures = new ConcurrentLinkedQueue[String]()
    val reportsDir = Files.createTempDirectory("parallel-unit-test-watchdog")
    try {
      val watchdog = ParallelUnitTestRunner.startSuiteWatchdog(
        runId = 1,
        workerId = 2,
        process = process,
        reportsDir = reportsDir,
        failures = failures,
        deadlineNanos = new AtomicLong(0L),
        currentSuite = () => Some("example.TimedOutSuite"),
        suiteTimeoutSeconds = 30)
      watchdog.join(TimeUnit.SECONDS.toMillis(5))

      assert(!watchdog.isAlive)
      assert(process.forciblyDestroyed)
      assert(!process.isAlive)
      assert(failures.contains(
        "wave-1 example.TimedOutSuite exceeded the 30s suite timeout in worker-2"))
    } finally {
      if (process.isAlive) {
        process.destroyForcibly()
      }
      FileUtil.fullyDelete(reportsDir.toFile)
    }
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

  test("cleanup worker state stops Spark sessions and contexts and cleans warehouses") {
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
    Files.createDirectories(warehouseDir)
    val warehouseSentinel = Files.createFile(warehouseDir.resolve("sentinel"))

    try {
      assert(!spark.sparkContext.isStopped)
      assert(SparkSession.getActiveSession.contains(spark))
      assert(SparkSession.getDefaultSession.contains(spark))

      ParallelUnitTestRunner.cleanupWorkerState(tmpDir)

      assert(spark.sparkContext.isStopped)
      assert(SparkSession.getActiveSession.isEmpty)
      assert(SparkSession.getDefaultSession.isEmpty)
      assert(!Files.exists(warehouseSentinel))
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

object ParallelUnitTestRunnerFixtureSuite {
  val failProperty: String = "rapids.parallelUnitTestRunner.fixture.fail"
}

class ParallelUnitTestRunnerFixtureSuite extends AnyFunSuite {
  test("configurable fixture") {
    assert(!java.lang.Boolean.getBoolean(ParallelUnitTestRunnerFixtureSuite.failProperty))
  }
}
