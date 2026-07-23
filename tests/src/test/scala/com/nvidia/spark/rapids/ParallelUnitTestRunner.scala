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
  BufferedReader, BufferedWriter, File, InputStreamReader, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.fs.FileUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/** Runs ScalaTest suites concurrently in isolated JVMs. */
object ParallelUnitTestRunner {
  private[rapids] case class SuiteTask(id: Int, suite: String, weight: Double)
  private[rapids] case class SuiteBatch(tasks: Seq[SuiteTask]) {
    require(tasks.nonEmpty, "A suite batch must not be empty")
  }

  private val unresolvedProperty = "${"
  private val parallelGpuAllocationRatio = 0.8
  private val parquetWriterSuite = "com.nvidia.spark.rapids.ParquetWriterSuite"
  private val dppSuites = Seq(
    "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOff",
    "org.apache.spark.sql.rapids.suites.RapidsDynamicPartitionPruningV1SuiteAEOn")
  private val sparkTestingProperty = "spark.testing"
  private val sparkWarehousePrefix = "spark-warehouse"
  private val workerMode = "worker"
  private val protocolPrefix = "__RAPIDS_PARALLEL_UT__"
  private val workerExitTimeoutSeconds = 10L
  private val workerDestroyTimeoutSeconds = 10L
  private val watchdogPollSeconds = 15L
  private val defaultSuiteTimeoutSeconds = 1800L

  def main(args: Array[String]): Unit = {
    if (args.headOption.contains(workerMode)) {
      workerMain(args.tail)
      return
    }

    val config = args.map { arg =>
      val separator = arg.indexOf('=')
      require(separator > 0, s"Invalid argument: $arg")
      arg.substring(0, separator) -> arg.substring(separator + 1)
    }.toMap

    val testClasses = Paths.get(config("testClasses")).toAbsolutePath
    val reportsDir = Paths.get(config("reportsDir")).toAbsolutePath
    val requestedForks = config("forkCount").toInt
    require(requestedForks > 1, "parallelForkCount must be greater than 1")
    val wildcardSuites = propertyList(config("wildcardSuites"))
    // The serial scalatest plugin honors these filters, but this runner does not; fail fast
    // instead of silently running the full suite set on the caller's GPU.
    val unsupportedFilters = Seq("suffixes" -> "-Dsuffixes", "testsFilter" -> "-Dtests")
        .collect { case (key, flag) if propertyValue(config.getOrElse(key, ""), "").nonEmpty =>
          flag
        }
    require(unsupportedFilters.isEmpty,
      s"${unsupportedFilters.mkString(" and ")} are not supported with -Dparallel=true; " +
          "use -DwildcardSuites or run without -Dparallel")
    val tagsToInclude = propertyList(config("tagsToInclude"))
    val tagsToExclude = propertyList(config("tagsToExclude"))
    val childJvmArgs = splitJvmArgs(config("argLine")) ++ splitJvmArgs(config("testJvmArgs"))
    val shuffleManagerOverride = propertyValue(config("shuffleManagerOverride"), "true")
    val allocationFraction = propertyDouble(config("allocationFraction"), 1.0)
    val maxAllocationFraction = propertyDouble(config("maxAllocationFraction"), 1.0)
    val minAllocationFraction = propertyDouble(config("minAllocationFraction"), 0.25)
    val suiteTimeoutSeconds = propertyDouble(
      config.getOrElse("suiteTimeoutSeconds", ""), defaultSuiteTimeoutSeconds.toDouble).toLong
    val testFailureIgnore = propertyValue(config("testFailureIgnore"), "false").toBoolean
    val configuredSparkConfs = propertySeparatedList(config("sparkConfs"), ';')
    val sparkConfs = if (configuredSparkConfs.isEmpty) {
      Seq(None)
    } else {
      configuredSparkConfs.map(Some(_))
    }

    Files.createDirectories(reportsDir)
    val allSuites = discoverSuiteNames(testClasses)
    require(allSuites.nonEmpty, "No ScalaTest suites were discovered")
    val discovered = allSuites.filter(matchesWildcard(_, wildcardSuites)).sorted
    if (discovered.isEmpty) {
      // Match the serial scalatest plugin: a filter that selects no suites is a successful no-op.
      println(s"No suites matched wildcardSuites=${wildcardSuites.mkString(",")}; nothing to run")
      return
    }
    val suiteTasks = orderSuites(discovered, testClasses)
        .zipWithIndex
        .map { case ((suite, weight), index) => SuiteTask(index + 1, suite, weight) }
    val suiteBatches = createSuiteBatches(suiteTasks)
    val workerCount = effectiveWorkerCount(requestedForks, suiteBatches)
    val perForkAllocation = allocationFraction * parallelGpuAllocationRatio / workerCount
    val perForkMaxAllocation = maxAllocationFraction * parallelGpuAllocationRatio / workerCount
    val perForkMinAllocation = math.min(minAllocationFraction / workerCount, perForkMaxAllocation)

    println(s"Running ${discovered.size} suites with at most $workerCount concurrent processes")
    suiteBatches.filter(_.tasks.size > 1).foreach { batch =>
      println(s"  serial suite batch: ${batch.tasks.map(_.suite).mkString(", ")}")
    }
    println(s"  worker pool: ${suiteBatches.size} batches across $workerCount persistent forks")

    val failures = sparkConfs.zipWithIndex.flatMap { case (sparkConf, runIndex) =>
      sparkConf.foreach(conf => println(s"Parallel test wave ${runIndex + 1}: SPARK_CONF=$conf"))
      val runId = runIndex + 1
      runWave(
        runId,
        sparkConf,
        suiteBatches,
        workerCount,
        testClasses,
        reportsDir,
        childJvmArgs,
        tagsToInclude,
        tagsToExclude,
        shuffleManagerOverride,
        perForkAllocation,
        perForkMaxAllocation,
        perForkMinAllocation,
        suiteTimeoutSeconds)
    }

    if (failures.nonEmpty) {
      val message = failures.mkString("Parallel unit tests failed: ", ", ", "")
      if (testFailureIgnore) {
        System.err.println(message)
      } else {
        throw new IllegalStateException(message)
      }
    }
  }

  private def runWave(
      runId: Int,
      sparkConf: Option[String],
      suiteBatches: Seq[SuiteBatch],
      workerCount: Int,
      testClasses: Path,
      reportsDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double,
      suiteTimeoutSeconds: Long): Seq[String] = {
    val failures = new ConcurrentLinkedQueue[String]()

    val taskQueue = new ConcurrentLinkedQueue[SuiteBatch]()
    suiteBatches.foreach(taskQueue.add)
    val workers = (1 to workerCount).map { workerId =>
      val thread = new Thread(s"parallel-unit-test-worker-$runId-$workerId") {
        override def run(): Unit = runPoolWorker(
          workerId,
          runId,
          sparkConf,
          taskQueue,
          failures,
          testClasses,
          reportsDir,
          childJvmArgs,
          tagsToInclude,
          tagsToExclude,
          shuffleManagerOverride,
          allocationFraction,
          maxAllocationFraction,
          minAllocationFraction,
          suiteTimeoutSeconds)
      }
      thread.start()
      thread
    }
    workers.foreach(_.join())
    var unprocessed = taskQueue.poll()
    while (unprocessed != null) {
      unprocessed.tasks.foreach { task =>
        failures.add(s"wave-$runId ${task.suite} was not processed")
      }
      unprocessed = taskQueue.poll()
    }

    val failureResults = ArrayBuffer.empty[String]
    val failureIterator = failures.iterator()
    while (failureIterator.hasNext) {
      failureResults += failureIterator.next()
    }
    failureResults.toSeq
  }

  private def runPoolWorker(
      workerId: Int,
      runId: Int,
      sparkConf: Option[String],
      taskQueue: ConcurrentLinkedQueue[SuiteBatch],
      failures: ConcurrentLinkedQueue[String],
      testClasses: Path,
      reportsDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double,
      suiteTimeoutSeconds: Long): Unit = {
    val tmpDir = reportsDir.resolve(s"tmp-wave-$runId-worker-$workerId")
    Files.createDirectories(tmpDir)
    val command = poolWorkerCommand(
      workerId,
      runId,
      testClasses,
      reportsDir,
      tmpDir,
      childJvmArgs,
      tagsToInclude,
      tagsToExclude,
      shuffleManagerOverride,
      allocationFraction,
      maxAllocationFraction,
      minAllocationFraction)
    // Keep stderr out of the protocol stream: unsynchronized stderr writes (Spark logging,
    // stack traces) could otherwise splice into a RESULT line and stall the RUN/RESULT exchange.
    val processBuilder = new ProcessBuilder(command: _*)
    sparkConf.foreach(processBuilder.environment().put("SPARK_CONF", _))
    var currentTask: Option[SuiteTask] = None
    var remainingBatchTasks = List.empty[SuiteTask]
    var process: Process = null
    try {
      process = processBuilder.start()
      val errorThread = streamLines(s"wave-$runId-worker-$workerId",
        new BufferedReader(new InputStreamReader(process.getErrorStream)))
      val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      // A hung suite would otherwise block this thread in readLine() until the CI job timeout;
      // the watchdog captures a thread dump and kills the worker so the run fails visibly.
      val suiteDeadlineNanos = new AtomicLong(Long.MaxValue)
      startSuiteWatchdog(runId, workerId, process, reportsDir, failures, suiteDeadlineNanos,
        () => currentTask.map(_.suite), suiteTimeoutSeconds)

      def pollNextTask(): Option[SuiteTask] = remainingBatchTasks match {
        case task :: tail =>
          remainingBatchTasks = tail
          Some(task)
        case Nil =>
          Option(taskQueue.poll()).flatMap { batch =>
            remainingBatchTasks = batch.tasks.toList
            pollNextTask()
          }
      }

      def sendNextTask(): Boolean = {
        pollNextTask() match {
          case Some(task) =>
            currentTask = Some(task)
            suiteDeadlineNanos.set(
              System.nanoTime() + TimeUnit.SECONDS.toNanos(suiteTimeoutSeconds))
            println(f"[wave-$runId-worker-$workerId] START ${task.suite} " +
                f"(estimated weight ${task.weight}%.1f)")
            writer.write(s"RUN\t${task.id}\t${task.suite}\n")
            writer.flush()
            true
          case None =>
            suiteDeadlineNanos.set(Long.MaxValue)
            requestWorkerStop(writer, runId, workerId)
            false
        }
      }

      var running = sendNextTask()
      var line = reader.readLine()
      while (line != null && running) {
        if (line.startsWith(s"$protocolPrefix\tRESULT\t")) {
          val fields = line.split("\\t", -1)
          val succeeded = fields.length == 4 && fields(3).toBoolean
          currentTask.foreach { task =>
            if (succeeded) {
              println(s"[wave-$runId-worker-$workerId] PASS ${task.suite}")
            } else {
              failures.add(s"wave-$runId ${task.suite} failed in worker-$workerId")
            }
          }
          currentTask = None
          suiteDeadlineNanos.set(Long.MaxValue)
          if (running) {
            running = sendNextTask()
          }
        } else {
          println(s"[wave-$runId-worker-$workerId] $line")
        }
        if (running) {
          line = reader.readLine()
        }
      }
      val outputThread = streamLines(s"wave-$runId-worker-$workerId", reader)
      val (exited, terminated) = stopWorkerProcess(process, runId, workerId)
      outputThread.join(TimeUnit.SECONDS.toMillis(workerDestroyTimeoutSeconds))
      errorThread.join(TimeUnit.SECONDS.toMillis(workerDestroyTimeoutSeconds))
      val exitCode = if (process.isAlive) None else Some(process.exitValue())
      if (!terminated) {
        failures.add(s"wave-$runId worker-$workerId could not be terminated")
      }
      currentTask.foreach { task =>
        failures.add(s"wave-$runId ${task.suite} lost when worker-$workerId exited " +
            s"with status ${exitCode.getOrElse("unknown")}")
      }
      remainingBatchTasks.foreach { task =>
        failures.add(s"wave-$runId ${task.suite} was not processed after worker-$workerId exited")
      }
      if (exited && exitCode.exists(_ != 0) && currentTask.isEmpty) {
        failures.add(s"wave-$runId worker-$workerId exited with status ${exitCode.get}")
      }
    } catch {
      case t: Throwable =>
        val unfinishedTasks = currentTask.toSeq ++ remainingBatchTasks
        if (unfinishedTasks.nonEmpty) {
          taskQueue.add(SuiteBatch(unfinishedTasks))
        }
        failures.add(s"wave-$runId worker-$workerId failed: ${t.getMessage}")
        // Do not leak a live worker JVM (and its GPU allocation) on coordinator errors.
        if (process != null && process.isAlive) {
          process.destroyForcibly()
        }
    }
  }

  private def startSuiteWatchdog(
      runId: Int,
      workerId: Int,
      process: Process,
      reportsDir: Path,
      failures: ConcurrentLinkedQueue[String],
      deadlineNanos: AtomicLong,
      currentSuite: () => Option[String],
      suiteTimeoutSeconds: Long): Thread = {
    val thread = new Thread(s"parallel-unit-test-watchdog-$runId-worker-$workerId") {
      override def run(): Unit = {
        while (process.isAlive) {
          val deadline = deadlineNanos.get()
          if (deadline != Long.MaxValue && System.nanoTime() - deadline > 0) {
            val suite = currentSuite().getOrElse("<unknown suite>")
            System.err.println(s"wave-$runId worker-$workerId: $suite exceeded " +
                s"$suiteTimeoutSeconds seconds; capturing a thread dump and killing the worker")
            failures.add(s"wave-$runId $suite exceeded the ${suiteTimeoutSeconds}s suite " +
                s"timeout in worker-$workerId")
            dumpWorkerThreads(s"wave-$runId-worker-$workerId", process, reportsDir)
            process.destroyForcibly()
            return
          }
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(watchdogPollSeconds))
          } catch {
            case _: InterruptedException => return
          }
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Best-effort jstack of a hung test JVM, echoed to the build log and saved to a file. */
  private def dumpWorkerThreads(label: String, process: Process, reportsDir: Path): Unit = {
    try {
      val jstack = Paths.get(System.getProperty("java.home"), "bin", "jstack").toString
      // Process.pid() is a Java 9+ API while this module still compiles against the Java 8 API;
      // resolve it reflectively (the test JVMs only ever run on JDK 9+).
      val pid = classOf[Process].getMethod("pid").invoke(process).toString
      val dumper = new ProcessBuilder(jstack, "-l", pid)
          .redirectErrorStream(true)
          .start()
      val reader = new BufferedReader(new InputStreamReader(dumper.getInputStream))
      val dump = new StringBuilder
      try {
        var line = reader.readLine()
        while (line != null) {
          dump.append(line).append('\n')
          line = reader.readLine()
        }
      } finally {
        reader.close()
      }
      if (!dumper.waitFor(60, TimeUnit.SECONDS)) {
        dumper.destroyForcibly()
      }
      val dumpFile = reportsDir.resolve(s"$label-timeout-jstack.txt")
      Files.write(dumpFile, dump.toString.getBytes(StandardCharsets.UTF_8))
      println(s"[$label] thread dump of the hung test JVM (also saved to $dumpFile):")
      print(dump.toString)
      System.out.flush()
    } catch {
      case t: Throwable =>
        System.err.println(s"[$label] failed to capture a thread dump: ${t.getMessage}")
    }
  }

  private[rapids] def stopWorkerProcess(
      process: Process,
      runId: Int,
      workerId: Int,
      exitTimeoutSeconds: Long = workerExitTimeoutSeconds,
      destroyTimeoutSeconds: Long = workerDestroyTimeoutSeconds): (Boolean, Boolean) = {
    val exited = process.waitFor(exitTimeoutSeconds, TimeUnit.SECONDS)
    val terminated = if (exited) {
      true
    } else {
      System.err.println(s"wave-$runId worker-$workerId did not exit within " +
          s"$exitTimeoutSeconds seconds; terminating it")
      process.destroy()
      if (process.waitFor(destroyTimeoutSeconds, TimeUnit.SECONDS)) {
        true
      } else {
        process.destroyForcibly()
        process.waitFor(destroyTimeoutSeconds, TimeUnit.SECONDS)
      }
    }
    exited -> terminated
  }

  private[rapids] def requestWorkerStop(
      writer: BufferedWriter,
      runId: Int,
      workerId: Int): Thread = {
    val thread = new Thread(s"parallel-unit-test-stop-$runId-worker-$workerId") {
      override def run(): Unit = try {
        writer.write("STOP\n")
        writer.flush()
      } catch {
        case _: IOException => // The worker may exit before consuming the stop request.
      } finally {
        try {
          writer.close()
        } catch {
          case _: IOException => // The process shutdown closes the pipe.
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def workerMain(args: Array[String]): Unit = {
    initializeSparkFunctionRegistry()
    val config = args.map { arg =>
      val separator = arg.indexOf('=')
      require(separator > 0, s"Invalid worker argument: $arg")
      arg.substring(0, separator) -> arg.substring(separator + 1)
    }.toMap
    val testClasses = Paths.get(config("testClasses")).toAbsolutePath
    val reportsDir = Paths.get(config("reportsDir")).toAbsolutePath
    val runId = config("runId").toInt
    val tagsToInclude = propertyList(config("tagsToInclude"))
    val tagsToExclude = propertyList(config("tagsToExclude"))
    val reader = new BufferedReader(new InputStreamReader(System.in))
    var line = reader.readLine()
    while (line != null && line != "STOP") {
      val fields = line.split("\\t", -1)
      require(fields.length == 3 && fields(0) == "RUN", s"Invalid worker command: $line")
      val taskId = fields(1).toInt
      val suite = fields(2)
      val runnerArgs = scalaTestArgs(
        suite,
        taskId,
        runId,
        testClasses,
        reportsDir,
        tagsToInclude,
        tagsToExclude)
      var succeeded = false
      try {
        succeeded = org.scalatest.tools.Runner.run(runnerArgs.toArray)
      } catch {
        case t: Throwable =>
          t.printStackTrace(System.out)
      } finally {
        try {
          cleanupWorkerState(Paths.get(System.getProperty("java.io.tmpdir")))
        } catch {
          case t: Throwable =>
            t.printStackTrace(System.out)
            succeeded = false
        }
      }
      println(s"$protocolPrefix\tRESULT\t$taskId\t$succeeded")
      System.out.flush()
      line = reader.readLine()
    }
    reader.close()
    System.exit(0)
  }

  private def initializeSparkFunctionRegistry(): Unit = {
    val originalSparkTesting = Option(System.getProperty(sparkTestingProperty))
    try {
      // Spark 3.3 conditionally registers test-only SQL functions when this object initializes.
      // Persistent workers may otherwise initialize it in a non-Spark suite before SparkFunSuite
      // sets spark.testing, leaving later upstream Spark suites with an incomplete registry.
      System.setProperty(sparkTestingProperty, "true")
      FunctionRegistry.builtin.listFunction()
    } finally {
      originalSparkTesting match {
        case Some(value) => System.setProperty(sparkTestingProperty, value)
        case None => System.clearProperty(sparkTestingProperty)
      }
    }
  }

  private[rapids] def cleanupWorkerState(tmpDir: Path): Unit = {
    val failures = ArrayBuffer.empty[Throwable]
    val warehouseDirs = ArrayBuffer.empty[File]
    def cleanup(body: => Unit): Unit = try {
      body
    } catch {
      case t: Throwable => failures += t
    }

    val sessions = (SparkSession.getActiveSession.toSeq ++
        SparkSession.getDefaultSession.toSeq).distinct
    sessions.foreach { session =>
      cleanup(session.catalog.clearCache())
      cleanup {
        warehouseDirs += new File(session.conf.get("spark.sql.warehouse.dir"))
      }
    }
    cleanup(cleanupSparkSessionAndContext())
    cleanup(clearCachedBatchSerializer())
    cleanup {
      warehouseDirs ++= Option(tmpDir.toFile.listFiles()).getOrElse(Array.empty[File])
          .filter(file => file.isDirectory && file.getName.startsWith(sparkWarehousePrefix))
    }
    warehouseDirs.distinct.foreach { warehouseDir =>
      cleanup {
        Option(warehouseDir.listFiles()).getOrElse(Array.empty[File])
            .foreach(FileUtil.fullyDelete)
      }
    }
    cleanup(SpillFramework.shutdown())
    if (failures.nonEmpty) {
      failures.tail.foreach(failures.head.addSuppressed)
      throw failures.head
    }
  }

  private def cleanupSparkSessionAndContext(): Unit = try {
    TrampolineUtil.cleanupAnyExistingSession()
  } finally {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  /**
   * Reset Spark's JVM-global cached CachedBatchSerializer between suites.
   *
   * The first time any suite builds a cached relation, Spark's InMemoryRelation caches the
   * serializer named by spark.sql.cache.serializer in a JVM-global field and reuses it for the
   * lifetime of the JVM, ignoring the configuration of later sessions. In a persistent worker a
   * suite that runs without the RAPIDS serializer would pin Spark's default serializer, so a later
   * suite such as RapidsCachedTableSuite would observe a serializer that no longer matches its own
   * spark.sql.cache.serializer and fail ("Cache serializer failed to load!"). Clearing the cache
   * between suites lets each suite rebuild the serializer from its own configuration.
   *
   * Called reflectively because InMemoryRelation.clearSerializer is not part of the public API.
   */
  private def clearCachedBatchSerializer(): Unit = {
    val module = Class.forName("org.apache.spark.sql.execution.columnar.InMemoryRelation$")
        .getField("MODULE$").get(null)
    module.getClass.getMethod("clearSerializer").invoke(module)
  }

  private def poolWorkerCommand(
      workerId: Int,
      runId: Int,
      testClasses: Path,
      reportsDir: Path,
      tmpDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double): Seq[String] = {
    javaCommand(childJvmArgs, childSystemProperties(
      tmpDir,
      shuffleManagerOverride,
      allocationFraction,
      maxAllocationFraction,
      minAllocationFraction)) ++ Seq(
      getClass.getName.stripSuffix("$"),
      workerMode,
      s"workerId=$workerId",
      s"runId=$runId",
      s"testClasses=$testClasses",
      s"reportsDir=$reportsDir",
      s"tagsToInclude=${tagsToInclude.mkString(",")}",
      s"tagsToExclude=${tagsToExclude.mkString(",")}")
  }

  private def javaCommand(childJvmArgs: Seq[String], systemProperties: Seq[String]): Seq[String] = {
    val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
    Seq(java) ++ childJvmArgs ++ systemProperties ++
        Seq("-cp", System.getProperty("java.class.path"))
  }

  private def childSystemProperties(
      tmpDir: Path,
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double): Seq[String] = Seq(
    "-Dcom.nvidia.spark.rapids.runningTests=true",
    s"-Drapids.shuffle.manager.override=$shuffleManagerOverride",
    "-Dai.rapids.refcount.debug=true",
    "-Djava.awt.headless=true",
    s"-Djava.io.tmpdir=$tmpDir",
    "-Dspark.ui.enabled=false",
    "-Dspark.ui.showConsoleProgress=false",
    "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    // GPU broadcasts are slowed by concurrent test JVMs sharing the device; give them more
    // headroom than the 300s default (observed broadcast timeout in premerge run 13641).
    "-Dspark.sql.broadcastTimeout=1200",
    s"-Drapids.test.gpu.allocFraction=$allocationFraction",
    s"-Drapids.test.gpu.maxAllocFraction=$maxAllocationFraction",
    s"-Drapids.test.gpu.minAllocFraction=$minAllocationFraction",
    s"-Dspark.rapids.memory.gpu.allocFraction=$allocationFraction",
    s"-Dspark.rapids.memory.gpu.maxAllocFraction=$maxAllocationFraction",
    s"-Dspark.rapids.memory.gpu.minAllocFraction=$minAllocationFraction")

  private[rapids] def scalaTestArgs(
      suite: String,
      taskId: Int,
      runId: Int,
      testClasses: Path,
      reportsDir: Path,
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String]): ArrayBuffer[String] = {
    val xmlReportsDir = reportsDir.resolve(s"wave-$runId")
    Files.createDirectories(xmlReportsDir)
    // The worker JVM already has testClasses on its system classpath. Passing -R here makes
    // ScalaTest create a new classloader for every Runner.run invocation. Spark retains JVM-wide
    // state between suites, so types loaded by the previous runner (for example,
    // ParquetCachedBatchSerializer) then fail identity checks in the next suite.
    val runnerArgs = ArrayBuffer[String](
      "-o",
      "-u", xmlReportsDir.toString,
      "-f", reportsDir.resolve(s"scala-test-output-wave-$runId-suite-$taskId.txt").toString)
    if (tagsToInclude.nonEmpty) {
      runnerArgs ++= Seq("-n", tagsToInclude.mkString(" "))
    }
    if (tagsToExclude.nonEmpty) {
      runnerArgs ++= Seq("-l", tagsToExclude.mkString(" "))
    }
    runnerArgs ++= Seq("-s", suite)
    runnerArgs
  }

  private def streamLines(label: String, reader: BufferedReader): Thread = {
    val thread = new Thread(s"parallel-unit-test-output-$label") {
      override def run(): Unit = try {
        var line = reader.readLine()
        while (line != null) {
          println(s"[$label] $line")
          line = reader.readLine()
        }
      } catch {
        case _: IOException => // The stream closes when the process is torn down.
      } finally {
        reader.close()
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def orderSuites(
      suites: Seq[String],
      testClasses: Path): Seq[(String, Double)] = {
    suites.map { suite =>
      val classFile = testClasses.resolve(suite.replace('.', File.separatorChar) + ".class")
      val weight = if (Files.exists(classFile)) Files.size(classFile).toDouble else 1.0
      suite -> weight
    }.sortBy { case (_, weight) => -weight }
  }

  private[rapids] def createSuiteBatches(tasks: Seq[SuiteTask]): Seq[SuiteBatch] = {
    val taskBySuite = tasks.map(task => task.suite -> task).toMap
    // Submit these first so the long Parquet suite gets one worker while both DPP suites are
    // pinned to another worker and execute serially. Each worker rejoins the general queue after
    // completing its special batch.
    val specialBatches = Seq(Seq(parquetWriterSuite), dppSuites).flatMap { suites =>
      val batchTasks = suites.flatMap(taskBySuite.get)
      if (batchTasks.nonEmpty) Some(SuiteBatch(batchTasks)) else None
    }
    val specialSuites = specialBatches.flatMap(_.tasks.map(_.suite)).toSet
    specialBatches ++ tasks.filterNot(task => specialSuites.contains(task.suite))
        .map(task => SuiteBatch(Seq(task)))
  }

  private[rapids] def effectiveWorkerCount(
      requestedForks: Int,
      suiteBatches: Seq[SuiteBatch]): Int = {
    math.min(requestedForks, suiteBatches.size)
  }

  private def discoverSuiteNames(testClasses: Path): Seq[String] = {
    // SuiteDiscoveryHelper is package-private in Scala source, but its JVM API is public.
    val discoveryClass = Class.forName("org.scalatest.tools.SuiteDiscoveryHelper$")
    val module = discoveryClass.getField("MODULE$").get(null)
    val method = discoveryClass.getMethod(
      "discoverSuiteNames",
      classOf[scala.collection.immutable.List[_]],
      classOf[ClassLoader],
      classOf[Option[_]])
    method.invoke(
      module,
      List(testClasses.toString),
      Thread.currentThread().getContextClassLoader,
      None)
        .asInstanceOf[scala.collection.immutable.Set[String]]
        .toSeq
  }

  private def matchesWildcard(suite: String, wildcards: Seq[String]): Boolean = {
    wildcards.isEmpty || wildcards.exists(suite.contains)
  }

  private def propertyList(value: String): Seq[String] = {
    propertySeparatedList(value, ',')
  }

  private def propertySeparatedList(value: String, separator: Char): Seq[String] = {
    if (value == null || value.isEmpty || value.startsWith(unresolvedProperty)) {
      Seq.empty
    } else {
      value.split(separator).map(_.trim).filter(_.nonEmpty).toSeq
    }
  }

  private def propertyValue(value: String, default: String): String = {
    if (value == null || value.isEmpty || value.startsWith(unresolvedProperty)) default else value
  }

  private def propertyDouble(value: String, default: Double): Double = {
    propertyValue(value, default.toString).toDouble
  }

  private def splitJvmArgs(value: String): Seq[String] = {
    if (value == null || value.isEmpty || value.startsWith(unresolvedProperty)) {
      Seq.empty
    } else {
      value.trim.split("\\s+").filter(_.nonEmpty).toSeq
    }
  }
}
