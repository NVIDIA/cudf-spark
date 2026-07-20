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

import java.io.{BufferedReader, BufferedWriter, File, InputStreamReader, OutputStreamWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import javax.xml.parsers.DocumentBuilderFactory

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.fs.FileUtil

import org.apache.spark.sql.SparkSession

/** Runs ScalaTest suites concurrently in isolated JVMs. */
object ParallelUnitTestRunner {
  private case class SuiteTask(id: Int, suite: String, weight: Double)

  private val unresolvedProperty = "${"
  private val parallelGpuAllocationRatio = 0.8
  private val dedicatedSuite = "com.nvidia.spark.rapids.ParquetWriterSuite"
  private val sparkWarehousePrefix = "spark-warehouse"
  private val workerMode = "worker"
  private val protocolPrefix = "__RAPIDS_PARALLEL_UT__"

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
    val wildcardSuites = propertyList(config("wildcardSuites"))
    val tagsToInclude = propertyList(config("tagsToInclude"))
    val tagsToExclude = propertyList(config("tagsToExclude"))
    val childJvmArgs = splitJvmArgs(config("argLine")) ++ splitJvmArgs(config("testJvmArgs"))
    val shuffleManagerOverride = propertyValue(config("shuffleManagerOverride"), "true")
    val allocationFraction = propertyDouble(config("allocationFraction"), 1.0)
    val maxAllocationFraction = propertyDouble(config("maxAllocationFraction"), 1.0)
    val minAllocationFraction = propertyDouble(config("minAllocationFraction"), 0.25)
    val testFailureIgnore = propertyValue(config("testFailureIgnore"), "false").toBoolean
    val configuredSparkConfs = propertySeparatedList(config("sparkConfs"), ';')
    val sparkConfs = if (configuredSparkConfs.isEmpty) {
      Seq(None)
    } else {
      configuredSparkConfs.map(Some(_))
    }

    Files.createDirectories(reportsDir)
    val discovered = discoverSuiteNames(testClasses)
        .filter(matchesWildcard(_, wildcardSuites))
        .sorted

    require(discovered.nonEmpty, "No ScalaTest suites were discovered")
    val forkCount = math.min(math.max(requestedForks, 1), discovered.size)
    val historicalTimings = loadTimings(reportsDir)
    val timingCoverage = discovered.count(historicalTimings.contains).toDouble / discovered.size
    val timings = if (timingCoverage >= 0.8) historicalTimings else Map.empty[String, Double]
    val suiteTasks = orderSuites(discovered, timings, testClasses)
        .zipWithIndex
        .map { case ((suite, weight), index) => SuiteTask(index + 1, suite, weight) }
    val perForkAllocation = allocationFraction * parallelGpuAllocationRatio / forkCount
    val perForkMaxAllocation = maxAllocationFraction * parallelGpuAllocationRatio / forkCount
    val perForkMinAllocation = math.min(minAllocationFraction / forkCount, perForkMaxAllocation)

    val dedicatedTask = suiteTasks.find(_.suite == dedicatedSuite)
    val pooledTasks = suiteTasks.filterNot(_.suite == dedicatedSuite)
    println(s"Running ${discovered.size} suites with at most $forkCount concurrent processes")
    dedicatedTask.foreach { _ =>
      println(s"  dedicated fork: $dedicatedSuite")
    }
    if (pooledTasks.nonEmpty) {
      val poolSize = if (dedicatedTask.nonEmpty && forkCount > 1) forkCount - 1 else forkCount
      println(s"  worker pool: ${pooledTasks.size} suites across $poolSize persistent forks")
    }

    val failures = sparkConfs.zipWithIndex.flatMap { case (sparkConf, runIndex) =>
      sparkConf.foreach(conf => println(s"Parallel test wave ${runIndex + 1}: SPARK_CONF=$conf"))
      val runId = runIndex + 1
      runWave(
        runId,
        sparkConf,
        dedicatedTask,
        pooledTasks,
        forkCount,
        testClasses,
        reportsDir,
        childJvmArgs,
        tagsToInclude,
        tagsToExclude,
        shuffleManagerOverride,
        perForkAllocation,
        perForkMaxAllocation,
        perForkMinAllocation)
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
      dedicatedTask: Option[SuiteTask],
      pooledTasks: Seq[SuiteTask],
      forkCount: Int,
      testClasses: Path,
      reportsDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double): Seq[String] = {
    val failures = new ConcurrentLinkedQueue[String]()

    def runDedicated(): Unit = dedicatedTask.foreach { task =>
      runSuite(
        task,
        runId,
        sparkConf,
        testClasses,
        reportsDir,
        childJvmArgs,
        tagsToInclude,
        tagsToExclude,
        shuffleManagerOverride,
        allocationFraction,
        maxAllocationFraction,
        minAllocationFraction).foreach(failures.add)
    }

    def runPool(workerCount: Int): Unit = {
      if (pooledTasks.nonEmpty) {
        val taskQueue = new ConcurrentLinkedQueue[SuiteTask]()
        pooledTasks.foreach(taskQueue.add)
        val workers = (1 to math.min(workerCount, pooledTasks.size)).map { workerId =>
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
              minAllocationFraction)
          }
          thread.start()
          thread
        }
        workers.foreach(_.join())
        var unprocessed = taskQueue.poll()
        while (unprocessed != null) {
          failures.add(s"wave-$runId ${unprocessed.suite} was not processed")
          unprocessed = taskQueue.poll()
        }
      }
    }

    if (dedicatedTask.nonEmpty && pooledTasks.nonEmpty && forkCount == 1) {
      runDedicated()
      runPool(1)
    } else {
      val dedicatedThread = dedicatedTask.map { _ =>
        val thread = new Thread(s"parallel-unit-test-dedicated-$runId") {
          override def run(): Unit = runDedicated()
        }
        thread.start()
        thread
      }
      val poolSize = if (dedicatedTask.nonEmpty) forkCount - 1 else forkCount
      runPool(poolSize)
      dedicatedThread.foreach(_.join())
    }

    val failureResults = ArrayBuffer.empty[String]
    val failureIterator = failures.iterator()
    while (failureIterator.hasNext) {
      failureResults += failureIterator.next()
    }
    failureResults.toSeq
  }

  private def runSuite(
      task: SuiteTask,
      runId: Int,
      sparkConf: Option[String],
      testClasses: Path,
      reportsDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double): Option[String] = {
    val tmpDir = reportsDir.resolve(s"tmp-wave-$runId-suite-${task.id}")
    Files.createDirectories(tmpDir)
    println(f"[wave-$runId-suite-${task.id}] START ${task.suite} " +
        f"(estimated weight ${task.weight}%.1f)")
    val command = childCommand(
      task,
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
    val processBuilder = new ProcessBuilder(command: _*).redirectErrorStream(true)
    sparkConf.foreach(processBuilder.environment().put("SPARK_CONF", _))
    val process = processBuilder.start()
    val outputThread = streamOutput(runId, task.id, process)
    val exitCode = process.waitFor()
    outputThread.join(TimeUnit.SECONDS.toMillis(10))
    if (exitCode == 0) {
      println(s"[wave-$runId-suite-${task.id}] PASS ${task.suite}")
      None
    } else {
      Some(s"wave-$runId ${task.suite} exited with status $exitCode")
    }
  }

  private def runPoolWorker(
      workerId: Int,
      runId: Int,
      sparkConf: Option[String],
      taskQueue: ConcurrentLinkedQueue[SuiteTask],
      failures: ConcurrentLinkedQueue[String],
      testClasses: Path,
      reportsDir: Path,
      childJvmArgs: Seq[String],
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String],
      shuffleManagerOverride: String,
      allocationFraction: Double,
      maxAllocationFraction: Double,
      minAllocationFraction: Double): Unit = {
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
    val processBuilder = new ProcessBuilder(command: _*).redirectErrorStream(true)
    sparkConf.foreach(processBuilder.environment().put("SPARK_CONF", _))
    var currentTask: Option[SuiteTask] = None
    try {
      val process = processBuilder.start()
      val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))

      def sendNextTask(): Boolean = {
        Option(taskQueue.poll()) match {
          case Some(task) =>
            currentTask = Some(task)
            println(f"[wave-$runId-worker-$workerId] START ${task.suite} " +
                f"(estimated weight ${task.weight}%.1f)")
            writer.write(s"RUN\t${task.id}\t${task.suite}\n")
            writer.flush()
            true
          case None =>
            writer.write("STOP\n")
            writer.flush()
            false
        }
      }

      var running = sendNextTask()
      var line = reader.readLine()
      while (line != null) {
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
          if (running) {
            running = sendNextTask()
          }
        } else {
          println(s"[wave-$runId-worker-$workerId] $line")
        }
        line = reader.readLine()
      }
      reader.close()
      writer.close()
      val exitCode = process.waitFor()
      currentTask.foreach { task =>
        failures.add(s"wave-$runId ${task.suite} lost when worker-$workerId exited " +
            s"with status $exitCode")
      }
      if (exitCode != 0 && currentTask.isEmpty) {
        failures.add(s"wave-$runId worker-$workerId exited with status $exitCode")
      }
    } catch {
      case t: Throwable =>
        currentTask.foreach(taskQueue.add)
        failures.add(s"wave-$runId worker-$workerId failed: ${t.getMessage}")
    }
  }

  private def workerMain(args: Array[String]): Unit = {
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
  }

  private def cleanupWorkerState(tmpDir: Path): Unit = {
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

  private def childCommand(
      task: SuiteTask,
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
    val runnerArgs = scalaTestArgs(
      task.suite,
      task.id,
      runId,
      testClasses,
      reportsDir,
      tagsToInclude,
      tagsToExclude)
    javaCommand(childJvmArgs, childSystemProperties(
      tmpDir,
      shuffleManagerOverride,
      allocationFraction,
      maxAllocationFraction,
      minAllocationFraction)) ++ Seq("org.scalatest.tools.Runner") ++ runnerArgs
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
    s"-Drapids.test.gpu.allocFraction=$allocationFraction",
    s"-Drapids.test.gpu.maxAllocFraction=$maxAllocationFraction",
    s"-Drapids.test.gpu.minAllocFraction=$minAllocationFraction",
    s"-Dspark.rapids.memory.gpu.allocFraction=$allocationFraction",
    s"-Dspark.rapids.memory.gpu.maxAllocFraction=$maxAllocationFraction",
    s"-Dspark.rapids.memory.gpu.minAllocFraction=$minAllocationFraction")

  private def scalaTestArgs(
      suite: String,
      taskId: Int,
      runId: Int,
      testClasses: Path,
      reportsDir: Path,
      tagsToInclude: Seq[String],
      tagsToExclude: Seq[String]): ArrayBuffer[String] = {
    val runnerArgs = ArrayBuffer[String](
      "-R", testClasses.toString,
      "-o",
      "-u", reportsDir.toString,
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

  private def streamOutput(runId: Int, suiteId: Int, process: Process): Thread = {
    val thread = new Thread(s"parallel-unit-test-output-$runId-$suiteId") {
      override def run(): Unit = {
        val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
        try {
          var line = reader.readLine()
          while (line != null) {
            println(s"[wave-$runId-suite-$suiteId] $line")
            line = reader.readLine()
          }
        } finally {
          reader.close()
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def orderSuites(
      suites: Seq[String],
      timings: Map[String, Double],
      testClasses: Path): Seq[(String, Double)] = {
    suites.map { suite =>
      val classFile = testClasses.resolve(suite.replace('.', File.separatorChar) + ".class")
      val fallbackWeight = if (Files.exists(classFile)) Files.size(classFile).toDouble else 1.0
      suite -> timings.getOrElse(suite, fallbackWeight)
    }.sortBy { case (_, weight) => -weight }
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

  private def loadTimings(reportsDir: Path): Map[String, Double] = {
    val factory = DocumentBuilderFactory.newInstance()
    val stream = Files.list(reportsDir)
    try {
      val iterator = stream.iterator()
      val timings = scala.collection.mutable.Map.empty[String, Double]
      while (iterator.hasNext) {
        val path = iterator.next()
        val name = path.getFileName.toString
        if (name.startsWith("TEST-") && name.endsWith(".xml")) {
          try {
            val root = factory.newDocumentBuilder().parse(path.toFile).getDocumentElement
            timings += root.getAttribute("name") -> root.getAttribute("time").toDouble
          } catch {
            case _: Exception => // Ignore incomplete or incompatible historical reports.
          }
        }
      }
      timings.toMap
    } finally {
      stream.close()
    }
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
