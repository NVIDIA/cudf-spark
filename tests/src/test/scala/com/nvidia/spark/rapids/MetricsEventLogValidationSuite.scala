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

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.io.Source

import com.nvidia.spark.rapids.shims.EventLogJsonShims
import org.json4s._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class MetricsEventLogValidationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val tempDir = new File(System.getProperty("java.io.tmpdir"), "metrics-eventlog-test")
  private val eventLogDir = new File(tempDir, "eventlogs")
  private var conf: SparkConf = _

  override def beforeEach(): Unit = {
    // Clean up temp directories
    if (tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
    tempDir.mkdirs()
    eventLogDir.mkdirs()

    conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MetricsEventLogValidation")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.batchSizeBytes", "65536")
      .set("spark.eventLog.enabled", "true")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.eventLog.dir", eventLogDir.getAbsolutePath)
      .set("spark.eventLog.rolling.enabled", "false") // Keep single file for easier parsing
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }

    if (tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
  }

  case class MetricRecord(name: String, value: Long, stage: Option[Int] = None)

  case class TaskTimeRecord(taskId: Long, executionTime: Long, stage: Option[Int] = None)

  /**
   * Ensure event logs are completely written and flushed to disk by stopping the Spark session.
   * This forces Spark to flush all event log buffers and close the files properly.
   * After calling this method, the current spark session will be stopped and set to null.
   */
  private def flushEventLogsByStoppingSpark(): Unit = {
    if (spark != null) {
      try {
        // Stop the current Spark session, which will flush and close all event logs
        spark.stop()
        spark = null

        // Give a small amount of time for file system operations to complete
        Thread.sleep(10)

        println("Spark session stopped to ensure event logs are flushed")
      } catch {
        case e: Exception =>
          println(s"Warning: Error stopping Spark session: ${e.getMessage}")
      }
    }
  }

  /**
   * Read lines from event log file, handling both compressed and uncompressed formats.
   * Uses Spark's CompressionCodec system with file name pattern matching.
   */
  private def readEventLogLines(file: File): List[String] = {
    val fileName = file.getName.toLowerCase

    try {
      // Determine compression codec name based on file extension
      val codecName: Option[String] = if (fileName.contains(".gz")) {
        println(s"Reading GZip compressed file: ${file.getName}")
        Some("gzip")
      } else if (fileName.contains(".bz2")) {
        println(s"Reading BZip2 compressed file: ${file.getName}")
        Some("bzip2")
      } else if (fileName.contains(".zst")) {
        println(s"Reading Zstandard compressed file: ${file.getName}")
        Some("zstd")
      } else if (fileName.contains(".lz4")) {
        println(s"Reading LZ4 compressed file: ${file.getName}")
        Some("lz4")
      } else if (fileName.contains(".snappy")) {
        println(s"Reading Snappy compressed file: ${file.getName}")
        Some("snappy")
      } else {
        None // Uncompressed file
      }

      codecName match {
        case Some(codecShortName) =>
          try {
            // Create Spark compression codec
            val codec = TrampolineUtil.createCodec(conf, codecShortName)

            // Read compressed file
            val fileInputStream = new java.io.FileInputStream(file)
            val decompressedStream = codec.compressedInputStream(fileInputStream)

            try {
              val reader = new BufferedReader(new InputStreamReader(decompressedStream, "UTF-8"))
              val lines = scala.collection.mutable.ListBuffer[String]()
              var line = reader.readLine()
              while (line != null) {
                lines += line
                line = reader.readLine()
              }
              lines.toList
            } finally {
              decompressedStream.close()
              fileInputStream.close()
            }
          } catch {
            case e: Exception =>
              println(s"Warning: Failed to create or use ${codecShortName} codec for " +
                s"${file.getName}: ${e.getMessage}")
              // Fall back to uncompressed reading
              Source.fromFile(file, "UTF-8").getLines().toList
          }
        case None =>
          // File is not compressed, read normally
          Source.fromFile(file, "UTF-8").getLines().toList
      }

    } catch {
      case e: Exception =>
        // Fall back to regular file reading if compression handling fails
        println(s"Warning: Failed to read file ${file.getName}, " +
          s"trying uncompressed read: ${e.getMessage}")
        try {
          Source.fromFile(file, "UTF-8").getLines().toList
        } catch {
          case ex: Exception =>
            println(s"Error: Cannot read file ${file.getName}: ${ex.getMessage}")
            List.empty[String]
        }
    }
  }

  private def parseEventLogs(): (List[MetricRecord], List[TaskTimeRecord]) = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val metrics = mutable.ListBuffer[MetricRecord]()
    val taskTimes = mutable.ListBuffer[TaskTimeRecord]()

    val eventLogFiles = eventLogDir.listFiles()
      .filter(f => f.getName.endsWith(".inprogress") || !f.getName.contains("_tmp_"))
      .toList

    eventLogFiles.foreach { file =>
      try {
        val lines = readEventLogLines(file)

        lines.foreach { line =>
          try {
            val json = EventLogJsonShims.parseJson(line)
            val eventType = (json \ "Event").extractOpt[String]

            eventType match {
              case Some("SparkListenerTaskEnd") =>
                val stageId = (json \ "Stage ID").extractOpt[Int]
                val taskInfo = (json \ "Task Info")

                // Extract task execution time from Task Metrics
                val taskId = EventLogJsonShims.extractLong(taskInfo \ "Task ID")
                val taskMetrics = (json \ "Task Metrics")
                // https://github.com/apache/spark/blob/450b415028c3b00f3a002126cd11318d3932e28f/
                // core/src/main/scala/org/apache/spark/ui/jobs/StagePage.scala#L151
                val executorRunTime =
                  EventLogJsonShims.extractLong(taskMetrics \ "Executor Run Time")

                (taskId, executorRunTime) match {
                  case (Some(tId), Some(runTime)) =>
                    taskTimes += TaskTimeRecord(tId, runTime, stageId)
                  case _ => // Skip if timing data is incomplete
                }

                // Extract operator time metrics
                val accumulables = (taskInfo \ "Accumulables").extract[List[JObject]]
                accumulables.foreach { acc =>

                  // refer org.apache.spark.scheduler.AccumulableInfo
                  val name = (acc \ "Name").extractOpt[String]
                  val value = (acc \ "Update").extractOpt[String]

                  (name, value) match {
                    case (Some(n), Some(v)) if n.equals("op time") => {
                      metrics += MetricRecord(n, v.toLong, stageId)
                    }
                    case _ => // Ignore other metrics
                  }
                }

              case _ => // Ignore other events
            }
          } catch {
            case _: Exception => // Skip malformed lines
          }
        }
      } catch {
        case e: Exception =>
          println(s"Warning: Could not parse event log ${file.getName}: ${e.getMessage}")
      }
    }

    (metrics.toList, taskTimes.toList)
  }

  test("operator time metrics are recorded in event logs with OpTimeTracking enabled") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Enable OpTimeTracking
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")

    val numRows = 5000000L
    val numTasks = 8

    // Run query that should generate operator time metrics
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 20 as group_key",
        "rand() * 100 as value"
      )
      .groupBy("group_key")
      .agg(
        count("*").as("count"),
        sum("value").as("sum_value"),
        avg("value").as("avg_value")
      )
      .filter($"count" > 1000)

    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")

    // Stop Spark session to ensure event logs are completely flushed to disk
    flushEventLogsByStoppingSpark()

    // Parse event logs to find metrics and task times
    val (metrics, taskTimes) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

    assert(operatorTimeMetrics.nonEmpty,
      s"Should find operator time metrics in event logs. " +
        s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")

    assert(taskTimes.nonEmpty,
      s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")

    // Calculate total operator time (in nanoseconds)
    val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

    // Calculate total task execution time
    // (Executor Run Time in milliseconds, convert to nanoseconds)
    val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum

    // Verify metric values are reasonable (> 0)
    operatorTimeMetrics.foreach { metric =>
      assert(metric.value > 0, s"operator time metric ${metric.name} " +
        s"should have positive value, got ${metric.value}")
    }

    taskTimes.foreach { taskTime =>
      assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
        s"should be positive, got ${taskTime.executionTime}")
    }

    println(s"Found ${operatorTimeMetrics.length} operator time metrics in event logs")
    println(s"Found ${taskTimes.length} executor run time records in event logs")
    println(f"Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
    println(f"Total executor run time: ${totalTaskExecutionTime / 1000000.0}%.2f ms")

    // Verify that operator time is within expected range of executor run time
    // Operator time should be between 50% and 100% of executor run time
    val minExpectedOperatorTime = totalTaskExecutionTime * 0.5
    val maxExpectedOperatorTime = totalTaskExecutionTime * 1.2 // allow some margin
    val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble

    println(f"Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% of executor run time")
    println(f"Expected range: 50.0%% - 100.0%% of executor run time")

    assert(totalOperatorTime >= minExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should be at least 50%% " +
        f"of total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was only ${operatorTimeRatio * 100.0}%.1f%%")

    assert(totalOperatorTime <= maxExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should not exceed " +
        f"total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was ${operatorTimeRatio * 100.0}%.1f%%")

    operatorTimeMetrics.foreach { m =>
      println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms (stage ${m.stage.getOrElse("unknown")})")
    }
  }

  test("operator time metrics are less when c2r and r2c happened") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Enable OpTimeTracking
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")

    spark.conf.set("spark.rapids.sql.exec.HashAggregateExec", "false")

    val numRows = 5000000L
    val numTasks = 8

    // Run query that should generate operator time metrics
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 20 as group_key",
        "rand() * 100 as value"
      )
      .groupBy("group_key")
      .agg(
        count("*").as("count"),
        sum("value").as("sum_value"),
        avg("value").as("avg_value")
      )
      .filter($"count" > 1000)

    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")

    // Stop Spark session to ensure event logs are completely flushed to disk
    flushEventLogsByStoppingSpark()

    // Parse event logs to find metrics and task times
    val (metrics, taskTimes) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

    assert(operatorTimeMetrics.nonEmpty,
      s"Should find operator time metrics in event logs. " +
        s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")

    assert(taskTimes.nonEmpty,
      s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")

    // Calculate total operator time (in nanoseconds)
    val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

    // Calculate total task execution time
    // (Executor Run Time in milliseconds, convert to nanoseconds)
    val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum

    // Verify metric values are reasonable (> 0)
    operatorTimeMetrics.foreach { metric =>
      assert(metric.value > 0, s"operator time metric ${metric.name} " +
        s"should have positive value, got ${metric.value}")
    }

    taskTimes.foreach { taskTime =>
      assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
        s"should be positive, got ${taskTime.executionTime}")
    }

    println(s"Found ${operatorTimeMetrics.length} operator time metrics in event logs")
    println(s"Found ${taskTimes.length} executor run time records in event logs")
    println(f"Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
    println(f"Total executor run time: ${totalTaskExecutionTime / 1000000.0}%.2f ms")

    // Verify that operator time is within expected range of executor run time
    // Operator time should be between 0% and 80% of executor run time
    val minExpectedOperatorTime = 0
    val maxExpectedOperatorTime = totalTaskExecutionTime * 0.8
    val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble

    println(f"Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% of executor run time")
    println(f"Expected range: 0.0%% - 80.0%% of executor run time")

    assert(totalOperatorTime > minExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should be at least >0%% " +
        f"of total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was only ${operatorTimeRatio * 100.0}%.1f%%")

    assert(totalOperatorTime <= maxExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should not exceed " +
        f"80%% of total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was ${operatorTimeRatio * 100.0}%.1f%%")

    operatorTimeMetrics.foreach { m =>
      println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms " +
        f"(stage ${m.stage.getOrElse("unknown")})")
    }
    println("Test completed successfully.")
  }

  test("operator time metrics are reasonable for parquet write jobs") {
    val sparkSession = spark
    import sparkSession.implicits._

    try {
      // Enable OpTimeTracking for this test
      spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")

      // Configure slow filesystem for testing and disable cache to prevent pollution
      val slowFsWriteDelayMs = 100L
      val numWritePartitions = 50
      val minExpectedStage5OperatorTimeFraction = 0.6
      // Keep AQE from coalescing the write shuffle partitions used by the slowfs lower bound.
      spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      spark.conf.set("fs.slowfs.impl.disable.cache", "true")
      spark.conf.set("fs.slowfs.impl", "com.nvidia.spark.rapids.SlowFileSystem")
      spark.conf.set("slowfs.write.delay.ms", slowFsWriteDelayMs.toString)

      val numRows = 5000000L
      val numTasks = 8
      val parquetOutputPath = "slowfs://" + new File(tempDir, "test_parquet").getAbsolutePath

      // Create test data for parquet write
      val testDataDF = spark.range(0, numRows, 1, numTasks)
        .selectExpr(
          "id",
          "id % 100 as category",
          "rand() * 1000 as price",
          "cast(rand() * 10000 as int) as quantity"
        )
        .groupBy("category")
        .agg(
          count("*").as("total_count"),
          sum("price").as("total_price"),
          avg("quantity").as("avg_quantity")
        )
        .filter($"total_count" > 5)

      // Use many small output tasks so SlowFileSystem delays are visible in write-stage op time.
      testDataDF
        .repartition(numWritePartitions)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(parquetOutputPath)

      // Verify the output file exists (check the actual file path, not the slowfs path)
      val actualOutputPath = new File(tempDir, "test_parquet")
      assert(actualOutputPath.exists() && actualOutputPath.listFiles().nonEmpty,
        "Parquet output files should be created")

      // Stop Spark session to ensure event logs are completely flushed to disk
      flushEventLogsByStoppingSpark()

      // Parse event logs to find metrics and task times
      val (metrics, taskTimes) = parseEventLogs()
      val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

      assert(operatorTimeMetrics.nonEmpty,
        s"Should find operator time metrics for parquet write job. " +
          s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")

      assert(taskTimes.nonEmpty,
        s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")

      // Calculate total operator time (in nanoseconds)
      val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

      // Calculate total task execution time
      // (Executor Run Time in milliseconds, convert to nanoseconds)
      val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum

      // Verify metric values are reasonable (> 0)
      operatorTimeMetrics.foreach { metric =>
        assert(metric.value > 0, s"operator time metric ${metric.name} " +
          s"should have positive value, got ${metric.value}")
      }

      println(s"Parquet write job: Found ${operatorTimeMetrics.length} operator time metrics")
      println(s"Parquet write job: Found ${taskTimes.length} executor run time records")
      println(f"Parquet write job: Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
      println(f"Parquet write job: Total executor run time: " +
        f"${totalTaskExecutionTime / 1000000.0}%.2f ms")

      val minExpectedOperatorTime = totalTaskExecutionTime * 0.3
      val maxExpectedOperatorTime = totalTaskExecutionTime * 1.2 // allow some margin
      val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble

      println(f"Parquet write job: Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% " +
        "of executor run time")
      println(f"Parquet write job: Expected range: 30.0%% - 100.0%% of executor run time")

      assert(totalOperatorTime >= minExpectedOperatorTime,
        f"Parquet write job: Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) " +
          f"should be at least 30%% of total executor run time " +
          f"(${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
          f"but was only ${operatorTimeRatio * 100.0}%.1f%%")

      assert(totalOperatorTime <= maxExpectedOperatorTime,
        f"Parquet write job: Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) " +
          f"should not exceed total executor run time " +
          f"(${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
          f"but was ${operatorTimeRatio * 100.0}%.1f%%")

      val stage5Metrics = operatorTimeMetrics.filter(_.stage.contains(5))
      val stage5OperatorTime = stage5Metrics.map(_.value).sum
      val stage5Ratio = if (totalOperatorTime > 0) {
        stage5OperatorTime.toDouble / totalOperatorTime.toDouble
      } else {
        0.0
      }
      // Allow margin for write path work outside the operator timing boundary.
      val minExpectedStage5OperatorTime =
        (TimeUnit.MILLISECONDS.toNanos(numWritePartitions * slowFsWriteDelayMs) *
          minExpectedStage5OperatorTimeFraction).toLong

      println(f"Parquet write job: Stage 5 operator time: " +
        f"${stage5OperatorTime / 1000000.0}%.2f ms")
      println(f"Parquet write job: Stage 5 ratio: ${stage5Ratio * 100.0}%.1f%% " +
        "of total operator time")
      println(f"Parquet write job: Stage 5 expected minimum operator time: " +
        f"${minExpectedStage5OperatorTime / 1000000.0}%.2f ms")

      assert(stage5Metrics.nonEmpty,
        "Should find operator time metrics for stage 5 (parquet write stage)")

      assert(stage5OperatorTime >= minExpectedStage5OperatorTime,
        f"Stage 5 (parquet write stage) operator time should be at least " +
          f"${minExpectedStage5OperatorTime / 1000000.0}%.2f ms based on " +
          f"${minExpectedStage5OperatorTimeFraction * 100.0}%.1f%% of " +
          f"$numWritePartitions write partitions and $slowFsWriteDelayMs ms slowfs delay, " +
          f"but was ${stage5OperatorTime / 1000000.0}%.2f ms")

      operatorTimeMetrics.foreach { m =>
        println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms " +
          f"(stage ${m.stage.getOrElse("unknown")})")
      }
      println("Parquet write job: Test completed successfully.")

    } finally {

      // Clear FileSystem cache to prevent contamination from SlowFileSystem
      try {
        import org.apache.hadoop.fs.FileSystem
        FileSystem.closeAll()
      } catch {
        case _: Exception => // Ignore cleanup failures to avoid breaking tests
      }
    }
  }

  test("no operator time metrics when OpTimeTracking is disabled") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Disable OpTimeTracking for this test
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "false")

    val numRows = 2000000L
    val numTasks = 4

    // Run a query that would normally generate operator time metrics
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 50 as bucket",
        "rand() * 500 as score"
      )
      .groupBy("bucket")
      .agg(
        count("*").as("record_count"),
        sum("score").as("total_score"),
        max("score").as("max_score"),
        min("score").as("min_score")
      )
      .filter($"record_count" > 10000)
      .orderBy($"total_score".desc)

    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")

    // Stop Spark session to ensure event logs are completely flushed to disk
    flushEventLogsByStoppingSpark()

    // Parse event logs to find metrics and task times
    val (metrics, taskTimes) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

    println(s"OpTimeTracking disabled: Found ${metrics.length} total metrics")
    println(s"OpTimeTracking disabled: Found ${operatorTimeMetrics.length} operator time metrics")
    println(s"OpTimeTracking disabled: Found ${taskTimes.length} executor run time records")

    // When OpTimeTracking is disabled, there should be no operator time metrics
    assert(operatorTimeMetrics.isEmpty,
      s"Should not find any operator time metrics when OpTimeTracking is disabled. " +
        s"Found ${operatorTimeMetrics.length} operator time metrics: " +
        s"${operatorTimeMetrics.map(m => s"${m.name}=${m.value}")}")

    // But we should still have task execution times
    assert(taskTimes.nonEmpty,
      s"Should still find executor run times even when OpTimeTracking is disabled. " +
        s"Found ${taskTimes.length} tasks")

    // Verify task execution times are reasonable
    taskTimes.foreach { taskTime =>
      assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
        s"should be positive, got ${taskTime.executionTime}")
    }

    val totalTaskExecutionTime = taskTimes.map(_.executionTime).sum
    println(f"OpTimeTracking disabled: Total executor run time: " +
      f"${totalTaskExecutionTime}%.2f ms")

    // Verify that we executed a meaningful workload (total execution time > 100ms)
    assert(totalTaskExecutionTime > 100,
      s"Total task execution time should be substantial to validate the test, " +
        s"got ${totalTaskExecutionTime} ms")
  }

  /**
   * Parse event logs into per-stage aggregates
   * (stageId -> (sum of "op time" in ns, sum of executorRunTime in ms)).
   *
   * Aggregating per stage rather than per task is deliberate: "op time" is
   * recorded in NANOSECONDS while Spark's "Executor Run Time" is truncated to
   * MILLISECONDS. On a sub-millisecond task that mismatch alone makes
   * sum(op_time) > executorRunTime (0.1 ms of ns-precision op_time vs a run
   * time that floors to 0 ms) -- a granularity artifact, not an over-count.
   * Summing both quantities over all tasks of a stage drowns that per-task
   * rounding noise (bounded by ~1 ms per empty task) under the stage's real
   * work (seconds), so the surviving signal is the structural bug-1 over-count
   * the write-path fixes (#14901) restore.
   */
  private def parseEventLogsPerStage(): Map[Int, (Long, Long)] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val opTimeByStage = mutable.Map[Int, Long]().withDefaultValue(0L)
    val runTimeByStage = mutable.Map[Int, Long]().withDefaultValue(0L)

    eventLogDir.listFiles()
      .filter(f => f.getName.endsWith(".inprogress") || !f.getName.contains("_tmp_"))
      .foreach { file =>
        readEventLogLines(file).foreach { line =>
          try {
            val json = EventLogJsonShims.parseJson(line)
            if ((json \ "Event").extractOpt[String].contains("SparkListenerTaskEnd")) {
              val taskInfo = json \ "Task Info"
              val taskMetrics = json \ "Task Metrics"
              val stageId = (json \ "Stage ID").extractOpt[Int]
              val runTime = EventLogJsonShims.extractLong(taskMetrics \ "Executor Run Time")
              (stageId, runTime) match {
                case (Some(sId), Some(rt)) =>
                  runTimeByStage(sId) = runTimeByStage(sId) + rt
                  (taskInfo \ "Accumulables").extract[List[JObject]].foreach { acc =>
                    val name = (acc \ "Name").extractOpt[String]
                    val value = (acc \ "Update").extractOpt[String]
                    (name, value) match {
                      // "op time" is the per-operator op_time metric; sum all of
                      // them (Insert + descendants) across the stage's tasks.
                      case (Some(n), Some(v)) if n == "op time" =>
                        opTimeByStage(sId) = opTimeByStage(sId) + v.toLong
                      case _ =>
                    }
                  }
                case _ =>
              }
            }
          } catch {
            case _: Exception => // skip malformed lines
          }
        }
      }
    runTimeByStage.map { case (sId, rt) => sId -> ((opTimeByStage(sId), rt)) }.toMap
  }

  test("write op_time stays within executorRunTime with empty partitions (#14901)") {
    // Default write path: WriteFilesExec on Spark 3.4+ (exercises the
    // GpuWriteFilesExec excludeMetrics forwarding), the direct
    // GpuFileFormatWriter.write on Spark 3.3.x.
    assertWriteOpTimeWithinExecRunTime(forceLegacyWritePath = false)
  }

  test("write op_time stays within executorRunTime on the non-WriteFilesExec path (#14901)") {
    // Disabling planned-write routes Spark 3.4+ through GpuFileFormatWriter.write
    // -- the path Hive/Delta/CTAS always take -- so the AdaptiveSparkPlanExec
    // unwrap in excludeMetrics collection is exercised on those versions too,
    // not just on Spark 3.3.x.
    assertWriteOpTimeWithinExecRunTime(forceLegacyWritePath = true)
  }

  private def assertWriteOpTimeWithinExecRunTime(forceLegacyWritePath: Boolean): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._

    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")
    // Keep the write partitions as-is so the empty ones survive to the write stage,
    // exercising the empty-partition `iterator.hasNext` path in executeTask.
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    // Rely on the suite-wide small batch size (64KB, set in beforeEach): it
    // splits the post-shuffle data into many GPU micro-batches, so the
    // descendant project's per-batch kernel-launch op_time accumulates and
    // dominates the write stage -- which is what makes the pre-fix Insert
    // double-count clear the 1.3x threshold. A modest row count keeps the batch
    // count (and wall time) small.
    if (forceLegacyWritePath) {
      // Spark 3.4+ inserts WriteFilesExec by default; disabling planned-write
      // routes the command through GpuFileFormatWriter.write instead. The config
      // is unknown (and harmless) on Spark 3.3.x, which always uses that path.
      spark.conf.set("spark.sql.optimizer.plannedWrite.enabled", "false")
    }

    val numRows = 200000L
    val numWritePartitions = 64
    val parquetOutputPath = new File(tempDir, "empty_part_write").getAbsolutePath

    // The write stage must carry non-trivial descendant op_time for the bug to
    // show: hash-repartition on a 4-value key into 64 partitions FIRST (leaving
    // ~60 partitions EMPTY at the write stage -> exercises the empty-partition
    // hasNext branch), THEN do heavy GPU compute AFTER the shuffle so it is a
    // descendant of the Insert inside the write stage. Pre-fix the Insert
    // op_time fails to exclude that descendant -- via missing excludeMetrics
    // forwarding on the WriteFilesExec path, or the AdaptiveSparkPlanExec match
    // gap on the direct write path -- so it double-counts the descendant and
    // the write stage's sum(op_time) exceeds the 1.3x-of-sum(executorRunTime)
    // detection threshold; post-fix the descendant is excluded and the ratio
    // returns to ~1x.
    //
    // The project is a chain of UNARY transcendental functions, not nested
    // binary arithmetic: on Spark 3.4.x BinaryArithmetic.dataType is an uncached
    // `def` that evaluates BOTH children (SPARK-45071, fixed by a `lazy val` in
    // 3.5.0), so a deeply nested arithmetic tree (e.g. a many-term
    // `mkString(" + ")`) makes the analyzer pathologically slow -- never-
    // completing analysis was observed on 3.4.0 before any GPU work ran. A
    // unary function resolves its dataType from a single child (linear), so the
    // chain is cheap to analyze on every shim while still issuing one GPU kernel
    // per layer. sin/cos keep the value bounded in [-1, 1] so it neither
    // overflows nor constant-folds.
    val d = "cast(id AS double)"
    val heavy = (0 until 24).foldLeft(s"abs($d) * 1.0e-9") { (e, _) => s"sin(cos($e))" }
    val df = spark.range(0, numRows, 1, 8)
      .selectExpr("id", "id % 4 as k")
      .repartition(numWritePartitions, $"k")
      .selectExpr("id", "k", s"($heavy) as h")
      .filter($"h" < 1.0e18)

    df.write.mode("overwrite").option("compression", "snappy").parquet(parquetOutputPath)
    assert(new File(parquetOutputPath).exists())

    flushEventLogsByStoppingSpark()

    val perStage = parseEventLogsPerStage()
    assert(perStage.nonEmpty, "should find per-stage records in event logs")

    // The accounting invariant the write-path fixes restore: within a stage,
    // the summed op_time of all operators cannot exceed the stage's summed
    // executor run time (each operator's op_time is a slice of its task's wall
    // time). Pre-fix the Insert op_time double-counts the heavy post-shuffle
    // project/filter that are its descendants -- the write stage's sum(op_time)
    // lands at ~2x its sum(executorRunTime). Post-fix the descendants are
    // excluded and the ratio drops to ~1x.
    //
    // Only stages that did non-trivial work (summed run time over the floor)
    // are checked, so we never divide by a stage whose run time is dominated by
    // millisecond-rounding noise. We additionally assert at least one such
    // stage exists, so the test fails loudly (rather than vacuously passing) if
    // the workload ever degrades to all-trivial stages on faster hardware.
    val marginFactor = 1.3
    val stageRunTimeFloorMs = 200L
    val meaningful = perStage.filter { case (_, (_, rtMs)) => rtMs >= stageRunTimeFloorMs }
    assert(meaningful.nonEmpty,
      s"expected at least one stage with >= $stageRunTimeFloorMs ms summed executorRunTime " +
        s"to make the ratio check meaningful; per-stage run times (ms) were " +
        s"${perStage.map { case (s, (_, rt)) => s -> rt }.toSeq.sorted}")

    val violators = meaningful.filter { case (_, (opNs, rtMs)) =>
      opNs.toDouble > rtMs.toDouble * 1000000.0 * marginFactor
    }

    if (violators.nonEmpty) {
      val sample = violators.toSeq.sortBy(_._1).map { case (sId, (opNs, rtMs)) =>
        f"stage $sId: sum(op_time)=${opNs / 1e6}%.0f ms vs sum(executorRunTime)=$rtMs ms " +
          f"(${opNs / 1e6 / math.max(rtMs, 1L)}%.2fx)"
      }.mkString("; ")
      fail(s"${violators.size}/${meaningful.size} stages have summed op_time exceeding " +
        f"$marginFactor%.1fx summed executorRunTime, indicating write-path op_time " +
        s"over-count (#14901 not fixed). $sample")
    }
  }

  test("shuffle-read op time is excluded from consumers across AQE query stages (#14933)") {
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")
    // Disable AQE partition coalescing so the reduce stage reads the GpuColumnarExchange
    // directly instead of through a GpuCustomShuffleReaderExec. AQE then materializes that
    // exchange as a ShuffleQueryStageExec -- a LeafExecNode whose children are empty -- so
    // a consumer's descendant-op-time walk only reaches the exchange's
    // "op time (shuffle read)" metric if it descends through the query stage.
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

    // A group-by forces a shuffle (partial aggregate -> exchange -> final aggregate).
    // collect() materializes the AQE query stages, so the executed plan contains the
    // ShuffleQueryStageExec wrapping the reduce-side exchange.
    val df = spark.range(0, 200000L, 1, 8).selectExpr("id % 1000 as k", "id as v")
    val resultDF = df.groupBy("k").agg(sum("v").as("sv"))
    assert(resultDF.collect().nonEmpty, "Query should produce results")

    val executed = resultDF.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case other => other
    }
    // Spark 4.0+ wraps the final stage in a ResultQueryStageExec (a QueryStageExec, i.e.
    // a LeafExecNode whose children are empty); descend into its `.plan` so the operator
    // tree below is reachable by the traversal. No-op on Spark 3.3 (no result stage).
    val finalPlan = executed match {
      case q: QueryStageExec => q.plan
      case other => other
    }

    // The in-stage consumer is the GpuExec whose child is the reduce-side query stage.
    val consumers = finalPlan.collect {
      case c: GpuExec if c.children.exists(_.isInstanceOf[ShuffleQueryStageExec]) => c
    }
    assert(consumers.nonEmpty,
      s"expected a GpuExec consuming a ShuffleQueryStageExec; plan was:\n$finalPlan")
    val consumer = consumers.head
    val stage = consumer.children.collectFirst { case q: ShuffleQueryStageExec => q }.get
    val exchange = stage.plan match {
      case r: ReusedExchangeExec => r.child
      case p => p
    }
    val readMetric = exchange match {
      case g: GpuExec => g.getOpTimeNewMetric
      case _ => None
    }
    assert(readMetric.isDefined,
      s"reduce-side exchange (${exchange.nodeName}) should expose an OP_TIME_NEW " +
        "(\"op time (shuffle read)\") metric")

    // The fix: a consumer's getDescendantOpTimeMetrics must descend through the
    // ShuffleQueryStageExec and collect the exchange's shuffle-read metric, so the
    // consumer's op_time excludes it. Pre-fix the walk stops at the (leaf) query stage and
    // the metric is missing, leaving the shuffle-read time double-counted (#14933).
    val descendants = consumer.getDescendantOpTimeMetrics
    assert(descendants.exists(_ eq readMetric.get),
      s"${consumer.nodeName}.getDescendantOpTimeMetrics must include the reduce-side " +
        "exchange's \"op time (shuffle read)\" metric (so it is excluded from the consumer's " +
        "op time); it was missing, so the shuffle-read time would be double-counted. " +
        s"Collected ${descendants.size} descendant op-time metrics.")
  }

}
