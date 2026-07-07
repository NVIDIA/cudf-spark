/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench

import java.io.{File, PrintWriter, StringWriter}
import com.fasterxml.jackson.core.util.{DefaultIndenter, DefaultPrettyPrinter}
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.udf.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * UDF benchmark runner. Measures the end-to-end runtime of:
 *   Read Parquet -> Execute (CPU or GPU) -> Write no-op sink
 *
 * Produces a JSON file with the benchmark results.
 * On error, also produces separate error log file.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.SparkBenchRunner \
 *     -Dexec.args="--mode cpu --data-path data/bench_data_10M_rows.parquet ..."
 */
object SparkBenchRunner {

  private val DefaultSparkLogLevel = "ERROR"

  def main(args: Array[String]): Unit = {
    val (parsed, sparkConfs) = parseArgs(args)

    val mode = parsed.getOrElse("mode",
      throw new IllegalArgumentException("--mode is required (cpu or gpu)"))
    val dataPath = parsed.getOrElse("data-path",
      throw new IllegalArgumentException("--data-path is required"))
    val resultPath = parsed.getOrElse("result-path",
      throw new IllegalArgumentException("--result-path is required"))
    val sparkLogLevel = parsed.getOrElse("spark-log-level", DefaultSparkLogLevel)

    // Resolve execution mode
    val executeFn: (SparkSession, DataFrame) => DataFrame = mode match {
      case "cpu" => BenchUtils.executeCpu
      case "gpu" => BenchUtils.executeGpu
      case other =>
        throw new IllegalArgumentException(
          s"Unknown mode: '$other'. Must be 'cpu' or 'gpu'.")
    }

    // Build Spark session
    val builder = SparkSession.builder().enableHiveSupport()
    SparkUtils.applySparkConfs(builder, sparkConfs)
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(sparkLogLevel)

    try {
      // --- START JOB ---
      val startTime = System.nanoTime()
      val df = spark.read.parquet(dataPath)
      val resultDf = executeFn(spark, df)
      resultDf.write.format("noop").mode("overwrite").save()
      val elapsed = (System.nanoTime() - startTime) / 1e9
      // --- END JOB ---

      System.err.println(s"E2E Runtime (s): ${f"$elapsed%.2f"}")

      writeReport(
        path = resultPath,
        mode = mode,
        dataPath = dataPath,
        elapsed = elapsed,
        status = "success",
        cliArgs = args)

    } catch {
      case e: Exception =>
        System.err.println(s"Benchmark run failed: ${e.getClass.getSimpleName}")
        e.printStackTrace(System.err)

        // Error stack trace is written to a separate error log file.
        val errorLogPath = resultPath.replace("_result.json", "_error.log")
        writeErrorLog(errorLogPath, e)

        writeReport(
          path = resultPath,
          mode = mode,
          dataPath = dataPath,
          elapsed = -1,
          status = "error",
          cliArgs = args,
          errorMessage = Option(e.getMessage),
          errorLogFile = Some(errorLogPath))

        sys.exit(1)
    } finally {
      spark.stop()
    }

    sys.exit(0)
  }

  /** Write a JSON benchmark report containing the result and args. */
  private def writeReport(
      path: String,
      mode: String,
      dataPath: String,
      elapsed: Double,
      status: String,
      cliArgs: Array[String],
      errorMessage: Option[String] = None,
      errorLogFile: Option[String] = None
  ): Unit = {
    val resultDir = new File(path).getParentFile
    if (resultDir != null) resultDir.mkdirs()

    import java.util.{LinkedHashMap => JLinkedHashMap, Arrays => JArrays}
    val report = new JLinkedHashMap[String, AnyRef]()
    report.put("mode", mode)
    report.put("data_path", dataPath)
    report.put("status", status)
    report.put("e2e_runtime", java.lang.Double.valueOf(elapsed))
    report.put("cli_args", JArrays.asList(cliArgs: _*))
    errorMessage.foreach { msg =>
      val error = new JLinkedHashMap[String, String]()
      error.put("error_message", msg)
      errorLogFile.foreach(f => error.put("error_log_file", f))
      report.put("error", error)
    }

    val mapper = new ObjectMapper()
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    val printer = new DefaultPrettyPrinter()
    printer.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
    mapper.writer(printer).writeValue(new File(path), report)
    System.err.println(s"Report written to: $path")
  }

  /** Write an exception to an error log file. */
  private def writeErrorLog(path: String, e: Exception): Unit = {
    val logDir = new File(path).getParentFile
    if (logDir != null) logDir.mkdirs()

    val pw = new PrintWriter(path)
    try {
      val sw = new StringWriter()
      e.printStackTrace(new java.io.PrintWriter(sw))
      pw.print(sw.toString)
    } finally {
      pw.close()
    }
    System.err.println(s"Error details written to: $path")
  }

  /** Parse CLI arguments. */
  private def parseArgs(args: Array[String]): (Map[String, String], Seq[String]) = {
    var map = Map.empty[String, String]
    var sparkConfs = Seq.empty[String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--mode"            => map += ("mode" -> args(i + 1)); i += 2
        case "--data-path"       => map += ("data-path" -> args(i + 1)); i += 2
        case "--result-path"     => map += ("result-path" -> args(i + 1)); i += 2
        case "--spark-log-level" => map += ("spark-log-level" -> args(i + 1)); i += 2
        case "--spark-conf"      => sparkConfs :+= args(i + 1); i += 2
        case other =>
          throw new IllegalArgumentException(s"Unknown argument: $other")
      }
    }
    (map, sparkConfs)
  }
}
