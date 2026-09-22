/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench

import com.udf.SparkUtils
import org.apache.spark.sql.SparkSession

/**
 * Generates benchmark data and optionally validates by running
 * BenchUtils.executeCpu and BenchUtils.executeGpu.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.GenData \
 *     -Dexec.args="--rows 1000 --validate --spark-conf k=v ..."
 */
object GenData {

  def main(args: Array[String]): Unit = {
    val (parsed, sparkConfs) = parseArgs(args)

    val rows = parsed.getOrElse("rows",
      throw new IllegalArgumentException("--rows is required")).toLong
    val partitions = parsed.getOrElse("partitions", "32").toInt
    val validate = parsed.contains("validate")
    val outputPath = parsed.get("output-path")

    // Build Spark session
    val builder = SparkSession.builder().appName("GenData").enableHiveSupport()
    SparkUtils.applySparkConfs(builder, sparkConfs)
    val spark = builder.getOrCreate()

    try {
      // Generate synthetic data
      val df = BenchUtils.generateSyntheticData(spark, rows, partitions)

      // Verify row count
      val actualRows = df.count()
      if (actualRows != rows) {
        System.err.println(s"Row count mismatch: expected=$rows, actual=$actualRows")
        sys.exit(1)
      }
      println(s"Generated $actualRows rows across $partitions partitions")

      if (validate) {
        // Validation mode — run both CPU and GPU execute, don't write
        for ((label, executeFn) <- Seq(
          ("cpu", BenchUtils.executeCpu _),
          ("gpu", BenchUtils.executeGpu _)
        )) {
          try {
            executeFn(spark, df).collect()
            println(s"Validation ($label) passed.")
          } catch {
            case e: Exception =>
              System.err.println(
                s"Validation ($label) failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
              e.printStackTrace(System.err)
              sys.exit(1)
          }
        }
      } else {
        // Generation mode — write to output path
        val path = outputPath.getOrElse(
          throw new IllegalArgumentException("--output-path is required when not in validation mode"))
        df.write.mode("overwrite").parquet(path)
        System.err.println(s"Successfully generated dataset and saved to: $path")
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to generate dataset: ${e.getClass.getSimpleName}")
        e.printStackTrace(System.err)
        sys.exit(1)
    } finally {
      spark.stop()
    }

    sys.exit(0)
  }

  /** Parse CLI arguments. */
  private def parseArgs(args: Array[String]): (Map[String, String], Seq[String]) = {
    var map = Map.empty[String, String]
    var sparkConfs = Seq.empty[String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--rows"        => map += ("rows" -> args(i + 1)); i += 2
        case "--partitions"  => map += ("partitions" -> args(i + 1)); i += 2
        case "--validate"    => map += ("validate" -> "true"); i += 1
        case "--output-path" => map += ("output-path" -> args(i + 1)); i += 2
        case "--spark-conf"  => sparkConfs :+= args(i + 1); i += 2
        case other =>
          throw new IllegalArgumentException(s"Unknown argument: $other")
      }
    }
    (map, sparkConfs)
  }
}
