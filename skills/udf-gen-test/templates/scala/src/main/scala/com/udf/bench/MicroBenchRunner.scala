/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench

import java.io.File

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{
  ColumnVector,
  Cuda,
  CudaMemInfo,
  HostColumnVector,
  Rmm,
  RmmAllocationMode,
  Table
}
import com.udf.Arm.{closeAll, withResource}

/**
 * Microbenchmark runner for CPU vs. RapidsUDF. Measures UDF execution time on in-memory dataset.
 *
 * Reads Parquet file (produced by GenData) via cuDF Table.readParquet.
 * Benchmarks CPU (row-by-row evaluate) and GPU (evaluateColumnar) paths.
 * Data loading and host/device transfers are not part of timing.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.MicroBenchRunner \
 *     -Dexec.args="--mode all --data-path data/bench_data --rows 1000000"
 */
object MicroBenchRunner {

  private val DefaultWarmup = 2
  private val DefaultMeasured = 4
  private val DefaultRmmAllocFraction = 0.9f

  /**
   * TODO: Extract column data from host memory into Scala objects.
   *
   * Called once before CPU timing loop. Convert HostColumnVectors to
   * array of Scala objects for executeCpu.
   * Use hostColumns(i).getJavaString(row), .getInt(row), .getDouble(row),
   * .getStruct(row), .getList(row), etc. to extract values into typed arrays.
   *
   * This is outside of the timing loop due to overhead of extracting/boxing
   * Java types from cuDF.
   *
   * Example for a UDF that takes (String, Int):
   * {{{
   *   val col0 = Array.tabulate(numRows)(i => hostColumns(0).getJavaString(i))
   *   val col1 = Array.tabulate(numRows)(i => hostColumns(1).getInt(i))
   *   Array[AnyRef](col0, col1.asInstanceOf[AnyRef])
   * }}}
   *
   * @param hostColumns all columns copied to host memory
   * @param numRows     number of rows in the dataset
   * @return array of typed arrays, one per UDF input column
   */
  def prepareCpuData(
      hostColumns: Array[HostColumnVector],
      numRows: Int
  ): Array[AnyRef] = ???

  /**
   * TODO: Execute the CPU UDF on Scala data row-by-row.
   *
   * Example:
   * {{{
   *   val col0 = data(0).asInstanceOf[Array[String]]
   *   val col1 = data(1).asInstanceOf[Array[Int]]
   *   val udf = new com.udf.PlaceholderUDFName()
   *   var i = 0
   *   while (i < numRows) {
   *     udf.apply(col0(i), col1(i))
   *     i += 1
   *   }
   * }}}
   *
   * @param data    typed arrays from [[prepareCpuData]]
   * @param numRows number of rows in the dataset
   */
  def executeCpu(data: Array[AnyRef], numRows: Int): Unit = ???

  /**
   * TODO: Execute the GPU UDF via evaluateColumnar and close its result.
   *
   * Example:
   * {{{
   *   val udf = new com.udf.PlaceholderRapidsUDFName()
   *   withResource(udf.evaluateColumnar(numRows,
   *     table.getColumn(0), table.getColumn(1))) { _ => }
   * }}}
   *
   * @param table   the dataset loaded on GPU
   * @param numRows number of rows in the dataset
   */
  def executeGpu(table: Table, numRows: Int): Unit = ???

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)

    val dataPath = parsed.getOrElse("data-path",
      throw new IllegalArgumentException("--data-path is required"))
    val mode = parsed.getOrElse("mode", "all")
    val maxRows = parsed.getOrElse("rows", "-1").toInt
    val rmmAllocFraction = parsed.getOrElse("pool-fraction", DefaultRmmAllocFraction.toString).toFloat
    val warmup = parsed.getOrElse("warmup", DefaultWarmup.toString).toInt
    val measured = parsed.getOrElse("measured", DefaultMeasured.toString).toInt
    val profile = parsed.contains("profile")

    mode match {
      case "cpu" | "gpu" | "all" =>
      case other => throw new IllegalArgumentException(
        s"Unknown mode: '$other'. Must be 'cpu', 'gpu', or 'all'.")
    }
    val runCpu = mode == "cpu" || mode == "all"
    val runGpu = mode == "gpu" || mode == "all"

    // Initialize RMM pool
    if (!Rmm.isInitialized()) {
      val memInfo = Cuda.memGetInfo()
      val poolSize = (memInfo.free * rmmAllocFraction).toLong & ~255L
      Rmm.initialize(RmmAllocationMode.POOL, null, poolSize)
    }

    // Read Parquet data into cuDF table
    withResource(readParquetData(dataPath, maxRows)) { table =>
      val numRows = table.getRowCount.toInt
      val numCols = table.getNumberOfColumns
      val mb = getTableSizeMB(table)
      println(f"Loaded $numRows%,d rows x $numCols columns ($mb%.1f MB) from: $dataPath")
      println(s"Microbenchmark: mode=$mode, warmup=$warmup, measured=$measured")

      var cpuMinMs: Option[Double] = None
      var gpuMinMs: Option[Double] = None

      // --- CPU Benchmark ---
      if (runCpu) {
        val hostColumns = copyAllToHost(table)
        try {
          val cpuData = prepareCpuData(hostColumns, numRows)
          val times = runBenchmark(warmup, measured) {
            executeCpu(cpuData, numRows)
          }
          val medianMs = times(times.length / 2) / 1e6
          val minMs = times(0) / 1e6
          cpuMinMs = Some(minMs)
          println(
            f"   CPU  | $numRows%,14d rows | median $medianMs%10.1f ms | min $minMs%10.1f ms")
        } catch {
          case e: Exception =>
            System.err.println(s"CPU benchmark failed: ${e.getMessage}")
            e.printStackTrace(System.err)
            System.exit(1)
        } finally {
          closeAll(hostColumns)
        }
      }

      // --- GPU Benchmark ---
      if (runGpu) {
        try {
          val times = runBenchmark(warmup, measured, profile = profile) {
            executeGpu(table, numRows)
          }
          val medianMs = times(times.length / 2) / 1e6
          val minMs = times(0) / 1e6
          gpuMinMs = Some(minMs)
          println(
            f"   GPU  | $numRows%,14d rows | median $medianMs%10.1f ms | min $minMs%10.1f ms")
        } catch {
          case e: Exception =>
            System.err.println(s"GPU benchmark failed: ${e.getMessage}")
            e.printStackTrace(System.err)
            System.exit(1)
        }
      }

      // --- Speedup ---
      for (cpu <- cpuMinMs; gpu <- gpuMinMs) {
        val speedup = cpu / gpu
        println(f">> Speedup: $speedup%.2fx (CPU/GPU best)")
      }
    }

    System.exit(0)
  }

  /**
   * Run warmup + measured iterations. Profile the measured iterations if enabled.
   * @return sorted array of measured elapsed times in nanoseconds
   */
  private def runBenchmark(warmup: Int, measured: Int, profile: Boolean = false)
      (block: => Unit): Array[Long] = {
    for (_ <- 0 until warmup) block
    (0 until measured).map { i =>
      if (profile) Cuda.profilerStart()
      val start = System.nanoTime()
      block
      val elapsed = System.nanoTime() - start
      if (profile) Cuda.profilerStop()
      elapsed
    }.toArray.sorted
  }

  /**
   * Read Parquet partition files from a directory into a cuDF Table.
   * Reads files in sorted order, stopping once maxRows is reached.
   * @param maxRows stop after accumulating this many rows; -1 means read all.
   */
  private def readParquetData(dataPath: String, maxRows: Int): Table = {
    val partFiles = new File(dataPath).listFiles((_, name) => name.endsWith(".parquet"))
    if (partFiles == null || partFiles.isEmpty) {
      throw new IllegalArgumentException(s"No .parquet files found in: $dataPath")
    }

    val tables = ArrayBuffer.empty[Table]
    var totalRows = 0L
    try {
      for (f <- partFiles.sorted if maxRows <= 0 || totalRows < maxRows) {
        val t = Table.readParquet(f)
        tables += t
        totalRows += t.getRowCount
      }
      if (tables.length == 1) {
        limitTable(tables(0), maxRows)
      } else {
        withResource(Table.concatenate(tables.toArray: _*)) { combined =>
          limitTable(combined, maxRows)
        }
      }
    } finally {
      closeAll(tables.toArray)
    }
  }

  /** Return a new Table with at most numRows rows. */
  private def limitTable(table: Table, numRows: Int): Table = {
    val n = if (numRows <= 0) table.getRowCount.toInt
      else Math.min(numRows, table.getRowCount).toInt
    val cols = new Array[ColumnVector](table.getNumberOfColumns)
    try {
      for (i <- cols.indices) {
        cols(i) = table.getColumn(i).subVector(0, n)
      }
      new Table(cols: _*)
    } finally {
      closeAll(cols)
    }
  }

  /** Get the size of the table in MB. */
  private def getTableSizeMB(table: Table): Double = {
    (0 until table.getNumberOfColumns)
      .map(i => table.getColumn(i).getDeviceMemorySize)
      .sum / (1024.0 * 1024.0)
  }

  /** Copy all device columns to host memory. */
  private def copyAllToHost(table: Table): Array[HostColumnVector] = {
    val hostCols = new Array[HostColumnVector](table.getNumberOfColumns)
    try {
      for (i <- hostCols.indices) {
        hostCols(i) = table.getColumn(i).copyToHost()
      }
      hostCols
    } catch {
      case e: Throwable =>
        closeAll(hostCols)
        throw e
    }
  }

  /** Parse CLI arguments. */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    var map = Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--mode"        => map += ("mode" -> args(i + 1)); i += 2
        case "--data-path"   => map += ("data-path" -> args(i + 1)); i += 2
        case "--warmup"      => map += ("warmup" -> args(i + 1)); i += 2
        case "--measured"    => map += ("measured" -> args(i + 1)); i += 2
        case "--rows"        => map += ("rows" -> args(i + 1)); i += 2
        case "--pool-fraction" => map += ("pool-fraction" -> args(i + 1)); i += 2
        case "--profile"     => map += ("profile" -> "true"); i += 1
        case other =>
          throw new IllegalArgumentException(s"Unknown argument: $other")
      }
    }
    map
  }
}
