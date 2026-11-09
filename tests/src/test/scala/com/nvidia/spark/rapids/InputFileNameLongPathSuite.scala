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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

/**
 * End to end checks for slicing scan batches that `input_file_name()` could not materialize as one
 * cudf string column. The internal test conf lowers the string limit so small data exercises the
 * same path that a 2 GiB column would.
 */
class InputFileNameLongPathSuite extends SparkQueryCompareTestSuite {
  private val NumRows = 20000L
  // Paths here are ~100-200 bytes, so this allows roughly 3K-5K rows per batch.
  private val LimitBytes = InputFileNameBatchSlicer.HEADROOM_BYTES + 100L * 5000

  private def conf(limit: Long): SparkConf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "false")
    .set(RapidsConf.METRICS_LEVEL.key, "DEBUG") // numOutputBatches is a DEBUG metric
    .set(RapidsConf.TEST_INPUT_FILE_NAME_MAX_STRING_BYTES.key, limit.toString)

  private def writeFile(dir: File, rows: Long = NumRows): String = {
    val path = new File(dir, "data").getAbsolutePath
    withCpuSparkSession(_.range(rows).coalesce(1).write.parquet(path))
    path
  }

  private def find[T <: SparkPlan](df: DataFrame)(pf: PartialFunction[SparkPlan, T]): Seq[T] =
    df.queryExecution.executedPlan.collect(pf)

  /** Runs `df` once, returning its rows and the number of batches its projects emitted. */
  private def run(df: DataFrame): (Array[Row], Long) = {
    val rows = df.collect()
    val batches = find(df) { case p: GpuExec if p.nodeName == "GpuProject" => p }
      .map(_.metrics("numOutputBatches").value).sum
    (rows, batches)
  }

  test("large batch is sliced: rows, order, name and metrics are preserved") {
    withTempPath { root =>
      val path = writeFile(root)
      withGpuSparkSession({ spark =>
        val df = spark.read.parquet(path).select(col("id"), input_file_name().as("f"))
        val (rows, batches) = run(df)
        assertResult(NumRows)(rows.length)
        assert(rows.map(_.getLong(0)).sameElements(0L until NumRows), "row order broke")
        assertResult(1)(rows.map(_.getString(1)).distinct.length)
        assert(rows.head.getString(1).contains("data"))
        assert(batches > 1, "expected the batch to be split")
        val scan = find(df) { case s: GpuFileSourceScanExec => s }.head
        assertResult(NumRows)(scan.metrics("numOutputRows").value)
      }, conf(LimitBytes))
    }
  }

  test("a limit that fits the batch leaves it whole") {
    withTempPath { root =>
      val path = writeFile(root)
      withGpuSparkSession({ spark =>
        val df = spark.read.parquet(path).select(col("id"), input_file_name().as("f"))
        assertResult(1L)(run(df)._2)
      }, conf(Int.MaxValue.toLong))
    }
  }

  test("path longer than the limit emits one row per slice") {
    withTempPath { root =>
      val path = writeFile(root, 300)
      withGpuSparkSession({ spark =>
        val df = spark.read.parquet(path).select(col("id"), input_file_name().as("f"))
        val (rows, batches) = run(df)
        assertResult(300)(rows.length)
        assertResult(300L)(batches)
      }, conf(InputFileNameBatchSlicer.HEADROOM_BYTES + 10))
    }
  }

  test("two files with different path lengths, first one split") {
    withTempPath { root =>
      val paths = Seq("x" * 150, "y").map { d =>
        val dir = new File(root, d)
        assert(dir.mkdirs())
        writeFile(dir)
      }
      def run(gpu: Boolean): Seq[(String, Long, Long, Long, Long)] = {
        val f = (spark: SparkSession) => spark.read.parquet(paths: _*)
          .groupBy(input_file_name().as("f"))
          .agg(count("*"), sum("id"), min("id"), max("id")).collect()
          .map(r => (r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4)))
          .sortBy(_._1).toSeq
        if (gpu) withGpuSparkSession(f, conf(LimitBytes)) else withCpuSparkSession(f)
      }
      val cpu = run(gpu = false)
      assertResult(2)(cpu.size)
      assert(cpu.forall(_._2 == NumRows))
      assertResult(cpu)(run(gpu = true))
    }
  }

  test("rows-only batches (all columns pruned) are sliced by count") {
    withTempPath { root =>
      val path = writeFile(root)
      withGpuSparkSession({ spark =>
        val df = spark.read.parquet(path).select(input_file_name().as("f"))
        val agg = df.groupBy("f").count()
        val (rows, batches) = run(agg)
        assertResult(1)(rows.length)
        assertResult(NumRows)(rows.head.getLong(1))
        assert(batches > 1)
      }, conf(LimitBytes))
    }
  }

  test("only input_file_name() asks the scan to slice") {
    withTempPath { root =>
      val path = writeFile(root)
      withGpuSparkSession({ spark =>
        def scan(df: DataFrame): GpuFileSourceScanExec = {
          df.collect()
          find(df) { case s: GpuFileSourceScanExec => s }.head
        }
        val t = spark.read.parquet(path)
        val byName = scan(t.select(input_file_name()))
        assert(byName.queryUsesInputFile && byName.queryUsesInputFileName)
        val byBlock = scan(t.select(expr("input_file_block_start()"),
          expr("input_file_block_length()")))
        assert(byBlock.queryUsesInputFile && !byBlock.queryUsesInputFileName)
        val plain = scan(t.select(col("id")))
        assert(!plain.queryUsesInputFile && !plain.queryUsesInputFileName)
      }, conf(LimitBytes))
    }
  }
}
