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

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{GpuProjectExec, RapidsConf, SparkQueryCompareTestSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class GpuRangeBoundaryPlanSuite extends SparkQueryCompareTestSuite {
  private val conf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.shuffle.partitions", "4")
    .set("spark.sql.sources.useV1SourceList", "parquet")

  private def rangeExchange(df: DataFrame): GpuShuffleExchangeExecBase = {
    df.queryExecution.executedPlan.collectFirst {
      case exchange: GpuShuffleExchangeExecBase => exchange
    }.getOrElse(fail(s"GPU range exchange not found in:\n${df.queryExecution.executedPlan}"))
  }

  private def writeInput(path: String): Unit = {
    withCpuSparkSession({ spark =>
      spark.range(100)
        .select(
          col("id").as("key"),
          (col("id") % 3).as("filter_col"),
          lit("payload").as("payload"))
        .repartition(4)
        .write
        .parquet(path)
    }, conf)
  }

  private def assertAscendingRangePartitioning(df: DataFrame): Unit = {
    val bounds = df.queryExecution.toRdd.mapPartitionsWithIndex { case (index, rows) =>
      val keys = rows.map(_.getLong(0)).toArray
      if (keys.isEmpty) Iterator.empty else Iterator.single((index, keys.min, keys.max))
    }.collect().sortBy(_._1)

    bounds.sliding(2).foreach {
      case Array((leftIndex, _, leftMax), (rightIndex, rightMin, _)) =>
        assert(leftMax <= rightMin,
          s"range partition $leftIndex has maximum key $leftMax greater than " +
            s"minimum key $rightMin in partition $rightIndex")
      case _ =>
    }
  }

  private def rangeInput(spark: org.apache.spark.sql.SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
      .filter(col("filter_col") > 0)
      .repartitionByRange(4, col("key"))
  }

  test("range boundary collection reads only keys and filter dependencies") {
    withTempPath { path =>
      writeInput(path.getCanonicalPath)

      withGpuSparkSession({ spark =>
        val result = rangeInput(spark, path.getCanonicalPath)
        val exchange = rangeExchange(result)
        val boundary = exchange.subqueries.collectFirst {
          case plan: GpuRangeBoundaryExec => plan
        }.getOrElse(fail(s"GPU range boundary plan not found in:\n$exchange"))

        assert(boundary.output.map(_.name) === Seq("key"))
        val scan = boundary.collectFirst {
          case fileScan: GpuFileSourceScanExec => fileScan
        }.getOrElse(fail(s"GPU file scan not found in:\n$boundary"))
        assert(scan.requiredSchema.fieldNames.toSeq === Seq("key", "filter_col"))
        assert(!scan.requiredSchema.fieldNames.contains("payload"))

        val rows = result.collect().sortBy(_.getLong(0))
        assert(rows.length === 66)
        assert(rows.forall(_.getString(2) == "payload"))
        assertAscendingRangePartitioning(result)
      }, conf)
    }
  }

  test("range boundary collection retains computed key dependencies") {
    withTempPath { path =>
      writeInput(path.getCanonicalPath)

      withGpuSparkSession({ spark =>
        val result = spark.read.parquet(path.getCanonicalPath)
          .select(
            (col("key") * 2 + col("filter_col")).as("range_key"),
            col("key"),
            col("payload"))
          .repartitionByRange(4, col("range_key"))
        val exchange = rangeExchange(result)
        val boundary = exchange.subqueries.collectFirst {
          case plan: GpuRangeBoundaryExec => plan
        }.getOrElse(fail(s"GPU range boundary plan not found in:\n$exchange"))

        assert(boundary.output.map(_.name) === Seq("range_key"))
        val project = boundary.collectFirst {
          case gpuProject: GpuProjectExec => gpuProject
        }.getOrElse(fail(s"GPU project not found in:\n$boundary"))
        assert(project.output.map(_.name) === Seq("range_key"))
        val scan = boundary.collectFirst {
          case fileScan: GpuFileSourceScanExec => fileScan
        }.getOrElse(fail(s"GPU file scan not found in:\n$boundary"))
        assert(scan.requiredSchema.fieldNames.toSeq === Seq("key", "filter_col"))
        assert(!scan.requiredSchema.fieldNames.contains("payload"))

        val rows = result.collect()
        assert(rows.length === 100)
        assert(rows.forall { row =>
          row.getLong(0) == row.getLong(1) * 2 + row.getLong(1) % 3 &&
            row.getString(2) == "payload"
        })
        assertAscendingRangePartitioning(result)
      }, conf)
    }
  }

  test("range boundary collection reads a partition-column-only key") {
    withTempPath { path =>
      withCpuSparkSession({ spark =>
        spark.range(100)
          .select(
            (col("id") % 5).as("partition_key"),
            col("id").as("row_id"),
            lit("payload").as("payload"))
          .write
          .partitionBy("partition_key")
          .parquet(path.getCanonicalPath)
      }, conf)

      withGpuSparkSession({ spark =>
        val result = spark.read
          .schema("row_id LONG, payload STRING, partition_key LONG")
          .parquet(path.getCanonicalPath)
          .select(col("partition_key"), col("row_id"), col("payload"))
          .repartitionByRange(4, col("partition_key"))
        val exchange = rangeExchange(result)
        val boundary = exchange.subqueries.collectFirst {
          case plan: GpuRangeBoundaryExec => plan
        }.getOrElse(fail(s"GPU range boundary plan not found in:\n$exchange"))

        assert(boundary.output.map(_.name) === Seq("partition_key"))
        val scan = boundary.collectFirst {
          case fileScan: GpuFileSourceScanExec => fileScan
        }.getOrElse(fail(s"GPU file scan not found in:\n$boundary"))
        assert(scan.requiredSchema.isEmpty)
        assert(scan.readPartitionSchema.fieldNames.toSeq === Seq("partition_key"))
        assert(scan.requiredPartitionSchema.map(_.fieldNames.toSeq) ===
          Some(Seq("partition_key")))

        val rows = result.collect()
        assert(rows.length === 100)
        assert(rows.map(_.getLong(1)).toSet === (0L until 100L).toSet)
        assert(rows.forall(_.getString(2) == "payload"))
        assertAscendingRangePartitioning(result)
      }, conf)
    }
  }

  test("key-only boundary collection can be disabled") {
    withTempPath { path =>
      writeInput(path.getCanonicalPath)

      val fallbackConf = conf.clone()
        .set(RapidsConf.RANGE_PARTITIONING_SAMPLE_KEYS_ONLY.key, "false")
      withGpuSparkSession({ spark =>
        val result = rangeInput(spark, path.getCanonicalPath)
        val exchange = rangeExchange(result)

        assert(!exchange.subqueries.exists(_.isInstanceOf[GpuRangeBoundaryExec]))
        assertAscendingRangePartitioning(result)
        val rows = result.collect()
        assert(rows.length === 66)
        assert(rows.forall(_.getString(2) == "payload"))
      }, fallbackConf)
    }
  }

  test("unsupported boundary input plan uses the original GPU input") {
    withGpuSparkSession({ spark =>
      val result = spark.range(100)
        .select(col("id").as("key"), lit("payload").as("payload"))
        .repartitionByRange(4, col("key"))
      val exchange = rangeExchange(result)

      // GpuRangeExec is not in the boundary-plan pruning allowlist, so constructing the
      // auxiliary plan must fail closed and retain the exchange's original GPU input.
      assert(!exchange.subqueries.exists(_.isInstanceOf[GpuRangeBoundaryExec]))
      assertAscendingRangePartitioning(result)
      val rows = result.collect()
      assert(rows.length === 100)
      assert(rows.forall(_.getString(1) == "payload"))
    }, conf)
  }

  test("nondeterministic range keys use the original boundary collection path") {
    withTempPath { path =>
      writeInput(path.getCanonicalPath)

      withGpuSparkSession({ spark =>
        val result = spark.read.parquet(path.getCanonicalPath)
          .select(rand(7).as("range_key"), col("payload"))
          .repartitionByRange(4, col("range_key"))
        val exchange = rangeExchange(result)

        assert(!exchange.subqueries.exists(_.isInstanceOf[GpuRangeBoundaryExec]))
      }, conf)
    }
  }
}
