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

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

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

  test("range boundary collection reads only keys and filter dependencies") {
    withTempPath { path =>
      writeInput(path.getCanonicalPath)

      withGpuSparkSession({ spark =>
        val result = spark.read.parquet(path.getCanonicalPath)
          .filter(col("filter_col") > 0)
          .repartitionByRange(4, col("key"))
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
      }, conf)
    }
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
