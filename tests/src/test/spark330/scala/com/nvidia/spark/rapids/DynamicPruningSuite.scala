/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, InSubqueryExec, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, GpuFileSourceScanExec}
import org.apache.spark.sql.rapids.execution.GpuSubqueryBroadcastExec
import org.apache.spark.sql.types._


class DynamicPruningSuite
    extends SparkQueryCompareTestSuite
    with AdaptiveSparkPlanHelper
    with FunSuiteWithTempDir
    with Logging {

  // Replace the GpuSubqueryBroadcastExec with a (CPU) SubqueryBroadcastExec in the plan used
  // by InSubqueryExec. This requires us replacing the underlying DynamicPruningExpression
  // in the partitionFilters with one that has the plan update and then replacing the
  // (Gpu)FileSourceScanExec as well. We also replace any of these underlying QueryStageExecs
  // from AQE

  private def replaceSubquery(p: SparkPlan): SparkPlan = {
    val updatePartitionFilters = (pf: Seq[Expression]) => {
      pf.map { filter =>
        filter.transformDown {
          case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
            inSub.plan match {
              // NOTE: We remove the AdaptiveSparkPlanExec since we can't re-run the new plan
              // under AQE because that fundamentally requires some rewrite and stage
              // ordering which we can't do for this test.
              case GpuSubqueryBroadcastExec(name, Seq(index), buildKeys, child) =>
                val newChild = child match {
                  case a @ AdaptiveSparkPlanExec(_, _, _, _, _) =>
                    (new GpuTransitionOverrides()).apply(ColumnarToRowExec(a.executedPlan))
                  case _ =>
                    (new GpuTransitionOverrides()).apply(ColumnarToRowExec(child))
                }
                dpe.copy(
                  inSub.copy(plan = SubqueryBroadcastExec(name, index, buildKeys, newChild)))
              case _ =>
                dpe
            }
        }
      }
    }

    p.transformDown {
      case ShuffleQueryStageExec(id, plan, _canonicalized) =>
        val newPlan = replaceSubquery(plan)
        ShuffleQueryStageExec(id, newPlan, _canonicalized)
      case BroadcastQueryStageExec(id, plan, _canonicalized) =>
        val newPlan = replaceSubquery(plan)
        BroadcastQueryStageExec(id, newPlan, _canonicalized)
      case g @ GpuFileSourceScanExec(r, o, rs, pf, obs, oncb, df, ti, dbs, quif, rps) =>
        val newPartitionFilters = updatePartitionFilters(pf)
        val rc = g.rapidsConf
        GpuFileSourceScanExec(r, o, rs, newPartitionFilters,
          obs, oncb, df, ti, dbs, quif, rps)(rc)
      case FileSourceScanExec(r, o, rs, pf, obs, oncb, df, ti, dbs) =>
        val newPartitionFilters = updatePartitionFilters(pf)
        FileSourceScanExec(r, o, rs, newPartitionFilters, obs, oncb, df, ti, dbs)
    }
  }

  test("AQE+DPP issue from EMR - https://github.com/NVIDIA/spark-rapids/issues/6978") {
    // We need to construct an artificially created plan that re-uses a GPU exchange when
    // doing a SubqueryBroadcast. This particular situation happens with AQE + DPP, so we use
    // combination to generate a plan that we can manipulate. Because of the nature of how
    // AQE+DPP cooperate, the SubqueryBroadcastExec actually ends up with a ReusedExchangeExec
    // child instead of the GpuBroadcastExchangeExec
    assumeSpark320orLater

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
        .set(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
        .set(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key, "true")
        .set(SQLConf.EXCHANGE_REUSE_ENABLED.key, "true")
        .set(RapidsConf.LOG_TRANSFORMATIONS.key, "true")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "SubqueryBroadcastExec,CollectLimitExec")

    // First, compute the CPU version of the query result, so that we have something to
    // compare the results too as a baseline.
    val cpuResult = withCpuSparkSession(spark => {
      setupTestData(spark)

      List("fact", "dim").foreach({ name =>
        val path = new File(TEST_FILES_ROOT, s"$name.parquet").getAbsolutePath
        spark.read.parquet(path).createOrReplaceTempView(name)
      })

      val filter = spark.sql("select filter from dim").first()(0)

      ExecutionPlanCaptureCallback.startCapture()
      val df = spark.sql(s"""
          SELECT fact.key, sum(fact.value)
          FROM fact
          JOIN dim
          ON fact.key = dim.key
          WHERE dim.filter = $filter AND fact.value > 0
          GROUP BY fact.key
      """)

      df.collect()
    })

    // Compute the GPU version of the query result using AQE+DPP. This will result
    // in a ReusedExchangeExec child of a GpuSubqueryBroadcastExec node used in DPP.
    // We then need to convert this to (CPU) SubqueryBroadcastExec, and then re-execute
    // a rewritten plan without the AdaptiveSparkPlanExec nodes so that we don't use AQE
    // to rewrite our artificial plan.
    withGpuSparkSession(spark => {
      setupTestData(spark)

      List("fact", "dim").foreach({ name =>
        val path = new File(TEST_FILES_ROOT, s"$name.parquet").getAbsolutePath
        spark.read.parquet(path).createOrReplaceTempView(name)
      })

      val filter = spark.sql("select filter from dim").first()(0)

      ExecutionPlanCaptureCallback.startCapture()
      val df = spark.sql(s"""
          SELECT fact.key, sum(fact.value)
          FROM fact
          JOIN dim
          ON fact.key = dim.key
          WHERE dim.filter = $filter AND fact.value > 0
          GROUP BY fact.key
      """)

      df.collect()

      val adPlan = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val executedPlan = adPlan.executedPlan
      val oldResult = executedPlan.executeCollect()
      val newPlan = replaceSubquery(executedPlan)
      // Assert that the SubqueryBroadcastExec is present and that there is a ReusedExchangeExec
      // Also, assert that the GpuBroadcastToRowExec node is now present to handle the re-used
      // GPU exchange.
      ExecutionPlanCaptureCallback.assertContains(newPlan, "SubqueryBroadcastExec")
      ExecutionPlanCaptureCallback.assertContains(newPlan, "ReusedExchangeExec")
      ExecutionPlanCaptureCallback.assertContains(newPlan, "GpuBroadcastToRowExec")
      val result = newPlan.executeCollect()

      compare(oldResult, result)
      compare(cpuResult, result)

    }, conf)

  }

  private def setupTestData(spark: SparkSession): Unit = {
    dimData(spark)
    factData(spark)
  }

  private def dimData(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("key", DataTypes.IntegerType, false),
      StructField("skey", DataTypes.IntegerType, false),
      StructField("ex_key", DataTypes.IntegerType, false),
      StructField("value", DataTypes.IntegerType),
      StructField("filter", DataTypes.IntegerType, false)
    ))
    val df = FuzzerUtils.generateDataFrame(spark, schema, 2000, FuzzerOptions(
      intMin = 0,
      intMax = 10
    ))
    registerAsParquetTable(spark, df, "dim", None)  }

  private def factData(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("key", DataTypes.IntegerType, false),
      StructField("skey", DataTypes.IntegerType, false),
      StructField("ex_key", DataTypes.IntegerType, false),
      StructField("value", DataTypes.IntegerType)
    ))
    val df = FuzzerUtils.generateDataFrame(spark, schema, 10000, FuzzerOptions(
      intMin = 0,
      intMax = 10
    ))
    registerAsParquetTable(spark, df, "fact", Some(List("key", "skey")))  }

  private def registerAsParquetTable(spark: SparkSession, df: Dataset[Row], name: String,
      partitionBy: Option[Seq[String]]): Unit = {
    val path = new File(TEST_FILES_ROOT, s"$name.parquet").getAbsolutePath

    partitionBy match {
      case Some(keys) =>
        df.write
            .mode(SaveMode.Overwrite)
            .partitionBy(keys: _*)
            .parquet(path)
      case _ =>
        df.write
            .mode(SaveMode.Overwrite)
            .parquet(path)
    }
  }
}
