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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

trait BloomFilterAggregateQuerySuiteBase extends SparkQueryCompareTestSuite {
  val bloomFilterEnabledConf = new SparkConf()

  // Spark 4.2 requires fully qualified function identifiers. Keep their construction overridable,
  // and initialize them lazily so version-specific overrides run after subclass initialization.
  protected def sessionFuncId(name: String): FunctionIdentifier = new FunctionIdentifier(name)
  protected lazy val funcId_bloom_filter_agg = sessionFuncId("bloom_filter_agg")
  protected lazy val funcId_might_contain = sessionFuncId("might_contain")

  protected def installSqlFuncs(spark: SparkSession): Unit = {
    spark.sessionState.functionRegistry.registerFunction(funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) => children.size match {
        case 1 => new BloomFilterAggregate(children.head)
        case 2 => new BloomFilterAggregate(children.head, children(1))
        case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
      })

    spark.sessionState.functionRegistry.registerFunction(funcId_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))
  }

  protected def uninstallSqlFuncs(spark: SparkSession): Unit = {
    spark.sessionState.functionRegistry.dropFunction(funcId_bloom_filter_agg)
    spark.sessionState.functionRegistry.dropFunction(funcId_might_contain)
  }

  protected def buildData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (Seq(Some(Long.MinValue), Some(0L), Some(Long.MaxValue), None) ++
        (1L to 10000L).map(x => Some(x)) ++
        (1L to 100L).map(_ => None)).toDF("col")
  }

  protected def withExposedSqlFuncs[T](spark: SparkSession)(func: SparkSession => T): T = {
    try {
      installSqlFuncs(spark)
      func(spark)
    } finally {
      uninstallSqlFuncs(spark)
    }
  }

  protected def doBloomFilterTest(
      numEstimated: Long, numBits: Long): DataFrame => DataFrame = {
    df =>
      val table = "bloom_filter_test"
      val sqlString =
        s"""
           |SELECT might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimated as long),
           |              cast($numBits as long))
           |             FROM $table),
           |            col) positive_membership_test,
           |       might_contain(
           |            (SELECT bloom_filter_agg(col,
           |              cast($numEstimated as long),
           |              cast($numBits as long))
           |             FROM values (-1L), (100001L), (20000L) as t(col)),
           |            col) negative_membership_test
           |FROM $table
          """.stripMargin
      df.createOrReplaceTempView(table)
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(sqlString)
      }
  }

  protected def getPlanValidator(exec: String): (SparkPlan, SparkPlan) => Unit = {
    def searchPlan(p: SparkPlan): Boolean = {
      ExecutionPlanCaptureCallback.didFallBack(p, exec) ||
        p.children.exists(searchPlan) ||
        p.subqueries.exists(searchPlan)
    }
    (_, gpuPlan) => {
      val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(gpuPlan)
      assert(searchPlan(executedPlan), s"Could not find $exec in the GPU plan:\n$executedPlan")
    }
  }

  protected def getPartialAggPlanValidator(exec: String): (SparkPlan, SparkPlan) => Unit = {
    def searchPlan(p: SparkPlan): Boolean = {
      ExecutionPlanCaptureCallback.didFallBack(p, exec) ||
        p.children.exists(searchPlan) ||
        p.subqueries.exists(searchPlan)
    }
    (_, gpuPlan) => {
      val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(gpuPlan)
      val planString = executedPlan.toString()
      assert(searchPlan(executedPlan), s"Could not find $exec in the GPU plan:\n$executedPlan")
      assert(planString.contains("partial_bloom_filter_agg"),
        s"Could not find partial_bloom_filter_agg in the GPU plan:\n$executedPlan")
    }
  }

  // Keep the empty-partition mixed CPU/GPU regression coverage in the shared base so both the
  // 3.3x/3.5x suites and the 4.1.1 suite exercise the same bridge behavior.
  // This is a defensive correctness path that is unlikely in normal workloads because a supported
  // BloomFilterAggregate would normally keep both partial and final stages on GPU.
  // The test forces the mixed plan on purpose: BloomFilterMightContain falls back to CPU,
  // hashAgg.replaceMode keeps only one aggregate stage on GPU, AQE stays off so the plan shape is
  // stable, and extra shuffle partitions guarantee empty build partitions.
  for (mode <- Seq("partial", "final")) {
    ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      s"might_contain GPU $mode build CPU probe with empty build partitions",
      spark => {
        import spark.implicits._
        spark.range(0, 4).select($"id".cast("long").as("col"))
      },
      Seq("ObjectHashAggregateExec", "ProjectExec", "ShuffleExchangeExec",
        "BloomFilterMightContain", "ScalarSubquery"),
      conf = bloomFilterEnabledConf.clone()
        .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
        .set("spark.rapids.sql.hashAgg.replaceMode", mode)
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.shuffle.partitions", "16")) {
      probeDf =>
        val probeTable = s"bloom_filter_empty_partition_probe_$mode"
        val buildTable = s"bloom_filter_empty_partition_build_$mode"
        probeDf.createOrReplaceTempView(probeTable)
        probeDf.sparkSession.range(0, 4)
          .selectExpr("cast(id as long) as col")
          .repartition(16)
          .createOrReplaceTempView(buildTable)
        withExposedSqlFuncs(probeDf.sparkSession) { spark =>
          spark.sql(
            s"""
               |SELECT might_contain(
               |         (SELECT bloom_filter_agg(col,
               |            cast(${SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get}
               |              as long),
               |            cast(${SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get}
               |              as long))
               |          FROM $buildTable),
               |         col)
               |FROM $probeTable
               |""".stripMargin)
        }
    }(getPartialAggPlanValidator("ObjectHashAggregateExec"))
  }
}
