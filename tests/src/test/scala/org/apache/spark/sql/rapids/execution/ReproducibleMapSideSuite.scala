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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions.{broadcast, col, rand, sum, udf}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * The walk that decides whether a rerun of a shuffle's map side reproduces its output, over
 * plans that a CPU session builds. The walk reads only the plan, so the suite needs no GPU.
 */
class ReproducibleMapSideSuite extends SparkQueryCompareTestSuite {

  /** The shuffle exchanges of a plan from the top down, inside AQE's stages too. */
  private def shuffleExchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    def nodes(node: SparkPlan): Seq[SparkPlan] = node match {
      case adaptive: AdaptiveSparkPlanExec => nodes(adaptive.executedPlan)
      case stage: QueryStageExec => nodes(stage.plan)
      case _ => node +: node.children.flatMap(nodes)
    }
    nodes(plan).collect { case exchange: ShuffleExchangeExec => exchange }
  }

  /** Whether the walk finds each shuffle exchange of a DataFrame's plan reproducible. */
  private def reproducible(df: DataFrame): Seq[Boolean] =
    shuffleExchanges(df.queryExecution.executedPlan)
      .map(UcxShuffleReclaimAdmission.mapSideIsReproducible)

  private def ids(spark: SparkSession): DataFrame = spark.range(0, 1000, 1, 4).toDF()

  /** Two shuffles on different keys, the earlier one over rand(). */
  private def randBelowEarlierShuffle(spark: SparkSession): DataFrame =
    ids(spark).select(col("id"), rand(7).as("r")).repartition(4, col("id"))
      .groupBy((col("id") % 3).as("g")).agg(sum("r"))

  test("a hash key on a column of a range is reproducible") {
    withCpuSparkSession { spark =>
      assertResult(Seq(true))(reproducible(ids(spark).repartition(4, col("id"))))
    }
  }

  test("rand() in a projected column is not reproducible") {
    withCpuSparkSession { spark =>
      val df = ids(spark).select(col("id"), rand(7).as("r")).repartition(4, col("id"))
      assertResult(Seq(false))(reproducible(df))
    }
  }

  test("rand() as the partition key is not reproducible") {
    withCpuSparkSession { spark =>
      assertResult(Seq(false))(reproducible(ids(spark).repartition(4, rand(7))))
    }
  }

  test("a UDF marked nondeterministic is not reproducible") {
    withCpuSparkSession { spark =>
      val plusOne = udf((id: Long) => id + 1).asNondeterministic()
      val df = ids(spark).select(plusOne(col("id")).as("v")).repartition(4, col("v"))
      assertResult(Seq(false))(reproducible(df))
    }
  }

  test("a deterministic UDF is reproducible") {
    withCpuSparkSession { spark =>
      val plusOne = udf((id: Long) => id + 1)
      val df = ids(spark).select(plusOne(col("id")).as("v")).repartition(4, col("v"))
      assertResult(Seq(true))(reproducible(df))
    }
  }

  test("a sample is not reproducible") {
    withCpuSparkSession { spark =>
      val df = ids(spark).sample(false, 0.5, 7L).repartition(4, col("id"))
      assertResult(Seq(false))(reproducible(df))
    }
  }

  test("a limit makes each exchange above it not reproducible, and leaves the one below it " +
      "reproducible") {
    withCpuSparkSession { spark =>
      // the limit plans a local limit, a single-partition exchange and a global limit
      val df = ids(spark).repartition(4, col("id")).limit(500).repartition(2, col("id"))
      assertResult(Seq(false, false, true))(reproducible(df))
    }
  }

  test("rand() below an earlier exchange makes both exchanges not reproducible") {
    withCpuSparkSession { spark =>
      assertResult(Seq(false, false))(reproducible(randBelowEarlierShuffle(spark)))
    }
  }

  test("rand() only on a broadcast join's build side is reproducible") {
    withCpuSparkSession { spark =>
      val build = spark.range(0, 10).select(col("id"), rand(7).as("r"))
      val df = ids(spark).join(broadcast(build), "id").repartition(4, col("id"))
      assertResult(Seq(true))(reproducible(df))
    }
  }

  test("with AQE on, rand() below an earlier query stage makes both exchanges not " +
      "reproducible") {
    withCpuSparkSession({ spark =>
      val df = randBelowEarlierShuffle(spark)
      // AQE plans its stages as it runs them
      df.collect()
      assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec],
        df.queryExecution.executedPlan)
      assertResult(Seq(false, false))(reproducible(df))
    }, new SparkConf().set("spark.sql.adaptive.enabled", "true"))
  }

  test("a reused exchange whose original holds rand() is not reproducible") {
    withCpuSparkSession { spark =>
      val withRand = ids(spark).select(col("id"), rand(7).as("r")).repartition(4, col("id"))
      val plan = withRand.union(withRand).queryExecution.executedPlan
      val reused = plan.collect { case r: ReusedExchangeExec => r }
      assertResult(1, s"reused exchanges in:\n$plan")(reused.size)
      assert(!UcxShuffleReclaimAdmission.mapSideIsReproducible(reused.head))
    }
  }

  test("an RDD's rows are not reproducible") {
    withCpuSparkSession { spark =>
      val rdd = spark.sparkContext.parallelize(Seq(Row(1L), Row(2L)), 2)
      val df = spark.createDataFrame(rdd, StructType(Seq(StructField("id", LongType))))
        .repartition(4, col("id"))
      assertResult(Seq(false))(reproducible(df))
    }
  }
}
