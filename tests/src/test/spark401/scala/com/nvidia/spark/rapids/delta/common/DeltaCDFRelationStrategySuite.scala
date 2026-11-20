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

/*** spark-rapids-shim-json-lines
{"spark":["401","411"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.delta.common

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.execution.{PlanLater, ProjectExec}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StringType, StructField, StructType}

class DeltaCDFRelationStrategySuite extends SparkQueryCompareTestSuite {
  private def withFixture(changesFor: DataFrame => DataFrame)(
      check: (SparkSession, LogicalRelation, DeltaCDFRelationStrategyBase) => Unit): Unit = {
    withCpuSparkSession { spark =>
      val metadata = new MetadataBuilder().putString("source", "cdf").build()
      val schema = StructType(Seq(
        StructField("value", LongType),
        StructField("commit", LongType, metadata = metadata)))
      val input = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1L, 2L)), 1), schema)
      val changes = changesFor(input)
      // Resolve the real DataFrame before any test optimizer rule is installed.
      changes.queryExecution.analyzed
      val cdf = mock[DeltaCDFRelation]
      when(cdf.startingVersion).thenReturn(Some(0L))
      when(cdf.sqlContext).thenReturn(spark.sqlContext)
      when(cdf.schema).thenReturn(changes.schema)
      val relation = LogicalRelation(cdf)
      val strategy = new DeltaCDFRelationStrategyBase {
        override protected def changesToBatchDF(cdf: DeltaCDFRelation): DataFrame = changes
      }
      check(spark, relation, strategy)
    }
  }

  test("restore attributes on an existing computed projection without stacking projections") {
    withFixture(_.selectExpr("value + 1L AS value", "commit")) { (_, relation, strategy) =>
      val planned = strategy(relation).head.asInstanceOf[ProjectExec]
      assert(planned.output == relation.output)
      val child = planned.child.asInstanceOf[PlanLater].plan
      assert(!child.isInstanceOf[Project])
      assert(planned.projectList.head.asInstanceOf[Alias].child.isInstanceOf[Add])
      assert(planned.projectList(1).asInstanceOf[Alias].child.isInstanceOf[AttributeReference])
      assert(planned.references.subsetOf(child.outputSet))
      assert(planned.output(1).metadata == relation.output(1).metadata)
    }
  }

  test("restoring attributes retains nondeterministic projection expressions exactly once") {
    withFixture(_.selectExpr("rand(17) AS value", "commit")) { (_, relation, strategy) =>
      val planned = strategy(relation).head.asInstanceOf[ProjectExec]
      assert(planned.output == relation.output)
      assert(!planned.child.asInstanceOf[PlanLater].plan.isInstanceOf[Project])
      val randoms = planned.projectList.flatMap(_.collect { case rand: Rand => rand })
      assert(randoms.size == 1)
      assert(planned.projectList.head.asInstanceOf[Alias].child.isInstanceOf[Rand])
    }
  }

  test("restore attributes when empty-input optimization removes the projection") {
    withFixture(_.where("false")) { (_, relation, strategy) =>
      val planned = strategy(relation).head.asInstanceOf[ProjectExec]
      assert(planned.output == relation.output)
      val child = planned.child.asInstanceOf[PlanLater].plan
      assert(child.isInstanceOf[LocalRelation])
      assert(planned.references.subsetOf(child.outputSet))
    }
  }

  private def withOptimizerResult(spark: SparkSession, result: LogicalPlan)(f: => Unit): Unit = {
    val previousRules = spark.experimental.extraOptimizations
    try {
      spark.experimental.extraOptimizations = Seq(new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = result
      })
      f
    } finally {
      spark.experimental.extraOptimizations = previousRules
    }
  }

  test("unchanged output does not add an attribute-restoring projection") {
    withFixture(input => input) { (spark, relation, strategy) =>
      withOptimizerResult(spark, LocalRelation(relation.output)) {
        assert(strategy(relation).head.isInstanceOf[PlanLater])
      }
    }
  }

  test("reject optimizer output with changed count, order or types") {
    withFixture(input => input) { (spark, relation, strategy) =>
      val invalidOutputs = Seq(
        relation.output.take(1),
        relation.output.reverse,
        Seq(AttributeReference("value", StringType)(), relation.output(1)))
      invalidOutputs.foreach { output =>
        withOptimizerResult(spark, LocalRelation(output)) {
          val error = intercept[IllegalArgumentException] {
            strategy(relation)
          }
          assert(error.getMessage.contains(
            "Delta CDF optimization changed the ordered output schema"))
        }
      }
    }
  }
}
