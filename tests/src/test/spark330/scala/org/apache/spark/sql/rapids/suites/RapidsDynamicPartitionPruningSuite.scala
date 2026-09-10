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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DynamicPartitionPruningSuiteBase
import org.apache.spark.sql.DynamicPartitionPruningV1SuiteAEOff
import org.apache.spark.sql.DynamicPartitionPruningV1SuiteAEOn
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.execution.ReusedSubqueryExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SubqueryBroadcastExec
import org.apache.spark.sql.execution.SubqueryExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.GpuSubqueryBroadcastExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

trait RapidsDynamicPartitionPruningSuiteBase extends DynamicPartitionPruningSuiteBase {
  this: RapidsSQLTestsTrait =>

  import testImplicits._

  override protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    super.collectDynamicPruningExpressions(plan) ++ flatMap(plan) {
      case s: GpuFileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case s: GpuBatchScanExec => s.runtimeFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  override def checkPartitionPruningPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
      case InSubqueryExec(_, ReusedSubqueryExec(_: SubqueryExec), _, _, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.flatMap {
      case InSubqueryExec(_, s: SubqueryBroadcastExec, _, _, _, _) => Some(s)
      case InSubqueryExec(_, s: GpuSubqueryBroadcastExec, _, _, _, _) => Some(s)
      case InSubqueryExec(_, ReusedSubqueryExec(s: SubqueryBroadcastExec), _, _, _, _) => Some(s)
      case InSubqueryExec(_, ReusedSubqueryExec(s: GpuSubqueryBroadcastExec), _, _, _, _) => Some(s)
      case _ => None
    }

    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(hasSubquery == withSubquery,
      s"$hasFilter trigger DPP with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast trigger DPP with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach { s =>
      s.child match {
        case _: ReusedExchangeExec =>
        case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) =>
        case b: BroadcastExchangeLike =>
          val hasReuse = plan.exists {
            case ReusedExchangeExec(_, e) => e eq b
            case _ => false
          }
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case a: AdaptiveSparkPlanExec =>
          val broadcastQueryStage = collectFirst(a) {
            case b: BroadcastQueryStageExec => b
          }
          val broadcastPlan = broadcastQueryStage.get.broadcast
          val hasReuse = find(plan) {
            case ReusedExchangeExec(_, e) => e eq broadcastPlan
            case b: BroadcastExchangeLike => b eq broadcastPlan
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case _ =>
          fail(s"Invalid child node found in\n$s")
      }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
      val subquery = s match {
        case r: ReusedSubqueryExec => r.child
        case o => o
      }
      assert(subquery.exists(_.isInstanceOf[AdaptiveSparkPlanExec]) == isMainQueryAdaptive)
    }
  }

  override def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    df.collect()

    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).flatMap {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _, _, _) => Seq(b.index)
      case InSubqueryExec(_, b: GpuSubqueryBroadcastExec, _, _, _, _) => b.indices
      case InSubqueryExec(_, ReusedSubqueryExec(b: SubqueryBroadcastExec), _, _, _, _) =>
        Seq(b.index)
      case InSubqueryExec(_, ReusedSubqueryExec(b: GpuSubqueryBroadcastExec), _, _, _, _) =>
        b.indices
      case _ => Nil
    }
    assert(buf.distinct.size == n)
  }

  testRapids("Make sure dynamic pruning works on uncorrelated queries") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT d.store_id,
          |       SUM(f.units_sold),
          |       (SELECT SUM(f.units_sold)
          |        FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
          |        WHERE d.country = 'US') AS total_prod
          |FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
          |WHERE d.country = 'US'
          |GROUP BY 1
        """.stripMargin)
      checkAnswer(df, Row(4, 50, 70) :: Row(5, 10, 70) :: Row(6, 10, 70) :: Nil)

      val plan = df.queryExecution.executedPlan
      val countSubqueryBroadcasts = collectWithSubqueries(plan) {
        case _: SubqueryBroadcastExec => 1
        case _: GpuSubqueryBroadcastExec => 1
      }.sum

      val countReusedSubqueryBroadcasts = collectWithSubqueries(plan) {
        case ReusedSubqueryExec(_: SubqueryBroadcastExec) => 1
        case ReusedSubqueryExec(_: GpuSubqueryBroadcastExec) => 1
      }.sum

      assert(countSubqueryBroadcasts == 1)
      assert(countReusedSubqueryBroadcasts == 1)
    }
  }

  testRapids("static scan metrics",
    DisableAdaptiveExecution("DPP in AQE must reuse broadcast")) {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      withTable("fact", "dim") {
        val numPartitions = 10

        spark.range(10)
          .map { x => Tuple3(x, x + 1, 0) }
          .toDF("did", "d1", "d2")
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("dim")

        spark.range(100)
          .map { x => Tuple2(x, x % numPartitions) }
          .toDF("f1", "fid")
          .write.partitionBy("fid")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("fact")

        def getFactScan(plan: SparkPlan): SparkPlan = {
          val scanOption = find(plan) {
            case s: GpuFileSourceScanExec =>
              s.output.exists(_.exists(attr =>
                attr.argString(maxFields = 100).contains("fid") ||
                  attr.argString(maxFields = 100).contains("f1")))
            case _ => false
          }
          assert(scanOption.isDefined)
          scanOption.get
        }

        val df1 = sql("SELECT sum(f1) FROM fact")
        df1.collect()
        val scan1 = getFactScan(df1.queryExecution.executedPlan)
        assert(!scan1.metrics.contains("staticFilesNum"))
        assert(!scan1.metrics.contains("staticFilesSize"))
        val allFilesNum = scan1.metrics("numFiles").value
        val allFilesSize = scan1.metrics("filesSize").value
        assert(scan1.metrics("numPartitions").value === numPartitions)
        assert(scan1.metrics("pruningTime").value === 0)

        val df2 = sql("SELECT sum(f1) FROM fact WHERE fid = 5")
        df2.collect()
        val scan2 = getFactScan(df2.queryExecution.executedPlan)
        assert(!scan2.metrics.contains("staticFilesNum"))
        assert(!scan2.metrics.contains("staticFilesSize"))
        val partFilesNum = scan2.metrics("numFiles").value
        val partFilesSize = scan2.metrics("filesSize").value
        assert(0 < partFilesNum && partFilesNum < allFilesNum)
        assert(0 < partFilesSize && partFilesSize < allFilesSize)
        assert(scan2.metrics("numPartitions").value === 1)
        assert(scan2.metrics("pruningTime").value === 0)

        val df3 = sql("SELECT sum(f1) FROM fact, dim WHERE fid = did AND d1 = 6")
        df3.collect()
        val scan3 = getFactScan(df3.queryExecution.executedPlan)
        assert(scan3.metrics("staticFilesNum").value == allFilesNum)
        assert(scan3.metrics("staticFilesSize").value == allFilesSize)
        assert(scan3.metrics("numFiles").value == partFilesNum)
        assert(scan3.metrics("filesSize").value == partFilesSize)
        assert(scan3.metrics("numPartitions").value === 1)
        assert(scan3.metrics("pruningTime").value !== -1)
      }
    }
  }
}

class RapidsDynamicPartitionPruningV1SuiteAEOff
  extends DynamicPartitionPruningV1SuiteAEOff
  with RapidsSQLTestsTrait
  with RapidsDynamicPartitionPruningSuiteBase {
}

class RapidsDynamicPartitionPruningV1SuiteAEOn
  extends DynamicPartitionPruningV1SuiteAEOn
  with RapidsSQLTestsTrait
  with RapidsDynamicPartitionPruningSuiteBase {
}
