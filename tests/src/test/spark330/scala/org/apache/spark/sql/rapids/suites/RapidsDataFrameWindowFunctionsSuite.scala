/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.GpuHashPartitioning
import com.nvidia.spark.rapids.window.GpuWindowBaseExec

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.DataFrameWindowFunctionsSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsDataFrameWindowFunctionsSuite
  extends DataFrameWindowFunctionsSuite
  with RapidsSQLTestsTrait {

  import testImplicits._

  testRapids("SPARK-38237: require all cluster " +
    "keys for child required distribution for window query") {
    def partitionExpressionsColumns(expressions: Seq[Expression]): Seq[String] = {
      expressions.flatMap {
        case ref: AttributeReference => Some(ref.name)
      }
    }

    def isShuffleExecByRequirement(
        plan: GpuShuffleExchangeExec,
        desiredClusterColumns: Seq[String]): Boolean = plan match {
      case GpuShuffleExchangeExec(op: GpuHashPartitioning, _, ENSURE_REQUIREMENTS, _) => {
        partitionExpressionsColumns(op.expressions) === desiredClusterColumns
      }
      case _ => false
    }

    val df = Seq(("a", 1, 1), ("a", 2, 2), ("b", 1, 3), ("b", 1, 4)).toDF("key1", "key2", "value")
    val windowSpec = Window.partitionBy("key1", "key2").orderBy("value")

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION.key -> "true") {

      val windowed = df
        // repartition by subset of window partitionBy keys which satisfies ClusteredDistribution
        .repartition($"key1")
        .select(
          lead($"key1", 1).over(windowSpec),
          lead($"value", 1).over(windowSpec))

      checkAnswer(windowed, Seq(Row("b", 4), Row(null, null), Row(null, null), Row(null, null)))

      val shuffleByRequirement = windowed.queryExecution.executedPlan.exists {
        case w: GpuWindowBaseExec =>
          w.child.exists {
            case s: GpuShuffleExchangeExec => isShuffleExecByRequirement(s, Seq("key1", "key2"))
            case _ => false
          }
        case _ => false
      }

      assert(shuffleByRequirement, "Can't find desired shuffle node from the query plan")
    }
  }

}

