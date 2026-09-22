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

import com.nvidia.spark.rapids.GpuTopN
import org.scalatest.Assertions

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.TakeOrderedAndProjectSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

object RapidsTakeOrderedAndProjectSuite extends Assertions {
  def assertTopN(df: DataFrame, executedPlan: Seq[SparkPlan]): Unit = {
    val gpuTopNs = executedPlan.collect {
      case topN: GpuTopN => topN
    }
    assert(gpuTopNs.nonEmpty, s"Expected GpuTopN in plan:\n${df.queryExecution.executedPlan}")
  }
}

class RapidsTakeOrderedAndProjectSuite
  extends TakeOrderedAndProjectSuite
  with RapidsSQLTestsBaseTrait {
  import testImplicits._

  testRapids("TakeOrderedAndProject query without project uses GpuTopN") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = Seq((3, 10), (1, 30), (2, 20), (4, 40)).toDF("a", "b")
        .orderBy($"a".desc, $"b".desc)
        .limit(2)

      assert(df.collect().toSeq === Seq(Row(4, 40), Row(3, 10)))
      RapidsTakeOrderedAndProjectSuite.assertTopN(df, getExecutedPlan(df))
    }
  }

  testRapids("TakeOrderedAndProject query with project uses GpuTopN") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = Seq((3, 10), (1, 30), (2, 20), (4, 40)).toDF("a", "b")
        .orderBy($"a".desc, $"b".desc)
        .limit(2)
        .select($"b")

      assert(df.collect().toSeq === Seq(Row(40), Row(10)))
      RapidsTakeOrderedAndProjectSuite.assertTopN(df, getExecutedPlan(df))
    }
  }
}
