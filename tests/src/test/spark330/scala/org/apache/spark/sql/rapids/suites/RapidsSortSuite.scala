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

import com.nvidia.spark.rapids.GpuSortExec

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SortSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsSortSuite extends SortSuite with RapidsSQLTestsBaseTrait {
  import testImplicits._

  testRapids("basic sorting query uses GpuSortExec") {
    val df = Seq(("Hello", 4, 2.0), ("Hello", 1, 1.0), ("World", 8, 3.0))
      .toDF("a", "b", "c")
      .sort($"a".asc, $"b".asc)

    assert(df.collect().toSeq === Seq(
      Row("Hello", 1, 1.0),
      Row("Hello", 4, 2.0),
      Row("World", 8, 3.0)))

    val gpuSorts = getExecutedPlan(df).collect {
      case sort: GpuSortExec => sort
    }
    assert(gpuSorts.nonEmpty, s"Expected GpuSortExec in plan:\n${df.queryExecution.executedPlan}")
  }

  testRapids("sort followed by limit query uses GpuTopN") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = (1 to 100).map(Tuple1(_)).toDF("a").sort($"a".desc).limit(10)
      assert(df.collect().toSeq === (100 to 91 by -1).map(v => Row(v)))

      RapidsTakeOrderedAndProjectSuite.assertTopN(df, getExecutedPlan(df))
    }
  }
}
