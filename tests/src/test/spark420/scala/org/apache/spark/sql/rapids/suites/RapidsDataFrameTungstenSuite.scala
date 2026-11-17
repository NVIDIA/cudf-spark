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
{"spark":["420"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.{GpuFilterExec, GpuProjectExec}

import org.apache.spark.sql.{DataFrameTungstenSuite, Row}
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsDataFrameTungstenSuite extends DataFrameTungstenSuite with RapidsSQLTestsTrait {
  testRapids("Spark 4.2 SQL framework executes GPU operators") {
    val df = spark.range(0, 4).where("id > 0").selectExpr("id + 1 AS value")
    checkAnswer(df, Seq(Row(2L), Row(3L), Row(4L)))
    info(s"Spark 4.2 executed plan:\n${df.queryExecution.executedPlan}")

    val plan = getExecutedPlan(df)
    assert(plan.exists(_.isInstanceOf[GpuProjectExec]), "Missing GPU projection")
    assert(plan.exists(_.isInstanceOf[GpuFilterExec]), "Missing GPU filter")
  }
}
