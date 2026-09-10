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

import com.nvidia.spark.rapids.GpuFilterExec
import com.nvidia.spark.rapids.shims.ShimPredicateHelper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.datasources.FileSourceStrategySuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsFileSourceStrategySuite
  extends FileSourceStrategySuite
  with RapidsSQLTestsTrait
  with ShimPredicateHelper {
  override def getPhysicalFilters(df: DataFrame): ExpressionSet = {
    ExpressionSet(
      df.queryExecution.executedPlan.collect {
        case FilterExec(f, _) => splitConjunctivePredicates(f)
        case GpuFilterExec(f, _) => splitConjunctivePredicates(f)
      }.flatten)
  }

  testRapids("partitioned table - after scan filters") {
    val table =
      createTable(
        files = Seq(
          "p1=1/file1" -> 10,
          "p1=2/file2" -> 10))

    val df1 = table.where("p1 = 1 AND (p1 + c1) = 2 AND c1 = 1")
    val filters1 = gpuPhysicalFilterStrings(df1)
    assert(filters1.exists(f => f.contains("c1") && f.contains("= 1")))
    assert(!filters1.exists(f => f.contains("p1") && f.contains("= 1")))

    val df2 = table.where("(p1 + c2) = 2 AND c1 = 1")
    val filters2 = gpuPhysicalFilterStrings(df2)
    assert(filters2.exists(f => f.contains("c1") && f.contains("= 1")))
    assert(filters2.exists(f =>
      f.contains("p1") && f.contains("c2") && f.contains("+") && f.contains("= 2")))
  }

  private def gpuPhysicalFilterStrings(df: DataFrame): Seq[String] = {
    df.queryExecution.executedPlan.collect {
      case GpuFilterExec(f, _) => splitConjunctivePredicates(f).map(_.toString)
    }.flatten
  }
}
