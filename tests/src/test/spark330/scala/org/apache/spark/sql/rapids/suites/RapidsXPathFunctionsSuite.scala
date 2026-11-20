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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.{GpuCpuBridgeExpression, GpuProjectExec}

import org.apache.spark.sql.XPathFunctionsSuite
import org.apache.spark.sql.catalyst.expressions.xml.XPathString
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsXPathFunctionsSuite
  extends XPathFunctionsSuite with RapidsSQLTestsTrait {
  testRapids("XPath string executes through a CPU bridge") {
    def query = spark.range(2).selectExpr(
      "xpath_string(IF(id = 0, '<a><b>x</b></a>', '<a><b>y</b></a>'), '/a/b')")
    withSQLConf("spark.rapids.sql.enabled" -> "false") {
      val cpu = query
      val expected = cpu.collect().toSeq
      withSQLConf("spark.rapids.sql.enabled" -> "true") {
        val gpu = query
        checkAnswer(gpu, expected)
        assert(gpu.schema == cpu.schema)
        val projects = gpu.queryExecution.executedPlan.collect {
          case project: GpuProjectExec => project
        }
        assert(projects.exists(_.expressions.exists(_.exists {
          case bridge: GpuCpuBridgeExpression =>
            bridge.cpuExpression.exists(_.isInstanceOf[XPathString])
          case _ => false
        })), s"Expected XPathString in a GPU CPU bridge: ${gpu.queryExecution.executedPlan}")
      }
    }
  }
}
