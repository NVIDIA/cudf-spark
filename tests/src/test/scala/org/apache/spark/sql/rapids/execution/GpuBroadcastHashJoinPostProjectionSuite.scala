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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.ProjectAstTestUtils.collectExpressions
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.IntegerType

class GpuBroadcastHashJoinPostProjectionSuite extends AnyFunSuite {
  private val x = AttributeReference("x", IntegerType)()
  private val y = AttributeReference("y", IntegerType)()

  for (tiered <- Seq(true, false)) {
    test(s"cached build-side projection does not keep Project AST evaluators: tiered=$tiered") {
      val conf = new SQLConf
      conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, tiered.toString)
      conf.setConfString(RapidsConf.ENABLE_PROJECT_AST.key, "true")
      conf.setConfString(RapidsConf.ENABLE_PROJECT_AST_JIT.key, "true")
      SQLConf.withExistingConf(conf) {
        val sum = GpuAdd(x, y, failOnError = false)()
        // GpuProjectExecMeta wraps Project outputs before the join extracts the projection.
        val projectList: Seq[NamedExpression] = Seq(
          GpuAlias(GpuAstJitExpression(GpuMultiply(sum, y, failOnError = false)()), "jit")(),
          GpuAlias(
            GpuProjectAstExpression(GpuSubtract(sum, x, failOnError = false)()), "legacy")(),
          x)

        val perTask = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
          projectList, Seq(x, y), conf, enableAstJit = true, enableAst = true)
        val cached = GpuBroadcastHashJoinExecBase.bindCachedBuildSideProjection(
          projectList, Seq(x, y), conf)

        assert(collectExpressions[GpuProjectAstExpressionBase](perTask.exprTiers.flatten).nonEmpty)
        assert(collectExpressions[GpuProjectAstExpressionBase](cached.exprTiers.flatten).isEmpty)
        assertResult(projectList.size)(cached.exprTiers.last.size)
      }
    }
  }
}
