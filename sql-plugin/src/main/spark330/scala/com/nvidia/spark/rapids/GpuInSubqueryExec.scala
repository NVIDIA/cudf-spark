/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId, Predicate}
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, InSubqueryExec}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of InSubqueryExec.
 * Unlike Spark CPU version, result parameter is *not* transient. It only works on the CPU because
 * the CPU generates code on the driver containing the result values, and that is what the executor
 * uses to evaluate the expression. The GPU does not use code generation, so we must have the
 * result value transferred to the executor in order to build the GpuInSet to perform the eval.
 */
case class GpuInSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    shouldBroadcast: Boolean,
    private var resultBroadcast: Broadcast[Array[Any]],
    private var result: Array[Any])
  extends ExecSubqueryExpression with Predicate with ShimExpression with GpuExpression {

  override def children: Seq[Expression] = Seq(child)

  @transient private lazy val inSet = GpuInSet(child, result)

  override def nullable: Boolean = child.nullable
  override def toString: String = s"child IN ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): GpuInSubqueryExec = copy(plan = plan)

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = if (plan.output.length > 1) {
      rows.asInstanceOf[Array[Any]]
    } else {
      rows.map(_.get(0, child.dataType))
    }
    if (shouldBroadcast) {
      resultBroadcast = plan.session.sparkContext.broadcast(result)
      // Set the result to null, since we should only be serializing data for either
      // result or resultBroadcast to the executor, not both.
      result = null
    }
  }

  private def prepareResult(): Unit = {
    require(result != null || resultBroadcast != null, s"$this has not finished")
    if (result == null && resultBroadcast != null) {
      result = resultBroadcast.value
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    prepareResult()
    inSet.columnarEval(batch)
  }

  override lazy val canonicalized: GpuInSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null,
      result = null)
  }
}

class InSubqueryExecMeta(
    expr: InSubqueryExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta(expr, conf, parent, rule) {

  override def convertToGpuImpl(): GpuExpression = {
    expr match {
      case InSubqueryExec(_, plan, exprId, shouldBroadcast, resultBroadcast, result) =>
        val gpuChild = childExprs.head.convertToGpu()
        GpuInSubqueryExec(gpuChild, plan, exprId, shouldBroadcast, resultBroadcast, result)
      case e => throw new IllegalStateException(s"Unexpected CPU expression $e")
    }
  }
}
