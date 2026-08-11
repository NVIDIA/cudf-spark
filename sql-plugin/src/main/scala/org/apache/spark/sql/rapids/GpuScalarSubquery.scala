/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuExpressionsUtils, GpuScalar}
import com.nvidia.spark.rapids.shims.{GpuScalarSubqueryShims, ShimExpression}

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU placeholder of ScalarSubquery, which returns the scalar result with columnarEval method.
 * This placeholder is to make ScalarSubquery working as a GPUExpression to cooperate
 * other GPU overrides.
 */
case class GpuScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression with GpuExpression with ShimExpression
    with GpuScalarSubqueryShims {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Seq.empty
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): GpuScalarSubquery = copy(plan = query)

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile protected var updated: Boolean = false

  override def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      throw new IllegalStateException(
        s"more than one row returned by a subquery" +
          s" used as an expression:\n$plan")
    } else if (rows.length == 1) {
      assert(rows.head.numFields == 1,
        s"Expects 1 field, but got ${rows.head.numFields}; something went wrong in analysis")
      result = rows.head.get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override lazy val canonicalized: Expression = {
    GpuScalarSubquery(
      plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      ExprId(0))
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    require(updated, s"$this has not finished")
    GpuScalar(result, dataType)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    require(updated, s"$this has not finished")
    GpuExpressionsUtils.resolveColumnVector(columnarEvalAny(batch), batch.numRows())
  }
}
