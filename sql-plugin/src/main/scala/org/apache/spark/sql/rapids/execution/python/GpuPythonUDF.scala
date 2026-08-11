/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.execution.python

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.window.GpuAggregateWindowFunction

import org.apache.spark.api.python._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.rapids.aggregate.{CudfAggregate, GpuAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Helper functions for [[GpuPythonUDF]]
 */
object GpuPythonUDF {
  private[this] val SCALAR_TYPES = Set(
    PythonEvalType.SQL_BATCHED_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_UDF,
    PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
  )

  def isScalarPythonUDF(e: Expression): Boolean = {
    e.isInstanceOf[GpuPythonUDF] && SCALAR_TYPES.contains(e.asInstanceOf[GpuPythonUDF].evalType)
  }

  def isGroupedAggPandasUDF(e: Expression): Boolean = {
    e.isInstanceOf[GpuPythonUDF] &&
        e.asInstanceOf[GpuPythonUDF].evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF
  }

  // This is currently same as GroupedAggPandasUDF, but we might support new types in the future,
  // e.g, N -> N transform.
  def isWindowPandasUDF(e: Expression): Boolean = isGroupedAggPandasUDF(e)
}

/**
 * A serialized version of a Python lambda function. This is a special expression, which needs a
 * dedicated physical operator to execute it, and thus can't be pushed down to data sources.
 */
abstract class GpuPythonFunction(
    name: String,
    val func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    val resultId: ExprId = NamedExpression.newExprId)
  extends Expression with GpuUnevaluable with NonSQLExpression
    with UserDefinedExpression with GpuAggregateWindowFunction with Serializable {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)
  override val selfNonDeterministic: Boolean = !udfDeterministic

  override def toString: String = s"$name(${children.mkString(", ")})"

  lazy val resultAttribute: Attribute = AttributeReference(toPrettySQL(this), dataType, nullable)(
    exprId = resultId)

  override def nullable: Boolean = true

  // Support window things
  override val windowInputProjection: Seq[Expression] = Seq.empty

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    throw new UnsupportedOperationException(s"GpuPythonUDF should run in a Python process.")
  }
}

case class GpuPythonUDF(
    name: String,
    override val func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    override val resultId: ExprId = NamedExpression.newExprId)
  extends GpuPythonFunction(name, func, dataType, children, evalType, udfDeterministic, resultId) {
  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }
}

case class GpuPythonUDAF(
    name: String,
    override val func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    evalType: Int,
    udfDeterministic: Boolean,
    override val resultId: ExprId = NamedExpression.newExprId)
  extends GpuPythonFunction(name, func, dataType, children, evalType, udfDeterministic, resultId)
    with GpuAggregateFunction {
  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // `resultId` can be seen as cosmetic variation in PythonUDF, as it doesn't affect the result.
    this.copy(resultId = ExprId(-1)).withNewChildren(canonicalizedChildren)
  }

  override val initialValues: Seq[Expression] = Seq.empty[Expression]

  override val inputProjection: Seq[Expression] = Seq.empty[Expression]

  override val updateAggregates: Seq[CudfAggregate] = Seq.empty[CudfAggregate]

  override val mergeAggregates: Seq[CudfAggregate] = Seq.empty[CudfAggregate]

  override val evaluateExpression: Expression = null

  override def aggBufferAttributes: Seq[AttributeReference] = {
    throw new UnsupportedOperationException("GpuPythonUDAF isn't a real aggregate function")
  }
}