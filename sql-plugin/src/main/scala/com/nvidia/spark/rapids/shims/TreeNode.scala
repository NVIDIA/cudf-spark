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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, TernaryExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryCommand}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}

trait ShimExpression extends Expression {
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    legacyWithNewChildren(newChildren)
  }
}

trait ShimUnaryExpression extends UnaryExpression {
  override def withNewChildInternal(newChild: Expression): Expression =
    legacyWithNewChildren(Seq(newChild))
}

trait ShimBinaryExpression extends BinaryExpression {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    legacyWithNewChildren(Seq(newLeft, newRight))
}

trait ShimTernaryExpression extends TernaryExpression {
  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    legacyWithNewChildren(Seq(newFirst, newSecond, newThird))
  }
}

trait ShimSparkPlan extends SparkPlan {
  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    legacyWithNewChildren(newChildren)
  }
}

trait ShimUnaryExecNode extends UnaryExecNode {
  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    legacyWithNewChildren(Seq(newChild))
  }
}

trait ShimBinaryExecNode extends BinaryExecNode {
  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    legacyWithNewChildren(Seq(newLeft, newRight))
  }
}

trait ShimUnaryCommand extends UnaryCommand {
  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    legacyWithNewChildren(Seq(newChild))
  }
}
