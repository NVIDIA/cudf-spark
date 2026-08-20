/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from CheckOverflowInTableWrite in the
 * Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.{CheckOverflowInTableWrite, DeltaErrors}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** GPU version of Delta's CheckOverflowInTableWrite expression. */
case class GpuCheckOverflowInTableWrite(child: GpuCast, columnName: String)
    extends ShimUnaryExpression with GpuExpression {

  override def dataType: DataType = child.dataType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    try {
      child.columnarEval(batch)
    } catch {
      case _: ArithmeticException =>
        throw DeltaErrors.castingCauseOverflowErrorInTableWrite(
          child.child.dataType,
          dataType,
          columnName)
    }
  }

  override def sql: String = child.sql

  override def toString: String = child.toString
}

object GpuCheckOverflowInTableWrite {
  val exprRule: ExprRule[CheckOverflowInTableWrite] =
    GpuOverrides.expr[CheckOverflowInTableWrite](
      "Casting a numeric value as another numeric type in a Delta table write",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.all, TypeSig.all),
      (check, conf, parent, rule) =>
        new UnaryExprMeta[CheckOverflowInTableWrite](check, conf, parent, rule) {
          override def convertToGpu(child: Expression): GpuExpression = child match {
            case cast: GpuCast => GpuCheckOverflowInTableWrite(cast, check.columnName)
            case _ =>
              throw new IllegalStateException("Expression child is not of type GpuCast")
          }
        })
}
