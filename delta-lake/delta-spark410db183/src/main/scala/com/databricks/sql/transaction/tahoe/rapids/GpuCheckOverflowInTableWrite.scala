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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionProxy}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** GPU version of Delta's CheckOverflowInTableWrite expression. */
case class GpuCheckOverflowInTableWrite(
    child: GpuExpression,
    columnName: String,
    sourceType: Option[DataType])
    extends ShimUnaryExpression with GpuExpression {

  override def dataType: DataType = child.dataType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    try {
      child.columnarEval(batch)
    } catch {
      case error: ArithmeticException =>
        sourceType match {
          case Some(fromType) =>
            throw DeltaErrors.castingCauseOverflowErrorInTableWrite(
              fromType, dataType, columnName)
          case None => throw error
        }
    }
  }

  override def sql: String = child.sql

  override def toString: String = child.toString
}

object GpuCheckOverflowInTableWrite {
  private class CheckOverflowInTableWriteMetaImpl(
    check: CheckOverflowInTableWrite,
    conf: com.nvidia.spark.rapids.RapidsConf,
    parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
    rule: com.nvidia.spark.rapids.DataFromReplacementRule)
    extends UnaryExprMeta[CheckOverflowInTableWrite](check, conf, parent, rule) {
      override def convertToGpu(child: Expression): GpuExpression = child match {
        case gpuChild: GpuExpression =>
          // Native DBR only replaces overflow errors for a cast or a proxy around a cast.
          val sourceType = check.child match {
            case cast: Cast => Some(cast.child.dataType)
            case proxy: ExpressionProxy => proxy.child match {
              case cast: Cast => Some(cast.child.dataType)
              case _ => None
            }
            case _ => None
          }
          GpuCheckOverflowInTableWrite(gpuChild, check.columnName, sourceType)
        case _ =>
          throw new IllegalStateException("Expression child cannot run on the GPU")
      }
    }

  private def createCheckOverflowInTableWriteMeta(
    check: CheckOverflowInTableWrite,
    conf: com.nvidia.spark.rapids.RapidsConf,
    parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
    rule: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new CheckOverflowInTableWriteMetaImpl(check, conf, parent, rule)

  val exprRule: ExprRule[CheckOverflowInTableWrite] =
    GpuOverrides.expr[CheckOverflowInTableWrite](
      "Casting a numeric value as another numeric type in a Delta table write",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.all, TypeSig.all),
      createCheckOverflowInTableWriteMeta)
}
