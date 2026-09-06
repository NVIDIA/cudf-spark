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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.DType
import com.databricks.sql.execution.metric.{ConditionalIncrementMetric, IncrementMetric}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{ShimExpression, ShimUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of the Databricks IncrementMetric expression: evaluates its child and adds the
 * number of rows evaluated to the wrapped SQL metric. Databricks' Delta commands wrap literals
 * and clause conditions in it to count source, matched, copied and written rows, so without
 * this rule every Filter or Project that carries one stays on the CPU. Mirrors the OSS Delta
 * version in DeltaProviderBase.
 */
case class GpuIncrementMetric(cpuInc: IncrementMetric, override val child: Expression)
  extends ShimUnaryExpression with GpuExpression {

  override def dataType: DataType = child.dataType

  override lazy val deterministic: Boolean = cpuInc.deterministic

  // The metric must only count the rows this expression is evaluated on, which matters when it
  // sits inside a conditional branch or on the right of a short-circuiting AND / OR.
  override def hasSideEffects: Boolean = true

  override def prettyName: String = "gpu_" + cpuInc.prettyName

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    cpuInc.metric.add(batch.numRows())
    child.columnarEval(batch)
  }
}

object GpuIncrementMetric {
  val exprRule: ExprRule[IncrementMetric] =
    GpuOverrides.expr[IncrementMetric](
      "Increments a Delta command metric by the number of rows evaluated",
      ExprChecks.unaryProject(TypeSig.all, TypeSig.all, TypeSig.all, TypeSig.all),
      (inc, conf, parent, rule) =>
        new ExprMeta[IncrementMetric](inc, conf, parent, rule) {
          override def convertToGpuImpl(): GpuExpression =
            GpuIncrementMetric(inc, childExprs.head.convertToGpu())
        })
}

/**
 * GPU version of the Databricks ConditionalIncrementMetric expression: evaluates its child and
 * adds the number of rows whose condition is true to the wrapped SQL metric (a null condition
 * does not count, as on the CPU). The Databricks UPDATE command counts its updated and copied
 * rows with it.
 */
case class GpuConditionalIncrementMetric(
    cpuInc: ConditionalIncrementMetric,
    child: Expression,
    condition: Expression)
  extends ShimExpression with GpuExpression {

  override def children: Seq[Expression] = Seq(child, condition)

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override lazy val deterministic: Boolean = cpuInc.deterministic

  override def hasSideEffects: Boolean = true

  override def prettyName: String = "gpu_" + cpuInc.prettyName

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val trueRows = withResourceIfAllowed(condition.columnarEvalAny(batch)) {
      case cond: GpuColumnVector => countTrue(cond)
      case cond: GpuScalar =>
        if (cond.isValid && cond.getValue == true) batch.numRows().toLong else 0L
      case other =>
        throw new IllegalStateException(s"Unexpected condition value $other (${other.getClass})")
    }
    cpuInc.metric.add(trueRows)
    child.columnarEval(batch)
  }

  private def countTrue(cond: GpuColumnVector): Long = {
    withResource(GpuScalar.from(1, IntegerType)) { one =>
      withResource(GpuScalar.from(0, IntegerType)) { zero =>
        // A null condition becomes a null count entry, which the sum leaves out.
        withResource(cond.getBase.ifElse(one, zero)) { counts =>
          withResource(counts.sum(DType.INT64)) { sum =>
            if (sum.isValid) sum.getLong else 0L
          }
        }
      }
    }
  }
}

object GpuConditionalIncrementMetric {
  val exprRule: ExprRule[ConditionalIncrementMetric] =
    GpuOverrides.expr[ConditionalIncrementMetric](
      "Increments a Delta command metric by the number of rows whose condition is true",
      ExprChecks.projectOnly(TypeSig.all, TypeSig.all,
        Seq(ParamCheck("child", TypeSig.all, TypeSig.all),
          ParamCheck("condition", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (inc, conf, parent, rule) =>
        new ExprMeta[ConditionalIncrementMetric](inc, conf, parent, rule) {
          override def convertToGpuImpl(): GpuExpression = {
            val Seq(child, condition) = childExprs.map(_.convertToGpu())
            GpuConditionalIncrementMetric(inc, child, condition)
          }
        })
}
