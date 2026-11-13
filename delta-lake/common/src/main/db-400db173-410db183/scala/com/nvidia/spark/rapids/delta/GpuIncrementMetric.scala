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
import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionRetryable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of the Databricks IncrementMetric expression: evaluates its child and adds the
 * number of rows evaluated to the wrapped SQL metric. Databricks' Delta commands wrap literals
 * and clause conditions in it to count source, matched, copied and written rows, so without
 * this rule every Filter or Project that carries one stays on the CPU. Mirrors the OSS Delta
 * version in DeltaProviderBase.
 *
 * The Databricks classes are Nondeterministic, so the expression is retryable only as a
 * GpuExpressionRetryable: the hosting operator checkpoints it before an attempt and restores
 * it when the attempt is retried after an OOM, and the restore takes back the rows the failed
 * attempt added, so the metric counts each batch exactly once and the child is evaluated
 * inside the operator's retry.
 */
case class GpuIncrementMetric(
    cpuInc: IncrementMetric,
    override val child: Expression,
    doContextCheck: Boolean)
  extends ShimUnaryExpression with GpuExpressionRetryable {

  override def dataType: DataType = child.dataType

  override lazy val deterministic: Boolean = cpuInc.deterministic

  override val selfNonDeterministic: Boolean = !deterministic

  // The metric must only count the rows this expression is evaluated on, which matters when it
  // sits inside a conditional branch or on the right of a short-circuiting AND / OR.
  override def hasSideEffects: Boolean = true

  override def prettyName: String = "gpu_" + cpuInc.prettyName

  // Rows added to the metric since the last checkpoint, taken back on a restore.
  @transient private var pendingRows: Long = 0L

  override def doCheckpoint(): Unit = pendingRows = 0L

  override def doRestore(): Unit = {
    cpuInc.metric.add(-pendingRows)
    pendingRows = 0L
  }

  override def doColumnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val result = child.columnarEval(batch)
    val rows = batch.numRows().toLong
    cpuInc.metric.add(rows)
    pendingRows += rows
    result
  }
}

// A named meta class so that its name shows in stack traces (issue #10838).
case class GpuIncrementMetricMeta(
    cpuInc: IncrementMetric,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[IncrementMetric](cpuInc, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuIncrementMetric(cpuInc, childExprs.head.convertToGpu(), conf.isRetryContextCheckEnabled)
}

object GpuIncrementMetric {
  val exprRule: ExprRule[IncrementMetric] =
    GpuOverrides.expr[IncrementMetric](
      "Increments a Delta command metric by the number of rows evaluated",
      ExprChecks.unaryProject(TypeSig.all, TypeSig.all, TypeSig.all, TypeSig.all),
      (inc, conf, parent, rule) => GpuIncrementMetricMeta(inc, conf, parent, rule))
}

/**
 * GPU version of the Databricks ConditionalIncrementMetric expression: evaluates its child and
 * adds the number of rows whose condition is true to the wrapped SQL metric (a null condition
 * does not count, as on the CPU). The Databricks UPDATE command counts its updated and copied
 * rows with it. Retryable the same way as GpuIncrementMetric: the condition, the count and the
 * child are evaluated inside the operator's retry, and a restore takes back the rows a failed
 * attempt added.
 */
case class GpuConditionalIncrementMetric(
    cpuInc: ConditionalIncrementMetric,
    child: Expression,
    condition: Expression,
    doContextCheck: Boolean)
  extends ShimExpression with GpuExpressionRetryable {

  override def children: Seq[Expression] = Seq(child, condition)

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override lazy val deterministic: Boolean = cpuInc.deterministic

  override val selfNonDeterministic: Boolean = !deterministic

  override def hasSideEffects: Boolean = true

  override def prettyName: String = "gpu_" + cpuInc.prettyName

  // Rows added to the metric since the last checkpoint, taken back on a restore.
  @transient private var pendingRows: Long = 0L

  override def doCheckpoint(): Unit = pendingRows = 0L

  override def doRestore(): Unit = {
    cpuInc.metric.add(-pendingRows)
    pendingRows = 0L
  }

  override def doColumnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val trueRows = withResourceIfAllowed(condition.columnarEvalAny(batch)) {
      case cond: GpuColumnVector => countTrue(cond)
      case cond: GpuScalar =>
        if (cond.isValid && cond.getValue == true) batch.numRows().toLong else 0L
      case other =>
        throw new IllegalStateException(s"Unexpected condition value $other (${other.getClass})")
    }
    val result = child.columnarEval(batch)
    cpuInc.metric.add(trueRows)
    pendingRows += trueRows
    result
  }

  private def countTrue(cond: GpuColumnVector): Long = {
    // The sum of a boolean column counts its true rows: true is 1, false is 0, and the
    // reduction leaves nulls out. An empty or all-null column gives an invalid scalar.
    withResource(cond.getBase.sum(DType.INT64)) { sum =>
      if (sum.isValid) sum.getLong else 0L
    }
  }
}

// A named meta class so that its name shows in stack traces (issue #10838).
case class GpuConditionalIncrementMetricMeta(
    cpuInc: ConditionalIncrementMetric,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ConditionalIncrementMetric](cpuInc, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    val Seq(child, condition) = childExprs.map(_.convertToGpu())
    GpuConditionalIncrementMetric(cpuInc, child, condition, conf.isRetryContextCheckEnabled)
  }
}

object GpuConditionalIncrementMetric {
  val exprRule: ExprRule[ConditionalIncrementMetric] =
    GpuOverrides.expr[ConditionalIncrementMetric](
      "Increments a Delta command metric by the number of rows whose condition is true",
      ExprChecks.projectOnly(TypeSig.all, TypeSig.all,
        Seq(ParamCheck("child", TypeSig.all, TypeSig.all),
          ParamCheck("condition", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (inc, conf, parent, rule) => GpuConditionalIncrementMetricMeta(inc, conf, parent, rule))
}
