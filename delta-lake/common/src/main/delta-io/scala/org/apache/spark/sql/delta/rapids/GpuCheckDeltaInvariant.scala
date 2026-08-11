/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
 *
 * This file was derived from CheckDeltaInvariant.scala in the
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

package org.apache.spark.sql.delta.rapids

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, BindReferences, Expression, NonSQLExpression}
import org.apache.spark.sql.delta.constraints.{CheckDeltaInvariant, Constraint}
import org.apache.spark.sql.delta.constraints.Constraints.{Check, NotNull}
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of Delta Lake's CheckDeltaInvariant.
 *
 * An expression that validates a specific invariant on a column, before writing into Delta.
 *
 * @param child The fully resolved expression to be evaluated to check the constraint.
 * @param columnExtractors Extractors for each referenced column. Used to generate readable errors.
 * @param constraint The original constraint definition.
 */
case class GpuCheckDeltaInvariant(
    child: Expression,
    columnExtractors: Map[String, Expression],
    constraint: Constraint)
  extends ShimUnaryExpression with GpuExpression with NonSQLExpression {

  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = true

  def withBoundReferences(input: AttributeSeq, 
    metrics: Map[String, GpuMetric]): GpuCheckDeltaInvariant = {
    GpuCheckDeltaInvariant(
      GpuBindReferences.bindReference(child, input, metrics),
      columnExtractors.map {
        case (column, extractor) => column -> BindReferences.bindReference(extractor, input)
      },
      constraint)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(child.columnarEval(batch)) { col =>
      constraint match {
        case n: NotNull =>
          if (col.getBase.hasNulls) {
            throw DeltaInvariantViolationException(n)
          }
        case c: Check =>
          if (col.getBase.hasNulls || hasFalse(col.getBase)) {
            throwCheckException(c, batch, col)
          }
        case c => throw new IllegalStateException(s"Unknown constraint: $c")
      }
    }
    null
  }

  private def hasFalse(c: ColumnVector): Boolean = {
    withResource(c.all()) { s =>
      s.isValid && !s.getBoolean
    }
  }

  /**
   * Grabs the first value in the batch that failed the check expression and
   * throws the proper Delta exception.
   *
   * @param check Check constraint for this error
   * @param batch Batch containing at least one row that failed the check
   * @param col GPU column containing the result of evaluating the check expression
   */
  private def throwCheckException(
      check: Check,
      batch: ColumnarBatch,
      col: GpuColumnVector): Unit = {
    val nullsReplaced = if (col.getBase.hasNulls) {
      withResource(Scalar.fromBool(false)) { falseScalar =>
        col.getBase.replaceNulls(falseScalar)
      }
    } else {
      col.getBase.incRefCount()
    }
    val filterMask = withResource(nullsReplaced) { _ =>
      nullsReplaced.not()
    }
    val filteredHostCols = withResource(filterMask) { _ =>
      withResource(GpuColumnVector.from(batch)) { table =>
        withResource(table.filter(filterMask)) { filteredTable =>
          GpuColumnVector.extractHostColumns(filteredTable,
            GpuColumnVector.extractTypes(batch))
        }
      }
    }
    withResource(filteredHostCols) { _ =>
      val hostBatch = new ColumnarBatch(filteredHostCols.toArray,
        filteredHostCols(0).getBase.getRowCount.toInt)
      val row = hostBatch.getRow(0)
      throw DeltaInvariantViolationException(check, columnExtractors.mapValues(_.eval(row)).toMap)
    }
  }
}

object GpuCheckDeltaInvariant extends Logging {
  private val exprRule = GpuOverrides.expr[CheckDeltaInvariant](
    "checks a Delta Lake invariant expression",
    ExprChecks.projectOnly(
      TypeSig.all,
      TypeSig.all,
      paramCheck = Seq(ParamCheck("input", TypeSig.all, TypeSig.all)),
      repeatingParamCheck = Some(RepeatingParamCheck("extra", TypeSig.all, TypeSig.all))
    ),
    (c, conf, p, r) => new GpuCheckDeltaInvariantMeta(c, conf, p, r))

  def maybeConvertToGpu(
      invariants: Seq[CheckDeltaInvariant],
      rapidsConf: RapidsConf): Option[Seq[GpuCheckDeltaInvariant]] = {
    val metas = invariants.map(new GpuCheckDeltaInvariantMeta(_, rapidsConf, None, exprRule))
    metas.foreach(_.tagForGpu())
    val canReplace = metas.forall(_.canExprTreeBeReplaced)

    if (rapidsConf.shouldExplainAll || (rapidsConf.shouldExplain && !canReplace)) {
      val exprExplains = metas.map(_.explain(rapidsConf.shouldExplainAll))
      val execWorkInfo = if (canReplace) {
        "will run on GPU"
      } else {
        "cannot run on GPU because not all invariant checks can be replaced"
      }
      logWarning(s"<DeltaInvariantCheckerExec> $execWorkInfo:\n  ${exprExplains.mkString("\n  ")}")
    }

    if (canReplace) {
      Some(metas.map(_.convertToGpu().asInstanceOf[GpuCheckDeltaInvariant]))
    } else {
      None
    }
  }
}

class GpuCheckDeltaInvariantMeta(
    check: CheckDeltaInvariant,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ExprMeta[CheckDeltaInvariant](check, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    wrapped.constraint match {
      case _: NotNull | _: Check =>
      case c => willNotWorkOnGpu(s"unsupported constraint $c")
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    val child = childExprs.head.convertToGpu()
    // Delta 4.0 provides columnExtractors as Seq[(String, Expression)] while older
    // versions provide Map[String, Expression]. Normalize to Map for GPU version.
    val colExtractorsAsMap: Map[String, Expression] = wrapped.columnExtractors.toMap
    GpuCheckDeltaInvariant(
      child,
      colExtractorsAsMap,
      wrapped.constraint)
  }
}
