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

import java.math.BigInteger

import ai.rapids.cudf.{BinaryOp, ColumnVector, DType, Scalar}
import com.databricks.sql.transaction.tahoe.commands.merge.checksum.{
  ChecksumExpression,
  ChecksumMergeSource
}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{ShimExpression, XxHash64Shims}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.{GpuPmod, GpuXxHash64, XxHash64Utils}
import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionRetryable
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of the Databricks ChecksumExpression. Delta MERGE wraps the materialized source in
 * a Filter on `checksum(cols) AND true`: the expression is always true, and as a side effect it
 * adds a per-row checksum of its children to a SQL metric, which MERGE later verifies against
 * the checksum of the rows it reads back. Without this rule that Filter stays on the CPU and
 * forces a columnar-to-row and a row-to-columnar transition around it.
 *
 * Per row the CPU expression computes
 * {{{
 *   combined = 1
 *   for each child: h = pmod(xxhash64(child, seed), MOD)
 *                   combined = (combined * (if (h == 0) 1 else h)) % MOD
 * }}}
 * and adds `combined` to the metric, unless skipChecksum is set or there are no children. The
 * metric is a multiplication modulo MOD metric: adding v sets value = value * v % MOD, and a
 * negative v is ignored. Here each batch adds the product of `combined` over its rows, which
 * leaves the same value. Retryable the same way as GpuIncrementMetric, except that a restore
 * cannot add a negative: it multiplies by the modular inverse of the product the failed attempt
 * added (MOD is prime).
 */
case class GpuChecksumExpression(
    cpuExpr: ChecksumExpression,
    override val children: Seq[Expression],
    doContextCheck: Boolean)
  extends ShimExpression with GpuExpressionRetryable {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override lazy val deterministic: Boolean = cpuExpr.deterministic

  override val selfNonDeterministic: Boolean = !deterministic

  // The metric must only count the rows this expression is evaluated on, which matters on the
  // right of a short-circuiting AND / OR.
  override def hasSideEffects: Boolean = true

  override def prettyName: String = "gpu_" + cpuExpr.prettyName

  // pmod(xxhash64(child, seed), MOD) for each child, as in the CPU expression.
  @transient private lazy val hashedChildren: Seq[Expression] = children.map { child =>
    GpuPmod(GpuXxHash64(Seq(child), cpuExpr.seed),
      GpuLiteral(ChecksumMergeSource.MOD, LongType), failOnError = false)
  }

  // Product of the values added to the metric since the last checkpoint, taken back on a restore.
  @transient private var pendingChecksum: Long = 1L

  override def doCheckpoint(): Unit = pendingChecksum = 1L

  override def doRestore(): Unit = {
    if (pendingChecksum != 1L) {
      val mod = BigInteger.valueOf(ChecksumMergeSource.MOD)
      cpuExpr.metric.add(BigInteger.valueOf(pendingChecksum).modInverse(mod).longValue())
    }
    pendingChecksum = 1L
  }

  override def doColumnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (!cpuExpr.skipChecksum && hashedChildren.nonEmpty && batch.numRows() > 0) {
      val checksum = batchChecksum(batch)
      cpuExpr.metric.add(checksum)
      pendingChecksum = pendingChecksum * checksum % ChecksumMergeSource.MOD
    }
    GpuLiteral(true, BooleanType).columnarEval(batch)
  }

  /** Product modulo MOD over the rows of the batch of the per-row combined checksum. */
  private def batchChecksum(batch: ColumnarBatch): Long = {
    // pmod is never negative, and a hash of 0 counts as 1 so that it does not zero the product.
    def factor(h: Expression): ColumnVector = {
      withResource(h.columnarEval(batch)) { hash =>
        withResource(Scalar.fromLong(1L))(hash.getBase.binaryOp(BinaryOp.NULL_MAX, _, DType.INT64))
      }
    }
    withResource(Scalar.fromLong(ChecksumMergeSource.MOD)) { mod =>
      // Fold each child into the running product as soon as it is hashed, so that only two
      // columns are alive at a time. Every factor is below MOD < 2^31, so a product fits in a long.
      val combined = hashedChildren.tail.foldLeft(factor(hashedChildren.head)) { (acc, h) =>
        withResource(acc) { _ =>
          withResource(factor(h))(f => withResource(acc.mul(f))(_.mod(mod)))
        }
      }
      withResource(combined)(productModulo(_, mod))
    }
  }

  /**
   * Product of the values of a column modulo MOD. There is no modular product reduction, so the
   * column is halved until one value is left: the first half is multiplied with the last half,
   * and the middle value of an odd length column is carried over.
   */
  private def productModulo(values: ColumnVector, mod: Scalar): Long = {
    var current = values.incRefCount()
    while (current.getRowCount > 1) {
      current = withResource(current)(halve(_, mod))
    }
    withResource(current) { last =>
      withResource(last.copyToHost())(_.getLong(0))
    }
  }

  /** One halving step of productModulo, which leaves ceil(rows / 2) values. */
  private def halve(cur: ColumnVector, mod: Scalar): ColumnVector = {
    val rows = cur.getRowCount.toInt
    val half = rows / 2
    val carried = rows % 2
    // Views of the first half, the middle value when the length is odd, and the last half.
    withResource(cur.splitAsViews(half, half + carried)) { views =>
      val reduced = withResource(views(0).mul(views(2)))(_.mod(mod))
      if (carried == 0) {
        reduced
      } else {
        withResource(reduced)(ColumnVector.concatenate(_, views(1)))
      }
    }
  }
}

// A named meta class so that its name shows in stack traces (issue #10838).
case class GpuChecksumExpressionMeta(
    cpuExpr: ChecksumExpression,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[ChecksumExpression](cpuExpr, conf, p, r) {

  // Every child goes through xxhash64, so it has the same nesting limit.
  override def tagExprForGpu(): Unit = {
    val maxDepth = cpuExpr.children.map(c => XxHash64Utils.computeMaxStackSize(c.dataType))
      .maxOption.getOrElse(0)
    if (maxDepth > XxHash64Utils.MAX_STACK_DEPTH) {
      willNotWorkOnGpu(s"The data type requires a stack depth of $maxDepth, which exceeds " +
        s"the GPU xxhash64 limit of ${XxHash64Utils.MAX_STACK_DEPTH}")
    }
  }

  override def convertToGpuImpl(): GpuExpression =
    GpuChecksumExpression(cpuExpr, childExprs.map(_.convertToGpu()),
      conf.isRetryContextCheckEnabled)
}

object GpuChecksumExpression {
  val exprRule: ExprRule[ChecksumExpression] =
    GpuOverrides.expr[ChecksumExpression](
      "Delta MERGE source checksum, always true, adds a row checksum to a metric",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", XxHash64Shims.supportedTypes, TypeSig.all))),
      (c, conf, parent, rule) => GpuChecksumExpressionMeta(c, conf, parent, rule))
}
