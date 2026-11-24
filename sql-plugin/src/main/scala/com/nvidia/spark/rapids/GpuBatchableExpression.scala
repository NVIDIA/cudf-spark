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

package com.nvidia.spark.rapids

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU expression that can evaluate a group of sibling instances of its runtime
 * class in one native call instead of one call per instance.
 *
 * Eligible instances must be deterministic and side-effect-free: a group runs at
 * its first member's position in `boundExprs`, its returned columns are placed at
 * the members' original positions, and split-retry re-evaluates the group as a unit.
 */
trait GpuBatchableExpression { self: GpuExpression =>

  /**
   * Whether this instance's configuration is supported by the batch path (for
   * example, no per-column debug/verify side channel). Checked per instance: an
   * ineligible expression falls back to per-column evaluation without
   * disqualifying eligible siblings.
   */
  def batchEligible: Boolean

  /**
   * Group identity within one runtime class. Siblings with equal keys share one
   * `batchEval` call. Bare `Expression` keys are compared with `semanticEquals`;
   * all other key types use `==` (wrap an `Expression` when canonicalization must
   * not merge distinct inputs).
   */
  def batchGroupKey: Any

  /**
   * The input expression this instance batches over (for example, `json` in
   * `GpuGetJsonObject(json, path)`). `GpuBatchFusion` requires this expression to
   * be deterministic and side-effect-free, evaluates each distinct instance at
   * most once per projection batch, and passes the resulting column to `batchEval`.
   */
  def batchInput: Expression

  /**
   * Evaluates `siblings` in one native call. `siblings` and `inputs` have the
   * same length, with `inputs(i)` holding the evaluated `batchInput` of
   * `siblings(i)` (owned by the caller; implementations must not close `inputs`).
   * Returns one caller-owned cuDF `ColumnVector` per sibling in the same order.
   * Must be stateless because split-retry re-invokes projection on sliced batches.
   */
  def batchEval(siblings: Seq[Expression], inputs: Seq[GpuColumnVector]): Seq[ColumnVector]
}

/**
 * Projection-level fusion hook for [[GpuBatchableExpression]]. Groups compatible
 * top-level projection expressions and evaluates each group in one `batchEval`
 * call while preserving side-effect ordering and per-column fallback semantics.
 */
object GpuBatchFusion extends Logging {

  // Per-JVM task counters (all increments are visible in local mode; per-executor
  // on a cluster). Atomic because concurrent tasks share an executor JVM;
  // package-private so tests can assert deltas via reset() and snapshot().
  private[rapids] val firedGroups = new AtomicLong()
  private[rapids] val batchedMembers = new AtomicLong()
  private[rapids] val droppedMembers = new AtomicLong()

  /** Current counter values: (firedGroups, batchedMembers, droppedMembers). */
  private[rapids] def snapshot(): (Long, Long, Long) =
    (firedGroups.get(), batchedMembers.get(), droppedMembers.get())

  /** Zeroes all counters; test hook for order-independent delta assertions. */
  private[rapids] def reset(): Unit = {
    firedGroups.set(0L)
    batchedMembers.set(0L)
    droppedMembers.set(0L)
  }

  /** Members of one runtime class implementing GpuBatchableExpression. */
  private[rapids] case class BatchGroup(
      receiver: GpuBatchableExpression, // first member: batchEval dispatch target
      members: mutable.ArrayBuffer[(Int, GpuBatchableExpression)]) {

    def matches(g: GpuBatchableExpression): Boolean =
      receiver.getClass == g.getClass && keyEquals(receiver.batchGroupKey, g.batchGroupKey)
  }

  private def keyEquals(a: Any, b: Any): Boolean = (a, b) match {
    case (x: Expression, y: Expression) => x.semanticEquals(y)
    case _ => a == b
  }

  /**
   * Strips `GpuAlias` wrappers (top-level `boundExprs` in `GpuProjectExec` are
   * alias-wrapped) and single-argument `GpuCoalesce` (an identity wrapper common
   * around `get_json_object`) to expose the underlying candidate expression.
   */
  private[rapids] def unwrap(e: Expression): Expression = e match {
    case a: GpuAlias => unwrap(a.child)
    case c: GpuCoalesce if c.children.length == 1 => unwrap(c.children.head)
    case other => other
  }

  private def isReorderable(e: Expression): Boolean = e match {
    case g: GpuExpression => g.deterministic && !g.hasSideEffects
    case _ => false
  }

  private[rapids] def findGroups(boundExprs: Seq[Expression]): Seq[BatchGroup] = {
    // Fast path: avoid buffer allocations when no expression is batch-eligible.
    if (!boundExprs.exists { e => unwrap(e) match {
          case g: GpuBatchableExpression => g.batchEligible
          case _ => false } }) {
      return Seq.empty
    }
    val closed = mutable.ArrayBuffer.empty[BatchGroup]
    val open   = mutable.ArrayBuffer.empty[BatchGroup]
    boundExprs.zipWithIndex.foreach { case (e, idx) =>
      unwrap(e) match {
        case g: GpuBatchableExpression
            // Shared inputs are evaluated once per distinct expression node, so
            // batchInput must be deterministic and side-effect-free.
            if g.batchEligible && isReorderable(g.batchInput) =>
          open.find(_.matches(g)) match {
            case Some(group) => group.members += ((idx, g))
            case None => open += BatchGroup(g, mutable.ArrayBuffer((idx, g)))
          }
        case other =>
          // A non-reorderable sibling seals open groups so later members are not
          // hoisted ahead of a side-effecting or nondeterministic expression.
          if (!isReorderable(other)) { closed ++= open; open.clear() }
      }
    }
    (closed ++ open).toSeq
  }

  private[rapids] def project(
      cb: ColumnarBatch,
      boundExprs: Seq[Expression],
      evalColumn: Expression => org.apache.spark.sql.vectorized.ColumnVector)
    : Option[ColumnarBatch] = {
    if (!RapidsConf.ENABLE_BATCH_FUSION.get(SQLConf.get)) {
      return None
    }
    val groups = findGroups(boundExprs)
    if (groups.isEmpty) {
      None
    } else {
      val outputs = new Array[org.apache.spark.sql.vectorized.ColumnVector](boundExprs.length)
      // Cache evaluated batchInput columns by reference identity so shared
      // Expression nodes are evaluated once via the caller's evalColumn.
      val inputCache =
        new java.util.IdentityHashMap[Expression, GpuColumnVector]()
      def inputFor(e: GpuBatchableExpression): Option[GpuColumnVector] = {
        val child = e.batchInput
        if (inputCache.containsKey(child)) {
          Some(inputCache.get(child))
        } else {
          evalColumn(child) match {
            case gcv: GpuColumnVector => inputCache.put(child, gcv); Some(gcv)
            case raw =>
              // Non-GPU column cannot be passed to batchEval; release it and fall back per-column.
              raw.safeClose()
              None
          }
        }
      }
      var projectionError: Throwable = null
      try {
        val grouped = mutable.BitSet()
        // Evaluate each group at its first member's index so a later group is never
        // hoisted ahead of an earlier side-effecting non-member.
        val firstIndexOf = groups.flatMap(g => g.members.map(_._1 -> g)).toMap
        val doneGroups = mutable.HashSet.empty[BatchGroup]
        boundExprs.indices.foreach { idx =>
          firstIndexOf.get(idx) match {
            case Some(g) if !doneGroups.contains(g) =>
              doneGroups += g
              // GpuBatchableExpression uses a self-type (self: GpuExpression), not subtyping.
              val members = g.members.map(_._2)
              val exprMembers = members.map(_.asInstanceOf[Expression])
              val inputs = members.map(inputFor)
              if (inputs.forall(_.isDefined)) {
                val cudfInputs = inputs.map(_.get)
                val results = g.receiver.batchEval(exprMembers.toSeq, cudfInputs.toSeq).toArray
                g.members.map(_._1).zip(results).zipWithIndex.foreach {
                  case ((i, col), ridx) =>
                    try {
                      outputs(i) = GpuColumnVector.from(col,
                        unwrap(boundExprs(i)).dataType)
                    } catch {
                      case t: Throwable =>
                        // Earlier results already belong to outputs; from did not consume this one.
                        results.drop(ridx).safeClose(t)
                        throw t
                    }
                }
                grouped ++= g.members.map(_._1)
                // Record one counter increment and INFO line per fired group per batch
                // (used by tests and benchmark provenance checks).
                val n = members.length
                firedGroups.incrementAndGet()
                batchedMembers.addAndGet(n)
                logInfo(s"batch-fusion: ${g.receiver.getClass.getSimpleName} " +
                    s"x$n at index ${g.members.head._1}")
              } else {
                // If any member input is not a GpuColumnVector, the group drops out and
                // its members evaluate individually at their original indices below.
                droppedMembers.addAndGet(members.length)
              }
            case _ => // non-first member position or already-handled group
          }
          if (!grouped.contains(idx)) {
            // Evaluate ungrouped expressions (and dropped group members) via their
            // original alias-wrapped boundExprs entry.
            outputs(idx) = evalColumn(boundExprs(idx))
          }
        }
        // Even if every group dropped out, the fallback columns are already evaluated.
        Some(new ColumnarBatch(outputs, cb.numRows()))
      } catch {
        case t: Throwable =>
          projectionError = t
          outputs.safeClose(t)
          throw t
      } finally {
        try {
          inputCache.values().asScala.toSeq.safeClose(projectionError)
        } catch {
          case t: Throwable if projectionError == null =>
            // If input cleanup fails, the caller never receives the completed batch.
            outputs.safeClose(t)
            throw t
        }
      }
    }
  }
}
