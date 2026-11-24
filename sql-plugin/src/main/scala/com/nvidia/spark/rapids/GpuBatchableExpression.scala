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

import scala.collection.mutable

import ai.rapids.cudf.ColumnVector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU expression that can evaluate a GROUP of sibling instances of its runtime
 * class in ONE native call instead of one call per instance.
 *
 * Implementations must be deterministic: the returned columns are stitched back
 * into the projection output at the members' original boundExprs positions, and
 * the whole group is retried as a single unit under split-retry.
 */
trait GpuBatchableExpression { self: GpuExpression =>

  /**
   * Per-instance self-check: THIS instance's semantics are supported by the
   * batch path (whitelisted type pair, no verify-mode side channel). No sibling
   * context — one ineligible member never disqualifies its peers; it simply
   * falls to the per-column path while its siblings batch.
   */
  def batchEligible: Boolean

  /**
   * Group identity within one runtime class. Members whose keys are equal share
   * ONE batchEval call. Expression-valued keys are compared with semanticEquals;
   * all other key types with `==` (precedent: canShareExplode in
   * higherOrderFunctions.scala).
   */
  def batchGroupKey: Any

  /**
   * The EXPLICIT input expression this instance batches over (cast → `child`,
   * GetJsonObject → `json`). The hook evaluates this expression — not an
   * implicit `children.head` — and internal nodes are already materialized
   * when batching begins.
   */
  def batchInput: Expression

  /**
   * Evaluate `siblings` in ONE native call. `siblings` contains ALL group
   * members (siblings.length == inputs.length); `inputs(i)` is the evaluated
   * `batchInput` of `siblings(i)`. Returns `siblings.length` owned cudf
   * `ColumnVector`s in the same order; ownership transfers to the caller. Must
   * be STATELESS: project is re-invoked on arbitrary row subsets by split-retry
   * (basicPhysicalOperators.scala projectWithRetrySingleBatch).
   */
  def batchEval(siblings: Seq[Expression], inputs: Seq[GpuColumnVector]): Seq[ColumnVector]
}

object GpuBatchFusion extends Logging {

  // Positive-provenance counters (overhead contract): `project` runs inside
  // tasks, so these are per-JVM (all increments visible to a local-mode test
  // JVM; per-executor on a real cluster). Atomic because tasks of one JVM can
  // increment concurrently. package-private so GetJsonObjectBatchSuite
  // asserts deltas order-independently (reset() before, snapshot() after).
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
   * Unwrap GpuAlias recursively AND descend single-arg GpuCoalesce
   * (COALESCE(x) is semantically identity, so the canonical
   * `COALESCE(get_json_object(...))` workload batches too); boundExprs entries
   * are alias-wrapped (basicPhysicalOperators.scala GpuProjectExec.bind matches
   * `case ga: GpuAlias`). Returns the leaf expression.
   */
  private[rapids] def unwrap(e: Expression): Expression = e match {
    case a: GpuAlias => unwrap(a.child)
    case c: GpuCoalesce if c.children.length == 1 => unwrap(c.children.head)
    case other => other
  }

  // Reorderability is tested on the UNWRAPPED expression: GpuAlias overrides
  // neither hasSideEffects nor deterministic propagation, while side-effect
  // overrides live on the operators themselves (e.g. GpuCast) — testing the raw
  // alias would miss them.
  private def isReorderable(e: Expression): Boolean = e match {
    case g: GpuExpression => g.deterministic && !g.hasSideEffects
    case _ => false
  }

  private[rapids] def findGroups(boundExprs: Seq[Expression]): Seq[BatchGroup] = {
    // cheap precheck: pure non-batch projections (the TPC-DS majority) never allocate
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
            // member input must be pure: the hook evaluates it ONCE (cached)
            // instead of once per member — only safe if side-effect-free
            if g.batchEligible && isReorderable(g.batchInput) =>
          open.find(_.matches(g)) match {
            case Some(group) => group.members += ((idx, g))
            case None => open += BatchGroup(g, mutable.ArrayBuffer((idx, g)))
          }
        case other =>
          // non-member that may not be reordered seals every open group, so a
          // throwing fallback expression keeps its projection-order semantics
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
      // Kill switch: fall through to per-column evaluation (basicPhysicalOperators
      // .orElse chain) so the runtime toggle covers all batch-fusion groups.
      return None
    }
    val groups = findGroups(boundExprs)
    if (groups.isEmpty) {
      None
    } else {
      val outputs = new Array[org.apache.spark.sql.vectorized.ColumnVector](boundExprs.length)
      // Per-child eval cached by input-child IDENTITY: each DISTINCT child is
      // evaluated EXACTLY ONCE even when N members share it (overhead
      // contract); evalColumn is the caller's lambda, never leaf.columnarEval
      // directly.
      val inputCache =
        new java.util.IdentityHashMap[Expression, GpuColumnVector]()
      def inputFor(e: GpuBatchableExpression): Option[GpuColumnVector] = {
        val child = e.batchInput // explicit input contract, not children.head
        if (inputCache.containsKey(child)) {
          Some(inputCache.get(child))
        } else {
          evalColumn(child) match {
            case gcv: GpuColumnVector => inputCache.put(child, gcv); Some(gcv)
            case raw =>
              // member drops out of its group (per-column path); the evaluated
              // column cannot be reused, so release it here
              raw.close()
              None
          }
        }
      }
      try {
        var anyGroup = false
        val grouped = mutable.BitSet()
        // INDEX-WALK execution: groups evaluate at their FIRST member's index —
        // never all-groups-first, which would hoist a later group across an
        // earlier side-effecting/throwing leftover.
        val firstIndexOf = groups.flatMap(g => g.members.map(_._1 -> g)).toMap
        val doneGroups = mutable.HashSet.empty[BatchGroup]
        boundExprs.indices.foreach { idx =>
          firstIndexOf.get(idx) match {
            case Some(g) if !doneGroups.contains(g) =>
              doneGroups += g
              // GpuBatchableExpression is a self-typed trait (self: GpuExpression),
              // so the members need an explicit cast to satisfy batchEval's
              // Seq[Expression]; inputFor keeps the trait view
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
                        // `from` either moves the result into `outputs` or
                        // throws without consuming it, so on failure this
                        // result and every later one are unwrapped; earlier
                        // ones are owned by `outputs` and closed by the
                        // outer failure handler
                        results.drop(ridx).foreach(_.close())
                        throw t
                    }
                }
                anyGroup = true
                grouped ++= g.members.map(_._1)
                // POSITIVE PROVENANCE (overhead contract): one INFO line per
                // fired group; GetJsonObjectBatchSuite asserts counter deltas,
                // and the benchmark harness greps them from driver AND
                // executor logs before timing. `project` runs per
                // batch, so the line count is groups x batches x partitions.
                val n = members.length
                firedGroups.incrementAndGet()
                batchedMembers.addAndGet(n)
                logInfo(s"batch-fusion: ${g.receiver.getClass.getSimpleName} " +
                    s"x$n at index ${g.members.head._1}")
              } else {
                // some member inputs did not evaluate to a GpuColumnVector:
                // the whole group falls back per-column at its members'
                // positions below (never null)
                droppedMembers.addAndGet(members.length)
              }
            case _ => // non-first member position or already-handled group
          }
          if (!grouped.contains(idx)) {
            // Non-grouped position (or a member whose input eval failed):
            // ORIGINAL alias-wrapped expression — matches project's own
            // boundExprs.safeMap(evalColumn) fallback shape.
            outputs(idx) = evalColumn(boundExprs(idx))
          }
        }
        if (!anyGroup) {
          // Nothing took ownership: every populated output came from evalColumn
          // (the defensive drop path), so close them instead of leaking.
          outputs.foreach { c => if (c != null) c.close() }
          None
        } else {
          Some(new ColumnarBatch(outputs, cb.numRows()))
        }
      } catch {
        case t: Throwable =>
          // Roll back partial outputs on failure (closeOnExcept pattern used
          // across the plugin); on success each is owned by the result batch.
          outputs.foreach { c => if (c != null) c.close() }
          throw t
      } finally {
        inputCache.values.forEach(_.close())
      }
    }
  }
}
