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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, ExprId, Literal}
import org.apache.spark.sql.execution.ScalarSubquery

/**
 * Optimizer to make decisions like, which expressions should we run on the CPU vs the GPU
 * and combining cpu expressions into an expression tree.
 */
object GpuCpuBridgeOptimizer extends Logging {

  def checkAndOptimizeExpressionMetas(exprs: Seq[BaseExprMeta[_]]): Unit = {
    if (exprs.nonEmpty && exprs.head.conf.isCpuBridgeEnabled) {
      exprs.foreach { child =>
        if (!child.canExprTreeBeReplaced && canRunOnCpuOrGpuRecursively(child)) {
          // Check if this expression tree contains an expression that prevents all bridge
          // optimization (e.g., CheckOverflow whose CPU version takes BigDecimal input)
          if (child.containsExpressionThatPreventsBridge) {
            logDebug(s"Skipping bridge optimization for ${child.wrapped.getClass.getSimpleName} " +
              "due to expression that prevents bridge optimization in tree")
          } else if (requiresAst(child)) {
            // Check if this expression tree requires AST conversion (marked during tagging)
            logDebug(s"Skipping bridge optimization for ${child.wrapped.getClass.getSimpleName} " +
              "due to AST requirement")
          } else {
            // Minimize data movement for this expression tree assuming the consumer is on GPU
            optimizeByMinimizingMovement(child)
          }
        }
      }
    }
  }

  /**
   * Bridge-optimize only the non-AST subtrees of a join condition, leaving AST-capable nodes
   * on GPU. This is called after join condition extraction has been proven possible, and before
   * any bridge optimization has been applied to the condition tree. That ordering matters
   * because bridge-marked nodes report canSelfBeAst = false, so running this after
   * whole-condition bridge optimization would misclassify AST-capable condition nodes as subtrees
   * to extract.
   */
  def checkAndOptimizeNonAstSubtrees(expr: BaseExprMeta[_]): Unit = {
    if (expr.conf.isCpuBridgeEnabled) {
      optimizeNonAstSubtreesRecursive(expr)
    }
  }

  private def optimizeNonAstSubtreesRecursive(expr: BaseExprMeta[_]): Unit = {
    if (!expr.canSelfBeAst) {
      checkAndOptimizeExpressionMetas(Seq(expr))
    } else {
      expr.childExprs.foreach(optimizeNonAstSubtreesRecursive)
    }
  }

  /**
   * Check if an expression tree requires AST conversion by looking for expressions
   * that have been marked as requiring AST during the tagging phase.
   */
  private def requiresAst(expr: BaseExprMeta[_]): Boolean = {
    // Check if this expression was marked as requiring AST during tagging
    expr.mustBeAstExpression || expr.childExprs.exists(requiresAst)
  }

  private def canRunOnCpuOrGpuRecursively(expr: BaseExprMeta[_]): Boolean = {
    if (!expr.canThisBeReplaced && !expr.canMoveToCpuBridge) {
      false
    } else {
      expr.childExprs.forall(canRunOnCpuOrGpuRecursively)
    }
  }

  /**
   * Determine whether an expression is effectively scalar-like for the purpose of
   * CPU/GPU placement. An expression is scalar-like if it is a `Literal`, a `ScalarSubquery`,
   * or if all of its inputs are scalar-like (yielding a scalar result as well).
   * Zero-arity non-literal expressions are not treated as scalar-like.
   */
  def isScalarLike(exprMeta: BaseExprMeta[_]): Boolean = {
    exprMeta.wrapped match {
      case _: Literal => true
      case _: ScalarSubquery => true
      case _ =>
        exprMeta.childExprs.nonEmpty && exprMeta.childExprs.forall(isScalarLike)
    }
  }

  // ------------------------------
  // Cost-based placement optimizer
  // ------------------------------

  private sealed trait ExecSide
  private case object OnGpu extends ExecSide
  private case object OnCpu extends ExecSide

  private sealed trait InputKey
  private case class AttrKey(exprId: ExprId) extends InputKey
  private case class BoundKey(ordinal: Int) extends InputKey

  private case class PlacementCost(gpuCost: Int, cpuCost: Int, cpuRequiredLeaves: Set[InputKey])

  /** Identify an input key for leaves that represent data-carrying inputs. */
  private def leafInputKey(expr: Expression): Option[InputKey] = expr match {
    case ar: AttributeReference => Some(AttrKey(ar.exprId))
    case BoundReference(ordinal, _, _) => Some(BoundKey(ordinal))
    case _ => None
  }

  // Child-arity cutoff for switching from exact subset enumeration to the O(n) dual-greedy
  // heuristic. Found empirically: on a modern CPU 8 children already costs over 1 ms to
  // enumerate exactly, while the heuristic stays under ~500 us.
  private val CpuSubsetExactMaxChildren = 7

  /**
   * One of the two greedy strategies behind [[chooseCpuSubset]] for large fan-out: keep each
   * child on the GPU unless running it on CPU is strictly cheaper (or GPU is not possible).
   * Returns (chosen child indices, local cost, CPU input leaves required by the chosen set).
   */
  private def cpuSubsetGpuBiased(
      childMetas: Seq[BaseExprMeta[_]],
      childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
    val n = childMetas.length
    var sumCpu = 0
    var sumGpu = 0
    var leaves: Set[InputKey] = Set.empty
    var subset: Set[Int] = Set.empty
    var ok = true
    var i = 0
    while (i < n && ok) {
      val meta = childMetas(i)
      val c = childCosts(i)
      val gpuOk = c.gpuCost != Int.MaxValue
      val cpuOk = c.cpuCost != Int.MaxValue
      (gpuOk, cpuOk) match {
        case (false, false) =>
          ok = false
        case (false, true) =>
          subset += i
          sumCpu += c.cpuCost
          leaves ++= c.cpuRequiredLeaves
        case (true, false) =>
          sumGpu += c.gpuCost + (if (isScalarLike(meta)) 0 else 1)
        case (true, true) =>
          val gpuAlt = c.gpuCost + (if (isScalarLike(meta)) 0 else 1)
          if (c.cpuCost < gpuAlt) {
            subset += i
            sumCpu += c.cpuCost
            leaves ++= c.cpuRequiredLeaves
          } else {
            sumGpu += gpuAlt
          }
      }
      i += 1
    }
    val local = if (ok) sumCpu + sumGpu else Int.MaxValue
    (subset, local, leaves)
  }

  /**
   * The other greedy strategy behind [[chooseCpuSubset]]: seed children onto CPU when doing so
   * reuses a CPU input leaf another CPU child already needs, otherwise prefer GPU. Children are
   * visited in descending order of leaf-reuse potential. Same return shape as
   * [[cpuSubsetGpuBiased]].
   */
  private def cpuSubsetCpuSeeding(
      childMetas: Seq[BaseExprMeta[_]],
      childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
    val n = childMetas.length
    // Order children by potential reuse: sum of leaf frequencies desc.
    val leafFreq = new scala.collection.mutable.HashMap[InputKey, Int]()
    var j = 0
    while (j < n) {
      val it = childCosts(j).cpuRequiredLeaves.iterator
      while (it.hasNext) {
        val k = it.next()
        leafFreq.put(k, leafFreq.getOrElse(k, 0) + 1)
      }
      j += 1
    }
    val order = (0 until n).sortBy { idx =>
      val ks = childCosts(idx).cpuRequiredLeaves
      // negative for descending
      -ks.foldLeft(0)((acc, k) => acc + leafFreq.getOrElse(k, 0))
    }
    var sumCpu = 0
    var sumGpu = 0
    var leaves: Set[InputKey] = Set.empty
    var subset: Set[Int] = Set.empty
    val seen = new scala.collection.mutable.HashSet[InputKey]()
    var ok = true
    var p = 0
    while (p < order.length && ok) {
      val i = order(p)
      val meta = childMetas(i)
      val c = childCosts(i)
      val gpuOk = c.gpuCost != Int.MaxValue
      val cpuOk = c.cpuCost != Int.MaxValue
      (gpuOk, cpuOk) match {
        case (false, false) =>
          ok = false
        case (false, true) =>
          subset += i
          sumCpu += c.cpuCost
          leaves ++= c.cpuRequiredLeaves
          val it2 = c.cpuRequiredLeaves.iterator
          while (it2.hasNext) seen.add(it2.next())
        case (true, false) =>
          sumGpu += c.gpuCost + (if (isScalarLike(meta)) 0 else 1)
        case (true, true) =>
          val gpuAlt = c.gpuCost + (if (isScalarLike(meta)) 0 else 1)
          val ks = c.cpuRequiredLeaves
          // Seed onto CPU if it reuses an already-seen leaf, or if CPU is strictly cheaper.
          var reuses = false
          val it3 = ks.iterator
          while (it3.hasNext && !reuses) {
            reuses = seen.contains(it3.next())
          }
          if (reuses || c.cpuCost < gpuAlt) {
            subset += i
            sumCpu += c.cpuCost
            leaves ++= ks
            val it4 = ks.iterator
            while (it4.hasNext) seen.add(it4.next())
          } else {
            sumGpu += gpuAlt
          }
      }
      p += 1
    }
    val local = if (ok) sumCpu + sumGpu else Int.MaxValue
    (subset, local, leaves)
  }

  /**
   * Choose which children of a CPU-placed parent also run on CPU, returning (chosen child
   * indices, local placement cost, CPU input leaves required by the chosen set). Small fan-out
   * is solved exactly; larger fan-out (above [[CpuSubsetExactMaxChildren]]) uses the dual-greedy
   * heuristic because exact enumeration is 2^n.
   */
  private def chooseCpuSubset(
      parent: BaseExprMeta[_],
      childMetas: Seq[BaseExprMeta[_]],
      childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
    if (childMetas.length > CpuSubsetExactMaxChildren) {
      chooseCpuSubsetHeuristic(parent, childMetas, childCosts)
    } else {
      chooseCpuSubsetExact(parent, childMetas, childCosts)
    }
  }

  /**
   * Approximate placement for large fan-out, where exact enumeration (2^n) is too slow: run both
   * greedy strategies and keep the one with the lower score = local cost plus the number of
   * distinct CPU input leaves it imports. Same return shape as [[cpuSubsetGpuBiased]].
   */
  private def chooseCpuSubsetHeuristic(
      parent: BaseExprMeta[_],
      childMetas: Seq[BaseExprMeta[_]],
      childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
    val (subG, localG, leavesG) = cpuSubsetGpuBiased(childMetas, childCosts)
    val (subC, localC, leavesC) = cpuSubsetCpuSeeding(childMetas, childCosts)
    val scoreG = if (localG == Int.MaxValue) Int.MaxValue else localG + leavesG.size
    val scoreC = if (localC == Int.MaxValue) Int.MaxValue else localC + leavesC.size
    val chooseCpuSeed = scoreC < scoreG || (scoreC == scoreG && subC.size < subG.size)
    val chosen = if (chooseCpuSeed) (subC, localC, leavesC) else (subG, localG, leavesG)
    logDebug(s"Heuristic dual for ${parent.wrapped.getClass.getSimpleName}: " +
      s"gpuScore=$scoreG cpuSeedScore=$scoreC chosen=${if (chooseCpuSeed) "CPU" else "GPU"} " +
      s"leaves=${chosen._3.size} subset=${chosen._1.mkString(",")}")
    chosen
  }

  /**
   * Exact placement for small fan-out: enumerate every subset of children placed on CPU and keep
   * the cheapest. Ties favor the GPU: prefer fewer children on CPU, then fewer CPU input leaves.
   * Same return shape as [[cpuSubsetGpuBiased]].
   */
  private def chooseCpuSubsetExact(
      parent: BaseExprMeta[_],
      childMetas: Seq[BaseExprMeta[_]],
      childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
    val n = childMetas.length
    var bestCost = Int.MaxValue
    var bestLeaves: Set[InputKey] = Set.empty
    var bestSubset: Set[Int] = Set.empty

    // Pre-compute scalar mask for efficiency in inner loop
    val scalarMask = {
      var mask = 0
      var i = 0
      while (i < n) {
        if (isScalarLike(childMetas(i))) mask |= (1 << i)
        i += 1
      }
      mask
    }

    var mask = 0
    val maxMask = 1 << n
    while (mask < maxMask) {
      var sumCpu = 0
      var sumGpu = 0
      var leaves = Set.empty[InputKey]
      var valid = true
      var i = 0
      while (i < n && valid) {
        val costs = childCosts(i)
        val onCpu = (mask & (1 << i)) != 0
        if (onCpu) {
          if (costs.cpuCost == Int.MaxValue) {
            valid = false
          } else {
            sumCpu += costs.cpuCost
            leaves ++= costs.cpuRequiredLeaves
          }
        } else {
          if (costs.gpuCost == Int.MaxValue) {
            valid = false
          } else {
            sumGpu += costs.gpuCost
            // Use pre-computed scalar mask for move cost (more efficient than isScalarLike call)
            if ((scalarMask & (1 << i)) == 0) {
              sumGpu += 1  // Move cost for non-scalar
            }
          }
        }
        i += 1
      }
      if (valid) {
        // IMPORTANT: Do NOT include leaves.size here; we charge import cost only at the
        // CPU region boundary of the current parent, not per child subtree. This prevents
        // double-charging when merging child CPU regions into a parent CPU region.
        val total = sumCpu + sumGpu
        // Tie-breaking policy: favor GPU on ties -> prefer fewer children on CPU, implemented by
        // preferring a smaller chosen subset (then fewer leaves) on equal total cost.
        val chosenSize = java.lang.Integer.bitCount(mask)
        val bestSize = java.lang.Integer.bitCount({
          var acc = 0
          bestSubset.foreach(idx => acc |= (1 << idx))
          acc
        })
        val takeThis = (total < bestCost) ||
          (total == bestCost &&
            (chosenSize < bestSize ||
              (chosenSize == bestSize && leaves.size < bestLeaves.size)))
        if (takeThis) {
          bestCost = total
          bestLeaves = leaves
          var chosen: Set[Int] = Set.empty
          var j = 0
          while (j < n) {
            if ((mask & (1 << j)) != 0) chosen += j
            j += 1
          }
          bestSubset = chosen
        }
      }
      mask += 1
    }
    logDebug(s"Best subset for ${parent.wrapped.getClass.getSimpleName}: " +
      s"cost=$bestCost leaves=${bestLeaves.size} chosen=${bestSubset.mkString(",")}")
    (bestSubset, bestCost, bestLeaves)
  }

  /**
   * Optimize an expression tree by minimizing estimated data movement, using a
   * bottom-up dynamic programming approach. We assume the parent/consumer is running
   * on the GPU at the root, so we include the cost of moving the root's output when
   * placed on CPU. If we need to add expression costs in the future, we can do so.
   */
  def optimizeByMinimizingMovement(root: BaseExprMeta[_]): Unit = {
    val startTimeNs = System.nanoTime()
    val costCache = scala.collection.mutable.HashMap[BaseExprMeta[_], PlacementCost]()

    def sideName(s: ExecSide): String = s match {
      case OnGpu => "GPU"
      case OnCpu => "CPU"
    }

    def cpuRegionCostForGpuConsumer(
        exprMeta: BaseExprMeta[_],
        costs: PlacementCost): Int = {
      if (costs.cpuCost == Int.MaxValue) {
        Int.MaxValue
      } else {
        costs.cpuCost + costs.cpuRequiredLeaves.size +
          (if (isScalarLike(exprMeta)) 0 else 1)
      }
    }

    def computeCosts(expr: BaseExprMeta[_]): PlacementCost = {
      costCache.getOrElseUpdate(expr, {
        // Base case: data-carrying leaf input (AttributeReference/BoundReference)
        val maybeLeaf: Option[PlacementCost] = expr.wrapped match {
          case e: Expression =>
            leafInputKey(e).map { key =>
              // Conceptually placeable on either side at zero subtree cost; when placed on
              // CPU, record the input as a required leaf so we can transfer it once per
              // CPU region.
              val pc = PlacementCost(
                gpuCost = 0,
                cpuCost = 0,
                cpuRequiredLeaves = Set[InputKey](key))
              logDebug(s"Cost[leaf] ${expr.wrapped.getClass.getSimpleName}: gpu=0 cpu=0 " +
                s"leaves=${pc.cpuRequiredLeaves.size}")
              pc
            }
          case _ => None
        }
        maybeLeaf.getOrElse {
          // Compute child costs first
          val childCosts = expr.childExprs.map(computeCosts)

          val gpuAllowed = expr.canThisBeReplaced
          val cpuAllowed = expr.canMoveToCpuBridge

          // Cost if this node runs on GPU
          val gpuTotal: Int = if (gpuAllowed) {
            expr.childExprs.zip(childCosts).map { case (childMeta, costs) =>
              val cpuChoice = cpuRegionCostForGpuConsumer(childMeta, costs)
              val gpuChoice = costs.gpuCost
              Math.min(cpuChoice, gpuChoice)
            }.sum
          } else Int.MaxValue

          // CPU cost via exact subset enumeration or the large-fanout heuristic
          val (_, cpuTotal, cpuLeaves): (Set[Int], Int, Set[InputKey]) = if (cpuAllowed) {
            chooseCpuSubset(expr, expr.childExprs, childCosts)
          } else (Set.empty[Int], Int.MaxValue, Set.empty[InputKey])

          val result = PlacementCost(gpuTotal, cpuTotal, cpuLeaves)
          logDebug(
            s"Cost ${expr.wrapped.getClass.getSimpleName}: gpuAllowed=$gpuAllowed " +
            s"cpuAllowed=$cpuAllowed gpuTotal=$gpuTotal cpuTotal=$cpuTotal " +
            s"cpuLeaves=${cpuLeaves.size}")
          result
        }
      })
    }

    // Compute subtree costs
    val rootCosts = computeCosts(root)

    // Root consumer is on GPU, so a CPU root has to import inputs and export its output.
    val rootCpuWithOutputMove = cpuRegionCostForGpuConsumer(root, rootCosts)
    val rootGpuWithOutputMove = rootCosts.gpuCost // output already on GPU; no move

    val chooseRootSide: ExecSide = {
      if (rootGpuWithOutputMove != Int.MaxValue &&
          rootGpuWithOutputMove <= rootCpuWithOutputMove) {
        OnGpu
      } else {
        OnCpu
      }
    }

    logDebug(
      s"Root costs: gpu=$rootGpuWithOutputMove cpu=$rootCpuWithOutputMove " +
      s"chosen=${sideName(chooseRootSide)}")

    // Second pass: assign sides to minimize costs and apply moveToCpuBridge where needed
    def assignSides(expr: BaseExprMeta[_], chosenSide: ExecSide): Unit = {
      // Place current node
      chosenSide match {
        case OnCpu if expr.canMoveToCpuBridge && !expr.willUseGpuCpuBridge => expr.moveToCpuBridge()
        case _ => // keep on GPU or already on CPU bridge
      }
      logDebug(s"Place ${expr.wrapped.getClass.getSimpleName} on ${sideName(chosenSide)}")

      // Place children to minimize (childCost + edge penalty)
      val childCosts = expr.childExprs.map { child =>
        costCache.getOrElse(child, computeCosts(child))
      }

      chosenSide match {
        case OnGpu =>
          expr.childExprs.zip(childCosts).foreach { case (childMeta, costs) =>
            val gpuCandidate = if (costs.gpuCost == Int.MaxValue) Int.MaxValue
              else costs.gpuCost
            val cpuCandidate = cpuRegionCostForGpuConsumer(childMeta, costs)

            val childSide =
              if (gpuCandidate <= cpuCandidate || cpuCandidate == Int.MaxValue) OnGpu else OnCpu

            logDebug(
              s"Child ${childMeta.wrapped.getClass.getSimpleName}: gpuCand=$gpuCandidate " +
              s"cpuCand=$cpuCandidate choose=${sideName(childSide)} " +
              s"parent=${sideName(chosenSide)}")

            assignSides(childMeta, childSide)
          }
        case OnCpu =>
          // Use exact best subset assignment under CPU parent
          val (subset, localTotal, leavesForSubset) = 
            chooseCpuSubset(expr, expr.childExprs, childCosts)
          val effectiveTotal = if (localTotal == Int.MaxValue) Int.MaxValue
            else localTotal + leavesForSubset.size
          logDebug(s"CPU parent assign: subset=${subset.mkString(",")} localTotal=$localTotal " +
            s"leaves=${leavesForSubset.size} effectiveWithImports=$effectiveTotal")
          expr.childExprs.zipWithIndex.foreach { case (childMeta, idx) =>
            // Force scalar-like children to co-locate with CPU parent
            val side = if (isScalarLike(childMeta) || subset.contains(idx)) OnCpu else OnGpu
            logDebug(s"Child ${childMeta.wrapped.getClass.getSimpleName}: " + 
              s"choose=${sideName(side)} parent=CPU")
            assignSides(childMeta, side)
          }
      }
    }

    assignSides(root, chooseRootSide)

    val elapsedMs = (System.nanoTime() - startTimeNs) / 1000000.0
    // Compute max depth of the expression meta tree (root counts as 1)
    def maxDepth(meta: BaseExprMeta[_]): Int =
      if (meta.childExprs.isEmpty) 1 else 1 + meta.childExprs.map(maxDepth).max
      
    logDebug(s"optimizeByMinimizingMovement: nodes=${costCache.size} " +
      s"depth=${maxDepth(root)} timeMs=$elapsedMs")
  }

  /**
   * Collapse a bridge whose GPU inputs are themselves bridges into a single bridge, so a chain
   * of CPU sub-trees becomes one CPU evaluation with one host round-trip.
   *
   * Called on the bridge that `convertForGpuCpuBridge` has just built for the current node. That
   * node's non-bridge GPU inputs are already fully converted GPU expressions, so only the bridge
   * inputs need merging; non-bridge expressions are returned unchanged.
   */
  def optimizeByMergingBridgeExpressions(expr: GpuExpression): GpuExpression = {
    expr match {
      case bridge: GpuCpuBridgeExpression =>
        val hasBridgeInputs = bridge.gpuInputs.exists(_.isInstanceOf[GpuCpuBridgeExpression])
        if (hasBridgeInputs) {
          mergeBridgeExpressions(bridge)
        } else {
          bridge
        }
      case other => other
    }
  }

  /**
   * Merge a bridge expression with its bridge input expressions.
   * Handles multiple bridge inputs by flattening GPU inputs and remapping BoundReferences.
   * 
   * @param parentBridge The outer bridge expression
   * @return A single merged bridge expression
   */
  private def mergeBridgeExpressions(
      parentBridge: GpuCpuBridgeExpression): GpuCpuBridgeExpression = {
    
    // Collect all GPU inputs and create mapping for BoundReference remapping
    val (flattenedGpuInputs, inputMapping) = flattenBridgeInputs(parentBridge.gpuInputs)
    
    // Rewrite the parent CPU expression to substitute bridge expressions and 
    // remap BoundReferences
    val mergedCpuExpr = rewriteCpuExpression(parentBridge.cpuExpression, inputMapping)
    
    val mergedExpression = GpuCpuBridgeExpression(
      gpuInputs = flattenedGpuInputs,
      cpuExpression = mergedCpuExpr,
      outputDataType = parentBridge.dataType,
      outputNullable = parentBridge.nullable
    )
    mergedExpression
  }
  
  /**
   * Flatten bridge inputs by collecting all non-bridge GPU inputs and creating a mapping
   * from original input positions to final flattened positions. Uses memoization to
   * prevent redundant recursive calls and improve performance with deep nesting.
   * Includes deduplication to reduce memory transfers and code size.
   * 
   * @param inputs The original bridge inputs (mix of bridges and GPU expressions)
   * @return (flattened GPU inputs, mapping from input index to position range in flattened inputs)
   */
  private def flattenBridgeInputs(
      inputs: Seq[Expression]): (Seq[Expression], Map[Int, InputMapping]) = {
    
    // Use identity-based memoization to avoid structural hash/equals walks of nested bridges.
    val flattenCache = new java.util.IdentityHashMap[GpuCpuBridgeExpression,
      (Seq[Expression], Map[Int, InputMapping])]()
    
    def flattenWithCache(inputs: Seq[Expression]): (Seq[Expression], Map[Int, InputMapping]) = {
    val flattenedInputs = scala.collection.mutable.ListBuffer[Expression]()
    val inputMapping = scala.collection.mutable.Map[Int, InputMapping]()
    
    inputs.zipWithIndex.foreach { case (input, originalIndex) =>
      input match {
        case bridge: GpuCpuBridgeExpression =>
            // Check cache first to avoid redundant work
            val cached = flattenCache.get(bridge)
            val (nestedInputs, _) = if (cached != null) {
              cached
            } else {
              val flattened = flattenWithCache(bridge.gpuInputs)
              flattenCache.put(bridge, flattened)
              flattened
            }
            
          val startIndex = flattenedInputs.length
          flattenedInputs ++= nestedInputs
          
          if (nestedInputs.nonEmpty) {
            val endIndex = flattenedInputs.length - 1
              val indices = startIndex to endIndex
              inputMapping(originalIndex) = BridgeInputMapping(bridge, indices)
          } else {
              // Bridge has no GPU inputs - use empty sequence
              inputMapping(originalIndex) = BridgeInputMapping(bridge, Seq.empty)
          }
          
        case gpuExpr =>
          // Regular GPU expression - add directly
          val index = flattenedInputs.length
          flattenedInputs += gpuExpr
          inputMapping(originalIndex) = DirectInputMapping(index)
      }
    }
      
      (flattenedInputs.toSeq, inputMapping.toMap)
    }
    
    val (flattenedInputs, inputMapping) = flattenWithCache(inputs)
    
    // Apply deduplication to the flattened inputs
    val (deduplicatedInputs, deduplicationMapping) = 
      deduplicateFlattenedInputs(flattenedInputs)
    
    // Update input mappings to account for deduplication
    val finalInputMapping = inputMapping.map { case (originalIndex, mapping) =>
      originalIndex -> updateMappingAfterDeduplication(mapping, deduplicationMapping)
    }
    
    (deduplicatedInputs, finalInputMapping)
  }
  
  /**
   * Deduplicate flattened GPU inputs using semantic equality.
   * @param flattenedInputs All flattened GPU inputs (may contain duplicates)
   * @return (deduplicated inputs, mapping from old indices to new indices)
   */
  private def deduplicateFlattenedInputs(
      flattenedInputs: Seq[Expression]): (Seq[Expression], Map[Int, Int]) = {
    
    import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionEquals
    
    val deduplicatedInputs = scala.collection.mutable.ListBuffer[Expression]()
    val seenExpressions = scala.collection.mutable.Map[GpuExpressionEquals, Int]()
    val deduplicationMapping = scala.collection.mutable.Map[Int, Int]()
        
    flattenedInputs.zipWithIndex.foreach { case (expr, oldIndex) =>
      val exprWrapper = GpuExpressionEquals(expr)
      seenExpressions.get(exprWrapper) match {
        case Some(existingIndex) =>
          // This expression is a duplicate - map to existing index
          deduplicationMapping(oldIndex) = existingIndex
        case None =>
          // This is a new unique expression - add it
          val newIndex = deduplicatedInputs.length
          deduplicatedInputs += expr
          seenExpressions(exprWrapper) = newIndex
          deduplicationMapping(oldIndex) = newIndex
      }
    }
    
    (deduplicatedInputs.toSeq, deduplicationMapping.toMap)
  }
  
  /**
   * Update an InputMapping to account for deduplication of the flattened inputs.
   * @param mapping The original mapping
   * @param deduplicationMapping Mapping from old flattened indices to deduplicated indices
   * @return Updated mapping with new indices
   */
  private def updateMappingAfterDeduplication(
      mapping: InputMapping,
      deduplicationMapping: Map[Int, Int]): InputMapping = {
    
    mapping match {
      case DirectInputMapping(oldIndex) =>
        DirectInputMapping(deduplicationMapping(oldIndex))
      case BridgeInputMapping(bridge, oldIndices) =>
        // Map each old index to its new position after deduplication
        val newIndices = oldIndices.map(deduplicationMapping(_))
        BridgeInputMapping(bridge, newIndices)
    }
  }
  
  /**
   * Rewrite a CPU expression by substituting bridge expressions and remapping BoundReferences.
   * 
   * @param cpuExpr The CPU expression to rewrite
   * @param inputMapping Mapping from original input positions to flattened positions
   * @return The rewritten CPU expression with proper BoundReference mappings
   */
  private def rewriteCpuExpression(
      cpuExpr: Expression,
      inputMapping: Map[Int, InputMapping]): Expression = {
    
    cpuExpr.transformUp {
      case BoundReference(ordinal, dataType, nullable) =>
        inputMapping.get(ordinal) match {
          case Some(BridgeInputMapping(bridge, indices)) =>
            // This BoundReference points to a bridge - substitute the bridge's CPU expression
            // and remap its BoundReferences using the index mapping
            remapBoundReferencesWithIndexMapping(bridge.cpuExpression, indices)
            
          case Some(DirectInputMapping(newIndex)) =>
            // This BoundReference points to a direct GPU input - update the index
            BoundReference(newIndex, dataType, nullable)
            
          case None =>
            throw new IllegalStateException(
                s"Invalid BoundReference ordinal $ordinal during bridge merging")
        }
    }
  }
  
  /**
   * Remap all BoundReferences in a CPU expression using an explicit index mapping.
   * Used when substituting a bridge's CPU expression where the indices may be non-contiguous.
   * 
   * @param expr The CPU expression to remap
   * @param indices The mapping from bridge-local ordinals to global flattened indices
   * @return The expression with remapped BoundReferences
   */
  private def remapBoundReferencesWithIndexMapping(
      expr: Expression,
      indices: Seq[Int]): Expression = {
    
    expr.transformUp {
      case BoundReference(ordinal, dataType, nullable) =>
        if (ordinal < 0) {
          throw new IllegalStateException(
            s"BoundReference ordinal $ordinal is negative during bridge merging")
        } else if (ordinal >= indices.length) {
          throw new IllegalStateException(
            s"BoundReference ordinal $ordinal out of bounds for indices $indices " +
            s"(max ordinal: ${indices.length - 1})")
        } else {
          val newIndex = indices(ordinal)
          if (newIndex < 0) {
            throw new IllegalStateException(
              s"Mapped index $newIndex is negative for ordinal $ordinal")
          }
          BoundReference(newIndex, dataType, nullable)
        }
    }
  }
  
  // Helper classes for input mapping
  sealed trait InputMapping
  case class DirectInputMapping(newIndex: Int) extends InputMapping
  case class BridgeInputMapping(bridge: GpuCpuBridgeExpression, 
    indices: Seq[Int]) extends InputMapping
}