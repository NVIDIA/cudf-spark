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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.rapids.catalyst.expressions.{
  GpuEquivalentExpressions, GpuExpressionEquals}
import org.apache.spark.sql.types.DataType

/**
 * Splits a Project expression forest at interpreted AST, AST JIT, and regular GPU boundaries.
 * Expressions on the same dependency frontier remain in the same physical tier so JIT can evaluate
 * their roots together. Values referenced across frontiers become tier outputs.
 */
private[rapids] object GpuProjectAstPlanner {
  private sealed trait Backend
  private case object AstJit extends Backend
  private case object AstInterpreted extends Backend
  private case object RegularGpu extends Backend

  private final case class Candidate(
      id: Int,
      expression: Expression,
      backend: Backend,
      depth: Int,
      alias: GpuAlias)

  private final case class FinalRoot(
      expression: Expression,
      child: Expression,
      backend: Option[Backend],
      depth: Int)

  // Driver-side eligibility becomes AST wrappers before executor serialization. Spark copies tags
  // onto rewritten nodes, so only retain eligibility for the same operator/types.
  private case class LegacySignature(
      operator: Class[_], outputType: DataType, inputTypes: Seq[DataType])
  private val legacySupport = TreeNodeTag[LegacySignature]("rapids.project.ast.support")

  private def signature(expression: Expression): LegacySignature =
    LegacySignature(expression.getClass, expression.dataType, expression.children.map(_.dataType))

  def tagLegacySupport(expression: Expression, supported: Boolean): Unit = {
    if (supported) expression.setTagValue(legacySupport, signature(expression))
    else expression.unsetTagValue(legacySupport)
  }

  private def selfSupportsLegacy(expression: Expression): Boolean =
    expression.getTagValue(legacySupport).contains(signature(expression))

  private def stripAlias(expression: Expression): Expression = expression match {
    case alias: GpuAlias => stripAlias(alias.child)
    case other => other
  }

  private def isLeaf(expression: Expression): Boolean = expression match {
    case _: AttributeReference | _: GpuBoundReference | _: GpuLiteral | _: Literal => true
    case _ => false
  }

  private def supportsLegacy(expression: Expression): Boolean = stripAlias(expression) match {
    case leaf if isLeaf(leaf) => true
    case expression => selfSupportsLegacy(expression) &&
      expression.children.forall(supportsLegacy)
  }

  private def isSimpleArithmetic(expression: Expression): Boolean = {
    val child = stripAlias(expression)
    child match {
      case _: GpuAdd | _: GpuSubtract | _: GpuMultiply =>
        child.children.forall(e => isLeaf(stripAlias(e)))
      case _ => false
    }
  }

  private def canUseLegacy(expression: Expression): Boolean = {
    val child = stripAlias(expression)
    val hasWork = child match {
      case literal: GpuLiteral =>
        literal.value != null && selfSupportsLegacy(literal)
      case other => !isLeaf(other)
    }
    hasWork && child.deterministic && GpuBatchUtils.isFixedWidth(child.dataType) &&
      supportsLegacy(child)
  }

  def unwrap(expression: Expression): Expression = expression match {
    case alias: GpuAlias => GpuProjectAstExpressionBase.replaceChild(alias, unwrap(alias.child))
    case ast: GpuProjectAstExpression =>
      // An explicit legacy wrapper already certifies AST conversion of its entire subtree.
      ast.child.foreach(e => tagLegacySupport(e, supported = true))
      ast.child
    case jit: GpuAstJitExpression => jit.child
    case other => other
  }

  def wrapOutputs(
      expressions: Seq[Expression],
      conf: SQLConf,
      enableAst: Boolean,
      enableAstJit: Boolean): Seq[Expression] = {
    val planner = new Planner(Seq.empty, enableAst, enableAstJit)
    val legacy = expressions.map {
      case alias @ GpuAlias(child: GpuExpression, _)
          if enableAst && canUseLegacy(child) && !isSimpleArithmetic(child) &&
            !planner.backendOf(child).contains(AstJit) =>
        GpuProjectAstExpression.wrap(alias)
      case other => other
    }
    if (enableAstJit) GpuAstJitExpression.wrapTierExpressions(legacy, conf) else legacy
  }

  def buildExprTiers(
      expressions: Seq[Expression],
      conf: SQLConf,
      enableAst: Boolean,
      enableAstJit: Boolean): Seq[Seq[Expression]] = {
    val unwrapped = expressions.map(unwrap)
    val combined = if (RapidsConf.ENABLE_COMBINED_EXPRESSIONS.get(conf)) {
      GpuEquivalentExpressions.replaceMultiExpressions(unwrapped, conf)
    } else {
      unwrapped
    }
    if (!enableAst && !enableAstJit) {
      return GpuEquivalentExpressions.getExprTiers(combined)
    }
    new Planner(combined, enableAst, enableAstJit).build()
        .map(wrapOutputs(_, conf, enableAst, enableAstJit))
  }

  private final class Planner(
      expressions: Seq[Expression],
      enableAst: Boolean,
      enableAstJit: Boolean) {
    private val candidatesByExpression =
      mutable.HashMap.empty[GpuExpressionEquals, Candidate]
    private val candidates = mutable.ArrayBuffer.empty[Candidate]

    private def rootChild(expression: Expression): Expression = expression match {
      case alias: GpuAlias => stripAlias(alias.child)
      case other => stripAlias(other)
    }

    private def replaceRootChild(expression: Expression, child: Expression): Expression = {
      expression match {
        case alias: GpuAlias => GpuProjectAstExpressionBase.replaceChild(alias, child)
        case other => other
      }
    }

    def backendOf(expression: Expression): Option[Backend] = stripAlias(expression) match {
      case leaf if isLeaf(leaf) => None
      case gpu: GpuExpression if gpu.deterministic =>
        if (enableAstJit && GpuAstJitExpression.canUseAstJit(gpu)) {
          Some(AstJit)
        } else if (enableAst && canUseLegacy(gpu) && !isSimpleArithmetic(gpu)) {
          Some(AstInterpreted)
        } else if (enableAstJit && gpu.selfSupportsAstJit && gpu.selfIsAstJitOperator &&
            GpuBatchUtils.isFixedWidth(gpu.dataType)) {
          Some(AstJit)
        } else if (enableAst && selfSupportsLegacy(gpu) && !isSimpleArithmetic(gpu) &&
            GpuBatchUtils.isFixedWidth(gpu.dataType)) {
          Some(AstInterpreted)
        } else {
          Some(RegularGpu)
        }
      case _ => Some(RegularGpu)
    }

    private def backendWithin(expression: Expression, owner: Backend): Option[Backend] = {
      val child = stripAlias(expression)
      if (owner == AstInterpreted && canUseLegacy(child)) Some(AstInterpreted)
      else backendOf(child)
    }

    private def childrenToPlan(expression: Expression): Seq[Expression] = {
      stripAlias(expression) match {
        case bridge: GpuCpuBridgeExpression => bridge.gpuInputs
        // Keep conditional evaluation and lambda scopes intact until placement models them.
        case _: GpuIf | _: GpuCaseWhen | _: GpuCoalesce => Seq.empty
        case gpu: GpuExpression if !gpu.disableTieredProjectCombine => gpu.children
        case _ => Seq.empty
      }
    }

    private def canCrossBoundary(expression: Expression, backend: Backend): Boolean = {
      expression.deterministic && (backend == RegularGpu || (expression match {
        case gpu: GpuExpression => !gpu.hasSideEffects
        case _ => false
      }))
    }

    private lazy val sharedAcrossBackends: Set[GpuExpressionEquals] = {
      if (!enableAst || !enableAstJit) {
        Set.empty
      } else {
        // Reuse CSE's maximal shared subtrees rather than materializing every shared descendant.
        val equivalents = new GpuEquivalentExpressions
        expressions.foreach(equivalents.addExprTree(_))
        val common = equivalents.getCommonSubexpressions.map(GpuExpressionEquals(_)).toSet
        val owners = mutable.HashMap.empty[GpuExpressionEquals, mutable.Set[Backend]]

        def visit(expression: Expression, owner: Backend): Unit = {
          val child = stripAlias(expression)
          val key = GpuExpressionEquals(child)
          if (common.contains(key) && !isLeaf(child) && canCrossBoundary(child, owner)) {
            owners.getOrElseUpdate(key, mutable.Set.empty) += owner
          }
          childrenToPlan(child).foreach { nested =>
            val backend = backendWithin(nested, owner).getOrElse(owner)
            val nestedOwner = if (canCrossBoundary(nested, backend)) backend else owner
            visit(nested, nestedOwner)
          }
        }

        expressions.foreach { expression =>
          val child = rootChild(expression)
          backendOf(child).foreach(visit(child, _))
        }
        owners.iterator.collect { case (key, backends) if backends.size > 1 => key }.toSet
      }
    }

    private def addDependency(
        dependencies: mutable.LinkedHashMap[Int, Candidate],
        candidate: Candidate): Unit = {
      dependencies.getOrElseUpdate(candidate.id, candidate)
    }

    private def collectDependencies(
        expression: Expression,
        ownerBackend: Backend): Seq[Candidate] = {
      val dependencies = mutable.LinkedHashMap.empty[Int, Candidate]
      val rootKey = GpuExpressionEquals(stripAlias(expression))

      def visit(node: Expression): Unit = {
        val child = stripAlias(node)
        val key = GpuExpressionEquals(child)
        val shared = key != rootKey && sharedAcrossBackends.contains(key)
        backendWithin(child, ownerBackend) match {
          case Some(childBackend)
              if (shared || childBackend != ownerBackend) &&
                canCrossBoundary(child, childBackend) =>
            addDependency(dependencies, candidateFor(child))
          case _ => childrenToPlan(child).foreach(visit)
        }
      }

      childrenToPlan(expression).foreach(visit)
      dependencies.values.toSeq
    }

    private def candidateFor(expression: Expression): Candidate = {
      val child = stripAlias(expression)
      require(child.deterministic, s"Cannot materialize non-deterministic expression $child")
      val key = GpuExpressionEquals(child)
      candidatesByExpression.getOrElse(key, {
        val backend = backendOf(child).getOrElse(RegularGpu)
        val dependencies = collectDependencies(child, backend)
        val depth = dependencies.map(_.depth + 1).foldLeft(0)(math.max)
        val id = candidates.size
        val alias = GpuAlias(child, s"project_wave_$id")()
        val candidate = Candidate(id, child, backend, depth, alias)
        candidatesByExpression.put(key, candidate)
        candidates += candidate
        candidate
      })
    }

    private def findCandidate(expression: Expression): Option[Candidate] = {
      val child = stripAlias(expression)
      if (child.deterministic) {
        candidatesByExpression.get(GpuExpressionEquals(child))
      } else {
        None
      }
    }

    private def rewrite(expression: Expression, currentDepth: Int): Expression = {
      def recurse(node: Expression, isRoot: Boolean): Expression = {
        val child = stripAlias(node)
        val earlierCandidate = if (isRoot) {
          None
        } else {
          findCandidate(child).filter(_.depth < currentDepth)
        }
        earlierCandidate.map(_.alias.toAttribute).getOrElse {
          child match {
            case bridge: GpuCpuBridgeExpression =>
              bridge.copy(gpuInputs = bridge.gpuInputs.map(recurse(_, isRoot = false)))
            case _ =>
              val children = childrenToPlan(child)
              child.withNewChildren(child.children.map { nested =>
                if (children.exists(_ eq nested)) recurse(nested, isRoot = false) else nested
              })
          }
        }
      }

      recurse(stripAlias(expression), isRoot = true)
    }

    private def regularCseTiers(regularExpressions: Seq[Expression]): Seq[Seq[Expression]] = {
      if (regularExpressions.isEmpty) {
        Seq.empty
      } else {
        GpuEquivalentExpressions.getExprTiers(regularExpressions)
      }
    }

    private def buildCandidateWave(waveCandidates: Seq[Candidate]): Seq[Seq[Expression]] = {
      val rewritten = waveCandidates.map { candidate =>
        val child = rewrite(candidate.expression, candidate.depth)
        candidate -> GpuProjectAstExpressionBase.replaceChild(candidate.alias, child)
      }
      val regularAliases = rewritten.collect {
        case (candidate, alias) if candidate.backend != AstJit => alias
      }
      val jitAliases = rewritten.collect {
        case (candidate, alias) if candidate.backend == AstJit => alias
      }
      val regularTiers = regularCseTiers(regularAliases)
      if (regularTiers.isEmpty) {
        Seq(jitAliases)
      } else {
        regularTiers.dropRight(1) :+ (regularTiers.last ++ jitAliases)
      }
    }

    private def buildFinalTiers(
        finalRoots: Seq[FinalRoot],
        finalProducers: Map[Int, Candidate],
        finalDepth: Int): Seq[Seq[Expression]] = {
      val rewrittenFinals = finalRoots.zipWithIndex.map { case (root, index) =>
        val child = finalProducers.get(index)
            .map(_.alias.toAttribute)
            .getOrElse(rewrite(root.child, finalDepth))
        replaceRootChild(root.expression, child)
      }
      val regularIndexes = rewrittenFinals.indices.filter { index =>
        !backendOf(rootChild(rewrittenFinals(index))).contains(AstJit)
      }
      val regularFinals = regularIndexes.map(rewrittenFinals(_))
      val regularTiers = regularCseTiers(regularFinals)
      if (regularTiers.isEmpty) {
        Seq(rewrittenFinals)
      } else {
        val rewrittenRegularFinals = regularTiers.last.iterator
        val regularIndexSet = regularIndexes.toSet
        val finalTier = rewrittenFinals.indices.map { index =>
          if (regularIndexSet.contains(index)) {
            rewrittenRegularFinals.next()
          } else {
            rewrittenFinals(index)
          }
        }
        regularTiers.dropRight(1) :+ finalTier
      }
    }

    def build(): Seq[Seq[Expression]] = {
      val finalRoots = expressions.map { expression =>
        val child = rootChild(expression)
        val backend = backendOf(child)
        val dependencies = backend.map(collectDependencies(child, _)).getOrElse(Seq.empty)
        val depth = dependencies.map(_.depth + 1).foldLeft(0)(math.max)
        FinalRoot(expression, child, backend, depth)
      }

      val hasAstCandidate = finalRoots.exists(_.backend.exists(_ != RegularGpu)) ||
        candidates.exists(_.backend != RegularGpu)
      if (!hasAstCandidate) {
        return GpuEquivalentExpressions.getExprTiers(expressions)
      }

      val finalDepth = finalRoots.map(_.depth).foldLeft(0)(math.max)
      val finalProducers = mutable.LinkedHashMap.empty[Int, Candidate]
      finalRoots.zipWithIndex.foreach { case (root, index) =>
        findCandidate(root.child).filter(_.depth < finalDepth).foreach { candidate =>
          finalProducers.put(index, candidate)
        }
        if (!finalProducers.contains(index) && root.depth < finalDepth &&
            root.backend.nonEmpty && root.child.deterministic) {
          finalProducers.put(index, candidateFor(root.child))
        }
      }

      val prioritized = finalProducers.values.toSeq.distinct
      val priorityIds = prioritized.map(_.id).toSet
      val orderedCandidates = prioritized ++ candidates.filterNot(c => priorityIds.contains(c.id))
      val candidateTiers = (0 until finalDepth).flatMap { depth =>
        val wave = orderedCandidates.filter(_.depth == depth)
        if (wave.nonEmpty) buildCandidateWave(wave) else Seq.empty
      }
      (candidateTiers ++ buildFinalTiers(finalRoots, finalProducers.toMap, finalDepth))
          .map(_.toList).toList
    }
  }
}
