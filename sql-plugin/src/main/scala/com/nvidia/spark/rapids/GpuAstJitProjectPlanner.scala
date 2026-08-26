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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.catalyst.expressions.{
  GpuEquivalentExpressions, GpuExpressionEquals}

/**
 * Splits a Project expression forest at AST JIT/regular GPU boundaries. Expressions on the same
 * dependency frontier remain in the same physical tier so the JIT executor can evaluate all of
 * their roots together. Values referenced across frontiers become tier outputs.
 */
private[rapids] object GpuAstJitProjectPlanner {
  private sealed trait Backend
  private case object AstJit extends Backend
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

  def buildExprTiers(expressions: Seq[Expression], conf: SQLConf): Seq[Seq[Expression]] = {
    val combined = if (RapidsConf.ENABLE_COMBINED_EXPRESSIONS.get(conf)) {
      GpuEquivalentExpressions.replaceMultiExpressions(expressions, conf)
    } else {
      expressions
    }
    new Planner(combined).build()
  }

  private final class Planner(expressions: Seq[Expression]) {
    private val candidatesByExpression =
      mutable.HashMap.empty[GpuExpressionEquals, Candidate]
    private val candidates = mutable.ArrayBuffer.empty[Candidate]

    private def stripAlias(expression: Expression): Expression = expression match {
      case alias: GpuAlias => stripAlias(alias.child)
      case other => other
    }

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

    private def backendOf(expression: Expression): Option[Backend] = stripAlias(expression) match {
      case _: AttributeReference | _: GpuBoundReference | _: GpuLiteral | _: Literal => None
      case gpuExpression: GpuExpression
          if gpuExpression.deterministic &&
            GpuBatchUtils.isFixedWidth(gpuExpression.dataType) &&
            gpuExpression.selfSupportsAstJit && gpuExpression.selfIsAstJitOperator =>
        Some(AstJit)
      case _ => Some(RegularGpu)
    }

    private def canDescend(expression: Expression): Boolean = stripAlias(expression) match {
      case gpuExpression: GpuExpression => !gpuExpression.disableTieredProjectCombine
      case _ => true
    }

    private def canCrossBoundary(expression: Expression, backend: Backend): Boolean = {
      backend != AstJit || (stripAlias(expression) match {
        case gpuExpression: GpuExpression => !gpuExpression.hasSideEffects
        case _ => false
      })
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

      def visit(node: Expression): Unit = {
        val child = stripAlias(node)
        backendOf(child) match {
          case Some(childBackend)
              if childBackend != ownerBackend && canCrossBoundary(child, childBackend) =>
            addDependency(dependencies, candidateFor(child))
          case _ if canDescend(child) => child.children.foreach(visit)
          case _ =>
        }
      }

      stripAlias(expression).children.foreach(visit)
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
          if (canDescend(child)) {
            child.mapChildren(recurse(_, isRoot = false))
          } else {
            child
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
        case (candidate, alias) if candidate.backend == RegularGpu => alias
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
        !GpuAstJitExpression.canUseAstJit(rootChild(rewrittenFinals(index)))
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

      val hasJitCandidate = finalRoots.exists(_.backend.contains(AstJit)) ||
        candidates.exists(_.backend == AstJit)
      if (!hasJitCandidate) {
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
