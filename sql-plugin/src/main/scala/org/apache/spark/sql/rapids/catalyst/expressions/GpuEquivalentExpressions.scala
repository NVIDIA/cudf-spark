/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

/* Note: This is derived from EquivalentExpressions in Apache Spark
 * with a lot of changes to adapt it for GPU.
 */
package org.apache.spark.sql.rapids.catalyst.expressions

import scala.annotation.tailrec
import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuAlias, GpuCaseWhen, GpuCoalesce, GpuCpuBridgeExpression, GpuExpression, GpuIf, GpuLeafExpression, GpuUnevaluable, RapidsConf}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, LeafExpression, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.internal.SQLConf

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class GpuEquivalentExpressions {
  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[GpuExpressionEquals, GpuExpressionStats]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    addExprToMap(expr, equivalenceMap)
  }

  private def addExprToMap(
      expr: Expression, map: mutable.HashMap[GpuExpressionEquals, GpuExpressionStats]): Boolean = {
    if (expr.deterministic) {
      val wrapper = GpuExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>
          stats.useCount += 1
          true
        case _ =>
          map.put(wrapper, new GpuExpressionStats(expr))
          false
      }
    } else {
      false
    }
  }

  /**
   * Adds only expressions which are common in each of given expressions, in a recursive way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`,
   * the common expression `(c + 1)` will be added into `equivalenceMap`.
   *
   * Note that as we don't know in advance if any child node of an expression will be common
   * across all given expressions, we count all child nodes when looking through the given
   * expressions. But when we call `addExprTree` to add common expressions into the map, we
   * will add recursively the child nodes. So we need to filter the child expressions first.
   * For example, if `((a + b) + c)` and `(a + b)` are common expressions, we only add
   * `((a + b) + c)`.
   */
  private def addCommonExprs(
      exprs: Seq[Expression],
      map: mutable.HashMap[GpuExpressionEquals, GpuExpressionStats]): Unit = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[GpuExpressionEquals, GpuExpressionStats]
    addExprTree(exprs.head, localEquivalenceMap)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[GpuExpressionEquals, GpuExpressionStats]
      addExprTree(expr, otherLocalEquivalenceMap)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    localEquivalenceMap.foreach { case (commonExpr, state) =>
      val possibleParents = localEquivalenceMap.filter { case (_, v) => v.height > state.height }
      val notChild = possibleParents.forall { case (k, _) =>
        k == commonExpr || k.e.find(_.semanticEquals(commonExpr.e)).isEmpty
      }
      if (notChild) {
        // If the `commonExpr` already appears in the equivalence map, calling `addExprTree` will
        // increase the `useCount` and mark it as a common subexpression. Otherwise, `addExprTree`
        // will recursively add `commonExpr` and its descendant to the equivalence map, in case
        // they also appear in other places. For example, `If(a + b > 1, a + b + c, a + b + c)`,
        // `a + b` also appears in the condition and should be treated as common subexpression.
        addExprTree(commonExpr.e, map)
      }
    }
  }

  private def hasSideEffects(e: Expression): Boolean = e match {
    case e: GpuExpression => e.hasSideEffects
    case _: AttributeReference => false // This is what we expect to see and it is okay
    case _ => true // Just assume that it does because we don't know
  }

  /**
   * This gets the child expressions that should be looked at for deduplication.
   * There are some special expressions that we should not recurse into all of its children.
   * Really this comes down to conditionals with side effects. This is because a conditional
   * on the CPU will not execute all of the paths and then pick the final answer out of the results.
   * So if a conditional expression GpuIf, GpuCaseWhen, and GpuCoalesce show up we can only
   * deduplicate expressions that are unconditionally executed. This is typically the first
   * predicate, or the first expression for GpuCoalesce. We also skip CodegenFallback
   * but we don't use it on the GPU so it is kind of unneeded.
   */
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil
    case bridge: GpuCpuBridgeExpression =>
      // Skip the CPU expressions we don't want to try and combine them
      bridge.gpuInputs
    case i: GpuIf =>
      if (hasSideEffects(i.trueExpr) || hasSideEffects(i.falseExpr)) {
        i.predicateExpr :: Nil
      } else {
        i.children
      }
    case c: GpuCaseWhen =>
      if (c.hasSideEffects) {
        c.children.head :: Nil
      } else {
        c.children
      }
    case c: GpuCoalesce =>
      if (c.children.exists(hasSideEffects)) {
        c.children.head :: Nil
      } else {
        c.children
      }
    case other => other.children
  }

  /**
   * This gets child expressions that should be looked at for deduplication, but
   * not in the same way as `childrenToRecurse` does. `childrenToRecurse` will
   * deduplicate expressions no matter where they show up. But this will only
   * deduplicate expressions if they appear in every possible path. For example
   * if we have a `GpuIf(x > y, a + 5, 100 DIV a)` and we are in ANSI mode.
   * in ANSI mode + can overflow and throw an exception so we cannot unconditionally
   * execute a + 5. Also `100 DIV a` could throw an exception if `a` is 0. So we cannot
   * unconditionally execute either of them until we know if x > y for each row.
   * But if we have something like `GpuIf(x > y, a + 5, (a + 5) DIV y)` the `a + 5`
   * will execute in all cases no matter that value of `x > y`. In those cases we can
   * deduplicate them.
   *
   * This will return zero or more groups of expressions where we can only
   * deduplicate an expression in that group if it is guaranteed to be executed in
   * all cases.
   */
  private def commonChildrenToRecurse(expr: Expression): Seq[Seq[Expression]] = expr match {
    case _: CodegenFallback => Nil
    case i: GpuIf =>
      if (hasSideEffects(i.trueExpr) || hasSideEffects(i.falseExpr)) {
        Seq(Seq(i.trueExpr, i.falseExpr))
      } else {
        Nil
      }
    case c: GpuCaseWhen =>
      if (c.hasSideEffects) {
        // For case-when predicates and values are checked separately
        // This is because the first predicate is guaranteed to execute,
        // but we are not guaranteed to have the first value run. Also
        // it is not uncommon to do something like
        // CASE
        //   WHEN some.foo <= 100 THEN X
        //   WHEN some.foo <= 200 THEN Y
        //   ELSE Z
        // END
        // In this case we can deduplicate `some.foo`, even if it never appears
        // in any value clauses
        val conditions = if (c.branches.length > 1) {
          c.branches.map(_._1)
        } else {
          // If there is only one branch, the first condition is already covered by
          // `childrenToRecurse` and we can ignore it
          Nil
        }
        // For an expression to be in all branch values of a CaseWhen statement, it must also
        // be in the elseValue, because the default else value is just a null literal
        val values = if (c.elseValue.nonEmpty) {
          c.branches.map(_._2) ++ c.elseValue
        } else {
          Nil
        }
        Seq(conditions, values)
      } else {
        Nil
      }
    // If there is only one child, the first child is already covered by
    // `childrenToRecurse` and we should exclude it here.
    case c: GpuCoalesce if c.children.length > 1 =>
      if (c.children.exists(hasSideEffects)) {
        // We cannot strip off the first child because it is the only one guaranteed to execute
        Seq(c.children)
      } else {
        Nil
      }
    case _ => Nil
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[GpuExpressionEquals, GpuExpressionStats] = equivalenceMap): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      expr.isInstanceOf[GpuLeafExpression] ||
      expr.isInstanceOf[GpuUnevaluable] ||
      (expr.isInstanceOf[GpuExpression] &&
          expr.asInstanceOf[GpuExpression].disableTieredProjectCombine) ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (expr.find(_.isInstanceOf[PlanExpression[_]]).isDefined && TaskContext.get != null)

    if (!skip && !addExprToMap(expr, map)) {
      childrenToRecurse(expr).foreach(addExprTree(_, map))
      commonChildrenToRecurse(expr).filter(_.length > 1).foreach(addCommonExprs(_, map))
    }
  }

  /**
   * Returns the state of the given expression in the `equivalenceMap`. Returns None if there is no
   * equivalent expressions.
   * Exposed for testing.
   */
  private[sql] def getExprState(e: Expression): Option[GpuExpressionStats] = {
    equivalenceMap.get(GpuExpressionEquals(e))
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(count: Int = 0): Seq[GpuExpressionStats] = {
    equivalenceMap.values.filter(_.useCount > count).toSeq.sortBy(_.height)
  }

  /**
   * Returns a sequence of expressions that more than one equivalent expressions.
   */
  def getCommonSubexpressions: Seq[Expression] = {
    getAllExprStates(1).map(_.expr)
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb = new java.lang.StringBuilder()
    sb.append("GPU Equivalent expressions:\n")
    equivalenceMap.values.filter(stats => all || stats.useCount > 1).foreach { stats =>
      sb.append("  ").append(s"${stats.expr}: useCount = ${stats.useCount}").append('\n')
    }
    sb.toString()
  }
}

object GpuEquivalentExpressions {
  /**
   * Recursively replaces semantic equal expression with its proxy expression in `substitutionMap`.
   */
  def replaceWithSemanticCommonRef(
      expr: Expression,
      substitutionMap: mutable.HashMap[GpuExpressionEquals, Expression]): Expression = {
    expr match {
      case e: AttributeReference => e
      case _ =>
        substitutionMap.get(GpuExpressionEquals(expr)) match {
          case Some(attr) => attr
          case None => expr.mapChildren(replaceWithSemanticCommonRef(_, substitutionMap))
        }
    }
  }

  /**
   * Recursively replaces expression with its proxy expression in `substitutionMap`.
   */
  private def replaceWithCommonRef(
      expr: Expression,
      substitutionMap: mutable.HashMap[Expression, Expression]): Expression = {
    expr match {
      case e: AttributeReference => e
      case _ =>
        substitutionMap.get(expr) match {
          case Some(attr) => attr
          case None => expr.mapChildren(replaceWithCommonRef(_, substitutionMap))
        }
    }
  }

  /**
   * Recursively calls getCommonSubexpressions to create tiers
   * of expressions, where earlier tiers contain subexpressions
   * for later tiers.
   */
  @tailrec
  private def recurseCommonExpressions(exprs: Seq[Expression],
      exprTiers: Seq[Seq[Expression]]): Seq[Seq[Expression]] = {
    val equivalentExpressions = new GpuEquivalentExpressions
    exprs.foreach(equivalentExpressions.addExprTree(_))
    val commonExprs = equivalentExpressions.getCommonSubexpressions
    if (commonExprs.isEmpty) {
      exprTiers
    } else {
      recurseCommonExpressions(commonExprs, (Seq(commonExprs) ++ exprTiers))
    }
  }

  /**
   * Applies substitutions to all expression tiers.
   */
  private def doSubstitutions(exprTiers: Seq[Seq[Expression]], currentTier: Seq[Expression],
      substitutionMap: mutable.HashMap[Expression, Expression]): Seq[Seq[Expression]] = {
    // Make substitutions in given tiers, filtering out matches from original current tier,
    // but don't filter the last tier - it needs to match original size
    val subTiers = exprTiers.dropRight(1)
    val lastTier = exprTiers.last
    val updatedSubTiers = subTiers.map {
      t => t.filter(e => !currentTier.contains(e)).map(replaceWithCommonRef(_, substitutionMap))
    }
    val updatedLastTier = lastTier.map(replaceWithCommonRef(_, substitutionMap))
    updatedSubTiers ++ Seq(updatedLastTier)
  }

  /**
   * Apply subexpression substitutions to all tiers.
   */
  @tailrec
  private def recurseUpdateTiers(exprTiers: Seq[Seq[Expression]],
      updatedTiers: Seq[Seq[Expression]],
      substitutionMap: mutable.HashMap[Expression, Expression],
      startIndex: Int): Seq[Seq[Expression]] = {
    exprTiers match {
      case Nil => updatedTiers
      case tier :: tail => {
        // Last tier should already be updated.
        if (tail.isEmpty) {
          updatedTiers ++ Seq(tier)
        } else {
          // Replace expressions in this tier with GpuAlias
          val aliasedTier = tier.zipWithIndex.map {
            case (e, i) =>
              GpuAlias(e, s"tiered_input_${startIndex + i}")()
          }
          // Add them to the map
          tier.zip(aliasedTier).foreach {
            case (expr, alias) => {
              substitutionMap.get(expr) match {
                case None => substitutionMap.put(expr, alias.toAttribute)
                case Some(_) =>
              }
            }
          }
          val newUpdatedTiers = doSubstitutions(tail, tier, substitutionMap)
          recurseUpdateTiers(newUpdatedTiers, updatedTiers ++ Seq(aliasedTier),
            substitutionMap, startIndex + aliasedTier.size)
        }
      }
    }
  }

  /**
   * This takes a set of expressions and finds all multi-expressions that can be replaced
   * and replaces them.
   */
  def replaceMultiExpressions(exprs: Seq[Expression], conf: SQLConf): Seq[Expression] = {
    // GpuEquivalentExpressions is really find all expressions that will execute
    // unconditionally, or at least it will be. This gives us the ability to
    // know that if we combine expressions into multi-expressions then we will
    // not accidentally cause side effects to happen that should have been hidden
    // by a conditional expression. It also guarantees that the dedupe code
    // will combine the multi-expressions so we don't run them multiple times.
    val equivalentExpressions = new GpuEquivalentExpressions
    exprs.foreach(equivalentExpressions.addExprTree(_))
    val combinableMap = mutable.HashMap.empty[GpuExpressionCombiner, GpuExpressionCombiner]
    val enabled = mutable.HashMap.empty[Class[_], Boolean]
    def isEnabled(clazz: Class[_]): Boolean = {
      enabled.getOrElse(clazz, {
        val confKey = RapidsConf.ENABLE_COMBINED_EXPR_PREFIX + clazz.getSimpleName
        val isEnabled = conf.getConfString(confKey, "true").trim.toBoolean
        enabled.put(clazz, isEnabled)
        isEnabled
      })
    }
    equivalentExpressions.equivalenceMap.values.map(_.expr).foreach {
      case e: GpuCombinable if isEnabled(e.getClass) =>
        val key = e.getCombiner()
        combinableMap.get(key).map { c =>
          c.addExpression(e)
        }.getOrElse {
          combinableMap.put(key, key)
        }
      case _ => //Noop
    }
    val filtered = combinableMap.filter {
      case (_, v) => v.useCount > 1
    }

    // We now have a set of values that should be replaced
    if (filtered.isEmpty) {
      exprs
    } else {
      exprs.map { expr =>
        expr.transform {
          case c: GpuCombinable if filtered.contains(c.getCombiner()) =>
            val replacement = filtered(c.getCombiner()).getReplacementExpression(c)
            replacement.getOrElse(c)
        }
      }
    }
  }

  def getExprTiers(expressions: Seq[Expression]): Seq[Seq[Expression]] = {
    // Get tiers of common expressions
    val expressionTiers = recurseCommonExpressions(expressions, Seq(expressions))
    val substitutionMap = mutable.HashMap.empty[Expression, Expression]
    // Update expression with common expressions from previous tiers
    recurseUpdateTiers(expressionTiers, Seq.empty, substitutionMap, 0)
  }

  // Determine which of the inputAttrs are needed for remaining tiers
  // Filter the inputAttrs using this set to determine which ones
  // we need for the next tier, and to maintain the ordering.
  // Exposed for testing.
  private[sql] def getAttrsForNextTier(inputAttrs: Seq[Attribute],
      exprTiers: Seq[Seq[Expression]]): Seq[Attribute] = {
    val needAttrs = exprTiers.tail match {
      case Nil => AttributeSet.empty
      case _ => AttributeSet(exprTiers.tail.flatten)
    }
    val curAttrs = exprTiers.head.filter(e => e.isInstanceOf[GpuAlias]).
        map(_.asInstanceOf[GpuAlias].toAttribute)
    (inputAttrs ++ curAttrs).filter(a => needAttrs.contains(a))
  }

  // Given expression tiers as created by getExprTiers and a set of input attributes,
  // return the tiers of input attributes that correspond with the expression tiers.
  def getInputTiers(exprTiers: Seq[Seq[Expression]], inputAttrs: AttributeSeq):
  Seq[AttributeSeq] = {
    @tailrec
    def recurse(exprs: Seq[Seq[Expression]], inputs: AttributeSeq,
        attrTiers: Seq[AttributeSeq]): Seq[AttributeSeq] = exprs match {
      case Nil => attrTiers
      case _ :: tail =>
        val nextAttrs = getAttrsForNextTier(inputs.attrs, exprs)
        recurse(tail, AttributeSeq(nextAttrs), attrTiers ++ Seq(inputs))
    }
    recurse(exprTiers, inputAttrs, Seq.empty)
  }
}

trait GpuExpressionCombiner {
  /**
   * Get a hash code that can be used to find other ExpressionCombiner instances
   * that could be combined together.
   */
  def hashCode: Int

  /**
   * Check to see if this ExpressionCombiner could be combined with another one.
   */
  def equals(o: Any): Boolean

  /**
   * Add another expression to this combiner. Note that equals must have returned true
   * already.
   */
  def addExpression(e: Expression): Unit

  /**
   * For a specific expression return a multi-expression that can be used to replace it.
   * Note that deduplication of these will happen as a part of tiered project.
   * @param e the expression to be replaced
   * @return the replacement expression, if one is available. It is possible for
   *         an expression to use the same combiner, but not be added to the combiner
   *         for numerous reasons. When this happens a None will be returned and
   *         the expression should not be replaced.
   */
  def getReplacementExpression(e: Expression): Option[Expression]

  /**
   * Get the number of expressions that can be combined (excluding duplicates)
   */
  def useCount: Int
}

trait GpuCombinable extends GpuExpression {
  /**
   * Get a combiner that can be used to find candidates to combine
   */
  def getCombiner(): GpuExpressionCombiner
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
case class GpuExpressionEquals(e: Expression) {
  override def equals(o: Any): Boolean = o match {
    case other: GpuExpressionEquals => e.semanticEquals(other.e)
    case _ => false
  }

  override def hashCode: Int = e.semanticHash()
}

/**
 * A wrapper in place of using Seq[Expression] to record a group of equivalent expressions.
 *
 * This saves a lot of memory when there are a lot of expressions in a same equivalence group.
 * Instead of appending to a mutable list/buffer of Expressions, just update the "flattened"
 * useCount in this wrapper in-place.
 */
class GpuExpressionStats(val expr: Expression) {
  var useCount: Int = 1
  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  lazy val height: Int = getHeight(expr)

  private def getHeight(tree: Expression): Int = {
    tree.children.map(getHeight).reduceOption(_ max _).getOrElse(0) + 1
  }
}
