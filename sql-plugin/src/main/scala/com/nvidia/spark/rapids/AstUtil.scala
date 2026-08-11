/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionEquals


object AstUtil {

  /**
   * Check whether it can be split into non-ast sub-expression if needed
   *
   * @return true when: 1) If all ast-able in expr; 2) all non-ast-able tree nodes don't contain
   *         attributes from both join sides. In such case, it's not able
   *         to push down into single child.
   */
  def canExtractNonAstConditionIfNeed(expr: BaseExprMeta[_], left: Seq[ExprId],
      right: Seq[ExprId]): Boolean = {
    if (!expr.canSelfBeAst) {
      // This expression cannot be AST. We will extract the entire sub-tree (this expression
      // and all its children). Check if this entire sub-tree only uses one side of the join.
      val exprRef = expr.wrapped.asInstanceOf[Expression]
      val hasLeft = exprRef.references.exists(r => left.contains(r.exprId))
      val hasRight = exprRef.references.exists(r => right.contains(r.exprId))
      // Can extract if it doesn't use both sides (entire sub-tree will be extracted)
      !(hasLeft && hasRight)
    } else {
      // This node is AST-able, so recursively check all children
      expr.childExprs.isEmpty || expr.childExprs.forall(
        canExtractNonAstConditionIfNeed(_, left, right))
    }
  }

  /**
   * Extract non-AST functions from join conditions and update the original join condition. Based
   * on the attributes, it decides which side the split condition belongs to. The replaced
   * condition is wrapped with GpuAlias with new intermediate attributes. It is assumed that
   * `canExtractNonAstConditionIfNeed` was already called and returned true.
   *
   * @param condition to be split if needed
   * @param left attributions from left child
   * @param right attributions from right child
   * @return a tuple of [[Expression]] for remained expressions, List of [[NamedExpression]] for
   *         left child if any, List of [[NamedExpression]] for right child if any
   */
  def extractNonAstFromJoinCond(condition: Option[BaseExprMeta[_]],
                                left: AttributeSeq, right: AttributeSeq):
  (Option[Expression], List[NamedExpression], List[NamedExpression]) = {
    
    condition match {
      case None => (None, List.empty, List.empty)
      case Some(cond) =>
        // List of expression pushing down to left side child
        val leftExprs: ListBuffer[NamedExpression] = ListBuffer.empty
        // List of expression pushing down to right side child
        val rightExprs: ListBuffer[NamedExpression] = ListBuffer.empty
        // Deduplication map to avoid processing the same expression multiple times
        val processed = mutable.HashMap.empty[GpuExpressionEquals, Expression]

        val leftExprIds = left.attrs.map(_.exprId).toSet
        val rightExprIds = right.attrs.map(_.exprId).toSet

        // Extract and convert in a single pass
        val updatedCondition = extractAndConvert(cond, leftExprIds, rightExprIds,
          leftExprs, rightExprs, processed)

        (Some(updatedCondition), leftExprs.toList, rightExprs.toList)
    }
  }

  /**
   * Recursively extract non-AST expressions and convert to GPU in a single pass.
   * 
   * @param expr the expression to process
   * @param leftExprIds expression IDs from the left side
   * @param rightExprIds expression IDs from the right side
   * @param leftExprs buffer to collect expressions for left child
   * @param rightExprs buffer to collect expressions for right child
   * @param processed map to avoid processing duplicates
   * @return the converted GPU expression with non-AST sub-trees replaced
   */
  private[this] def extractAndConvert(
      expr: BaseExprMeta[_],
      leftExprIds: Set[ExprId],
      rightExprIds: Set[ExprId],
      leftExprs: ListBuffer[NamedExpression],
      rightExprs: ListBuffer[NamedExpression],
      processed: mutable.HashMap[GpuExpressionEquals, Expression]): Expression = {
    if (!expr.canSelfBeAst) {
      // This expression cannot be converted to AST - extract the entire sub-tree
      val exprRef = expr.wrapped.asInstanceOf[Expression]
      val gpuExpr = expr.convertToGpu()
      
      // Check if we've already processed this expression (for deduplication)
      processed.get(GpuExpressionEquals(gpuExpr)) match {
        case Some(replacement) => 
          replacement
        case None =>
          // Determine which side this expression belongs to based on its references
          val referencedExprIds = exprRef.references.map(_.exprId).toSet
          val referencesLeft = referencedExprIds.exists(leftExprIds.contains)
          val referencesRight = referencedExprIds.exists(rightExprIds.contains)
          
          // Create an alias and add to appropriate side
          // Note: if it references both sides or neither, it shouldn't happen if 
          // canExtractNonAstConditionIfNeed passed, but we'll default to left
          val alias = if (referencesRight && !referencesLeft) {
            val a = GpuAlias(gpuExpr, s"_agpu_non_ast_r_${rightExprs.size}")()
            rightExprs += a
            a
          } else {
            val a = GpuAlias(gpuExpr, s"_agpu_non_ast_l_${leftExprs.size}")()
            leftExprs += a
            a
          }
          
          // Create an AttributeReference explicitly to avoid issues with unresolved aliases
          val attributeRef = AttributeReference(alias.name, gpuExpr.dataType, 
            gpuExpr.nullable, alias.metadata)(alias.exprId, alias.qualifier)
          processed.put(GpuExpressionEquals(gpuExpr), attributeRef)
          attributeRef
      }
    } else {
      // This expression can be converted to AST
      // Recursively process children, then convert this node to GPU
      val convertedChildren = expr.childExprs.map { child =>
        extractAndConvert(child, leftExprIds, rightExprIds, leftExprs, rightExprs, processed)
      }
      
      // Convert to GPU and replace children with the processed versions
      val gpuExpr = expr.convertToGpu()
      if (convertedChildren.isEmpty) {
        gpuExpr
      } else {
        gpuExpr.withNewChildren(convertedChildren)
      }
    }
  }
}
