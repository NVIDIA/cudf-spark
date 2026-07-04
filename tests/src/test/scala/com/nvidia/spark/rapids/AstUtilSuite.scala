/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuAnd, GpuGreaterThan, GpuLength, GpuLessThan, GpuStringTrim}
import org.apache.spark.sql.rapids.execution.GpuHashJoin
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType, StringType}


class AstUtilSuite extends GpuUnitTests {

  private[this] def stubLegacyAstConversions(
      meta: BaseExprMeta[Expression], expression: Expression): Unit = {
    when(meta.convertToGpu).thenReturn(expression)
    when(meta.convertToGpuForLegacyAst()).thenReturn(expression)
    when(meta.convertToGpuForLegacyAstRewrite(any[Seq[Expression]]())).thenAnswer { invocation =>
      val rewrittenChildren = invocation.getArgument[Seq[Expression]](0)
      if (rewrittenChildren.isEmpty) expression else expression.withNewChildren(rewrittenChildren)
    }
  }

  private[this] def testSingleNode(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    val expr = mock(classOf[Expression])
    val attributeSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    when(expr.references).thenReturn(attributeSet)

    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(Seq.empty)
    when(exprMeta.canSelfBeLegacyAst).thenReturn(!containsNonAstAble)
    when(exprMeta.canBePrecomputedForJoin).thenReturn(true)
    when(exprMeta.wrapped).thenReturn(expr)

    AstUtil.canExtractNonAstConditionIfNeed(exprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2).map
    (_.exprId))
  }

  private[this] def testMultiNodes(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    val attributeSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val leftExprMeta = buildLeaf(attributeSet, containsNonAstAble)

    val rightExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rightExprMeta.childExprs).thenReturn(Seq.empty)
    when(rightExprMeta.canSelfBeLegacyAst).thenReturn(true)
    when(rightExprMeta.canBePrecomputedForJoin).thenReturn(true)

    val rootExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootExprMeta.childExprs).thenReturn(Seq(leftExprMeta, rightExprMeta))

    when(rootExprMeta.canSelfBeLegacyAst).thenReturn(true)
    when(rootExprMeta.canBePrecomputedForJoin).thenReturn(true)

    AstUtil.canExtractNonAstConditionIfNeed(rootExprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2)
        .map(_.exprId))
  }

  private[this] def buildLeaf(attributeSet: AttributeSet, containsNonAstAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = mock(classOf[Expression])
    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(Seq.empty)
    when(exprMeta.canSelfBeLegacyAst).thenReturn(!containsNonAstAble)
    when(exprMeta.canBePrecomputedForJoin).thenReturn(true)

    when(expr.references).thenReturn(attributeSet)
    when(exprMeta.wrapped).thenReturn(expr)
    exprMeta
  }

  private[this] def testMultiNodes2(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    // Build left
    val leftAttrSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val leftExprMeta = buildLeaf(leftAttrSet, containsNonAstAble)

    // Build right
    val rightAttrSet = if (!crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val rightExprMeta = buildLeaf(rightAttrSet, containsNonAstAble)

    // Build root
    val rootExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootExprMeta.childExprs).thenReturn(Seq(leftExprMeta, rightExprMeta))
    when(rootExprMeta.canSelfBeLegacyAst).thenReturn(true)
    when(rootExprMeta.canBePrecomputedForJoin).thenReturn(true)

    AstUtil.canExtractNonAstConditionIfNeed(rootExprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2)
        .map(_.exprId))
  }

  test("Single node tree for ast split if needed") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (true, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testSingleNode(containsNonAstAble, crossMultiChildPlan))
    }
  }

  test("Multi-nodes tree for ast split if needed") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (true, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testMultiNodes(containsNonAstAble, crossMultiChildPlan))
    }
  }

  test("Multi-nodes tree for ast split if needed complex case") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (false, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testMultiNodes2(containsNonAstAble, crossMultiChildPlan))
    }
  }

  // ======== test cases for AST split ========
  // Build a simple tree: string_trim(a:string). string_trim's AST-ability is controlled by
  // astAble for different test purposes
  private[this] def buildTree1(attSet: AttributeReference, astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuStringTrim(attSet)
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootMeta.childExprs).thenReturn(Seq.empty)
    when(rootMeta.canSelfBeLegacyAst).thenReturn(astAble)
    when(rootMeta.canBePrecomputedForJoin).thenReturn(true)
    stubLegacyAstConversions(rootMeta, expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  // Build a simple tree: length(string_trim(a:string)). string_length's AST-ability is
  // controlled by astAble for different test purposes
  private[this] def buildTree2(attSet: AttributeReference, astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuLength(GpuStringTrim(attSet))
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    val childExprs = Seq(buildTree1(attSet, astAble))
    when(rootMeta.childExprs).thenReturn(childExprs)
    when(rootMeta.canSelfBeLegacyAst).thenReturn(astAble)
    when(rootMeta.canBePrecomputedForJoin).thenReturn(true)
    stubLegacyAstConversions(rootMeta, expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  // Build a complex tree:
  //   length(trim(a1:string)) > length(trim(a2:string))
  private[this] def buildTree3(attSet1: AttributeReference, attSet2: AttributeReference,
      astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuGreaterThan(GpuLength(GpuStringTrim(attSet1)), GpuLength(GpuStringTrim(attSet2)))
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    val childExprs = Seq(buildTree2(attSet1, astAble), buildTree2(attSet2, astAble))
    when(rootMeta.childExprs).thenReturn(childExprs)
    when(rootMeta.canSelfBeLegacyAst).thenReturn(true)
    when(rootMeta.canBePrecomputedForJoin).thenReturn(true)
    stubLegacyAstConversions(rootMeta, expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  test("Tree of single ast-able node should not split") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree1(l1, true)), Seq(l1), Seq(l2))
    assertResult(true)(l.isEmpty)
    assertResult(true)(r.isEmpty)
    assertResult(true)(e.get.isInstanceOf[GpuStringTrim])
  }

  test("Project pushing down to same child") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l1, false)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 1)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(r.isEmpty)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(checkEquals(e.get, GpuGreaterThan(l(0).toAttribute, l(0).toAttribute)))
  }

  private def realExpr(expr: Expression): Expression = expr match {
    case e: GpuAlias => e.child
    case _ => expr
  }

  private def checkEquals(expr: Expression, other: Expression): Boolean = {
    realExpr(expr).semanticEquals(realExpr(other))
  }

  test("Project pushing down to different childern") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l2, false)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 1)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(r.size == 1)
    assertResult(true)(r.exists(checkEquals(_, GpuLength(GpuStringTrim(l2)))))
    assertResult(true)(
      checkEquals(e.get, GpuGreaterThan(l(0).toAttribute, r(0).toAttribute)))
  }

  test("A tree with multiple ast-able childern should not split") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l2, true)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 0)
    assertResult(true)(r.size == 0)
    assertResult(true)(checkEquals(e.get,
      GpuGreaterThan(GpuLength(GpuStringTrim(l1)), GpuLength(GpuStringTrim(l2)))))
  }

  // Build a simple tree representing: cast(a:int as boolean)
  // This simulates a condition like: SELECT * FROM t1 JOIN t2 ON t1.intCol
  // where intCol is cast to boolean for the join condition
  private[this] def buildTreeWithCast(attr: AttributeReference): BaseExprMeta[Expression] = {
    // Create the mock cast expression (int -> boolean)
    val castExpr = mock(classOf[Expression])
    when(castExpr.references).thenReturn(AttributeSet(Seq(attr)))
    when(castExpr.dataType).thenReturn(BooleanType)
    
    val castMeta = mock(classOf[BaseExprMeta[Expression]])
    when(castMeta.childExprs).thenReturn(Seq.empty)
    when(castMeta.canSelfBeLegacyAst).thenReturn(false) // Cast cannot be legacy AST
    when(castMeta.canBePrecomputedForJoin).thenReturn(true)
    stubLegacyAstConversions(castMeta, castExpr)
    when(castMeta.wrapped).thenReturn(castExpr)
    castMeta
  }

  test("Top-level non-AST expression on single side should be extracted") {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()

    // Test with left side only
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTreeWithCast(l1)), Seq(l1), Seq(r1))

    // Should extract the cast to the left side
    assertResult(1)(leftExprs.size)
    assertResult(0)(rightExprs.size)
    assertResult(true)(expr.isDefined)

     // The returned expression should be an attribute reference (the replacement)
     assertResult(true)(expr.get.isInstanceOf[AttributeReference])
 
     // The attribute should match the one from the left expression alias
     val leftAlias = leftExprs.head.asInstanceOf[GpuAlias]
     val expectedAttr = AttributeReference(leftAlias.name, leftAlias.child.dataType,
       leftAlias.child.nullable, leftAlias.metadata)(leftAlias.exprId, leftAlias.qualifier)
     assertResult(expectedAttr)(expr.get)
  }

  test("Top-level non-AST expression on right side should be extracted to right") {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()
    
    // Test with right side only
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTreeWithCast(r1)), Seq(l1), Seq(r1))
    
    // Should extract the cast to the right side
    assertResult(0)(leftExprs.size)
    assertResult(1)(rightExprs.size)
    
     // The returned expression should be an attribute reference (the replacement)
     assertResult(true)(expr.get.isInstanceOf[AttributeReference])
     
     // The attribute should match the one from the right expression alias
     val rightAlias = rightExprs.head.asInstanceOf[GpuAlias]
     val expectedAttr = AttributeReference(rightAlias.name, rightAlias.child.dataType,
       rightAlias.child.nullable, rightAlias.metadata)(rightAlias.exprId, rightAlias.qualifier)
     assertResult(expectedAttr)(expr.get)
  }

  // ============================================================================
  // Tests for extracting non-AST expressions from equi-join residual conditions.
  // These simulate range predicates where the comparison is AST-able, but a child
  // expression on one join side must be precomputed before the join.
  // ============================================================================

  private[this] def expressionWithReferences(
      dataType: DataType,
      refs: AttributeReference*): Expression = {
    val expr = mock(classOf[Expression])
    when(expr.references).thenReturn(AttributeSet(refs))
    when(expr.dataType).thenReturn(dataType)
    expr
  }

  private[this] def expressionMeta(
      wrapped: Expression,
      canSelfBeLegacyAst: Boolean,
      canBePrecomputedForJoin: Boolean,
      convertToGpu: Option[Expression] = None,
      childExprs: Seq[BaseExprMeta[_]] = Seq.empty): BaseExprMeta[Expression] = {
    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(childExprs)
    when(exprMeta.canSelfBeLegacyAst).thenReturn(canSelfBeLegacyAst)
    when(exprMeta.canBePrecomputedForJoin).thenReturn(canBePrecomputedForJoin)
    val gpuExpr = convertToGpu.getOrElse(wrapped)
    stubLegacyAstConversions(exprMeta, gpuExpr)
    when(exprMeta.wrapped).thenReturn(wrapped)
    exprMeta
  }

  private[this] def attrMeta(attr: AttributeReference): BaseExprMeta[Expression] = {
    expressionMeta(
      expressionWithReferences(attr.dataType, attr),
      canSelfBeLegacyAst = true,
      canBePrecomputedForJoin = true,
      convertToGpu = Some(attr))
  }

  private[this] def nonAstExpressionMeta(
      dataType: DataType,
      refs: AttributeReference*): BaseExprMeta[Expression] = {
    expressionMeta(
      expressionWithReferences(dataType, refs: _*),
      canSelfBeLegacyAst = false,
      canBePrecomputedForJoin = true)
  }

  /**
   * Build a tree representing: left_attr < non_ast_expr(right_attr)
   * The comparison is AST-able, but the right-side child expression is not.
   */
  private[this] def buildComparisonWithRightNonAstExpr(
      leftAttr: AttributeReference,
      rightAttr: AttributeReference,
      comparison: (Expression, Expression) => Expression = GpuLessThan
  ): BaseExprMeta[Expression] = {
    val rightNonAstMeta = nonAstExpressionMeta(LongType, rightAttr)
    val comparisonExpr = comparison(leftAttr, rightNonAstMeta.convertToGpu)
    expressionMeta(
      comparisonExpr,
      canSelfBeLegacyAst = true,
      canBePrecomputedForJoin = true,
      childExprs = Seq(attrMeta(leftAttr), rightNonAstMeta))
  }

  /**
   * Build a compound condition:
   *   (left1 < non_ast_expr(right1)) AND (left2 > non_ast_expr(right2))
   */
  private[this] def buildCompoundJoinConditionWithRightNonAstExprs(
      leftStart: AttributeReference,
      leftEnd: AttributeReference,
      rightStart: AttributeReference,
      rightEnd: AttributeReference): BaseExprMeta[Expression] = {
    val ltMeta = buildComparisonWithRightNonAstExpr(leftStart, rightEnd)
    val gtMeta = buildComparisonWithRightNonAstExpr(leftEnd, rightStart, GpuGreaterThan)
    expressionMeta(
      GpuAnd(ltMeta.convertToGpu, gtMeta.convertToGpu),
      canSelfBeLegacyAst = true,
      canBePrecomputedForJoin = true,
      childExprs = Seq(ltMeta, gtMeta))
  }

  private[this] case class JoinConditionTestInput(
      conditionMeta: BaseExprMeta[Expression],
      leftAttrs: Seq[AttributeReference],
      rightAttrs: Seq[AttributeReference]) {
    def canExtract: Boolean = {
      AstUtil.canExtractNonAstConditionIfNeed(
        conditionMeta, leftAttrs.map(_.exprId), rightAttrs.map(_.exprId))
    }

    def extract = {
      AstUtil.extractNonAstFromJoinCond(Some(conditionMeta), leftAttrs, rightAttrs)
    }
  }

  private[this] def singleRightNonAstInput(): JoinConditionTestInput = {
    val lStart = AttributeReference("range_start", LongType)()
    val rEnd = AttributeReference("b_end", IntegerType)()
    JoinConditionTestInput(
      buildComparisonWithRightNonAstExpr(lStart, rEnd),
      Seq(lStart),
      Seq(rEnd))
  }

  private[this] def compoundRightNonAstInput(): JoinConditionTestInput = {
    val lStart = AttributeReference("range_start", LongType)()
    val lEnd = AttributeReference("range_end", LongType)()
    val rStart = AttributeReference("b_start", IntegerType)()
    val rEnd = AttributeReference("b_end", IntegerType)()
    JoinConditionTestInput(
      buildCompoundJoinConditionWithRightNonAstExprs(lStart, lEnd, rStart, rEnd),
      Seq(lStart, lEnd),
      Seq(rStart, rEnd))
  }

  private[this] def bothSidesNonAstInput(): JoinConditionTestInput = {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()
    JoinConditionTestInput(nonAstExpressionMeta(LongType, l1, r1), Seq(l1), Seq(r1))
  }

  test("Equi-join pattern: canExtractNonAstConditionIfNeed with non-AST single side") {
    // Simulates: a.range_start < non_ast_expr(b.b_end)
    // The non-AST expression references only the right side, so it is extractable.
    assertResult(true)(singleRightNonAstInput().canExtract)
  }

  test("Equi-join pattern: extractNonAstFromJoinCond extracts right-side expression") {
    // Simulates: a.range_start < non_ast_expr(b.b_end)
    val (expr, leftExprs, rightExprs) = singleRightNonAstInput().extract

    // The non-AST child should be extracted to the right side.
    assertResult(0)(leftExprs.size)
    assertResult(1)(rightExprs.size)
    assertResult(true)(expr.isDefined)
    // Rewritten condition should be a LessThan with left unchanged, right replaced
    assertResult(true)(expr.get.isInstanceOf[GpuLessThan])
  }

  test("Equi-join pattern: compound range condition with right-side non-AST expressions") {
    // Simulates:
    //   a.range_start < non_ast_expr(b.b_end)
    //     AND a.range_end > non_ast_expr(b.b_start)
    // Both non-AST expressions reference only the right side.
    assertResult(true)(compoundRightNonAstInput().canExtract)
  }

  test("Equi-join pattern: full range condition extraction produces right-side projections") {
    val (expr, leftExprs, rightExprs) = compoundRightNonAstInput().extract

    // Both non-AST expressions are on right-side attributes.
    assertResult(0)(leftExprs.size)
    assertResult(2)(rightExprs.size)
    assertResult(true)(expr.isDefined)
    // Rewritten condition should be an AND expression
    assertResult(true)(expr.get.isInstanceOf[GpuAnd])
  }

  test("Equi-join pattern: non-AST expression referencing both sides is NOT extractable") {
    // Simulates: non_ast_expr(a.col, b.col)
    // Cannot extract because the non-AST expression references both sides.
    assertResult(false)(bothSidesNonAstInput().canExtract)
  }

  test("Equi-join pattern: non-legacy-AST expression must be precomputable") {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()
    val conditionMeta = expressionMeta(
      expressionWithReferences(LongType, l1),
      canSelfBeLegacyAst = false,
      canBePrecomputedForJoin = false)

    val input = JoinConditionTestInput(conditionMeta, Seq(l1), Seq(r1))
    assertResult(false)(input.canExtract)
  }

  test("Equi-join pattern: fully AST-able condition needs no extraction") {
    // When all expressions are AST-able, canExtractNonAstConditionIfNeed returns true
    // but extractNonAstFromJoinCond should produce empty left/right lists
    val l1 = AttributeReference("l1", StringType)()
    val r1 = AttributeReference("r1", StringType)()

    val input = JoinConditionTestInput(buildTree3(l1, r1, true), Seq(l1), Seq(r1))
    val (e, l, r) = input.extract
    // No extraction needed — all AST-able
    assertResult(0)(l.size)
    assertResult(0)(r.size)
    assertResult(true)(e.isDefined)
  }

  test("Join-condition extraction does not convert a condition that will be rewritten") {
    val leftAttr = AttributeReference("l1", LongType)()
    val rightAttr = AttributeReference("r1", LongType)()
    val condition = GpuLessThan(leftAttr, GpuLiteral(1L, LongType))
    val conditionMeta = expressionMeta(
      condition,
      canSelfBeLegacyAst = false,
      canBePrecomputedForJoin = true)
    when(conditionMeta.canThisBeLegacyAst).thenReturn(false)

    val left = mock(classOf[SparkPlan])
    val right = mock(classOf[SparkPlan])
    when(left.output).thenReturn(Seq(leftAttr))
    when(right.output).thenReturn(Seq(rightAttr))

    val extracted = GpuHashJoin.extractJoinConditionIfNeeded(
      Some(conditionMeta), Inner, left, right)

    assert(extracted.joinCondition.nonEmpty)
    assert(extracted.filterCondition.isEmpty)
    assert(extracted.left.isInstanceOf[GpuProjectExec])
    verify(conditionMeta, never()).convertToGpu()
    verify(conditionMeta).convertToGpuForLegacyAst()
  }
}
