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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, AttributeSeq, Cast,
  EqualTo, Expression, LessThan, Literal, StringInstr, Subtract, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rapids.{GpuAdd, GpuContains, GpuLessThan, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.{BooleanType, ByteType, DecimalType, DoubleType, FloatType,
  IntegerType, LongType, ShortType, StringType}

class AstExpressionMetaSuite extends AnyFunSuite {

  private case class UnregisteredUnary(child: Expression)
      extends UnaryExpression with CodegenFallback {
    override def dataType = child.dataType
    override protected def nullSafeEval(input: Any): Any = input
    override protected def withNewChildInternal(newChild: Expression): Expression =
      copy(child = newChild)
  }

  private class CountingUnaryMeta(
      expr: UnregisteredUnary,
      childMeta: BaseExprMeta[_],
      conf: RapidsConf,
      conversionCount: AtomicInteger)
    extends ExprMeta[UnregisteredUnary](
      expr, conf, None, new NoRuleDataFromReplacementRule) {

    override val childExprs: Seq[BaseExprMeta[_]] = Seq(childMeta)

    override def convertToGpuImpl(): GpuExpression = {
      conversionCount.incrementAndGet()
      GpuAlias(childExprs.head.convertToGpu(), "counted")()
    }
  }

  private val conf = new RapidsConf(Map.empty[String, String])
  private val rowIrJitConf = new RapidsConf(Map(
    RapidsConf.ENABLE_PROJECT_AST_ROW_IR.key -> "true"))

  private def countingUnaryMeta(depth: Int, conversionCount: AtomicInteger): BaseExprMeta[_] = {
    val input = AttributeReference("input", IntegerType)()
    val root = (0 until depth).foldLeft[Expression](input) { case (child, _) =>
      UnregisteredUnary(child)
    }

    def wrap(expr: Expression): BaseExprMeta[_] = expr match {
      case unary: UnregisteredUnary =>
        new CountingUnaryMeta(unary, wrap(unary.child), conf, conversionCount)
      case other => GpuOverrides.wrapExpr(other, conf, None)
    }

    wrap(root)
  }

  private def assertMixedOperandTypesCannotUseAst(expr: Expression): Unit = {
    val meta = GpuOverrides.wrapExpr(expr, conf, None)
    meta.tagForGpu()

    assert(!meta.canThisBeAst)
    assert(meta.explainAst(all = true).contains(
      "AST binary expression operand types must match, found IntegerType,LongType"))
  }

  test("Add with mixed operand types cannot use AST") {
    assertMixedOperandTypesCannotUseAst(Add(
      AttributeReference("lhs", IntegerType)(),
      AttributeReference("rhs", LongType)()))
  }

  test("Subtract with mixed operand types cannot use AST") {
    assertMixedOperandTypesCannotUseAst(Subtract(
      AttributeReference("lhs", IntegerType)(),
      AttributeReference("rhs", LongType)()))
  }

  test("safe primitive AST cast whitelist") {
    Seq(
      ByteType -> BooleanType,
      ShortType -> BooleanType,
      IntegerType -> BooleanType,
      LongType -> BooleanType,
      FloatType -> BooleanType,
      DoubleType -> BooleanType,
      BooleanType -> ByteType,
      BooleanType -> ShortType,
      BooleanType -> IntegerType,
      BooleanType -> LongType,
      BooleanType -> FloatType,
      BooleanType -> DoubleType,
      ByteType -> ShortType,
      ByteType -> IntegerType,
      ByteType -> LongType,
      ShortType -> IntegerType,
      ShortType -> LongType,
      IntegerType -> LongType,
      DoubleType -> FloatType,
      FloatType -> DoubleType).foreach { case (from, to) =>
      assert(GpuCast.canCastToAst(from, to), s"Expected $from to $to to be AST-compatible")
    }

    Seq(
      LongType -> IntegerType,
      FloatType -> ByteType,
      FloatType -> ShortType,
      FloatType -> IntegerType,
      FloatType -> LongType,
      DoubleType -> ByteType,
      DoubleType -> ShortType,
      DoubleType -> IntegerType,
      DoubleType -> LongType,
      IntegerType -> FloatType,
      LongType -> FloatType,
      LongType -> DoubleType,
      DecimalType(10, 0) -> BooleanType,
      IntegerType -> DecimalType(10, 0)).foreach { case (from, to) =>
      assert(!GpuCast.canCastToAst(from, to), s"Expected $from to $to to stay out of AST")
    }
  }

  test("row IR JIT cast is not compatible with legacy AST") {
    val cast = Cast(AttributeReference("input", IntegerType)(), LongType)
    val meta = GpuOverrides.wrapExpr(cast, rowIrJitConf, None)
    meta.tagForGpu()

    assert(meta.canThisBeAst)
    assert(!meta.canThisBeLegacyAst)
  }

  test("no-op decimal widening cast is compatible with legacy AST") {
    Seq(
      DecimalType(7, 2) -> DecimalType(9, 2),
      DecimalType(10, 2) -> DecimalType(18, 2),
      DecimalType(19, 2) -> DecimalType(38, 2)).foreach { case (from, to) =>
      val cast = Cast(AttributeReference("input", from)(), to)
      val meta = GpuOverrides.wrapExpr(cast, rowIrJitConf, None)
      meta.tagForGpu()

      assert(meta.canThisBeAst)
      assert(meta.canThisBeLegacyAst)
    }
  }

  test("decimal casts with row IR operations are not compatible with legacy AST") {
    Seq(
      DecimalType(9, 2) -> DecimalType(10, 2),
      DecimalType(7, 2) -> DecimalType(9, 4),
      DecimalType(9, 2) -> DecimalType(7, 2)).foreach { case (from, to) =>
      val cast = Cast(AttributeReference("input", from)(), to)
      val meta = GpuOverrides.wrapExpr(cast, rowIrJitConf, None)
      meta.tagForGpu()

      assert(meta.canThisBeAst)
      assert(!meta.canThisBeLegacyAst)
    }
  }

  test("CPU bridge expression cannot use or be precomputed for legacy join AST") {
    val left = AttributeReference("left_i", IntegerType)()
    val right = AttributeReference("right_i", IntegerType)()
    val bridgeConf = new RapidsConf(Map(
      RapidsConf.ENABLE_CPU_BRIDGE.key -> "true",
      GpuOverrides.expressions(classOf[Add]).confKey -> "false"))
    val meta = GpuOverrides.wrapExpr(
      EqualTo(Add(left, Literal(1)), right), bridgeConf, None)

    meta.tagForGpu()
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(meta))

    val addMeta = meta.childExprs.head
    assert(addMeta.canThisBeReplaced)
    assert(addMeta.willUseGpuCpuBridge)
    assert(addMeta.convertToGpu().isInstanceOf[GpuCpuBridgeExpression])
    assert(!addMeta.canSelfBeLegacyAst)
    assert(!addMeta.canBePrecomputedForJoin)
    assert(meta.canSelfBeLegacyAst)
    assert(!meta.canThisBeLegacyAst)
    assert(!AstUtil.canExtractNonAstConditionIfNeed(
      meta, Seq(left.exprId), Seq(right.exprId)))
  }

  test("legacy AST capability follows CPU bridge state changes") {
    val left = AttributeReference("left_i", IntegerType)()
    val right = AttributeReference("right_i", IntegerType)()
    val bridgeConf = new RapidsConf(Map(RapidsConf.ENABLE_CPU_BRIDGE.key -> "true"))
    val meta = GpuOverrides.wrapExpr(EqualTo(left, right), bridgeConf, None)

    meta.tagForGpu()
    assert(meta.canSelfBeLegacyAst)
    assert(meta.canBePrecomputedForJoin)

    meta.moveToCpuBridge()
    assert(meta.willUseGpuCpuBridge)
    assert(!meta.canSelfBeLegacyAst)
    assert(!meta.canBePrecomputedForJoin)

    meta.undoBridgeOptimization()
    assert(!meta.willUseGpuCpuBridge)
    assert(meta.canSelfBeLegacyAst)
    assert(meta.canBePrecomputedForJoin)
  }

  test("legacy AST capability does not convert an unreplaced descendant") {
    val left = AttributeReference("left_i", IntegerType)()
    val right = AttributeReference("right_i", IntegerType)()
    val meta = GpuOverrides.wrapExpr(EqualTo(left, UnregisteredUnary(right)), conf, None)

    meta.tagForGpu()

    assert(!meta.canExprTreeBeReplaced)
    assert(!AstUtil.canExtractNonAstConditionIfNeed(
      meta, Seq(left.exprId), Seq(right.exprId)))
  }

  test("legacy AST capability converts each subtree once") {
    val conversionCount = new AtomicInteger()
    val depth = 4
    val meta = countingUnaryMeta(depth, conversionCount)
    meta.tagForGpu()

    assert(meta.canThisBeLegacyAst)
    assert(conversionCount.get() == depth)

    meta.convertToGpu()
    assert(conversionCount.get() == depth * 2)
  }

  test("top-down legacy AST extraction check converts each subtree once") {
    val conversionCount = new AtomicInteger()
    val depth = 4
    val meta = countingUnaryMeta(depth, conversionCount)
    meta.tagForGpu()

    assert(AstUtil.canExtractNonAstConditionIfNeed(meta, Seq.empty, Seq.empty))
    assert(conversionCount.get() == depth)

    val attributes = meta.wrapped.asInstanceOf[Expression].references.toSeq
    val (rewritten, leftExprs, rightExprs) = AstUtil.extractNonAstFromJoinCond(
      Some(meta), AttributeSeq(attributes), AttributeSeq(Seq.empty))
    assert(rewritten.exists(_.collect { case _: GpuAlias => 1 }.size == depth))
    assert(leftExprs.isEmpty)
    assert(rightExprs.isEmpty)
    assert(conversionCount.get() == depth)

    meta.convertToGpu()
    assert(conversionCount.get() == depth * 2)
  }

  test("legacy AST rewrite runs conversion with rewritten children") {
    val right = AttributeReference("right_s", StringType)()
    val condition = LessThan(Literal(0), StringInstr(right, Literal("x")))
    val meta = GpuOverrides.wrapExpr(condition, conf, None)
    meta.tagForGpu()

    assert(AstUtil.canExtractNonAstConditionIfNeed(meta, Seq.empty, Seq(right.exprId)))
    val (rewritten, leftExprs, rightExprs) = AstUtil.extractNonAstFromJoinCond(
      Some(meta), AttributeSeq(Seq.empty), AttributeSeq(Seq(right)))

    assert(leftExprs.isEmpty)
    assert(rightExprs.size == 1)
    rewritten.get match {
      case GpuLessThan(_: GpuLiteral, _: AttributeReference) =>
      case other => fail(s"Expected rewritten GpuLessThan but found $other")
    }
    meta.convertToGpu() match {
      case _: GpuContains =>
      case other => fail(s"Expected the original conversion after rewrite but found $other")
    }
  }

  test("TRY integral arithmetic is nullable row IR without side effects") {
    val left = AttributeReference("left_i", IntegerType, nullable = false)()
    val right = AttributeReference("right_i", IntegerType, nullable = false)()
    val expressions = Seq(
      GpuAdd(left, right, failOnError = false, tryMode = true)(),
      GpuSubtract(left, right, failOnError = false, tryMode = true)(),
      GpuMultiply(left, right, failOnError = false, tryMode = true)())

    expressions.foreach { expr =>
      assert(expr.nullable)
      assert(expr.selfUsesRowIrJitAst)
      assert(expr.selfAstJitErrorSite.isEmpty)
      assert(!expr.hasSideEffects)
    }

    assert(GpuCanonicalize.execute(expressions.head)
      .asInstanceOf[GpuAdd].tryMode)
    assert(GpuCanonicalize.execute(expressions.last)
      .asInstanceOf[GpuMultiply].tryMode)
  }
}
