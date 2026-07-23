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

import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, IntegerType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuArrayHofFusionSuite extends GpuUnitTests {
  private val arrayArg =
    GpuBoundReference(0, ArrayType(IntegerType, containsNull = true),
      nullable = true)(ExprId(0), "a")
  private val otherArrayArg =
    GpuBoundReference(1, ArrayType(IntegerType, containsNull = true),
      nullable = true)(ExprId(1), "other")
  private val outerB =
    GpuBoundReference(2, IntegerType, nullable = true)(ExprId(2), "b")
  private val outerC =
    GpuBoundReference(3, IntegerType, nullable = true)(ExprId(3), "c")

  private case class NonDeterministicExpr() extends GpuLeafExpression {
    override lazy val deterministic: Boolean = false
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      throw new UnsupportedOperationException("test-only expression")
  }

  private case class SideEffectExpr() extends GpuLeafExpression {
    override def hasSideEffects: Boolean = true
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      throw new UnsupportedOperationException("test-only expression")
  }

  private def lambda(resultType: DataType, argCount: Int = 1): GpuLambdaFunction = {
    val args = (0 until argCount).map { index =>
      GpuNamedLambdaVariable(s"x$index", IntegerType, nullable = true, ExprId(100 + index))
    }
    val value = resultType match {
      case BooleanType => true
      case LongType => 1L
      case _ => 1
    }
    GpuLambdaFunction(GpuLiteral(value, resultType), args)
  }

  private def transform(
      argument: Expression = arrayArg,
      argCount: Int = 1,
      boundIntermediate: Seq[GpuExpression] = Seq.empty): GpuArrayTransform =
    GpuArrayTransform(argument, lambda(IntegerType, argCount), isBound = true, boundIntermediate)

  private def executableTransform(exprId: Long): GpuArrayTransform = {
    val arg = GpuNamedLambdaVariable("x", IntegerType, nullable = true, ExprId(exprId))
    val boundArg = GpuBoundReference(0, IntegerType, nullable = true)(arg.exprId, arg.name)
    GpuArrayTransform(arrayArg, GpuLambdaFunction(boundArg, Seq(arg)), isBound = true)
  }

  private def sideEffectTransform(): GpuArrayTransform = {
    val arg = GpuNamedLambdaVariable("x", IntegerType, nullable = true, ExprId(200))
    GpuArrayTransform(arrayArg, GpuLambdaFunction(SideEffectExpr(), Seq(arg)), isBound = true)
  }

  private def filter(
      argument: Expression = arrayArg,
      argCount: Int = 1,
      boundIntermediate: Seq[GpuExpression] = Seq.empty): GpuArrayFilter =
    GpuArrayFilter(argument, lambda(BooleanType, argCount), isBound = true, boundIntermediate)

  private def aggregate(
      argument: Expression = arrayArg,
      boundIntermediate: Seq[GpuExpression]): GpuArrayAggregate =
    GpuArrayAggregate(argument, GpuLiteral(0L, LongType), lambda(LongType), SumOp,
      isBound = true, boundIntermediate)

  private def alias(expr: GpuExpression, name: String): GpuAlias =
    GpuAlias(expr, name)()

  private def literalProject(name: String): GpuAlias =
    alias(GpuLiteral(1, IntegerType), name)

  private def nonDeterministicProject(name: String): GpuAlias =
    alias(NonDeterministicExpr(), name)

  test("finds heterogeneous array HOFs over the same argument") {
    val exprs = Seq(
      alias(transform(boundIntermediate = Seq(outerB)), "t"),
      literalProject("safe"),
      alias(filter(boundIntermediate = Seq(outerC)), "f"),
      alias(aggregate(boundIntermediate = Seq(outerB, outerC)), "a"))

    assertResult(Seq(Seq(0, 2, 3))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("does not fuse different arguments or lambda arities") {
    val exprs = Seq(
      alias(transform(), "one_arg"),
      alias(filter(argCount = 2), "two_arg"),
      alias(transform(otherArrayArg), "other_arg"))

    assertResult(Seq.empty[Seq[Int]]) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("does not group unsupported HOFs or treat them as barriers") {
    val exprs = Seq(
      alias(transform(), "left"),
      alias(filter(argCount = 3), "unsupported"),
      alias(filter(), "right"))

    assertResult(Seq(Seq(0, 2))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("splits groups whose shared intermediates would be wider than each HOF") {
    val exprs = Seq(
      alias(transform(boundIntermediate = Seq(outerB)), "b_transform"),
      alias(filter(boundIntermediate = Seq(outerC)), "c_filter"),
      alias(filter(boundIntermediate = Seq(outerB)), "b_filter"),
      alias(transform(boundIntermediate = Seq(outerC)), "c_transform"))

    assertResult(Seq(Seq(0, 2), Seq(1, 3))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("does not fuse disjoint singleton intermediate sets") {
    val exprs = Seq(
      alias(transform(boundIntermediate = Seq(outerB)), "b_transform"),
      alias(filter(boundIntermediate = Seq(outerC)), "c_filter"))

    assertResult(Seq.empty[Seq[Int]]) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("splits fused groups around a non-deterministic barrier") {
    val exprs = Seq(
      alias(transform(), "before_left"),
      alias(filter(), "before_right"),
      nonDeterministicProject("barrier"),
      alias(transform(boundIntermediate = Seq(outerB)), "after_left"),
      literalProject("safe"),
      alias(filter(boundIntermediate = Seq(outerB, outerC)), "after_right"))

    assertResult(Seq(Seq(0, 1), Seq(3, 5))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("does not fuse side-effecting HOFs or cross side-effecting barriers") {
    val exprs = Seq(
      alias(transform(), "before_left"),
      alias(filter(), "before_right"),
      alias(sideEffectTransform(), "side_effect_hof"),
      alias(transform(), "middle_left"),
      alias(filter(), "middle_right"),
      alias(SideEffectExpr(), "side_effect_project"),
      alias(transform(), "after_left"),
      alias(filter(), "after_right"))

    assertResult(Seq(Seq(0, 1), Seq(3, 4), Seq(6, 7))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("executes a non-contiguous fused group for empty and non-empty batches") {
    val arrayType = ArrayType(IntegerType, containsNull = true)
    val schema = FuzzerUtils.createSchema(arrayType)
    val exprs = Seq(
      alias(executableTransform(300), "left"),
      literalProject("middle"),
      alias(executableTransform(301), "right"))

    def check(batch: ColumnarBatch): Unit = {
      val fused = GpuArrayHofFusion.project(batch, exprs)
      assert(fused.isDefined)
      withResource(fused.get) { projected =>
        assertResult(3)(projected.numCols())
        assertResult(batch.numRows())(projected.numRows())
        assert((0 until projected.numCols()).forall { index =>
          projected.column(index) != null
        })
      }
    }

    withResource(GpuColumnVector.emptyBatchFromTypes(Array(arrayType)))(check)
    withResource(FuzzerUtils.createColumnarBatch(schema, 8))(check)
  }
}
