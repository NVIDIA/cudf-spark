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

import org.apache.spark.sql.catalyst.expressions.{Add, And, ArrayAggregate, Ascii,
  AttributeReference, CaseWhen, Cast, CheckOverflow, Divide, EqualTo, Expression, GreaterThan,
  Greatest, If, LambdaFunction, Least, Literal, Multiply, NamedExpression, NamedLambdaVariable,
  Or, Subtract}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DecimalType, DoubleType,
  IntegerType, LongType, StringType}

// Extends GpuUnitTests so SQLConf.get is available for the default evalMode / failOnError
// parameter on Add/Subtract/Multiply/Divide (the field name differs across Spark versions;
// letting Spark apply its own default keeps this test shim-agnostic).
class ArrayAggregateDecomposerSuite extends GpuUnitTests {
  import ArrayAggregateDecomposer.decompose

  private def lv(
      name: String,
      dt: DataType = IntegerType,
      nullable: Boolean = true): NamedLambdaVariable =
    NamedLambdaVariable(name, dt, nullable, exprId = NamedExpression.newExprId)

  private def merge(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(body, Seq(acc, x))

  private def identityFinish(acc: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(acc, Seq(acc))

  /** Wrap zeroType in an ArrayType(_, containsNull = false) for the typical happy path. */
  private def arrTy(zeroType: DataType): ArrayType = ArrayType(zeroType, containsNull = false)

  private def assertDecomposes(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable,
      expectedOp: AggOp,
      expectedG: Option[Expression] = None,
      zeroType: DataType = IntegerType,
      argType: Option[DataType] = None,
      zeroNullable: Boolean = false): ArrayAggregateDecomposition = {
    val d = decompose(merge(body, acc, x), identityFinish(acc),
      argType.getOrElse(arrTy(zeroType)), zeroType, zeroNullable)
    val r = d.getOrElse(fail(s"expected decomposition for body=$body, got Left: $d"))
    assert(r.op == expectedOp)
    expectedG.foreach { g =>
      assert(r.g.fastEquals(g), s"expected g=$g, got ${r.g}")
    }
    assert(r.accVarExprId == acc.exprId)
    assert(r.elemVar.exprId == x.exprId)
    r
  }

  private def assertRejects(
      mergeBody: LambdaFunction,
      finish: Expression,
      reason: String,
      zeroType: DataType = IntegerType,
      argType: Option[DataType] = None,
      zeroNullable: Boolean = false): String = {
    val d = decompose(mergeBody, finish, argType.getOrElse(arrTy(zeroType)), zeroType,
      zeroNullable)
    assert(d.isLeft, s"$reason — expected Left but got: $d")
    d.swap.getOrElse(fail("unreachable"))
  }

  private def withPromotePrecision(child: Expression)(f: Expression => Unit): Unit = {
    try {
      val cls = Class.forName("org.apache.spark.sql.catalyst.expressions.PromotePrecision")
      f(cls.getConstructor(classOf[Expression]).newInstance(child).asInstanceOf[Expression])
    } catch {
      case _: ClassNotFoundException =>
    }
  }

  test("Add(acc, x) -> SUM, g on the right") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Add(acc, x), acc, x, SumOp)
  }

  test("Add(x, acc) (commuted) -> SUM, g on the left") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Add(x, acc), acc, x, SumOp)
  }

  test("Multiply(acc, x) -> PRODUCT") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Multiply(acc, x), acc, x, ProductOp)
  }

  test("Greatest(acc, x) -> MAX") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Greatest(Seq(acc, x)), acc, x, MaxOp)
  }

  test("Least(acc, x) -> MIN") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Least(Seq(acc, x)), acc, x, MinOp)
  }

  test("And(acc, x) -> ALL") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    assertDecomposes(And(acc, x), acc, x, AllOp,
      zeroType = BooleanType)
  }

  test("Or(acc, x) -> ANY") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    assertDecomposes(Or(acc, x), acc, x, AnyOp,
      zeroType = BooleanType)
  }

  test("ArrayAggregate tags lifted g sub-expression compatibility") {
    val a = AttributeReference("a", ArrayType(StringType, containsNull = false),
      nullable = false)()
    val acc = lv("acc", IntegerType)
    val x = lv("x", StringType, nullable = false)
    val expr = ArrayAggregate(a, Literal(0), merge(Add(acc, Ascii(x)), acc, x),
      identityFinish(acc))
    val meta = GpuOverrides.wrapExpr(expr, new RapidsConf(Map.empty[String, String]), None)

    meta.tagForGpu()

    assert(!meta.canThisBeReplaced)
    val explain = meta.explain(all = false)
    assert(explain.contains("lifted g sub-expression cannot run on GPU"))
    assert(explain.contains("Ascii"))
  }

  test("ALL and ANY accept nullable zero because combineWithZero is null-aware") {
    val allAcc = lv("allAcc", BooleanType)
    val anyAcc = lv("anyAcc", BooleanType)
    val x = lv("x", BooleanType, nullable = false)
    assertDecomposes(And(allAcc, x), allAcc, x, AllOp,
      zeroType = BooleanType, zeroNullable = true)
    assertDecomposes(Or(anyAcc, x), anyAcc, x, AnyOp,
      zeroType = BooleanType, zeroNullable = true)
  }

  test("Complex g(x) with no acc ref decomposes (g on the right)") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    val g = Cast(Add(Multiply(x, Literal(2)), Literal(1)), LongType)
    assertDecomposes(Add(acc, g), acc, x, SumOp, zeroType = LongType)
  }

  test("CheckOverflow around decimal Add falls back as unsupported SUM type") {
    val dt = DecimalType(38, 2)
    val acc = lv("acc", dt); val x = lv("x", dt)
    val body = CheckOverflow(Add(acc, x), dt, nullOnOverflow = true)
    val msg = assertRejects(merge(body, acc, x), identityFinish(acc),
      "Decimal SUM should fall back after unwrapping CheckOverflow",
      zeroType = dt, argType = Some(ArrayType(dt, containsNull = false)))
    assert(msg.contains("SUM"), s"expected SUM-related error, got: $msg")
  }

  test("PromotePrecision around decimal Add operands falls back as unsupported SUM type") {
    val dt = DecimalType(38, 2)
    val acc = lv("acc", dt); val x = lv("x", dt)
    withPromotePrecision(acc) { promotedAcc =>
      withPromotePrecision(x) { promotedX =>
        val body = CheckOverflow(Add(promotedAcc, promotedX), dt, nullOnOverflow = true)
        val msg = assertRejects(merge(body, acc, x), identityFinish(acc),
          "Decimal SUM should fall back after unwrapping PromotePrecision",
          zeroType = dt, argType = Some(ArrayType(dt, containsNull = false)))
        assert(msg.contains("SUM"), s"expected SUM-related error, got: $msg")
      }
    }
  }

  test("Cast wrapping the acc side is unwrapped (single layer)") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    assertDecomposes(Add(Cast(acc, IntegerType), x), acc, x, SumOp)
  }

  test("Cast wrapping the acc side is unwrapped (chained)") {
    val acc = lv("acc"); val x = lv("x")
    val doubleCastAcc = Cast(Cast(acc, LongType), IntegerType)
    assertDecomposes(Add(doubleCastAcc, x), acc, x, SumOp)
  }

  test("Subtract is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Subtract(acc, x), acc, x), identityFinish(acc),
      "Subtract is not in the registered AggOps")
  }

  test("Divide is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Divide(acc, x), acc, x), identityFinish(acc),
      "Divide is not in the registered AggOps")
  }

  test("Greatest with arity != 2 is not decomposed") {
    val acc = lv("acc"); val x = lv("x")
    val body = Greatest(Seq(acc, x, Literal(1)))
    assertRejects(merge(body, acc, x), identityFinish(acc),
      "Greatest with 3 children is not a 2-operand op")
  }

  test("g that references acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Add(acc, Multiply(acc, x)), acc, x), identityFinish(acc),
      "g must not reference acc")
  }

  test("both sides reference acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Add(acc, acc), acc, x), identityFinish(acc),
      "neither side is a 'pure non-acc'")
  }

  test("neither side is a pure acc ref is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Add(Add(acc, Literal(1)), x), acc, x), identityFinish(acc),
      "left side isn't a naked acc ref")
  }

  test("non-identity finish is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val badFinish = LambdaFunction(Add(finishAcc, Literal(1)), Seq(finishAcc))
    assertRejects(merge(Add(acc, x), acc, x), badFinish,
      "finish that multiplies the accumulator isn't identity")
  }

  test("finish referencing a different variable id is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val otherVar = lv("other")
    val badFinish = LambdaFunction(otherVar, Seq(finishAcc))
    assertRejects(merge(Add(acc, x), acc, x), badFinish,
      "finish body refers to a variable that isn't its own arg")
  }

  test("finish cast of acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val badFinish = LambdaFunction(Cast(finishAcc, LongType), Seq(finishAcc))
    assertRejects(merge(Add(acc, x), acc, x), badFinish,
      "finish body must be the exact lambda variable")
  }

  test("merge with wrong arg count is rejected") {
    val acc = lv("acc"); val x = lv("x"); val extra = lv("extra")
    val body = LambdaFunction(Add(acc, x), Seq(acc, x, extra))
    assertRejects(body, identityFinish(acc), "merge must take 2 lambda args")
  }

  test("merge that isn't a LambdaFunction at all is rejected") {
    val acc = lv("acc")
    assert(decompose(Add(Literal(1), Literal(2)), identityFinish(acc),
      arrTy(IntegerType), IntegerType).isLeft)
  }

  test("finish that isn't a LambdaFunction is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assert(decompose(merge(Add(acc, x), acc, x), Literal(0),
      arrTy(IntegerType), IntegerType).isLeft)
  }

  // The decomposer now owns the "is this shape ever GPU-able" decision, so it must also
  // reject unsupported types and AllOp/AnyOp on null-bearing arrays.

  test("MaxOp on Double is rejected (NaN propagation differs from cuDF)") {
    val acc = lv("acc", DoubleType); val x = lv("x", DoubleType)
    val msg = assertRejects(merge(Greatest(Seq(acc, x)), acc, x), identityFinish(acc),
      "MAX should fall back on Double",
      zeroType = DoubleType)
    assert(msg.contains("MAX"), s"expected MAX-related error, got: $msg")
  }

  test("ALL on array<bool> with containsNull rejects") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    val msg = assertRejects(merge(And(acc, x), acc, x), identityFinish(acc),
      "ALL on null-bearing array should fall back",
      zeroType = BooleanType,
      argType = Some(ArrayType(BooleanType, containsNull = true)))
    assert(msg.contains("ALL"), s"expected ALL-related error, got: $msg")
  }

  test("ALL rejects when lifted g is nullable") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    val nullableG = If(EqualTo(x, Literal(true)), Literal.create(null, BooleanType),
      Literal(false))
    val msg = assertRejects(merge(And(acc, nullableG), acc, x), identityFinish(acc),
      "ALL with nullable g should fall back",
      zeroType = BooleanType)
    assert(msg.contains("nullable"), s"expected nullable-related error, got: $msg")
  }

  test("ANY rejects when lifted g is nullable") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    val nullableG = If(EqualTo(x, Literal(true)), Literal.create(null, BooleanType),
      Literal(true))
    val msg = assertRejects(merge(Or(acc, nullableG), acc, x), identityFinish(acc),
      "ANY with nullable g should fall back",
      zeroType = BooleanType)
    assert(msg.contains("nullable"), s"expected nullable-related error, got: $msg")
  }

  test("g type mismatch with zero type rejects") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    // body sums a non-cast Int element into a Long acc — g.dataType=Int doesn't match
    // zeroType=Long, so this must fall back even though the shape is otherwise OK.
    val msg = assertRejects(merge(Add(acc, x), acc, x), identityFinish(acc),
      "g type mismatch should fall back",
      zeroType = LongType)
    assert(msg.contains("does not match"), s"expected type-mismatch error, got: $msg")
  }

  // If / CaseWhen normalize: branches that are op-of-acc (or bare acc treated as
  // op(acc, identity)) get lifted out so cond-driven count-if patterns run on the GPU.

  test("If(cond, acc + t, acc) decomposes to SUM (revans's pattern)") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(EqualTo(x, Literal(7)), Add(acc, Literal(1)), acc)
    assertDecomposes(body, acc, x, SumOp)
  }

  test("If(cond, acc, acc + t) decomposes to SUM (commuted branches)") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(EqualTo(x, Literal(7)), acc, Add(acc, Literal(1)))
    assertDecomposes(body, acc, x, SumOp)
  }

  test("If(cond, acc + t1, acc + t2) — both branches op-of-acc — decomposes") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(x, Literal(0)), Add(acc, x), Add(acc, Literal(0)))
    assertDecomposes(body, acc, x, SumOp)
  }

  test("If with MAX (greatest(acc, x)) on one branch and bare acc on the other") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(x, Literal(0)), Greatest(Seq(acc, x)), acc)
    assertDecomposes(body, acc, x, MaxOp)
  }

  test("If with MAX and bare acc rejects when zero can be nullable") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(x, Literal(0)), Greatest(Seq(acc, x)), acc)
    val msg = assertRejects(merge(body, acc, x), identityFinish(acc),
      "MAX with bare acc and nullable zero should fall back",
      zeroNullable = true)
    assert(msg.contains("initial zero can be null"),
      s"expected nullable-zero bare-acc error, got: $msg")
  }

  test("CaseWhen with MIN and bare acc rejects when zero can be nullable") {
    val acc = lv("acc"); val x = lv("x")
    val body = CaseWhen(
      Seq((GreaterThan(x, Literal(0)), Least(Seq(acc, x)))),
      Some(acc))
    val msg = assertRejects(merge(body, acc, x), identityFinish(acc),
      "MIN with bare acc and nullable zero should fall back",
      zeroNullable = true)
    assert(msg.contains("initial zero can be null"),
      s"expected nullable-zero bare-acc error, got: $msg")
  }

  test("If with And on boolean acc decomposes to ALL") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType, nullable = false)
    val body = If(EqualTo(x, Literal(true)), And(acc, x), acc)
    assertDecomposes(body, acc, x, AllOp,
      zeroType = BooleanType)
  }

  test("CaseWhen with multiple acc+t branches and acc else decomposes") {
    val acc = lv("acc"); val x = lv("x")
    val body = CaseWhen(
      Seq(
        (EqualTo(x, Literal(1)), Add(acc, Literal(10))),
        (EqualTo(x, Literal(2)), Add(acc, Literal(20)))),
      Some(acc))
    assertDecomposes(body, acc, x, SumOp)
  }

  test("If condition references acc — rejected (g must not depend on acc)") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(acc, Literal(100)), Add(acc, Literal(1)), acc)
    assertRejects(merge(body, acc, x), identityFinish(acc),
      "cond referencing acc breaks per-element parallelism")
  }

  test("If branches use different ops — rejected") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(x, Literal(0)), Add(acc, Literal(1)), Multiply(acc, Literal(2)))
    assertRejects(merge(body, acc, x), identityFinish(acc),
      "branches mixing Add and Multiply have no single op to lift")
  }

  test("If branches put acc on different sides — decomposes") {
    val acc = lv("acc"); val x = lv("x")
    val body = If(GreaterThan(x, Literal(0)), Add(acc, x), Add(x, acc))
    assertDecomposes(body, acc, x, SumOp)
  }

  test("CaseWhen branches put acc on different sides — decomposes") {
    val acc = lv("acc"); val x = lv("x")
    val body = CaseWhen(
      Seq(
        (EqualTo(x, Literal(1)), Add(acc, Literal(10))),
        (EqualTo(x, Literal(2)), Add(Literal(20), acc))),
      Some(acc))
    assertDecomposes(body, acc, x, SumOp)
  }

  test("CaseWhen without else — rejected") {
    val acc = lv("acc"); val x = lv("x")
    val body = CaseWhen(
      Seq((EqualTo(x, Literal(1)), Add(acc, Literal(10)))),
      None)
    assertRejects(merge(body, acc, x), identityFinish(acc),
      "CaseWhen with no else has implicit null fallthrough we don't model")
  }

  test("Nested If is decomposed recursively") {
    val acc = lv("acc"); val x = lv("x")
    // if(c1, if(c2, acc + 1, acc + 2), acc)
    val inner = If(GreaterThan(x, Literal(10)), Add(acc, Literal(1)), Add(acc, Literal(2)))
    val outer = If(GreaterThan(x, Literal(0)), inner, acc)
    assertDecomposes(outer, acc, x, SumOp)
  }
}
