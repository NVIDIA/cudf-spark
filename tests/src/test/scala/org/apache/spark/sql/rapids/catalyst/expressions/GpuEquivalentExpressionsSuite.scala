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

package org.apache.spark.sql.rapids.catalyst.expressions

import com.nvidia.spark.rapids.{GpuAlias, GpuCaseWhen, GpuCast, GpuCoalesce, GpuIf, GpuIsNull, GpuLiteral, GpuMonotonicallyIncreasingID}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, Expression}
import org.apache.spark.sql.rapids.{GpuAbs, GpuAdd, GpuAnd, GpuDecimalMultiply, GpuGreaterThan, GpuLessThanOrEqual, GpuMultiply, GpuSqrt, GpuSubtract}
import org.apache.spark.sql.rapids.aggregate.GpuExtractChunk32
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}

/*
 * Many of these tests were derived from SubexpressionEliminationSuite in Apache Spark,
 * and changed to use GPU expressions.
 */
class GpuEquivalentExpressionsSuite extends AnyFunSuite with Logging {

  test("Gpu Expression Equivalence - basic") {
    val equivalence = new GpuEquivalentExpressions
    assert(equivalence.getAllExprStates().isEmpty)

    val oneA = GpuLiteral(1)
    val oneB = GpuLiteral(1)
    val twoA = GpuLiteral(2)

    assert(equivalence.getExprState(oneA).isEmpty)
    assert(equivalence.getExprState(twoA).isEmpty)

    // GpuAdd oneA and test if it is returned. Since it is a group of one, it does not.
    assert(!equivalence.addExpr(oneA))
    assert(equivalence.getExprState(oneA).get.useCount == 1)
    assert(equivalence.getExprState(twoA).isEmpty)
    assert(equivalence.addExpr(oneA))
    assert(equivalence.getExprState(oneA).get.useCount == 2)

    // GpuAdd B and make sure they can see each other.
    assert(equivalence.addExpr(oneB))
    // Use exists and reference equality because of how equals is defined.
    assert(equivalence.getExprState(oneA).exists(_.expr eq oneA))
    assert(equivalence.getExprState(oneB).exists(_.expr eq oneA))
    assert(equivalence.getExprState(twoA).isEmpty)
    assert(equivalence.getAllExprStates().size == 1)
    assert(equivalence.getAllExprStates().head.useCount == 3)
    assert(equivalence.getAllExprStates().head.expr eq oneA)

    val add1 = GpuAdd(oneA, oneB, failOnError = false)()
    val add2 = GpuAdd(oneA, oneB, failOnError = false)()

    equivalence.addExpr(add1)
    equivalence.addExpr(add2)

    assert(equivalence.getAllExprStates().size == 2)
    assert(equivalence.getExprState(add1).exists(_.expr eq add1))
    assert(equivalence.getExprState(add2).get.useCount == 2)
    assert(equivalence.getExprState(add2).exists(_.expr eq add1))
  }

  test("Get Expression Tiers - two the same") {
    val oneA = AttributeReference("oneA", IntegerType)()
    val oneB = AttributeReference("oneB", IntegerType)()

    val add1 = GpuAdd(oneA, oneB, failOnError = false)()
    val add2 = GpuAdd(oneA, oneB, failOnError = false)()

    val initialExprs = Seq(add1, add2)
    val inputAttrs = AttributeSeq(Seq(oneA, oneB))
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq(add1, add2), // Both are the same, so sub-expression should match both
      Seq(),
      Seq(add1, add2)) // Both will be updated
  }

  test("GpuExpression Equivalence - Trees") {
    val one = GpuLiteral(1)
    val two = GpuLiteral(2)

    val add = GpuAdd(one, two, failOnError = false)()
    val abs = GpuAbs(add, failOnError = false)
    val add2 = GpuAdd(add, add, failOnError = false)()

    var equivalence = new GpuEquivalentExpressions
    equivalence.addExprTree(add)
    equivalence.addExprTree(abs)
    equivalence.addExprTree(add2)

    // Should only have one equivalence for `one + two`
    assert(equivalence.getAllExprStates(1).size == 1)
    assert(equivalence.getAllExprStates(1).head.useCount == 4)

    // Set up the expressions
    //   one * two,
    //   (one * two) * (one * two)
    //   sqrt( (one * two) * (one * two) )
    //   (one * two) + sqrt( (one * two) * (one * two) )
    equivalence = new GpuEquivalentExpressions
    val mul = GpuMultiply(one, two, failOnError = false)()
    val mul2 = GpuMultiply(mul, mul, failOnError = false)()
    val sqrt = GpuSqrt(mul2)
    val sum = GpuAdd(mul2, sqrt, failOnError = false)()
    equivalence.addExprTree(mul)
    equivalence.addExprTree(mul2)
    equivalence.addExprTree(sqrt)
    equivalence.addExprTree(sum)

    // (one * two), (one * two) * (one * two) and sqrt( (one * two) * (one * two) ) should be found
    assert(equivalence.getAllExprStates(1).size == 3)
    assert(equivalence.getExprState(mul).get.useCount == 3)
    assert(equivalence.getExprState(mul2).get.useCount == 3)
    assert(equivalence.getExprState(sqrt).get.useCount == 2)
    assert(equivalence.getExprState(sum).get.useCount == 1)
  }

  test("Get Expression Tiers - Trees") {
    // Set up the expressions
    //   one * two,
    //   (one * two) * (one * two)
    //   sqrt( (one * two) * (one * two) )
    //   (one * two) + sqrt( (one * two) * (one * two) )
    val one = AttributeReference("one", DoubleType)()
    val two = AttributeReference("two", DoubleType)()
    val mul = GpuMultiply(one, two, failOnError = false)()
    val mul2 = GpuMultiply(mul, mul, failOnError = false)()
    val sqrt = GpuSqrt(mul2)
    val sum = GpuAdd(mul2, sqrt, failOnError = false)()

    // (one * two), (one * two) * (one * two) and sqrt( (one * two) * (one * two) ) are all subs
    val initialExprs = Seq(mul, mul2, sqrt, sum)
    val inputAttrs = AttributeSeq(Seq(one, two))
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq(mul, mul2, sqrt),
      Seq(),
      Seq(mul, mul2, sqrt, sum))
  }

  test("Get Expression Tiers - empty") {
    val initialExprs : Seq[Expression] = Seq.empty
    val inputAttrs = AttributeSeq(Seq.empty)
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq.empty, Seq.empty, Seq.empty)
  }

  test("Get Expression Tiers - simple example") {
    // Set up the expressions
    //   one + two,
    //   (one + two) + three
    //   ((one + two) + three) + four
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val add1 = GpuAdd(one, two, failOnError = false)()
    val add2 = GpuAdd(add1, three, failOnError = false)()
    val add3 = GpuAdd(add2, four, failOnError = false)()

    // (one + two), ((one + two) + three) are both subs
    val initialExprs = Seq(add1, add2, add3)
    val inputAttrs = AttributeSeq(Seq(one, two, three, four))
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq(add1, add2), //subexpressions
      Seq(), // no unchanged
      Seq(add1, add2, add3)) // all original expressions are updated
  }

  test("Gpu Expression equivalence - non deterministic") {
    val sum = GpuAdd(GpuMonotonicallyIncreasingID(),
      GpuMonotonicallyIncreasingID(), failOnError = false)()
    val equivalence = new GpuEquivalentExpressions
    equivalence.addExpr(sum)
    equivalence.addExpr(sum)
    assert(equivalence.getAllExprStates().isEmpty)
  }

  test("Get Expression Tiers - non deterministic") {
    val sum = GpuAdd(GpuMonotonicallyIncreasingID(),
      GpuMonotonicallyIncreasingID(), failOnError = false)()
    val initialExprs = Seq(sum)
    val inputAttrs = AttributeSeq(Seq.empty)
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq.empty, // No subexpressions
      Seq(sum),  // Should be unchanged
      Seq.empty) // No modified expressions
  }

  test("Children of conditional expressions: GpuIf with side effects") {
    val add = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = true)()
    val condition = GpuGreaterThan(add, GpuLiteral(3))

    val ifExpr1 = GpuIf(condition, add, add)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(ifExpr1)

    // `add` is in both two branches of `If` and predicate.
    assert(equivalence1.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 2).head.expr eq add)
    // one-time expressions: only ifExpr and its predicate expression
    assert(equivalence1.getAllExprStates().count(_.useCount == 1) == 2)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq ifExpr1))
    assert(equivalence1.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq condition))

    // Repeated `add` is only in one branch, so we don't count it.
    val ifExpr2 = GpuIf(condition, GpuAdd(GpuLiteral(1), GpuLiteral(3), failOnError = true)(),
      GpuAdd(add, add, failOnError = true)())
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(ifExpr2)

    assert(equivalence2.getAllExprStates(1).isEmpty)
    assert(equivalence2.getAllExprStates().count(_.useCount == 1) == 3)

    val ifExpr3 = GpuIf(condition, ifExpr1, ifExpr1)
    val equivalence3 = new GpuEquivalentExpressions
    equivalence3.addExprTree(ifExpr3)

    // `add`: 2, `condition`: 2
    assert(equivalence3.getAllExprStates().count(_.useCount == 2) == 2)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 2).exists(_.expr eq condition))
    assert(equivalence3.getAllExprStates().filter(_.useCount == 2).exists(_.expr eq add))

    // `ifExpr1`, `ifExpr3`
    assert(equivalence3.getAllExprStates().count(_.useCount == 1) == 2)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq ifExpr1))
    assert(equivalence3.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq ifExpr3))
  }


  test("Children of conditional expressions: GpuIf no side effects") {
    val add = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = false)()
    val condition = GpuGreaterThan(add, GpuLiteral(3))

    val ifExpr1 = GpuIf(condition, add, add)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(ifExpr1)

    // `add` is in both two branches of `If` and predicate.
    assert(equivalence1.getAllExprStates().count(_.useCount == 3) == 1)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 3).head.expr eq add)
    // one-time expressions: only ifExpr and its predicate expression
    assert(equivalence1.getAllExprStates().count(_.useCount == 1) == 2)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq ifExpr1))
    assert(equivalence1.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq condition))

    // Repeated `add` is only in one branch, so we don't count it.
    val ifExpr2 = GpuIf(condition, GpuAdd(GpuLiteral(1), GpuLiteral(3), failOnError = false)(),
      GpuAdd(add, add, failOnError = false)())
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(ifExpr2)

    assert(equivalence2.getAllExprStates(1).length == 1)
    assert(equivalence2.getAllExprStates().count(_.useCount == 1) == 4)

    val ifExpr3 = GpuIf(condition, ifExpr1, ifExpr1)
    val equivalence3 = new GpuEquivalentExpressions
    equivalence3.addExprTree(ifExpr3)

    // add: 3
    assert(equivalence3.getAllExprStates().count(_.useCount == 3) == 1)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 3).exists(_.expr eq add))

    // `ifExpr1`: 2, `condition`: 2
    assert(equivalence3.getAllExprStates().count(_.useCount == 2) == 2)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 2).exists(_.expr eq condition))
    assert(equivalence3.getAllExprStates().filter(_.useCount == 2).exists(_.expr eq ifExpr1))

    // `ifExpr3`
    assert(equivalence3.getAllExprStates().count(_.useCount == 1) == 1)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 1).exists(_.expr eq ifExpr3))
  }

  test("Get Expression Tiers GpuIf no side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val add = GpuAdd(one, two, failOnError = false)()
    val condition = GpuGreaterThan(add, three)
    // if ((one + two) > three) then (one + two) else (one + two)
    // `add` is in both branches of `If` and predicate.
    val ifExpr1 = GpuIf(condition, add, add)
    val initialExprs = Seq(ifExpr1)
    val inputAttrs = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs, inputAttrs,
    Seq(add), // subexpressions
    Seq.empty,  // Should be unchanged
    Seq(ifExpr1)) // modified expressions

    // if ((one + two) > three) then (one + three) else ((one + two) + (one + two))
    // Repeated `add` is only in one branch, so we don't count it.
    val ifExpr2 = GpuIf(condition, GpuAdd(one, three, failOnError = false)(),
      GpuAdd(add, add, failOnError = false)())
    val initialExprs2 = Seq(ifExpr2)
    val inputAttrs2 = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq(add), // subexpressions
      Seq.empty, // Should be unchanged
      Seq(ifExpr2)) // modified expressions

    // if ((one + two) > three)
    //   if ((one + two) > three) then (one + two) else (one + two)
    // else
    //   if ((one + two) > three) then (one + two) else (one + two)
    val ifExpr3 = GpuIf(condition, ifExpr1, ifExpr1)
    val initialExprs3 = Seq(ifExpr3)
    val inputAttrs3 = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs3, inputAttrs3,
      Seq(add, condition), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(condition, ifExpr1, ifExpr3)) // modified expressions
  }

  test("Get Expression Tiers GpuIf has side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val add = GpuAdd(one, two, failOnError = true)()
    val condition = GpuGreaterThan(add, three)
    // if ((one + two) > three) then (one + two) else (one + two)
    // `add` is in both branches of `If` and predicate.
    val ifExpr1 = GpuIf(condition, add, add)
    val initialExprs = Seq(ifExpr1)
    val inputAttrs = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq(add), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(ifExpr1)) // modified expressions

    // if ((one + two) > three) then (one + three) else ((one + two) + (one + two))
    // Repeated `add` is only in one branch, so we don't count it.
    val ifExpr2 = GpuIf(condition, GpuAdd(one, three, failOnError = true)(),
      GpuAdd(add, add, failOnError = true)())
    val initialExprs2 = Seq(ifExpr2)
    val inputAttrs2 = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq.empty, // subexpressions
      Seq(ifExpr2),  // Should be unchanged
      Seq.empty) // modified expressions

    // if ((one + two) > three)
    //   if ((one + two) > three) then (one + two) else (one + two)
    // else
    //   if ((one + two) > three) then (one + two) else (one + two)
    val ifExpr3 = GpuIf(condition, ifExpr1, ifExpr1)
    val initialExprs3 = Seq(ifExpr3)
    val inputAttrs3 = AttributeSeq(Seq(one, two, three))
    validateExpressionTiers(initialExprs3, inputAttrs3,
      Seq(add, condition), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(condition, ifExpr1, ifExpr3)) // modified expressions
  }

  test("Children of conditional expressions: GpuCaseWhen no side effects") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = false)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = false)()
    val conditions1 = (GpuGreaterThan(add2, GpuLiteral(3)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr1 = GpuCaseWhen(conditions1, None)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(caseWhenExpr1)

    // `add2` is repeatedly in all conditions.
    assert(equivalence1.getAllExprStates().count(_.useCount == 3) == 2)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 3).exists(_.expr eq add2))
    assert(equivalence1.getAllExprStates().filter(_.useCount == 3).exists(_.expr eq add1))

    val conditions2 = (GpuGreaterThan(add1, GpuLiteral(3)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr2 = GpuCaseWhen(conditions2, Some(add1))
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(caseWhenExpr2)

    assert(equivalence2.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence2.getAllExprStates().filter(_.useCount == 2).head.expr eq add2)
    assert(equivalence2.getAllExprStates().count(_.useCount == 5) == 1)
    assert(equivalence2.getAllExprStates().filter(_.useCount == 5).head.expr eq add1)

    val conditions3 = (GpuGreaterThan(add1, GpuLiteral(3)), add2) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr3 = GpuCaseWhen(conditions3, None)
    val equivalence3 = new GpuEquivalentExpressions
    equivalence3.addExprTree(caseWhenExpr3)
    assert(equivalence3.getAllExprStates().count(_.useCount == 3) == 2)
    assert(equivalence3.getAllExprStates().filter(_.useCount == 3).exists(_.expr eq add2))
    assert(equivalence3.getAllExprStates().filter(_.useCount == 3).exists(_.expr eq add1))
  }

  test("Children of conditional expressions: GpuCaseWhen with side effects") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = true)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = true)()
    val conditions1 = (GpuGreaterThan(add2, GpuLiteral(3)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr1 = GpuCaseWhen(conditions1, None)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(caseWhenExpr1)

    // `add2` is repeatedly in all conditions.
    assert(equivalence1.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 2).head.expr eq add2)

    val conditions2 = (GpuGreaterThan(add1, GpuLiteral(3)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr2 = GpuCaseWhen(conditions2, Some(add1))
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(caseWhenExpr2)

    // `add1` is repeatedly in all branch values, and first predicate.
    assert(equivalence2.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence2.getAllExprStates().filter(_.useCount == 2).head.expr eq add1)

    // Negative case. `add1` or `add2` is not commonly used in all predicates/branch values.
    val conditions3 = (GpuGreaterThan(add1, GpuLiteral(3)), add2) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr3 = GpuCaseWhen(conditions3, None)
    val equivalence3 = new GpuEquivalentExpressions
    equivalence3.addExprTree(caseWhenExpr3)
    assert(equivalence3.getAllExprStates().count(_.useCount == 2) == 0)
  }

  test("Get Expression Tiers - GpuCaseWhen no side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val five = AttributeReference("five", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = false)()
    val add2 = GpuAdd(two, three, failOnError = false)()
    val cond1 = GpuGreaterThan(add2, three)
    val cond2 = GpuGreaterThan(add2, four)
    val cond3 = GpuGreaterThan(add2, five)
    val cond4 = GpuGreaterThan(add1, three)
    val conditions1 = (cond1, add1) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr1 = GpuCaseWhen(conditions1, None)
    val inputAttrs1 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs1 = Seq(caseWhenExpr1)
    // `add2` is repeatedly in all conditions.
    validateExpressionTiers(initialExprs1, inputAttrs1,
      Seq(add2), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(cond1, cond2, cond3, caseWhenExpr1)) // modified expressions

    val conditions2 = (cond4, add1) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr2 = GpuCaseWhen(conditions2, Some(add1))
    val inputAttrs2 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs2 = Seq(caseWhenExpr2)
    // `add1` is repeatedly in all branch values, and first predicate.
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq(add1), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(caseWhenExpr2)) // modified expressions

    // Negative case. `add1` or `add2` is not commonly used in all predicates/branch values.
    val conditions3 = (cond4, add2) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr3 = GpuCaseWhen(conditions3, None)
    val inputAttrs3 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs3 = Seq(caseWhenExpr3)
    // `add1` is repeatedly in all branch values, and first predicate.
    validateExpressionTiers(initialExprs3, inputAttrs3,
      Seq(add1), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(caseWhenExpr3)) // modified expressions
  }

  test("Get Expression Tiers - GpuCaseWhen with side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val five = AttributeReference("five", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = true)()
    val add2 = GpuAdd(two, three, failOnError = true)()
    val cond1 = GpuGreaterThan(add2, three)
    val cond2 = GpuGreaterThan(add2, four)
    val cond3 = GpuGreaterThan(add2, five)
    val cond4 = GpuGreaterThan(add1, three)
    val conditions1 = (cond1, add1) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr1 = GpuCaseWhen(conditions1, None)
    val inputAttrs1 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs1 = Seq(caseWhenExpr1)
    // `add2` is repeatedly in all conditions.
    validateExpressionTiers(initialExprs1, inputAttrs1,
      Seq(add2), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(cond1, cond2, cond3, caseWhenExpr1)) // modified expressions

    val conditions2 = (cond4, add1) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr2 = GpuCaseWhen(conditions2, Some(add1))
    val inputAttrs2 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs2 = Seq(caseWhenExpr2)
    // `add1` is repeatedly in all branch values, and first predicate.
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq(add1), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(caseWhenExpr2)) // modified expressions

    // Negative case. `add1` or `add2` is not commonly used in all predicates/branch values.
    val conditions3 = (cond4, add2) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr3 = GpuCaseWhen(conditions3, None)
    val inputAttrs3 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs3 = Seq(caseWhenExpr3)
    // `add1` is repeatedly in all branch values, and first predicate.
    validateExpressionTiers(initialExprs3, inputAttrs3,
      Seq.empty, // subexpressions
      Seq(caseWhenExpr3),  // Should be unchanged
      Seq.empty) // modified expressions
  }

  test("Children of conditional expressions: GpuCoalesce no side effects") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = false)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = false)()
    val conditions1 = GpuGreaterThan(add2, GpuLiteral(3)) ::
        GpuGreaterThan(add2, GpuLiteral(4)) ::
        GpuGreaterThan(add2, GpuLiteral(5)) :: Nil

    val coalesceExpr1 = GpuCoalesce(conditions1)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(coalesceExpr1)

    // `add2` is repeatedly in all conditions.
    assert(equivalence1.getAllExprStates().count(_.useCount == 3) == 1)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 3).head.expr eq add2)

    // Negative case. `add1` and `add2` both are not used in all branches.
    val conditions2 = GpuGreaterThan(add1, GpuLiteral(3)) ::
        GpuGreaterThan(add2, GpuLiteral(4)) ::
        GpuGreaterThan(add2, GpuLiteral(5)) :: Nil

    val coalesceExpr2 = GpuCoalesce(conditions2)
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(coalesceExpr2)

    assert(equivalence2.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence2.getAllExprStates().filter(_.useCount == 2).head.expr eq add2)
  }

  test("Children of conditional expressions: GpuCoalesce with side effects") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = true)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = true)()
    val conditions1 = GpuGreaterThan(add2, GpuLiteral(3)) ::
      GpuGreaterThan(add2, GpuLiteral(4)) ::
      GpuGreaterThan(add2, GpuLiteral(5)) :: Nil

    val coalesceExpr1 = GpuCoalesce(conditions1)
    val equivalence1 = new GpuEquivalentExpressions
    equivalence1.addExprTree(coalesceExpr1)

    // `add2` is repeatedly in all conditions.
    assert(equivalence1.getAllExprStates().count(_.useCount == 2) == 1)
    assert(equivalence1.getAllExprStates().filter(_.useCount == 2).head.expr eq add2)

    // Negative case. `add1` and `add2` both are not used in all branches.
    val conditions2 = GpuGreaterThan(add1, GpuLiteral(3)) ::
      GpuGreaterThan(add2, GpuLiteral(4)) ::
      GpuGreaterThan(add2, GpuLiteral(5)) :: Nil

    val coalesceExpr2 = GpuCoalesce(conditions2)
    val equivalence2 = new GpuEquivalentExpressions
    equivalence2.addExprTree(coalesceExpr2)

    assert(equivalence2.getAllExprStates().count(_.useCount == 2) == 0)
  }

  test("Get Expression Tiers: GpuCoalesce no side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val five = AttributeReference("five", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = false)()
    val add2 = GpuAdd(two, three, failOnError = false)()
    val cond1 = GpuGreaterThan(add2, three)
    val cond2 = GpuGreaterThan(add2, four)
    val cond3 = GpuGreaterThan(add2, five)
    val cond4 = GpuGreaterThan(add1, three)

    val conditions1 = cond1 :: cond2 :: cond3 :: Nil
    val coalesceExpr1 = GpuCoalesce(conditions1)
    val inputAttrs1 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs1 = Seq(coalesceExpr1)
    // `add2` is repeatedly in many conditions.
    validateExpressionTiers(initialExprs1, inputAttrs1,
      Seq(add2), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(cond1, cond2, cond3, coalesceExpr1)) // modified expressions

    val conditions2 = cond4 :: cond2 :: cond3 :: Nil
    val coalesceExpr2 = GpuCoalesce(conditions2)
    val inputAttrs2 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs2 = Seq(coalesceExpr2)
    // Negative case. `add1` and `add2` both are not used in all branches.
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq(add2), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(cond2, cond3, cond4, coalesceExpr2)) // modified expressions
  }

  test("Get Expression Tiers: GpuCoalesce with side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val five = AttributeReference("five", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = true)()
    val add2 = GpuAdd(two, three, failOnError = true)()
    val cond1 = GpuGreaterThan(add2, three)
    val cond2 = GpuGreaterThan(add2, four)
    val cond3 = GpuGreaterThan(add2, five)
    val cond4 = GpuGreaterThan(add1, three)

    val conditions1 = cond1 :: cond2 :: cond3 :: Nil
    val coalesceExpr1 = GpuCoalesce(conditions1)
    val inputAttrs1 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs1 = Seq(coalesceExpr1)
    // `add2` is repeatedly in all conditions.
    validateExpressionTiers(initialExprs1, inputAttrs1,
      Seq(add2), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(cond1, cond2, cond3, coalesceExpr1)) // modified expressions

    val conditions2 = cond4 :: cond2 :: cond3 :: Nil
    val coalesceExpr2 = GpuCoalesce(conditions2)
    val inputAttrs2 = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs2 = Seq(coalesceExpr2)
    // Negative case. `add1` and `add2` both are not used in all branches.
    validateExpressionTiers(initialExprs2, inputAttrs2,
      Seq.empty, // subexpressions
      Seq(coalesceExpr2),  // Should be unchanged
      Seq.empty) // modified expressions
  }

  test("SPARK-35410: SubExpr elimination should not include redundant child exprs " +
    "for conditional expressions (with side effects)") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = true)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = true)()
    val add3 = GpuAdd(add1, add2, failOnError = true)()
    val condition = (GpuGreaterThan(add3, GpuLiteral(3)), add3) :: Nil

    val caseWhenExpr = GpuCaseWhen(condition,
      Some(GpuAdd(add3, GpuLiteral(1), failOnError = true)()))
    val equivalence = new GpuEquivalentExpressions
    equivalence.addExprTree(caseWhenExpr)

    val commonExprs = equivalence.getAllExprStates(1)
    assert(commonExprs.size == 1)
    assert(commonExprs.head.useCount == 2)
    assert(commonExprs.head.expr eq add3)
  }

  test("Get Expression Tiers - SPARK-35410: SubExpr elimination should not include " +
      "redundant child exprs for conditional expressions") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = false)()
    val add2 = GpuAdd(two, three, failOnError = false)()
    val add3 = GpuAdd(add1, add2, failOnError = false)()
    val add4 = GpuAdd(add3, one, failOnError = false)()
    val condition = (GpuGreaterThan(add3, three), add3) :: Nil
    val caseWhenExpr = GpuCaseWhen(condition, Some(add4))
    val inputAttrs = AttributeSeq(Seq(one, two, three))
    val initialExprs = Seq(caseWhenExpr)
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq(add3), // subexpressions
      Seq.empty,  // Should be unchanged
      Seq(caseWhenExpr)) // modified expressions
  }

  test("SPARK-35439: Children subexpr should come first than parent subexpr") {
    val add = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = false)()

    val equivalence1 = new GpuEquivalentExpressions

    equivalence1.addExprTree(add)
    assert(equivalence1.getAllExprStates().head.expr eq add)

    equivalence1.addExprTree(GpuAdd(GpuLiteral(3), add, failOnError = false)())
    assert(equivalence1.getAllExprStates().map(_.useCount) === Seq(2, 1))
    assert(equivalence1.getAllExprStates().map(_.expr) ===
        Seq(add, GpuAdd(GpuLiteral(3), add, failOnError = false)()))

    equivalence1.addExprTree(GpuAdd(GpuLiteral(3), add, failOnError = false)())
    assert(equivalence1.getAllExprStates().map(_.useCount) === Seq(2, 2))
    assert(equivalence1.getAllExprStates().map(_.expr) ===
        Seq(add, GpuAdd(GpuLiteral(3), add, failOnError = false)()))

    val equivalence2 = new GpuEquivalentExpressions

    equivalence2.addExprTree(GpuAdd(GpuLiteral(3), add, failOnError = false)())
    assert(equivalence2.getAllExprStates().map(_.useCount) === Seq(1, 1))
    assert(equivalence2.getAllExprStates().map(_.expr) ===
        Seq(add, GpuAdd(GpuLiteral(3), add, failOnError = false)()))

    equivalence2.addExprTree(add)
    assert(equivalence2.getAllExprStates().map(_.useCount) === Seq(2, 1))
    assert(equivalence2.getAllExprStates().map(_.expr) ===
        Seq(add, GpuAdd(GpuLiteral(3), add, failOnError = false)()))

    equivalence2.addExprTree(GpuAdd(GpuLiteral(3), add, failOnError = false)())
    assert(equivalence2.getAllExprStates().map(_.useCount) === Seq(2, 2))
    assert(equivalence2.getAllExprStates().map(_.expr) ===
        Seq(add, GpuAdd(GpuLiteral(3), add, failOnError = false)()))
  }

  test("SPARK-35499: Subexpressions should only be extracted from CaseWhen "
      + "values with an elseValue (with side effects)") {
    val add1 = GpuAdd(GpuLiteral(1), GpuLiteral(2), failOnError = true)()
    val add2 = GpuAdd(GpuLiteral(2), GpuLiteral(3), failOnError = true)()
    val conditions = (GpuGreaterThan(add1, GpuLiteral(3)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(4)), add1) ::
      (GpuGreaterThan(add2, GpuLiteral(5)), add1) :: Nil

    val caseWhenExpr = GpuCaseWhen(conditions, None)
    val equivalence = new GpuEquivalentExpressions
    equivalence.addExprTree(caseWhenExpr)

    // `add1` is not in the elseValue, so we can't extract it from the branches
    assert(equivalence.getAllExprStates().count(_.useCount == 2) == 0)
  }

  test("Get Expression Tiers - SPARK-35499: Subexpressions should only be extracted " +
      "from CaseWhen values with an elseValue if there are side effects") {
    val one = AttributeReference("one", IntegerType)()
    val two = AttributeReference("two", IntegerType)()
    val three = AttributeReference("three", IntegerType)()
    val four = AttributeReference("four", IntegerType)()
    val five = AttributeReference("five", IntegerType)()

    val add1 = GpuAdd(one, two, failOnError = true)()
    val add2 = GpuAdd(two, three, failOnError = true)()
    val cond1 = GpuGreaterThan(add1, three)
    val cond2 = GpuGreaterThan(add2, four)
    val cond3 = GpuGreaterThan(add2, five)
    val conditions = (cond1, add1) :: (cond2, add1) :: (cond3, add1) :: Nil
    val caseWhenExpr = GpuCaseWhen(conditions, None)
    // `add1` is not in the elseValue, so we can't extract it from the branches
    val inputAttrs = AttributeSeq(Seq(one, two, three, four, five))
    val initialExprs = Seq(caseWhenExpr)
    // Negative case. `add1` and `add2` both are not used in all branches.
    validateExpressionTiers(initialExprs, inputAttrs,
      Seq.empty, // subexpressions
      Seq(caseWhenExpr),  // Should be unchanged
      Seq.empty) // modified expressions
  }

  test("Get Expression Tiers - Query derived from nds q4") {
    val customer: AttributeReference = AttributeReference("customer", IntegerType)()
    val quantity: AttributeReference = AttributeReference("quantity", IntegerType)()
    val price: AttributeReference = AttributeReference("price", DecimalType(7, 2))()
    val inputAttrs = AttributeSeq(Seq(customer, quantity, price))

    val product = GpuDecimalMultiply(
      GpuCast(quantity, DecimalType(10, 0))(), price, DecimalType(18,2))
    val nullCheck = GpuIsNull(product)
    val castProduct = GpuCast(product, DecimalType(28,2))()
    val extract0 = GpuExtractChunk32(castProduct, 0, replaceNullsWithZero = true)
    val extract1 = GpuExtractChunk32(castProduct, 1, replaceNullsWithZero = true)
    val extract2 = GpuExtractChunk32(castProduct, 2, replaceNullsWithZero = true)
    val extract3 = GpuExtractChunk32(castProduct, 3, replaceNullsWithZero = true)
    val initialExprs = Seq(customer, extract0, extract1, extract2, extract3, nullCheck)
    val exprTiers = GpuEquivalentExpressions.getExprTiers(initialExprs)
    validateExprTiers(exprTiers, initialExprs,
      Seq(product, castProduct),  // Common sub-expression
      Seq(customer), // Unchanged
      Seq(extract0, extract1, extract2, extract3, nullCheck)) // updated
    validateInputTiers(exprTiers, inputAttrs)
  }

  test("Get Expression Tiers - Query derived from nds q62") {
    val group: AttributeReference = AttributeReference("group", StringType)()
    val smType: AttributeReference = AttributeReference("type", StringType)()
    val webName: AttributeReference = AttributeReference("web name", StringType)()
    val shipDate: AttributeReference = AttributeReference("ship date", IntegerType)()
    val soldDate: AttributeReference = AttributeReference("sold date", IntegerType)()
    val inputAttrs = AttributeSeq(Seq(group, smType, webName, shipDate, soldDate))

    val dateDiff = GpuSubtract(shipDate, soldDate, failOnError = false)()
    val caseWhen1 =
      GpuCaseWhen(
        Seq((GpuLessThanOrEqual(dateDiff, GpuLiteral(30)), GpuLiteral(1))), Some(GpuLiteral(0)))
    val caseWhen2 =
      GpuCaseWhen(
        Seq((GpuAnd(
          GpuGreaterThan(dateDiff, GpuLiteral(30)),
          GpuLessThanOrEqual(dateDiff, GpuLiteral(60))), GpuLiteral(1))), Some(GpuLiteral(0)))
    val caseWhen3 =
      GpuCaseWhen(
        Seq((GpuAnd(
          GpuGreaterThan(dateDiff, GpuLiteral(60)),
          GpuLessThanOrEqual(dateDiff, GpuLiteral(90))), GpuLiteral(1))), Some(GpuLiteral(0)))
    val caseWhen4 =
      GpuCaseWhen(Seq((GpuAnd(
        GpuGreaterThan(dateDiff, GpuLiteral(90)),
        GpuLessThanOrEqual(dateDiff, GpuLiteral(120))), GpuLiteral(1))), Some(GpuLiteral(0)))
    val caseWhen5 =
        GpuCaseWhen(
          Seq((GpuGreaterThan(dateDiff, GpuLiteral(120)), GpuLiteral(1))),
          Some(GpuLiteral(0)))

    val initialExprs =
      Seq(group, smType, webName, caseWhen1, caseWhen2, caseWhen3, caseWhen4, caseWhen5)
    val exprTiers = GpuEquivalentExpressions.getExprTiers(initialExprs)
    validateExprTiers(exprTiers, initialExprs,
      Seq(dateDiff),                // sub-expressions
      Seq(group, smType, webName),  // unchanged exprs
      Seq(caseWhen1, caseWhen2, caseWhen2, caseWhen3, caseWhen4, caseWhen5))  // updated exprs
    validateInputTiers(exprTiers, inputAttrs)
  }

  private def realExpr(expr: Expression): Expression = expr match {
    case e: GpuAlias => e.child
    case _ => expr
  }

  private def checkEquals(expr: Expression, other: Expression): Boolean = {
    realExpr(expr).semanticEquals(realExpr(other))
  }

  /**
   * ValidateExprTiers: run checks on exprTiers vs what is expected
   * Args:
   *   exprTiers - expression tiers we are checking
   *   initialExprs - original list of expressions
   *   subExprs - expected lowest level sub-expressions
   *   unchanged - expressions that are unmodified from the original list
   *   updated - expressions from the original list that should be updated
   */
  private def validateExprTiers(exprTiers: Seq[Seq[Expression]], initialExprs: Seq[Expression],
      subExprs: Seq[Expression], unChanged: Seq[Expression], updated: Seq[Expression]): Unit = {
    if (subExprs.isEmpty) {
      assert(exprTiers.size == 1)
      // The one tier should match the initial list
      initialExprs.foreach(e => assert(exprTiers(0).contains(e)))
    } else {
      // Should be more than one tier
      assert(exprTiers.size > 1)
    }
    // Last tier should be same size as initial list
    assert(exprTiers.last.size == initialExprs.size)

    // substituted expressions should be in one of the tiers before the last one.
    val unSubbed = undoSubstitutions(exprTiers.dropRight(1).flatten)
    subExprs.foreach(sub =>
      assert(unSubbed.exists(e => checkEquals(e, sub)),
        "Expected: " + sub.toString() + " not found in: " + unSubbed.toString()))

    // Unchanged expressions should be in the last tier.
    unChanged.foreach(expected =>
      assert(exprTiers.last.contains(expected),
        "Expected: " + expected.toString() + " not found in: " + exprTiers.last.toString()))

    // Updated expressions should not match, since they have been updated
    updated.foreach(expected =>
      assert(exprTiers.last.forall(e => !checkEquals(e, expected)),
        "Unexpected: " + expected.toString() + " was found in: " + exprTiers.last.toString()))
  }

  private def validateInputTiers(exprTiers: Seq[Seq[Expression]],
      initialInputs: AttributeSeq): Unit = {
    val inputTiers = GpuEquivalentExpressions.getInputTiers(exprTiers, initialInputs)
    assert(exprTiers.size == inputTiers.size)
    // First tier should have same inputs as original inputs
    // Subsequent tiers should remove eclipsed inputs and add inputs for each expr in previous tier
    var currentAttrs = initialInputs.attrs
    var curTier = 0
    while (curTier < inputTiers.size) {
      assert(inputTiers(curTier).attrs.size == currentAttrs.size)
      assert(inputTiers(curTier).attrs == currentAttrs)
      currentAttrs = GpuEquivalentExpressions.getAttrsForNextTier(currentAttrs,
            exprTiers.drop(curTier))
      curTier += 1
    }
  }

  /**
   * ValidateGetExprTiers: run checks on exprTiers vs what is expected
   * Args:
   *   initialExprs - original list of expressions
   *   inputAttrs - original list of input attributes
   *   subExprs - expected lowest level sub-expressions
   *   unchanged - expressions that are unmodified from the original list
   *   updated - expressions from the original list that should be updated
   */
  private def validateExpressionTiers(initialExprs: Seq[Expression], inputAttrs: AttributeSeq,
      subExprs: Seq[Expression], unChanged: Seq[Expression], updated: Seq[Expression]): Unit = {
    val exprTiers = GpuEquivalentExpressions.getExprTiers(initialExprs)
    validateExprTiers(exprTiers, initialExprs, subExprs, unChanged, updated)
    validateInputTiers(exprTiers, inputAttrs)
  }

  def restoreOriginalExpr(
      expr: Expression,
      substitutionMap: Map[Expression, Expression]): Expression = {
    val newExpr = substitutionMap.get(expr) match {
      case Some(e) => e
      case None => expr
    }
    newExpr.mapChildren(restoreOriginalExpr(_, substitutionMap))
  }

  private def undoSubstitutions(subExprs: Seq[Expression]): Seq[Expression] = {
    if (subExprs.isEmpty) {
      subExprs
    } else {
      val subMap = subExprs.filter(p => p.isInstanceOf[GpuAlias]).map {
        case e: GpuAlias => (e.toAttribute, e.child)
      }.toMap[Expression, Expression]
      subExprs.map(restoreOriginalExpr(_, subMap))
    }
  }
}
