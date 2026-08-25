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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ArrayIntersect, CollectionExpressionsSuite,
  Literal, MapFromEntries, Shuffle}
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DoubleType,
  FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}

/**
 * RAPIDS GPU tests for collection expressions (array, map operations).
 *
 * This test suite validates collection expression execution on GPU.
 * It extends the original Spark CollectionExpressionsSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    CollectionExpressionsSuite.scala
 * Test count: 48 tests
 *
 * Migration notes:
 * - CollectionExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Array operations: Size, Contains, Overlap, Union, Intersect, Except, etc.
 *   - Map operations: MapKeys, MapValues, MapEntries, MapConcat, etc.
 *   - Collection functions: Slice, ArrayJoin, ArraysZip, Flatten, etc.
 *   - Sequence generation with various data types
 *   - Array sorting and manipulation functions
 *   - Special value handling (NaN, null, infinity)
 */
class RapidsCollectionExpressionsSuite
  extends CollectionExpressionsSuite
  with RapidsTestsTrait {
  // All 48 tests from CollectionExpressionsSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific expression configuration is handled by RapidsTestsTrait

  // GPU-specific test for "Array Intersect"
  // Original test: CollectionExpressionsSuite.scala lines 2168-2278
  // Comment out tests that checks the order of the elements in the result array.
  // See https://github.com/NVIDIA/spark-rapids/issues/13696 for more details.
  testRapids("Array Intersect") {
    val a00 = Literal.create(Seq(1, 2, 4), ArrayType(IntegerType, false))
    val a01 = Literal.create(Seq(4, 2), ArrayType(IntegerType, false))
    val a02 = Literal.create(Seq(1, 2, 1, 4), ArrayType(IntegerType, false))
    // val a03 = Literal.create(Seq(4, 2, 4), ArrayType(IntegerType, false))
    val a04 = Literal.create(Seq(1, 2, null, 4, 5, null), ArrayType(IntegerType, true))
    val a05 = Literal.create(Seq(-5, 4, null, 2, -1, null), ArrayType(IntegerType, true))
    val a06 = Literal.create(Seq.empty[Int], ArrayType(IntegerType, false))
    val abl0 = Literal.create(Seq[Boolean](true, false, true), ArrayType(BooleanType, false))
    val abl1 = Literal.create(Seq[Boolean](true, true), ArrayType(BooleanType, false))
    val ab0 = Literal.create(Seq[Byte](1, 2, 3, 2), ArrayType(ByteType, containsNull = false))
    val ab1 = Literal.create(Seq[Byte](4, 2, 4), ArrayType(ByteType, containsNull = false))
    val as0 = Literal.create(Seq[Short](1, 2, 3, 2), ArrayType(ShortType, containsNull = false))
    val as1 = Literal.create(Seq[Short](4, 2, 4), ArrayType(ShortType, containsNull = false))
    val af0 = Literal.create(Seq[Float](1.1F, 2.2F, 3.3F, 2.2F), ArrayType(FloatType, false))
    val af1 = Literal.create(Seq[Float](4.4F, 2.2F, 4.4F), ArrayType(FloatType, false))
    val ad0 = Literal.create(Seq[Double](1.1, 2.2, 3.3, 2.2), ArrayType(DoubleType, false))
    val ad1 = Literal.create(Seq[Double](4.4, 2.2, 4.4), ArrayType(DoubleType, false))

    // val a10 = Literal.create(Seq(1L, 2L, 4L), ArrayType(LongType, false))
    // val a11 = Literal.create(Seq(4L, 2L), ArrayType(LongType, false))
    val a12 = Literal.create(Seq(1L, 2L, 1L, 4L), ArrayType(LongType, false))
    // val a13 = Literal.create(Seq(4L, 2L, 4L), ArrayType(LongType, false))
    val a14 = Literal.create(Seq(1L, 2L, null, 4L, 5L, null), ArrayType(LongType, true))
    // val a15 = Literal.create(Seq(-5L, 4L, null, 2L, -1L, null), ArrayType(LongType, true))
    val a16 = Literal.create(Seq.empty[Long], ArrayType(LongType, false))

    val a20 = Literal.create(Seq("b", "a", "c"), ArrayType(StringType, false))
    val a21 = Literal.create(Seq("c", "a"), ArrayType(StringType, false))
    // val a22 = Literal.create(Seq("b", "a", "c", "a"), ArrayType(StringType, false))
    val a23 = Literal.create(Seq("c", "a", null, "f"), ArrayType(StringType, true))
    val a24 = Literal.create(Seq("b", null, "a", "g", null), ArrayType(StringType, true))
    val a25 = Literal.create(Seq.empty[String], ArrayType(StringType, false))

    val a30 = Literal.create(Seq(null, null), ArrayType(IntegerType))
    val a31 = Literal.create(null, ArrayType(StringType))

    // checkEvaluation(ArrayIntersect(a00, a01), Seq(2, 4))
    // checkEvaluation(ArrayIntersect(a01, a00), Seq(4, 2))
    // checkEvaluation(ArrayIntersect(a02, a03), Seq(2, 4))
    // checkEvaluation(ArrayIntersect(a03, a02), Seq(4, 2))
    // checkEvaluation(ArrayIntersect(a00, a04), Seq(1, 2, 4))
    // checkEvaluation(ArrayIntersect(a04, a05), Seq(2, null, 4))
    checkEvaluation(ArrayIntersect(a02, a06), Seq.empty)
    checkEvaluation(ArrayIntersect(a06, a04), Seq.empty)
    checkEvaluation(ArrayIntersect(abl0, abl1), Seq[Boolean](true))
    checkEvaluation(ArrayIntersect(ab0, ab1), Seq[Byte](2))
    checkEvaluation(ArrayIntersect(as0, as1), Seq[Short](2))
    checkEvaluation(ArrayIntersect(af0, af1), Seq[Float](2.2F))
    checkEvaluation(ArrayIntersect(ad0, ad1), Seq[Double](2.2D))

    // checkEvaluation(ArrayIntersect(a10, a11), Seq(2L, 4L))
    // checkEvaluation(ArrayIntersect(a11, a10), Seq(4L, 2L))
    // checkEvaluation(ArrayIntersect(a12, a13), Seq(2L, 4L))
    // checkEvaluation(ArrayIntersect(a13, a12), Seq(4L, 2L))
    // checkEvaluation(ArrayIntersect(a14, a15), Seq(2L, null, 4L))
    checkEvaluation(ArrayIntersect(a12, a16), Seq.empty)
    checkEvaluation(ArrayIntersect(a16, a14), Seq.empty)

    // checkEvaluation(ArrayIntersect(a20, a21), Seq("a", "c"))
    // checkEvaluation(ArrayIntersect(a21, a20), Seq("c", "a"))
    // checkEvaluation(ArrayIntersect(a22, a21), Seq("a", "c"))
    // checkEvaluation(ArrayIntersect(a21, a22), Seq("c", "a"))
    // checkEvaluation(ArrayIntersect(a23, a24), Seq("a", null))
    // checkEvaluation(ArrayIntersect(a24, a23), Seq(null, "a"))
    checkEvaluation(ArrayIntersect(a24, a25), Seq.empty)
    checkEvaluation(ArrayIntersect(a25, a24), Seq.empty)

    checkEvaluation(ArrayIntersect(a30, a30), Seq(null))
    checkEvaluation(ArrayIntersect(a20, a31), null)
    checkEvaluation(ArrayIntersect(a31, a20), null)

    val b0 = Literal.create(
      Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2), Array[Byte](3, 4)),
      ArrayType(BinaryType))
    val b1 = Literal.create(
      Seq[Array[Byte]](Array[Byte](2, 1), Array[Byte](3, 4), Array[Byte](5, 6)),
      ArrayType(BinaryType))
    val b2 = Literal.create(
      Seq[Array[Byte]](Array[Byte](3, 4), Array[Byte](1, 2), Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val b3 = Literal.create(Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](3, 4), null),
      ArrayType(BinaryType))
    val b4 = Literal.create(Seq[Array[Byte]](null, Array[Byte](3, 4), null),
      ArrayType(BinaryType))
    val b5 = Literal.create(Seq.empty, ArrayType(BinaryType))
    val arrayWithBinaryNull = Literal.create(Seq(null), ArrayType(BinaryType))
    checkEvaluation(ArrayIntersect(b0, b1),
      Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](3, 4)))
    checkEvaluation(ArrayIntersect(b1, b0),
      Seq[Array[Byte]](Array[Byte](3, 4), Array[Byte](5, 6)))
    checkEvaluation(ArrayIntersect(b0, b2),
      Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](3, 4)))
    checkEvaluation(ArrayIntersect(b2, b0),
      Seq[Array[Byte]](Array[Byte](3, 4), Array[Byte](1, 2)))
    checkEvaluation(ArrayIntersect(b2, b3),
      Seq[Array[Byte]](Array[Byte](3, 4), Array[Byte](1, 2)))
    checkEvaluation(ArrayIntersect(b3, b2),
      Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](3, 4)))
    checkEvaluation(ArrayIntersect(b3, b4), Seq[Array[Byte]](Array[Byte](3, 4), null))
    checkEvaluation(ArrayIntersect(b4, b3), Seq[Array[Byte]](null, Array[Byte](3, 4)))
    checkEvaluation(ArrayIntersect(b4, b5), Seq.empty)
    checkEvaluation(ArrayIntersect(b5, b4), Seq.empty)
    checkEvaluation(ArrayIntersect(b4, arrayWithBinaryNull), Seq[Array[Byte]](null))

    val aa0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4), Seq[Int](1, 2)),
      ArrayType(ArrayType(IntegerType)))
    val aa1 = Literal.create(Seq[Seq[Int]](Seq[Int](3, 4), Seq[Int](2, 1), Seq[Int](3, 4)),
      ArrayType(ArrayType(IntegerType)))
    checkEvaluation(ArrayIntersect(aa0, aa1), Seq[Seq[Int]](Seq[Int](3, 4)))
    checkEvaluation(ArrayIntersect(aa1, aa0), Seq[Seq[Int]](Seq[Int](3, 4)))

    assert(ArrayIntersect(a00, a01).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(ArrayIntersect(a00, a04).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(ArrayIntersect(a04, a05).dataType.asInstanceOf[ArrayType].containsNull)
    assert(ArrayIntersect(a20, a21).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(ArrayIntersect(a23, a24).dataType.asInstanceOf[ArrayType].containsNull)
  }

  // GPU-specific test for "Shuffle"
  // Original test: CollectionExpressionsSuite.scala lines 1973-2038
  // Adjust the expected results to match the running by --master local[2].
  testRapids("Shuffle") {
    // Primitive-type elements
    val ai0 = Literal.create(Seq(1, 2, 3, 4, 5), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai2 = Literal.create(Seq(null, 1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ai3 = Literal.create(Seq(2, null, 4, null), ArrayType(IntegerType, containsNull = true))
    val ai4 = Literal.create(Seq(null, null, null), ArrayType(IntegerType, containsNull = true))
    val ai5 = Literal.create(Seq(1), ArrayType(IntegerType, containsNull = false))
    val ai6 = Literal.create(Seq.empty, ArrayType(IntegerType, containsNull = false))
    val ai7 = Literal.create(null, ArrayType(IntegerType, containsNull = true))

    checkEvaluation(Shuffle(ai0, Some(0)), Seq(2, 1, 5, 4, 3))
    checkEvaluation(Shuffle(ai1, Some(0)), Seq(2, 1, 3))
    checkEvaluation(Shuffle(ai2, Some(0)), Seq(1, null, null, 3))
    checkEvaluation(Shuffle(ai3, Some(0)), Seq(null, 2, 4, null))
    checkEvaluation(Shuffle(ai4, Some(0)), Seq(null, null, null))
    checkEvaluation(Shuffle(ai5, Some(0)), Seq(1))
    checkEvaluation(Shuffle(ai6, Some(0)), Seq.empty)
    checkEvaluation(Shuffle(ai7, Some(0)), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("a", "b", "c", "d"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as2 = Literal.create(Seq(null, "a", null, "c"), ArrayType(StringType, containsNull = true))
    val as3 = Literal.create(Seq("b", null, "d", null), ArrayType(StringType, containsNull = true))
    val as4 = Literal.create(Seq(null, null, null), ArrayType(StringType, containsNull = true))
    val as5 = Literal.create(Seq("a"), ArrayType(StringType, containsNull = false))
    val as6 = Literal.create(Seq.empty, ArrayType(StringType, containsNull = false))
    val as7 = Literal.create(null, ArrayType(StringType, containsNull = true))
    val aa = Literal.create(
      Seq(Seq("a", "b"), Seq("c", "d"), Seq("e")),
      ArrayType(ArrayType(StringType)))

    checkEvaluation(Shuffle(as0, Some(0)), Seq("b", "a", "c", "d"))
    checkEvaluation(Shuffle(as1, Some(0)), Seq("b", "a", "c"))
    checkEvaluation(Shuffle(as2, Some(0)), Seq("a", null, null, "c"))
    checkEvaluation(Shuffle(as3, Some(0)), Seq(null, "b", "d", null))
    checkEvaluation(Shuffle(as4, Some(0)), Seq(null, null, null))
    checkEvaluation(Shuffle(as5, Some(0)), Seq("a"))
    checkEvaluation(Shuffle(as6, Some(0)), Seq.empty)
    checkEvaluation(Shuffle(as7, Some(0)), null)
    checkEvaluation(Shuffle(aa, Some(0)), Seq(Seq("c", "d"), Seq("a", "b"), Seq("e")))

    val r = new Random(1234)
    val seed1 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Shuffle(ai0, seed1)) ===
      evaluateWithoutCodegen(Shuffle(ai0, seed1)))
    assert(evaluateWithMutableProjection(Shuffle(ai0, seed1)) ===
      evaluateWithMutableProjection(Shuffle(ai0, seed1)))
    assert(evaluateWithUnsafeProjection(Shuffle(ai0, seed1)) ===
      evaluateWithUnsafeProjection(Shuffle(ai0, seed1)))

    val seed2 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Shuffle(ai0, seed1)) !==
      evaluateWithoutCodegen(Shuffle(ai0, seed2)))
    assert(evaluateWithMutableProjection(Shuffle(ai0, seed1)) !==
      evaluateWithMutableProjection(Shuffle(ai0, seed2)))
    assert(evaluateWithUnsafeProjection(Shuffle(ai0, seed1)) !==
      evaluateWithUnsafeProjection(Shuffle(ai0, seed2)))

    val shuffle = Shuffle(ai0, seed1)
    assert(shuffle.fastEquals(shuffle))
    assert(!shuffle.fastEquals(Shuffle(ai0, seed1)))
    assert(!shuffle.fastEquals(shuffle.freshCopy()))
    assert(!shuffle.fastEquals(Shuffle(ai0, seed2)))
  }

  // Verify GPU matches CPU for the mixed edge case:
  // array contains BOTH a null struct entry AND a valid struct with a null key.
  // CPU short-circuits on the first null struct entry and returns null without
  // ever inspecting the null key in the second entry.
  // GPU should match this behavior (return null, not throw "Cannot use null as map key").
  testRapids("MapFromEntries - mixed null struct entry and null key in valid struct") {
    // arrayType / row are private to CollectionExpressionsSuite, so we inline the construction.
    val entryType = ArrayType(
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))),
      containsNull = true)
    // [null_struct, {null_key, 20}]: null struct entry at index 0, null key at index 1.
    // CPU short-circuits on the first null struct entry and returns null without
    // evaluating subsequent entries.  GPU must match this (return null, not throw).
    val mixed = Literal.create(
      Seq(null, InternalRow.fromSeq(Seq(null, 20))),
      entryType)
    checkEvaluation(MapFromEntries(mixed), null)
  }
}
