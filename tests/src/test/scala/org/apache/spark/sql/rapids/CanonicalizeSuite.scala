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

package org.apache.spark.sql.rapids

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.types.IntegerType

class CanonicalizeSuite extends AnyFunSuite {
  /* In the future, if we decide to implement the Spark 3.3 algorithm to perform canonicalization
   * this unit test should still pass. We should use the implementation made in
   * https://github.com/apache/spark/pull/37851 (SPARK-40362) as a base.
   */
  test("SPARK-40362: Commutative operator under BinaryComparison") {
    Seq(GpuEqualTo, GpuEqualNullSafe, GpuGreaterThan,
        GpuLessThan, GpuGreaterThanOrEqual, GpuLessThanOrEqual)
      .foreach( bc => {
        assert(bc(GpuAdd($"a", $"b", true)(), Literal(10))
            .semanticEquals(bc(GpuAdd($"b", $"a", true)(), Literal(10))))
      })
  }

  test("checked arithmetic canonicalization preserves grouping") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    def add(
        left: Expression,
        right: Expression,
        failOnError: Boolean = false): GpuAdd =
      GpuAdd(left, right, failOnError = failOnError)()
    def multiply(
        left: Expression,
        right: Expression,
        failOnError: Boolean = false): GpuMultiply =
      GpuMultiply(left, right, failOnError = failOnError)()

    assert(!add(add(a, b, failOnError = true), c, failOnError = true)
      .semanticEquals(add(a, add(b, c, failOnError = true), failOnError = true)))
    assert(!multiply(multiply(a, b, failOnError = true), c, failOnError = true)
      .semanticEquals(multiply(a, multiply(b, c, failOnError = true), failOnError = true)))
    assert(add(a, b, failOnError = true).semanticEquals(add(b, a, failOnError = true)))
    assert(multiply(a, b, failOnError = true)
      .semanticEquals(multiply(b, a, failOnError = true)))
    assert(add(add(a, b), c).semanticEquals(add(a, add(b, c))))
    assert(multiply(multiply(a, b), c).semanticEquals(multiply(a, multiply(b, c))))
  }
}
