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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.GpuIn
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
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

  test("GpuIn list order") {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val left = GpuIn(a, Seq(1, 2), Seq(b, c))
    val right = GpuIn(a, Seq(2, 1), Seq(c, b))

    assert(left.semanticEquals(right))
  }
}
