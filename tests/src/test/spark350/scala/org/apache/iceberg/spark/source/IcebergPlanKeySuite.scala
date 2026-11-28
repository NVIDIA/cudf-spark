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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["400"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.perf.PlanKey
import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.{Binder, Expression, Expressions}
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

/** The history key of an Iceberg scan from its Iceberg filters, without a Spark session. */
class IcebergPlanKeySuite extends AnyFunSuite {

  private val read = StructType(Seq(StructField("id", LongType), StructField("p", IntegerType),
    StructField("s", StringType)))

  private def key(filters: Expression*): PlanKey = IcebergPlanKey.of("db.t", read, filters, Nil).get

  test("the order of conjuncts and of IN values does not change the key") {
    val a = Expressions.equal("p", 1)
    val b = Expressions.greaterThan("id", 5L)
    assert(key(a, b) == key(b, a))
    assert(key(Expressions.and(a, b)) == key(Expressions.and(b, a)))
    assert(key(Expressions.in("p", 1, 2, 3)) == key(Expressions.in("p", 3, 1, 2)))
  }

  test("another value changes only the literals hash") {
    for ((x, y) <- Seq(
        Expressions.equal("p", 1) -> Expressions.equal("p", 2),
        Expressions.greaterThan("id", 5L) -> Expressions.greaterThan("id", 6L),
        Expressions.equal("s", "a") -> Expressions.equal("s", "b"),
        Expressions.in("p", 1, 2) -> Expressions.in("p", 1, 3))) {
      assert(key(x).plan == key(y).plan, x)
      assert(key(x).literals != key(y).literals, x)
    }
  }

  test("another operator, column or table changes the plan hash") {
    val base = key(Expressions.equal("p", 1))
    assert(key(Expressions.notEqual("p", 1)).plan != base.plan)
    assert(key(Expressions.lessThan("p", 1)).plan != base.plan)
    assert(key(Expressions.equal("id", 1)).plan != base.plan)
    assert(key(Expressions.isNull("p")).plan != base.plan)
    assert(IcebergPlanKey.of("db.u", read, Seq(Expressions.equal("p", 1)), Nil).get.plan !=
      base.plan)
    assert(key().literals == PlanKey.NONE)
    assert(key(Expressions.isNull("p")).literals == PlanKey.NONE)
  }

  test("runtime filters are part of the key, their values in the literals hash") {
    val pushed = Seq(Expressions.greaterThan("id", 5L))
    val without = IcebergPlanKey.of("db.t", read, pushed, Nil).get
    val one = IcebergPlanKey.of("db.t", read, pushed, Seq(Expressions.in("p", 1, 2))).get
    val other = IcebergPlanKey.of("db.t", read, pushed, Seq(Expressions.in("p", 3))).get
    assert(one.plan != without.plan)
    assert(one.plan == other.plan && one.literals != other.literals)
  }

  test("a bound filter keys like the unbound one") {
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "p", Types.IntegerType.get()),
      Types.NestedField.optional(3, "s", Types.StringType.get()))
    val filters = Seq(Expressions.and(Expressions.greaterThan("id", 5L), Expressions.in("p", 1, 2)),
      Expressions.startsWith("s", "ab"), Expressions.notNull("s"))
    val bound = filters.map(f => Binder.bind(schema.asStruct(), f, true))
    assert(key(bound: _*) == key(filters: _*))
  }
}
