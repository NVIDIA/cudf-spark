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

package com.nvidia.spark.rapids.perf

import java.lang.management.ManagementFactory

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference,
  DynamicPruningExpression, EqualTo, Expression, ExprId, GreaterThan, In, InSet, IsNotNull,
  Literal}
import org.apache.spark.sql.execution.{InSubqueryExec, LeafExecNode, SubqueryExec}
import org.apache.spark.sql.types.{DateType, DecimalType, LongType, StructField, StructType}

/** A leaf node with the given output, built the same way on every Spark version. */
private case class Leaf(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
}

/** The plan key of a file scan and of any other plan node. */
class PlanKeySuite extends AnyFunSuite {

  /** The lineitem scan of the NDS-H runs: read columns, one date filter and not-null checks. */
  private val lineitem = StructType(Seq(
    StructField("l_orderkey", LongType),
    StructField("l_discount", DecimalType(11, 2)),
    StructField("l_extendedprice", DecimalType(11, 2)),
    StructField("l_shipdate", DateType)))

  private val roots = Seq(new Path("hdfs://sparkb-09.nvidia.com:8020/data/ndsh/lineitem"))

  /** Fresh attributes every time, as Spark creates them in every run and session. */
  private def shipdate = AttributeReference("l_shipdate", DateType)()
  private def orderkey = AttributeReference("l_orderkey", LongType)()

  private def keyOf(
      dataFilters: Seq[Expression],
      partitionFilters: Seq[Expression] = Nil,
      schema: StructType = lineitem): PlanKey =
    PlanKey.forFileScan(None, roots, schema, partitionFilters, dataFilters).get

  private def lineitemKey(day: String): PlanKey = keyOf(Seq(
    GreaterThan(shipdate, Literal(java.sql.Date.valueOf(day))),
    IsNotNull(orderkey),
    IsNotNull(shipdate)))

  test("the lineitem key is pinned, and another day changes only the literals") {
    val key = lineitemKey("1996-12-31")
    assert(key.planHex == "d2cc295d9a40798d" && key.literalsHex == "06effab8c21a4570", key)
    assert(key.source == "lineitem" && key.literalsText == "1996-12-31")
    assert(key == lineitemKey("1996-12-31"), "deterministic, with fresh attributes each time")
    val other = lineitemKey("1996-06-30")
    assert(other.plan == key.plan && other.literals != key.literals)
    assert(other.literalsText == "1996-06-30")
  }

  test("conjunct order does not change the key, the values stay with their columns") {
    val x = AttributeReference("x", LongType)()
    val y = AttributeReference("y", LongType)()
    val xy = keyOf(Seq(And(EqualTo(x, Literal(1L)), EqualTo(y, Literal(2L)))))
    assert(xy == keyOf(Seq(And(EqualTo(y, Literal(2L)), EqualTo(x, Literal(1L))))))
    val xAgain = EqualTo(x, Literal(1L))
    assert(xy == keyOf(Seq(EqualTo(y, Literal(2L)), xAgain, xAgain)), "and a repeated conjunct")
    val swapped = keyOf(Seq(EqualTo(x, Literal(2L)), EqualTo(y, Literal(1L))))
    assert(swapped.plan == xy.plan && swapped.literals != xy.literals)
    val columns = lineitem.fields
    assert(keyOf(Nil, schema = StructType(columns.reverse)) == keyOf(Nil), "column order")
    assert(keyOf(Nil, schema = StructType(columns.drop(1))).plan != keyOf(Nil).plan)
  }

  /** A dynamic pruning filter; `keys` is what its broadcast produced, if it has run. */
  private def pruning(keys: Option[Array[Any]], id: Long): Expression =
    DynamicPruningExpression(InSubqueryExec(shipdate,
      SubqueryExec("dynamicpruning", Leaf(Nil)), ExprId(id), false, null,
      keys.orNull))

  test("an IN list or a dynamic pruning key set goes into the literals, in any order") {
    def in(values: Long*): Expression = In(orderkey, values.map(Literal(_)))
    val three = keyOf(Seq(in(3, 1, 2)))
    assert(keyOf(Seq(in(2, 1))).plan == three.plan && keyOf(Seq(in(2, 1))) != three)
    assert(keyOf(Seq(in(1, 2, 3))) == three, "order of the list")
    assert(keyOf(Seq(InSet(orderkey, Set(3L, 1L, 2L)))) == three, "a set is a list")

    val first = keyOf(Nil, Seq(pruning(Some(Array[Any](3, 1, 2)), 7L)))
    assert(first == keyOf(Nil, Seq(pruning(Some(Array[Any](2, 3, 1)), 99L))),
      "neither the order of the keys nor the subquery id matters")
    val unresolved = keyOf(Nil, Seq(pruning(None, 1L)))
    assert(unresolved.plan == first.plan && unresolved.literals != first.literals)
    assert(unresolved.literalsText == "unresolved" && first.literalsText == "3,1,2")
  }

  test("no literals gives NONE, and a scan with neither a name nor roots has no key") {
    assert(keyOf(Seq(IsNotNull(orderkey))).literals == PlanKey.NONE)
    assert(PlanKey.forFileScan(None, Nil, lineitem, Nil, Nil).isEmpty)
    assert(keyOf(Seq(IsNotNull(orderkey))).literalsText == "-")
  }

  test("a table name or the set of normalized roots identifies the scan") {
    def key(table: Option[String], paths: String*): PlanKey =
      PlanKey.forFileScan(table, paths.map(new Path(_)), lineitem, Nil, Nil).get
    val (a, b) = ("hdfs://nn:8020/data/a", "hdfs://nn:8020/data/b")
    val byPath = key(None, a, b)
    assert(key(Some("db.t"), a) == key(Some("db.t"), b) && key(Some("db.t"), a) != key(None, a))
    assert(key(None, "hdfs://user@nn:8020/data/a/", b) == byPath, "user info, trailing /")
    assert(key(None, b, a, b) == byPath, "order of the roots, a repeated root")
    assert(key(None, a, "hdfs://nn:8020/data/c") != byPath)
  }

  test("the text is readable, capped and only built when asked for") {
    val many = keyOf(Seq(In(orderkey, (1 to 5000).map(i => Literal(i.toLong))),
      GreaterThan(shipdate, Literal(java.sql.Date.valueOf("1996-12-31")))))
    assert(many.literalsText.length <= 103 && many.literalsText.endsWith("..."))
    assert(many.text.length < 1200 && many.text.startsWith("lineitem columns=["), many.text)
    assert(!many.text.contains("#"), many.text)
    var built = false
    new PlanKey(1L, 2L, "s", _ => { built = true; "" })
    assert(!built)
  }

  test("distinct keys do not collide over a few thousand generated ones") {
    val keys = (0 until 4000).map { i =>
      keyOf(Seq(EqualTo(AttributeReference(s"c$i", LongType)(), Literal(i * 7L))))
    }
    assert(keys.map(_.plan).distinct.size == keys.size)
    assert(keys.map(_.literals).distinct.size == keys.size)
  }

  test("timing: a scan of 2,000 conjuncts and an IN list of 5,000 values") {
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    def measured(name: String)(body: => Any): Unit = {
      val bytes = bean.getThreadAllocatedBytes(Thread.currentThread.getId)
      val start = System.nanoTime()
      body
      val ms = (System.nanoTime() - start) / 1e6
      val kib = (bean.getThreadAllocatedBytes(Thread.currentThread.getId) - bytes) / 1024
      info(f"$name: $ms%.2f ms, $kib KiB allocated")
    }
    val conjuncts = (0 until 2000).map(i =>
      EqualTo(AttributeReference(s"c$i", LongType)(), Literal(i.toLong)): Expression) :+
      In(orderkey, (0 until 5000).map(i => Literal(i.toLong)))
    measured("scan key, first call")(keyOf(conjuncts))
    measured("scan key, second call")(keyOf(conjuncts))
  }
}
