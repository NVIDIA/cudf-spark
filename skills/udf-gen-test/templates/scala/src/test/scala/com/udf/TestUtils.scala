/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import org.apache.spark.sql.{DataFrame, Row}

/**
 * Shared test utilities.
 */
object TestUtils {

  /**
   * Compare two DataFrames row-by-row, reporting per-column mismatches.
   *
   * @param rtol if > 0, floats/doubles match within this tolerance;
   *             if <= 0 (default) they must be bit-identical.
   */
  def assertDataFrameEquals(
    actual: DataFrame,
    expected: DataFrame,
    rtol: Double = 0.0
  ): Unit = {
    assert(actual.schema == expected.schema,
      s"Schema mismatch:\n  actual:   ${actual.schema}\n  expected: ${expected.schema}")

    val actualRows = actual.collect().sortBy(alignKey)
    val expectedRows = expected.collect().sortBy(alignKey)

    assert(actualRows.length == expectedRows.length,
      s"Row count mismatch: actual=${actualRows.length}, expected=${expectedRows.length}")

    val fields = actual.schema.fieldNames
    val mismatches = scala.collection.mutable.ArrayBuffer.empty[String]
    for (i <- actualRows.indices) {
      val aRow = actualRows(i)
      val eRow = expectedRows(i)
      for (idx <- fields.indices) {
        val aVal = aRow.get(idx)
        val eVal = eRow.get(idx)
        if (!compare(aVal, eVal, rtol)) {
          mismatches += s"  [row $i] ${fields(idx)}: actual=$aVal, expected=$eVal"
        }
      }
    }

    if (mismatches.nonEmpty) {
      throw new AssertionError(
        s"\nFound ${mismatches.length} column-level mismatches:\n${mismatches.mkString("\n")}\n")
    }
  }

  /**
   * Deep value-equality, adapted from
   * https://github.com/NVIDIA/cudf-spark/blob/main/tests/src/test/scala/com/nvidia/spark/rapids/SparkQueryCompareTestSuite.scala
   */
  def compare(actual: Any, expected: Any, rtol: Double = 0.0): Boolean = {
    (actual, expected) match {
      case (a: Float, b: Float) if a.isNaN && b.isNaN => true
      case (a: Double, b: Double) if a.isNaN && b.isNaN => true
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r, rtol) }
      case (a: scala.collection.Map[_, _], b: scala.collection.Map[_, _]) =>
        val am = a.asInstanceOf[scala.collection.Map[Any, Any]]
        val bm = b.asInstanceOf[scala.collection.Map[Any, Any]]
        am.size == bm.size && am.keys.forall { aKey =>
          bm.keys.find(bKey => compare(aKey, bKey))
            .exists(bKey => compare(am(aKey), bm(bKey), rtol))
        }
      case (a: scala.collection.Iterable[_], b: scala.collection.Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r, rtol) }
      case (a: Product, b: Product) =>
        compare(a.productIterator.toSeq, b.productIterator.toSeq, rtol)
      case (a: Row, b: Row) =>
        compare(a.toSeq, b.toSeq, rtol)
      // bit-compare when there is no tolerance, so 0.0 and -0.0 are treated as different.
      case (a: Double, b: Double) if rtol <= 0 =>
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      case (a: Double, b: Double) =>
        if (b != 0) Math.abs((a - b) / b) <= rtol else Math.abs(a - b) <= rtol
      case (a: Float, b: Float) if rtol <= 0 =>
        java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
      case (a: Float, b: Float) =>
        if (b != 0) Math.abs((a - b) / b) <= rtol else Math.abs(a - b) <= rtol
      case (a, b) => a == b
    }
  }

  /**
   * Canonical sort key used to align rows.
   * Maps or anything that might contain a map needs special handling to sort the map, since
   * `toString` by default uses map insertion order. For other types `toString` suffices.
   */
  private def alignKey(v: Any): String = v match {
    case null => "∅"
    case r: Row => r.toSeq.map(alignKey).mkString("(", ",", ")")
    case m: scala.collection.Map[_, _] =>
      m.iterator.map { case (k, vv) => s"${alignKey(k)}=${alignKey(vv)}" }.toSeq.sorted
        .mkString("{", ",", "}")
    case s: scala.collection.Seq[_] => s.map(alignKey).mkString("[", ",", "]")
    case a: Array[_] => a.map(alignKey).mkString("[", ",", "]")
    case str: String => "s" + str.length + ":" + str  // prefix prevents string contents from coincidentally matching other types
    case x => x.toString
  }
}
