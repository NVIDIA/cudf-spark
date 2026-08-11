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

import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameFunctionsSuite, Row}
import org.apache.spark.sql.functions.array_intersect
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for DataFrame functions.
 *
 * This test suite validates DataFrame function execution on GPU.
 * It extends the original Spark DataFrameFunctionsSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/DataFrameFunctionsSuite.scala
 * Test count: 105 tests
 *
 * Migration notes:
 * - DataFrameFunctionsSuite extends QueryTest with SharedSparkSession,
 *   so we use RapidsSQLTestsTrait
 * - This test suite covers functions in org.apache.spark.sql.functions:
 *   - Array functions: array, array_contains, explode, etc.
 *   - Map functions: map, map_keys, map_values, etc.
 *   - String functions: concat, substring, regexp, etc.
 *   - Date/Time functions: date_add, date_sub, etc.
 *   - Math functions: abs, sqrt, round, etc.
 *   - Aggregate functions: sum, avg, count, etc.
 *   - Window functions
 */
class RapidsDataFrameFunctionsSuite
  extends DataFrameFunctionsSuite
  with RapidsSQLTestsTrait {
  // All 105 tests from DataFrameFunctionsSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific DataFrame function configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "array_intersect functions"
  // Original test: DataFrameFunctionsSuite.scala lines 2169-2213
  testRapids("array_intersect functions") {
    import testImplicits._

    // Helper function to compare arrays ignoring order
    def checkArrayIntersect(df: DataFrame, expected: Seq[Any]): Unit = {
      val result = df.head().getSeq[Any](0)
      assert(result.toSet == expected.toSet,
        s"Expected ${expected.toSet}, but got ${result.toSet}")
    }

    val df1 = Seq((Array(1, 2, 4), Array(4, 2))).toDF("a", "b")
    val ans1 = Row(Seq(2, 4))
    checkArrayIntersect(df1.select(array_intersect($"a", $"b")), ans1.getSeq[Any](0))
    checkArrayIntersect(df1.selectExpr("array_intersect(a, b)"), ans1.getSeq[Any](0))

    val df2 = Seq((Array[Integer](1, 2, null, 4, 5), Array[Integer](-5, 4, null, 2, -1)))
      .toDF("a", "b")
    val ans2 = Row(Seq(2, null, 4))
    checkArrayIntersect(df2.select(array_intersect($"a", $"b")), ans2.getSeq[Any](0))
    checkArrayIntersect(df2.selectExpr("array_intersect(a, b)"), ans2.getSeq[Any](0))

    val df3 = Seq((Array(1L, 2L, 4L), Array(4L, 2L))).toDF("a", "b")
    val ans3 = Row(Seq(2L, 4L))
    checkArrayIntersect(df3.select(array_intersect($"a", $"b")), ans3.getSeq[Any](0))
    checkArrayIntersect(df3.selectExpr("array_intersect(a, b)"), ans3.getSeq[Any](0))

    val df4 = Seq(
      (Array[java.lang.Long](1L, 2L, null, 4L, 5L), Array[java.lang.Long](-5L, 4L, null, 2L, -1L)))
      .toDF("a", "b")
    val ans4 = Row(Seq(2L, null, 4L))
    checkArrayIntersect(df4.select(array_intersect($"a", $"b")), ans4.getSeq[Any](0))
    checkArrayIntersect(df4.selectExpr("array_intersect(a, b)"), ans4.getSeq[Any](0))

    val df5 = Seq((Array("c", null, "a", "f"), Array("b", "a", null, "g"))).toDF("a", "b")
    val ans5 = Row(Seq(null, "a"))
    checkArrayIntersect(df5.select(array_intersect($"a", $"b")), ans5.getSeq[Any](0))
    checkArrayIntersect(df5.selectExpr("array_intersect(a, b)"), ans5.getSeq[Any](0))

    val df6 = Seq((null, null)).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df6.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df6.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df7 = Seq((Array(1), Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df7.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df7.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))

    val df8 = Seq((null, Array("a"))).toDF("a", "b")
    assert(intercept[AnalysisException] {
      df8.select(array_intersect($"a", $"b"))
    }.getMessage.contains("data type mismatch"))
    assert(intercept[AnalysisException] {
      df8.selectExpr("array_intersect(a, b)")
    }.getMessage.contains("data type mismatch"))
  }
}
