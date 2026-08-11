/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import org.apache.spark.sql.{DataFrameAggregateSuite, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.types._

class RapidsDataFrameAggregateSuite extends DataFrameAggregateSuite with RapidsSQLTestsTrait {
  import testImplicits._

  testRapids("collect functions") {
    val df = Seq((1, 2), (2, 2), (3, 4)).toDF("a", "b")
    checkAnswer(
      df.select(sort_array(collect_list($"a")), sort_array(collect_list($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(2, 2, 4)))
    )
    checkAnswer(
      df.select(sort_array(collect_set($"a")), sort_array(collect_set($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(2, 4)))
    )

    checkDataset(
      df.select(sort_array(collect_set($"a")).as("aSet")).as[Set[Int]],
      Set(1, 2, 3))
    checkDataset(
      df.select(sort_array(collect_set($"b")).as("bSet")).as[Set[Int]],
      Set(2, 4))
    checkDataset(
      df.select(sort_array(collect_set($"a")), sort_array(collect_set($"b")))
        .as[(Set[Int], Set[Int])], Seq(Set(1, 2, 3) -> Set(2, 4)): _*)
  }

  testRapids("collect functions structs") {
    val df = Seq((1, 2, 2), (2, 2, 2), (3, 4, 1))
      .toDF("a", "x", "y")
      .select($"a", struct($"x", $"y").as("b"))
    checkAnswer(
      df.select(sort_array(collect_list($"a")), sort_array(collect_list($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(Row(2, 2), Row(2, 2), Row(4, 1))))
    )
    checkAnswer(
      df.select(sort_array(collect_set($"a")), sort_array(collect_set($"b"))),
      Seq(Row(Seq(1, 2, 3), Seq(Row(2, 2), Row(4, 1))))
    )
  }

  testRapids("SPARK-17641: collect functions should not collect null values") {
    val df = Seq(("1", 2), (null, 2), ("1", 4)).toDF("a", "b")
    checkAnswer(
      df.select(sort_array(collect_list($"a")), sort_array(collect_list($"b"))),
      Seq(Row(Seq("1", "1"), Seq(2, 2, 4)))
    )
    checkAnswer(
      df.select(sort_array(collect_set($"a")), sort_array(collect_set($"b"))),
      Seq(Row(Seq("1"), Seq(2, 4)))
    )
  }

  testRapids("collect functions should be able to cast to array type with no null values") {
    val df = Seq(1, 2).toDF("a")
    checkAnswer(df.select(sort_array(collect_list("a")) cast ArrayType(IntegerType, false)),
      Seq(Row(Seq(1, 2))))
    checkAnswer(df.select(sort_array(collect_set("a")) cast ArrayType(FloatType, false)),
      Seq(Row(Seq(1.0, 2.0))))
  }

  testRapids("SPARK-24788: RelationalGroupedDataset.toString " +
    "with unresolved exprs should not fail") {
    // Checks if these raise no exception
    val expected = if (getJavaMajorVersion() >= 11) "" else "GroupBy"
    assert(testData.groupBy($"key").toString.contains(
      s"[grouping expressions: [key], value: [key: int, value: string], type: ${expected}]"))
    assert(testData.groupBy(col("key")).toString.contains(
      s"[grouping expressions: [key], value: [key: int, value: string], type: ${expected}]"))
    assert(testData.groupBy(current_date()).toString.contains(
      "grouping expressions: [current_date(None)], value: [key: int, value: string], " +
        s"type: ${expected}]"))
  }
}
