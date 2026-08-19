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

import org.apache.spark.sql.DatasetSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for Dataset operations.
 *
 * This test suite validates Dataset operation execution on GPU.
 * It extends the original Spark DatasetSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/DatasetSuite.scala
 * Test count: 161 tests
 *
 * Migration notes:
 * - DatasetSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper,
 *   so we use RapidsSQLTestsTrait
 * - This is a comprehensive Dataset test suite covering:
 *   - Typed Dataset operations (map, filter, flatMap, etc.)
 *   - Encoders and serialization
 *   - Joins with typed Datasets
 *   - Aggregations with typed APIs
 *   - Conversions between Dataset and DataFrame
 *   - Type-safe operations
 */
class RapidsDatasetSuite
  extends DatasetSuite
  with RapidsSQLTestsTrait {
  import testImplicits._

  // All 161 tests from DatasetSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific Dataset configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "groupBy single field class, count"
  // Original test: DatasetSuite.scala lines 576-584
  // This test is modified to sort the results for consistent ordering
  testRapids("groupBy single field class, count") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val count = ds.groupByKey(s => Tuple1(s.length)).count()

    // Sort the results for consistent ordering
    val sortedCount = count.collect().sortBy { case (Tuple1(len), _) => len }

    // Check against expected sorted results
    assert(sortedCount.toSeq === Seq((Tuple1(3), 2L), (Tuple1(5), 1L)))
  }

  // GPU-specific test for "dropDuplicates"
  // Original test: DatasetSuite.scala lines 1306-1317
  // This test is modified to sort the results for consistent ordering
  testRapids("dropDuplicates") {
    val ds = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    checkDataset(
      ds.dropDuplicates("_1"),
      ("a", 1), ("b", 1))
    checkDataset(
      ds.dropDuplicates("_2"),
      ("a", 1), ("a", 2))
    // Sort the results for consistent ordering
    val sortedCount = ds.dropDuplicates("_1", "_2").collect().sortBy { case (t1, _) => t1 }
    assert(sortedCount.toSeq === Seq(("a", 1), ("a", 2), ("b", 1)))
  }

  // GPU-specific test for "dropDuplicates: columns with same column name"
  // Original test: DatasetSuite.scala lines 1319-1327
  testRapids("dropDuplicates: columns with same column name") {
    val ds1 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    val ds2 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    // The dataset joined has two columns of the same name "_2".
    val joined = ds1.join(ds2, "_1").select(ds1("_2").as[Int], ds2("_2").as[Int])

    val sortedCount = joined.dropDuplicates().collect().sortBy { case (t1, _) => t1 }
    assert(sortedCount.toSeq === Seq((1, 2), (1, 1), (2, 1), (2, 2)))
  }

  // GPU-specific test for "SPARK-24762: typed agg on Option[Product] type"
  // Original test: DatasetSuite.scala lines 1889-1895
  testRapids("SPARK-24762: typed agg on Option[Product] type") {
    val ds = Seq(Some((1, 2)), Some((2, 3)), Some((1, 3))).toDS()

    // Sort results for consistent ordering
    val count1 = ds.groupByKey(_.get._1).count().collect().sortBy { case (k, _) => k }
    assert(count1.toSeq === Seq((1, 2), (2, 1)))

    val count2 = ds.groupByKey(x => x).count().collect().sortBy {
      case (Some((a, b)), _) => (a, b)
    }
    assert(count2.toSeq ===
      Seq((Some((1, 2)), 1), (Some((1, 3)), 1), (Some((2, 3)), 1)))
  }

  // GPU-specific test for "Check RelationalGroupedDataset toString: Single data"
  // Original test: DatasetSuite.scala lines 1658-1664
  // In JDK 11+ the RelationalGroupedDataset.toString method returns an empty string for the type.
  testRapids("Check RelationalGroupedDataset toString: Single data") {
    val kvDataset = (1 to 3).toDF("id").groupBy("id")
    val expectedTypeString = if (getJavaMajorVersion() >= 11) "" else "GroupBy"
    val expected = "RelationalGroupedDataset: [" +
      s"grouping expressions: [id: int], value: [id: int], type: ${expectedTypeString}]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  // GPU-specific test for "Check RelationalGroupedDataset toString: over length schema"
  // Original test: DatasetSuite.scala lines 1666-1675
  testRapids("Check RelationalGroupedDataset toString: over length schema") {
    val kvDataset = (1 to 3).map(x => (x, x.toString, x.toLong))
      .toDF("id", "val1", "val2").groupBy("id")
    val expectedTypeString = if (getJavaMajorVersion() >= 11) "" else "GroupBy"
    val expected = "RelationalGroupedDataset:" +
      " [grouping expressions: [id: int]," +
      " value: [id: int, val1: string ... 1 more field]," +
      s" type: ${expectedTypeString}]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }
}
