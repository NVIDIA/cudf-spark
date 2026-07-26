/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types.DataTypes

class LimitExecSuite extends SparkQueryCompareTestSuite {

  def enableCollectLimitExec(conf: SparkConf = new SparkConf()): SparkConf = {
    enableCsvConf(conf).set("spark.rapids.sql.exec.CollectLimitExec", "true")
  }

  IGNORE_ORDER_testSparkResultsAreEqual("limit more than rows", intCsvDf,
      conf = enableCollectLimitExec()) {
      frame => frame.limit(10).repartition(2)
  }

  IGNORE_ORDER_testSparkResultsAreEqual("limit less than rows equal to batchSize", intCsvDf,
    conf = enableCollectLimitExec(makeBatchedBytes(1))) {
    frame => frame.limit(1).repartition(2)
  }

  IGNORE_ORDER_testSparkResultsAreEqual("limit less than rows applied with batches pending",
    intCsvDf,
    conf = enableCollectLimitExec(makeBatchedBytes(3))) {
    frame => frame.limit(2).repartition(2)
  }

  IGNORE_ORDER_testSparkResultsAreEqual("limit with no real columns", intCsvDf,
    conf = enableCollectLimitExec(makeBatchedBytes(3)), repart = 0) {
    frame => frame.limit(2).selectExpr("pi()")
  }

  IGNORE_ORDER_testSparkResultsAreEqual("collect with limit", testData,
    conf = enableCollectLimitExec()) {
    frame => {
      import frame.sparkSession.implicits._
      val results: Array[Row] = frame.limit(16).repartition(2).collect()
      results.map(row => if (row.isNullAt(0)) 0 else row.getInt(0)).toSeq.toDF("c0")
    }
  }

  // The unordered limit result is materialized into a LocalTableScan whose canonicalized plan
  // includes row data. RAPIDS shuffle can return rows in a different order across runs, so the
  // canonicalization comparison is not meaningful here; CPU/GPU result comparison still runs.
  IGNORE_ORDER_testSparkResultsAreEqual("collect with limit, repart=4", intCsvDf,
    conf = enableCollectLimitExec(), repart = 4, skipCanonicalizationCheck = true) {
    frame => {
      import frame.sparkSession.implicits._
      val results: Array[Row] = frame.limit(16).collect()
      results.map(row => if (row.isNullAt(0)) 0 else row.getInt(0)).toSeq.toDF("c0")
    }
  }

  private def testData(spark: SparkSession): DataFrame = {
    FuzzerUtils.generateDataFrame(spark, FuzzerUtils.createSchema(Seq(DataTypes.IntegerType)), 100)
  }

  IGNORE_ORDER_testSparkResultsAreEqual("nested limit", testNested, conf = new SparkConf()
      .set("spark.sql.execution.sortBeforeRepartition", "false")) {
    frame => {
      frame.limit(2).repartition(2)
    }
  }

  private def testNested(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, ("2", Seq(("2", "2")).toMap, Array(1L, 2L)), Array[Int]()),
      (3, ("4", Seq(("4", "4")).toMap, Array(3L, 4L)), Array(3)),
      (5, ("6", Seq(("6", "6")).toMap, Array(5L, 6L)), Array(5)),
      (7, ("8", Seq(("8", "8")).toMap, Array(7L, 8L)), Array(7)),
      (9, ("10", Seq(("10", "10")).toMap, Array(9L, 10L)), Array(9)))
        .toDF("a", "b", "c")
  }

  test("collect limit uses GpuLocalLimitExec for columnar child") {
    val conf = enableCollectLimitExec()
    withGpuSparkSession(spark => {
      val df = spark.range(100).limit(5)
      val plan = df.queryExecution.executedPlan
      assert(
        plan.find(_.isInstanceOf[GpuLocalLimitExec]).isDefined,
        "Expected GpuLocalLimitExec for columnar child" +
          s"\n${plan.treeString}")
    }, conf)
  }

  test("collect limit falls back to CPU for row-based child") {
    val conf = enableCollectLimitExec()
      .set("spark.rapids.sql.exec.RangeExec", "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "CollectLimitExec,RangeExec")
    withGpuSparkSession(spark => {
      val df = spark.range(100).limit(5)
      val plan = df.queryExecution.executedPlan
      assert(
        plan.find(_.isInstanceOf[CollectLimitExec]).isDefined,
        "Expected CPU CollectLimitExec when child is row-based" +
          s"\n${plan.treeString}")
      assert(
        plan.find(_.isInstanceOf[GpuLocalLimitExec]).isEmpty,
        "GpuLocalLimitExec should not appear when " +
          s"CollectLimitExec falls back to CPU\n${plan.treeString}")
    }, conf)
  }
}
