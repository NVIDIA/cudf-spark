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

import com.nvidia.spark.rapids.TestUtils.findOperator

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuHashJoin}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.{DataFrame, SparkSession}

class BroadcastHashJoinSuite extends SparkQueryCompareTestSuite {
  private def broadcastReuseConf: SparkConf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.rapids.sql.join.hashTable.reuse", "true")
    .set("spark.rapids.sql.join.buildSide", "FIXED")
    .set("spark.rapids.sql.batchSizeBytes", "1")

  private def broadcastAutoReuseConf: SparkConf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.rapids.sql.join.hashTable.reuse", "true")
    .set("spark.rapids.sql.metrics.level", "DEBUG")

  private def streamedProbeDf(spark: SparkSession): DataFrame =
    spark.range(0, 128).selectExpr(
      "CAST(id % 8 AS INT) AS join_key",
      "CAST(id AS INT) AS probe_value")

  private def distinctBuildDf(spark: SparkSession): DataFrame =
    spark.range(0, 8).selectExpr(
      "CAST(id AS INT) AS join_key",
      "CAST(id * 10 AS INT) AS build_value")

  private def nonDistinctBuildDf(spark: SparkSession): DataFrame =
    spark.range(0, 16).selectExpr(
      "CAST(id % 4 AS INT) AS join_key",
      "CAST(id AS INT) AS build_value")

  private def nullableProbeDf(spark: SparkSession): DataFrame =
    spark.range(0, 8).selectExpr(
      "CAST(CASE CAST(id AS INT) " +
        "WHEN 0 THEN NULL " +
        "WHEN 1 THEN 0 " +
        "WHEN 2 THEN 1 " +
        "WHEN 3 THEN 2 " +
        "WHEN 4 THEN 3 " +
        "WHEN 5 THEN 4 " +
        "WHEN 6 THEN 5 " +
        "ELSE 8 END AS INT) AS join_key",
      "CAST(id AS INT) AS probe_value")

  private def nullableDistinctBuildDf(spark: SparkSession): DataFrame =
    spark.range(0, 8).selectExpr(
      "CAST(CASE CAST(id AS INT) " +
        "WHEN 0 THEN NULL " +
        "WHEN 1 THEN 0 " +
        "WHEN 2 THEN 1 " +
        "WHEN 3 THEN 2 " +
        "WHEN 4 THEN 3 " +
        "WHEN 5 THEN 4 " +
        "WHEN 6 THEN 6 " +
        "ELSE 9 END AS INT) AS join_key",
      "CAST(id * 10 AS INT) AS build_value")

  test("broadcast hint isn't propagated after a join") {
    val conf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark)
      val df2 = nonZeroLongsDf(spark)

      val df3 = df1.join(broadcast(df2), Seq("longs"), "inner").drop(df2("longs"))
      val df4 = longsDf(spark)
      val df5 = df4.join(df3, Seq("longs"), "inner")

      // execute the plan so that the final adaptive plan is available when AQE is on
      df5.collect()
      val plan = df5.queryExecution.executedPlan

      val bhjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      assert(bhjCount.size === 1)

      val shjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuShuffledSymmetricHashJoinExec])
      assert(shjCount.size === 1)
    }, conf)
  }

  test("broadcast hint in SQL") {
    withGpuSparkSession(spark => {
      longsDf(spark).createOrReplaceTempView("t")
      longsDf(spark).createOrReplaceTempView("u")

      for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
        val plan1 = spark.sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.longs = u.longs")
        val plan2 = spark.sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.longs = u.longs")

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan1.collect()
        val finalPlan1 = findOperator(plan1.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan1.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildLeft)

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan2.collect()
        val finalPlan2 = findOperator(plan2.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan2.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildRight)
      }
    })
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct inner build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct left outer build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "left")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct right outer build left",
    distinctBuildDf,
    streamedProbeDf,
    conf = broadcastReuseConf) {
    (build, probe) => broadcast(build).join(probe, Seq("join_key"), "right")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct inner build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct inner build left",
    nonDistinctBuildDf,
    streamedProbeDf,
    conf = broadcastReuseConf) {
    (build, probe) => broadcast(build).join(probe, Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct left outer build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "left")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct right outer build left",
    nonDistinctBuildDf,
    streamedProbeDf,
    conf = broadcastReuseConf) {
    (build, probe) => broadcast(build).join(probe, Seq("join_key"), "right")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct left semi build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "leftsemi")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct left anti build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "leftanti")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct inner nullable keys build right",
    nullableProbeDf,
    nullableDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse conditional inner build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) =>
      probe.alias("p").join(
        broadcast(build.alias("b")),
        col("p.join_key") === col("b.join_key") &&
          col("p.probe_value") > col("b.build_value"),
        "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse conditional distinct inner build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) =>
      probe.alias("p").join(
        broadcast(build.alias("b")),
        col("p.join_key") === col("b.join_key") &&
          col("p.probe_value") > col("b.build_value"),
        "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse conditional distinct left semi build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) =>
      probe.alias("p").join(
        broadcast(build.alias("b")),
        col("p.join_key") === col("b.join_key") &&
          col("p.probe_value") > col("b.build_value"),
        "leftsemi")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse conditional left outer build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) =>
      probe.alias("p").join(
        broadcast(build.alias("b")),
        col("p.join_key") === col("b.join_key") &&
          col("p.probe_value") > col("b.build_value"),
        "left")
  }

  test("broadcast hash join reuse same broadcast in multiple joins plan") {
    val conf = broadcastReuseConf.clone()
      .set("spark.sql.exchange.reuse", "true")
      .set("spark.rapids.sql.metrics.level", "DEBUG")
    withGpuSparkSession(spark => {
      val probe = streamedProbeDf(spark)
      val build = broadcast(distinctBuildDf(spark))
      val joined = probe
        .join(build, Seq("join_key"), "inner")
        .select("join_key", "probe_value")
        .join(build, Seq("join_key"), "inner")
        .select("join_key", "probe_value")

      joined.collect()
      val plan = joined.queryExecution.executedPlan
      val bhjs = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      val reusedExchanges = PlanUtils.findOperators(plan, _.isInstanceOf[ReusedExchangeExec])
      assertResult(2)(bhjs.size)
      assert(reusedExchanges.nonEmpty)

      val totalBuilds = bhjs.map(_.metrics("hashTableBuilds").value).sum
      val totalReuses = bhjs.map(_.metrics("hashTableReuses").value).sum
      assertResult(1L)(totalBuilds)
      assert(totalReuses > 0L, s"expected at least one hash-table reuse, got $totalReuses")
    }, conf)
  }

  test("AUTO admits a cold broadcast hash build after repeated smaller numeric probes") {
    withGpuSparkSession(spark => {
      val probe = spark.range(0, 512, 1, 8).selectExpr(
        "CAST(id % 256 AS INT) AS join_key",
        "CAST(id AS INT) AS probe_value")
      val build = spark.range(0, 2048, 1, 1).selectExpr(
        "CAST(id % 256 AS INT) AS join_key",
        "CAST(id AS INT) AS build_value")
      val joined = probe.join(broadcast(build), Seq("join_key"), "inner")

      assertResult(4096)(joined.collect().length)
      val bhj = findOperator(joined.queryExecution.executedPlan,
        _.isInstanceOf[GpuBroadcastHashJoinExec]).get
      assertResult(1L)(bhj.metrics("hashTableBuilds").value)
      assert(bhj.metrics("hashTableReuses").value > 0L)
    }, broadcastAutoReuseConf)
  }
}
