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

package com.nvidia.spark.rapids.spill

import com.nvidia.spark.rapids.{GpuHashAggregateExec, GpuUnionExec, SparkQueryCompareTestSuite, TestUtils}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExec
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class SpillGpuModelCheckingSuite extends SparkQueryCompareTestSuite {
  import SpillLifecycleModel._

  private case class Graph[StateType](states: Vector[StateType], edges: Set[(Long, Long)]) {
    val ids: Set[Long] = states.indices.map(_.toLong).toSet

    def predecessor(targets: Set[Long]): Set[Long] =
      edges.collect { case (source, target) if targets.contains(target) => source }

    def until(allowed: Set[Long], goal: Set[Long]): Set[Long] = {
      var reached = goal
      var nextReached = reached ++ (allowed intersect predecessor(reached))
      while (nextReached != reached) {
        reached = nextReached
        nextReached = reached ++ (allowed intersect predecessor(reached))
      }
      reached
    }
  }

  private def graph(): Graph[State] = {
    val states = explore().map(_.state)
    val ids = states.zipWithIndex.map { case (state, index) => state -> index.toLong }.toMap
    val transitions = states.zipWithIndex.flatMap { case (state, index) =>
      actions.flatMap(action => next(state, action).map(ids(_))).map(index.toLong -> _)
    }.toSet
    val terminalLoops = states.indices.map(_.toLong).filterNot(source =>
      transitions.exists(_._1 == source)).map(source => source -> source)
    Graph(states, transitions ++ terminalLoops)
  }

  private def ids(frame: DataFrame): Set[Long] =
    frame.collect().map(_.getLong(0)).toSet

  private def relation(spark: SparkSession, values: Set[Long]): DataFrame = {
    val rows = spark.sparkContext.parallelize(values.toSeq.map(Row(_)), 1)
    spark.createDataFrame(rows, StructType(Seq(StructField("state_id", LongType))))
  }

  private def checkedResult(frame: DataFrame, requiredGpuJoins: Int,
      requireRows: Boolean = false): Set[Long] = {
    val result = ids(frame)
    val plan = frame.queryExecution.executedPlan
    val gpuJoins = plan.collect { case join: GpuBroadcastHashJoinExec => join }
    assert(gpuJoins.size >= requiredGpuJoins,
      s"CTL query expected $requiredGpuJoins GPU joins, found ${gpuJoins.size}:\n$plan")
    assert(gpuJoins.forall(_.metrics.get("numOutputRows").isDefined))
    if (requireRows) {
      assert(gpuJoins.head.metrics("numOutputRows").value > 0L)
    }
    result
  }

  private def predecessor(edges: DataFrame, targets: DataFrame): DataFrame = {
    val joined = edges.join(broadcast(targets.withColumnRenamed("state_id", "target_id")),
      Seq("target_id"))
      .select(col("source_id").as("state_id")).distinct()
    joined
  }

  private def until(spark: SparkSession, edges: DataFrame,
      allowed: Set[Long], goal: Set[Long]): Set[Long] = {
    var reached = goal
    def step(current: Set[Long]): Set[Long] = {
      val prior = relation(spark, current)
      val candidates = predecessor(edges, prior)
        .join(broadcast(relation(spark, allowed)), Seq("state_id"))
      val nextStates = prior.union(candidates).distinct()
      val result = checkedResult(nextStates, requiredGpuJoins = 2)
      val plan = nextStates.queryExecution.executedPlan
      assert(TestUtils.findOperator(plan, _.isInstanceOf[GpuUnionExec]).isDefined)
      assert(TestUtils.findOperator(plan, _.isInstanceOf[GpuHashAggregateExec]).isDefined)
      result
    }
    var nextReached = step(reached)
    while (nextReached != reached) {
      reached = nextReached
      nextReached = step(reached)
    }
    reached
  }

  private def complement(spark: SparkSession,
      all: Set[Long], excluded: Set[Long]): Set[Long] = {
    val remaining = relation(spark, all)
      .join(broadcast(relation(spark, excluded)), Seq("state_id"), "left_anti")
    checkedResult(remaining, requiredGpuJoins = 1)
  }

  test("GPU CTL detects a release-order violation in a real spill trace") {
    val trace = SpillReleaseConformanceTrace.capture()
    assert(trace.states.size == 4)
    assert(!trace.states.head.closed)
    assert(trace.states(1).copyPublished && trace.states(1).hostOwned)
    assert(trace.states(2).closed && !trace.states(2).syncComplete)
    assert(trace.states.last.syncComplete)

    val model = Graph(trace.states, trace.edges)
    val safe = model.ids.filter(id => !model.states(id.toInt).violatesReleaseOrder)
    val cpuAgSafe = model.ids -- model.until(model.ids, model.ids -- safe)
    val unsafe = !cpuAgSafe.contains(0L)
    assert(trace.shortestViolation.isDefined == unsafe)
    if (unsafe) {
      assert(trace.shortestViolation.contains(Vector("StartSpillAndPublishHost", "Close")))
    }
    sys.env.get("SPILL_CONFORMANCE_EXPECTED").foreach {
      case "unsafe" => assert(unsafe)
      case "safe" => assert(!unsafe)
      case other => fail(s"unknown SPILL_CONFORMANCE_EXPECTED value: $other")
    }

    withGpuSparkSession { spark =>
      val rows = spark.sparkContext.parallelize(model.edges.toSeq.map {
        case (source, target) => Row(source, target)
      }, 1)
      val schema = StructType(Seq(StructField("source_id", LongType),
        StructField("target_id", LongType)))
      val edges = spark.createDataFrame(rows, schema)
      val gpuAgSafe = complement(spark, model.ids,
        until(spark, edges, model.ids, model.ids -- safe))
      assert(gpuAgSafe == cpuAgSafe)
    }
    info(s"SPILL_CONFORMANCE unsafe=$unsafe witness=${trace.shortestViolation} " +
      s"states=${trace.states}")
  }

  test("GPU predecessor joins evaluate EX, EU, and AG over spill lifecycle graph") {
    val model = graph()
    assert(model.states.nonEmpty)
    assert(model.edges.nonEmpty)
    withGpuSparkSession { spark =>
      val rows = spark.sparkContext.parallelize(model.edges.toSeq.map {
        case (source, target) => Row(source, target)
      }, 1)
      val schema = StructType(Seq(StructField("source_id", LongType),
        StructField("target_id", LongType)))
      val edges = spark.createDataFrame(rows, schema)
      val open = model.ids.filter(id => !model.states(id.toInt).closed)
      val host = model.ids.filter(id => model.states(id.toInt).hostOwned)

      assert(checkedResult(predecessor(edges, relation(spark, host)),
        requiredGpuJoins = 1, requireRows = true) ==
        model.predecessor(host))
      assert(until(spark, edges, open, host) == model.until(open, host))
      val gpuAgOpen = complement(spark, model.ids,
        until(spark, edges, model.ids, model.ids -- open))
      val cpuAgOpen = model.ids -- model.until(model.ids, model.ids -- open)
      assert(gpuAgOpen == cpuAgOpen)
      assert(!gpuAgOpen.contains(0L))

      val safe = model.ids.filter(id => violations(model.states(id.toInt)).isEmpty)
      val gpuAgSafe = complement(spark, model.ids,
        until(spark, edges, model.ids, model.ids -- safe))
      val cpuAgSafe = model.ids -- model.until(model.ids, model.ids -- safe)
      assert(gpuAgSafe == cpuAgSafe)
      assert(gpuAgSafe.contains(0L))
    }
  }
}
