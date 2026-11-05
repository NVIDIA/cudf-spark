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

package org.apache.spark.sql.rapids

import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

import com.nvidia.spark.rapids.{LogCaptureUtils, RowBasedShuffleChecksumConf,
  ShuffleCleanupManager, ShuffleReclaimRequest, SparkRapidsShuffleReclaimEvent}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{FetchFailed, FutureAction, SPARK_VERSION, SparkContext, SparkException,
  SparkFirehoseListener, TaskContext}
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobEnd,
  SparkListenerJobStart, SparkListenerStageSubmitted, SparkListenerTaskEnd,
  SparkListenerTaskStart, StageInfo}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}
import org.apache.spark.sql.functions.{col, sum, udf}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils

/**
 * Runs the UCX policy on a local session with Spark's sort shuffle and no plugin, where every
 * CPU shuffle stands in for a GPU one. The default sink stands in for the reclaim manager: it
 * runs the real driver step and posts the fence on the tracker's thread, so one bounded drain
 * covers a decision, its clear, its fence and the fence's verdict. Each case starts its own
 * session, which `afterEach` stops.
 */
class UcxReclaimSessionSuite extends AnyFunSuite with BeforeAndAfterEach {
  import UcxReclaimSessionSuite._

  private var spark: SparkSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    // getOrCreate would reuse a session or context that an earlier suite in this JVM left
    TrampolineUtil.cleanupAnyExistingSession()
    SparkContext.getActive.foreach(_.stop())
  }

  override def afterEach(): Unit = {
    try {
      openGate()
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterEach()
    }
  }

  /**
   * Starts this case's session on Spark's sort shuffle, with `conf` applied last. Spark 4.1+
   * removes a classic query's shuffles at its end when `spark.testing` is set, as an earlier
   * suite in this JVM may leave it, so that cleanup is off.
   */
  private def startSession(master: String, conf: (String, String)*): SparkSession = {
    val builder = SparkSession.builder()
      .master(master)
      .appName("UcxReclaimSessionSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
      .config(PartitionsKey, "4")
    conf.foreach { case (key, value) => builder.config(key, value) }
    spark = builder.getOrCreate()
    spark
  }

  /**
   * Adds the UCX policy over `handoff` to the tracker's own queue, between a recorder that gets
   * each event before the policy and one that gets it after. The policy reads the queue's real
   * drop counter through the plugin's own helper, and the event key is on, since with it off the
   * policy sends no skip or close.
   */
  private def installTracker[H <: ShuffleReclaimHandoff](
      handoff: H,
      clock: () => Long = () => System.currentTimeMillis(),
      isCandidate: Int => Boolean = _ => true): Tracker[H] = {
    val sc = spark.sparkContext
    val seen = new EventRecorder
    val calls = new CallRecorder(handoff, seen)
    val policy = new UcxReclaimPolicy(calls, clock, isCandidate, eventLogEnabled = true,
      droppedEvents =
        () => TrampolineUtil.listenerQueueDroppedEvents(sc, ShuffleCleanupListener.QUEUE_NAME))
    val listener = new ShuffleCleanupListener(policy)
    val handled = new EventRecorder
    sc.listenerBus.addToQueue(seen, ShuffleCleanupListener.QUEUE_NAME)
    sc.listenerBus.addToQueue(listener, ShuffleCleanupListener.QUEUE_NAME)
    sc.listenerBus.addToQueue(handled, ShuffleCleanupListener.QUEUE_NAME)
    new Tracker(sc, policy, listener, calls, seen, handled, handoff)
  }

  /** Starts this case's session, and the tracker over a new default sink. */
  private def startTracker(master: String, conf: (String, String)*): Tracker[DefaultSink] = {
    startSession(master, conf: _*)
    installTracker(new DefaultSink(spark.sparkContext))
  }

  /** Waits until every job and task the tracker's queue reported has ended, then drains. */
  private def settle(t: Tracker[_]): Unit = {
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      val events = t.handled.events
      val tasks = events.collect { case e: SparkListenerTaskStart => e.taskInfo.taskId }.toSet --
        events.collect { case e: SparkListenerTaskEnd => e.taskInfo.taskId }
      val jobs = events.collect { case e: SparkListenerJobStart => e.jobId }.toSet --
        events.collect { case e: SparkListenerJobEnd => e.jobId }
      assert(tasks.isEmpty && jobs.isEmpty, s"tasks $tasks and jobs $jobs have not ended")
    }
    t.drain()
  }

  /** Runs `body` on this thread with `action` as the action tag of every job it submits. */
  private def tagged[T](action: String)(body: => T): T = {
    val sc = spark.sparkContext
    sc.setLocalProperty(ActionKey, action)
    try {
      body
    } finally {
      sc.setLocalProperty(ActionKey, null)
    }
  }

  /** Runs `body` on a thread of its own, with `action` as the action tag of its jobs. */
  private def startAction[T](action: String)(body: => T): ActionThread[T] = {
    val sc = spark.sparkContext
    new ActionThread(s"ucx-reclaim-$action", () => {
      sc.setLocalProperty(ActionKey, action)
      body
    })
  }

  private def executionId(properties: Properties): Option[Long] =
    Option(properties).flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .map(_.toLong)

  private def actionOf(properties: Properties): Option[String] =
    Option(properties).flatMap(p => Option(p.getProperty(ActionKey)))

  /** The one SQL execution that ran the jobs tagged `action`. */
  private def executionOf(t: Tracker[_], action: String): Long = {
    val ids = t.jobStarts.filter(j => actionOf(j.properties).contains(action))
      .flatMap(j => executionId(j.properties)).distinct
    assertResult(1, s"executions of the jobs of $action")(ids.size)
    ids.head
  }

  /** The shuffles whose map stage `execution` submitted, which it owns, in submission order. */
  private def ownedBy(t: Tracker[_], execution: Long): List[Int] =
    t.stagesSubmitted.filter(s => executionId(s.properties).contains(execution))
      .flatMap(_.stageInfo.shuffleDepId).distinct

  /** The stage that writes a shuffle, as its first submission reported it. */
  private def mapStageOf(t: Tracker[_], shuffleId: Int): StageInfo =
    t.stagesSubmitted.map(_.stageInfo).find(_.shuffleDepId.contains(shuffleId))
      .getOrElse(fail(s"no map stage of shuffle $shuffleId was submitted"))

  /** Every stage that the jobs of `execution` list, skipped or not. */
  private def stagesOf(t: Tracker[_], execution: Long): Set[Int] =
    t.jobStarts.filter(j => executionId(j.properties).contains(execution))
      .flatMap(_.stageInfos.map(_.stageId)).toSet

  private def single(shuffleIds: Seq[Int], what: String): Int = {
    assertResult(1, what)(shuffleIds.size)
    shuffleIds.head
  }

  private def skipped(
      t: Tracker[_],
      shuffleId: Int,
      execution: Long,
      rule: ShuffleReclaimRule): Boolean =
    t.calls.skips.exists(k =>
      k.shuffleId == shuffleId && k.executionId == execution && k.rule == rule)

  /**
   * Checks that `owner` reclaimed a shuffle once: one decision, a driver step that found and
   * cleared its outputs while keeping its status, and a release by the decision's fence.
   */
  private def assertReclaimedOnce(t: Tracker[DefaultSink], shuffleId: Int, owner: Long): Unit = {
    val decisions = t.calls.decisionsFor(shuffleId)
    assertResult(List(owner), s"owners of the decisions for shuffle $shuffleId")(
      decisions.map(_._2.owner))
    assertResult(List(true), s"driver steps that found shuffle $shuffleId's status")(
      t.handoff.stepsFor(shuffleId))
    assertResult(List((decisions.head._1, Release)), s"verdicts on shuffle $shuffleId")(
      t.handoff.verdictsFor(shuffleId))
    assertResult(Some(0), s"available outputs of shuffle $shuffleId")(
      TestTrampolineUtil.availableMapOutputs(shuffleId))
  }

  /**
   * Checks that no shuffle of `execution` was dispatched before the tracker handled the
   * execution's End and the last TaskEnd of its stages. A call's `seen` count says that the
   * `seen - 1`th event of the tracker's queue caused it.
   */
  private def assertDispatchedAfterEnd(t: Tracker[_], execution: Long): Unit = {
    val stages = stagesOf(t, execution)
    val events = t.seen.events
    val end = events.lastIndexWhere {
      case e: SparkListenerSQLExecutionEnd => e.executionId == execution
      case _ => false
    }
    val lastTaskEnd = events.lastIndexWhere {
      case e: SparkListenerTaskEnd => stages.contains(e.stageId)
      case _ => false
    }
    assert(end >= 0, s"the tracker saw no End of execution $execution")
    val after = math.max(end, lastTaskEnd)
    t.calls.dispatches.filter(_.shuffles.exists(_.owner == execution)).foreach { d =>
      assert(d.seen - 1 >= after, s"$d came before event $after, the End or the last TaskEnd")
    }
  }

  private def assertNoFetchFailure(t: Tracker[_]): Unit = {
    val failures = t.taskEnds.map(_.reason).filter(_.isInstanceOf[FetchFailed])
    assert(failures.isEmpty, s"fetch failures: $failures")
  }

  /** Checks that `execution` started with an adaptive plan, so AQE planned its query. */
  private def assertPlannedAdaptively(t: Tracker[_], execution: Long): Unit =
    assert(t.handled.events.exists {
      case start: SparkListenerSQLExecutionStart =>
        start.executionId == execution && start.sparkPlanInfo.nodeName == "AdaptiveSparkPlan"
      case _ => false
    }, s"execution $execution did not plan adaptively")

  /** Every node of a physical plan, through AQE's wrapper and into its query stages. */
  private def planNodes(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case adaptive: AdaptiveSparkPlanExec => planNodes(adaptive.executedPlan)
    case stage: QueryStageExec => stage +: planNodes(stage.plan)
    case node => node +: node.children.flatMap(planNodes)
  }

  private def rowsOf(df: DataFrame): List[String] = df.collect().map(_.toString).sorted.toList

  /** The rows of `df`, read by a foreachPartition, whose job runs outside AQE's plan lock. */
  private def rowsOfPartitions(df: DataFrame): List[String] = {
    val rows = spark.sparkContext.collectionAccumulator[String]("rows")
    val add: Iterator[Row] => Unit = _.foreach(row => rows.add(row.toString))
    df.foreachPartition(add)
    rows.value.asScala.toList.sorted
  }

  /** Two chained aggregations: two determinate shuffles, the second reading the first. */
  private def twoAggregates(): DataFrame =
    spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .groupBy("k").agg(sum("id").as("s"))
      .groupBy((col("k") % 2).as("p")).agg(sum("s").as("t"))

  /** A groupBy count over `keys` keys: one hash shuffle, which R7 admits. */
  private def oneShuffle(keys: Int): DataFrame =
    spark.range(0, 100, 1, 2).selectExpr(s"id % $keys AS k").groupBy("k").count()

  /** A groupBy count whose two map tasks call `gated`, so the gate can hold one of them. */
  private def heldMapQuery(): DataFrame =
    spark.range(0, 100, 1, 2).select(gated(col("id")).as("id")).selectExpr("id % 7 AS k")
      .groupBy("k").count()

  test("an execution of several jobs has its shuffles reclaimed only after it ends") {
    val t = startTracker("local[2]", AqeKey -> "true")
    val rows = tagged("query")(rowsOf(twoAggregates()))
    settle(t)
    val e = executionOf(t, "query")
    val jobs = t.jobStarts.count(j => executionId(j.properties).contains(e))
    assert(jobs > 1, s"AQE ran execution $e as $jobs job")
    val owned = ownedBy(t, e)
    assertResult(2, "shuffles the execution owns")(owned.size)
    owned.foreach(assertReclaimedOnce(t, _, e))
    assertDispatchedAfterEnd(t, e)
    assertResult(TwoAggregatesRows)(rows)
  }

  test("a shuffle that a reused exchange reads twice in one execution is reclaimed once") {
    val t = startTracker("local[2]", AqeKey -> "true", BroadcastKey -> "-1")
    val agg = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .groupBy("k").agg(sum("id").as("s"))
    val rows = tagged("self-join")(rowsOf(agg.as("a").join(agg.as("b"), "k")))
    settle(t)
    val e = executionOf(t, "self-join")
    // without the reuse, each side of the join would write a shuffle of its own
    val s = single(ownedBy(t, e), "shuffles the execution owns")
    assertReclaimedOnce(t, s, e)
    assertResult(List(s), "dispatched shuffles")(
      t.calls.dispatches.flatMap(_.shuffles.map(_.shuffleId)))
    assertResult(7, "rows")(rows.size)
  }

  test("a broadcast that AQE builds from a shuffle counts as reading that shuffle") {
    // with no static broadcast, a broadcast join in the final plan is AQE's conversion
    val t = startTracker("local[2]", AqeKey -> "true", BroadcastKey -> "-1",
      "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10m")
    val left = spark.range(0, 2000, 1, 4).selectExpr("id % 100 AS k", "id AS v")
    val right = spark.range(0, 100, 1, 2).selectExpr("id AS k", "id * 2 AS w")
    val df = left.join(right, "k")
    val rows = tagged("join")(rowsOf(df))
    settle(t)
    val plan = df.queryExecution.executedPlan
    assert(planNodes(plan).exists(_.isInstanceOf[BroadcastHashJoinExec]),
      s"AQE kept the sort-merge join:\n$plan")
    val e = executionOf(t, "join")
    val owned = ownedBy(t, e)
    assertResult(2, "shuffles the execution owns")(owned.size)
    owned.foreach(assertReclaimedOnce(t, _, e))
    assert(t.calls.skips.isEmpty, t.calls.all)
    assertResult(2000, "rows")(rows.size)
  }

  test("a failed execution's shuffles are reclaimed after its tasks end, read or not") {
    val t = startTracker("local[2]", AqeKey -> "false", PartitionsKey -> "2",
      "spark.task.maxFailures" -> "1")
    tagged("succeeds")(rowsOf(oneShuffle(7)))
    val failing = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .groupBy("k").agg(sum("id").as("s"))
      .select(failsOnKeyThree(col("k")).as("k"), col("s"))
      .groupBy((col("k") % 2).as("p")).agg(sum("s"))
    intercept[SparkException](tagged("fails")(failing.collect()))
    settle(t)
    val succeeded = executionOf(t, "succeeds")
    assertReclaimedOnce(t, single(ownedBy(t, succeeded), "shuffles the succeeded query owns"),
      succeeded)
    val failed = executionOf(t, "fails")
    val owned = ownedBy(t, failed)
    assertResult(2, "shuffles the failed query owns")(owned.size)
    // the failing reduce stage wrote the second shuffle, which no stage then read
    val unreadMapStage = mapStageOf(t, owned(1)).stageId
    assert(!t.stagesSubmitted.exists(_.stageInfo.parentIds.contains(unreadMapStage)),
      s"a stage read shuffle ${owned(1)}")
    owned.foreach(assertReclaimedOnce(t, _, failed))
    assertDispatchedAfterEnd(t, failed)
  }

  test("a task that outlives its cancelled execution holds the reclaim, and one WARN names it") {
    val nowMs = new AtomicLong(System.currentTimeMillis())
    startSession("local[2]", AqeKey -> "false")
    // jobs before the tracker starts move apart the shuffle, stage and task ids the WARN names
    (1 to 3).foreach { _ =>
      spark.sparkContext.parallelize(1 to 8, 4).map(i => (i % 2, i))
        .reduceByKey(_ + _, 2).count()
    }
    val t = installTracker(new DefaultSink(spark.sparkContext), clock = () => nowMs.get())
    resetGate(1)
    val held = startAction("held") {
      spark.sparkContext.setJobGroup(JobGroup, "a map task held at the gate",
        interruptOnCancel = false)
      rowsOf(heldMapQuery())
    }
    try {
      assert(awaitEntered(), "no map task reached the gate")
      t.drain()
      spark.sparkContext.cancelJobGroup(JobGroup)
      val cancelled = held.await()
      assert(held.hasEnded && cancelled.isFailure, s"the cancelled collect ended with $cancelled")
      t.drain()
      val e = executionOf(t, "held")
      val s = single(ownedBy(t, e), "shuffles the execution owns")
      val stage = mapStageOf(t, s).stageId
      val task = heldTaskId
      assert(t.calls.decisionsFor(s).isEmpty, s"reclaimed while task $task runs: ${t.calls.all}")
      // the map task that the gate let through has ended, so the held task runs alone
      eventually(Timeout(Span(WaitSeconds, Seconds))) {
        assertResult(1, s"ended tasks of stage $stage")(t.taskEnds.count(_.stageId == stage))
      }
      val logged = LogCaptureUtils.captureLogsFrom(Seq(PolicyLogger)) {
        nowMs.addAndGet(TimeUnit.MINUTES.toMillis(11))
        // the second update finds the shuffle's deadline already reported
        (1 to 2).foreach { _ =>
          t.sc.listenerBus.post(UcxReclaimEventBuilder.driverMetricsUpdate)
          t.drain()
        }
      }
      val warns = logged.filter(_.startsWith(UcxReclaimPolicy.R6_WARN_PREFIX))
      assertResult(1, warns.mkString("\n"))(warns.length)
      val warn = warns.head
      assert(raw"\bshuffle $s\b".r.findFirstIn(warn).isDefined, warn)
      assert(raw"\bstage $stage\b".r.findFirstIn(warn).isDefined, warn)
      assert(raw"\b1 running\b".r.findFirstIn(warn).isDefined, warn)
      assert(raw"task\D*\b$task\b".r.findFirstIn(warn).isDefined, warn)
      openGate()
      settle(t)
      assertResult(List(e), s"owners of the decisions for shuffle $s")(
        t.calls.decisionsFor(s).map(_._2.owner))
    } finally {
      openGate()
      held.await()
    }
  }

  test("stopping the context with a task held reclaims nothing and logs the undecided count") {
    val t = startTracker("local[2]", AqeKey -> "false")
    resetGate(1)
    val held = startAction("held")(rowsOf(heldMapQuery()))
    try {
      assert(awaitEntered(), "no map task reached the gate")
      t.drain()
      // Spark 3 stops its listener bus before the driver plugin that shuts the policy down
      val stop = new ActionThread("ucx-reclaim-stop", () => spark.stop())
      assert(stop.await().isSuccess, "the context did not stop")
      val logged = LogCaptureUtils.captureLogsFrom(Seq(PolicyLogger)) {
        val shutdown = new ActionThread("ucx-reclaim-shutdown", () => t.listener.shutdown())
        assert(shutdown.await().isSuccess, "the policy did not shut down")
      }
      assert(t.calls.dispatches.isEmpty, t.calls.all)
      // the held map task leaves the query's one shuffle undecided
      assert(logged.exists(line => OneUndecided.findFirstIn(line).isDefined),
        logged.mkString("\n"))
    } finally {
      openGate()
      held.await()
    }
  }

  /**
   * Reads one DataFrame with `rows` on two threads, A then B, while the gate holds A's result
   * tasks. The gate opens once the tracker has handled B's JobStart, so R2 has run by then.
   */
  private def assertSecondExecutionLeavesShuffleToGc(
      aqe: Boolean,
      rows: DataFrame => List[String] = rowsOf(_)): Unit = {
    val t = startTracker("local[2]", AqeKey -> aqe.toString, PartitionsKey -> "2")
    val df = spark.range(0, 100, 1, 4).selectExpr("id % 7 AS k").groupBy("k").count()
      .select(col("k"), gated(col("count")).as("count"))
    resetGate(-1)
    t.handled.onEach {
      case j: SparkListenerJobStart if actionOf(j.properties).contains("B") => openGate()
      case _ =>
    }
    val first = startAction("A")(rows(df))
    var second: Option[ActionThread[List[String]]] = None
    try {
      assert(awaitEntered(), "no result task reached the gate")
      second = Some(startAction("B")(rows(df)))
      val rowsA = first.await().get
      val rowsB = second.get.await().get
      settle(t)
      val e1 = executionOf(t, "A")
      val s = single(ownedBy(t, e1), "shuffles A's execution owns")
      if (aqe) {
        assertPlannedAdaptively(t, e1)
      }
      assert(t.jobStarts.exists(j => actionOf(j.properties).contains("B") &&
        j.stageInfos.exists(_.shuffleDepId.contains(s))), s"B's job does not read shuffle $s")
      assert(t.handoff.stepsFor(s).isEmpty, s"the driver step ran for shuffle $s")
      assert(skipped(t, s, e1, ShuffleReclaimRule.R2), t.calls.all)
      assertResult(rowsA)(rowsB)
    } finally {
      openGate()
      first.await()
      second.foreach(_.await())
    }
  }

  test("a second execution's job that reads an owned shuffle leaves it to GC, with AQE off") {
    assertSecondExecutionLeavesShuffleToGc(aqe = false)
  }

  test("a second execution's job that reads an owned shuffle leaves it to GC, with AQE on") {
    // from Spark 4.0, AQE's plan lock holds the second collect until the first one's result
    // stage ends, so its job could not start while the gate holds that stage
    assume(VersionUtils.majorVersion(SPARK_VERSION) < 4, "AQE's plan lock serializes the collects")
    assertSecondExecutionLeavesShuffleToGc(aqe = true)
  }

  test("a second execution's job that reads an owned shuffle leaves it to GC, with AQE on, " +
      "through foreachPartition") {
    // foreachPartition's job runs outside AQE's plan lock, so from Spark 4.0 too the second
    // action's job can start while the gate holds the first one's result stage
    assertSecondExecutionLeavesShuffleToGc(aqe = true, rowsOfPartitions)
  }

  test("a repeated action reruns the cleared map stage, and its own execution reclaims it again") {
    val t = startTracker("local[2]", AqeKey -> "false")
    val df = spark.range(0, 1010, 1, 4).selectExpr("id % 101 AS k").groupBy("k").count()
    val firstRows = tagged("first")(rowsOf(df))
    settle(t)
    val e1 = executionOf(t, "first")
    val s = single(ownedBy(t, e1), "shuffles the first execution owns")
    assertReclaimedOnce(t, s, e1)
    val secondRows = tagged("second")(rowsOf(df))
    settle(t)
    val e2 = executionOf(t, "second")
    assertResult(List(s), "shuffles whose map stage the second collect ran")(ownedBy(t, e2))
    assertResult(List(e1, e2), s"owners of the decisions for shuffle $s")(
      t.calls.decisionsFor(s).map(_._2.owner))
    assertResult(List(true, true), s"driver steps that found shuffle $s's status")(
      t.handoff.stepsFor(s))
    assertResult(List(Release, Release), s"verdicts on shuffle $s")(
      t.handoff.verdictsFor(s).map(_._2))
    assertResult(Some(0), s"available outputs of shuffle $s")(
      TestTrampolineUtil.availableMapOutputs(s))
    assertResult(101, "rows")(firstRows.size)
    assertResult(firstRows)(secondRows)
    assertNoFetchFailure(t)
  }

  test("toLocalIterator's last shuffle waits for GC, and the shuffle before it is reclaimed") {
    val t = startTracker("local[2]", AqeKey -> "true")
    val iterator = tagged("iterator")(twoAggregates().toLocalIterator())
    // the execution has ended, and its result jobs run only as the rows are read
    settle(t)
    val rows = iterator.asScala.map(_.toString).toList.sorted
    settle(t)
    val e = executionOf(t, "iterator")
    val owned = ownedBy(t, e)
    assertResult(2, "shuffles the execution owns")(owned.size)
    assertReclaimedOnce(t, owned.head, e)
    val last = owned(1)
    assert(t.calls.decisionsFor(last).isEmpty, t.calls.all)
    assert(skipped(t, last, e, ShuffleReclaimRule.R4), t.calls.all)
    assertResult(TwoAggregatesRows)(rows)
    assertNoFetchFailure(t)
    // the iterator's stages read only decided shuffles, so a held one means an exclusion that
    // dropped the shuffle's state without recording it
    assertResult(Set.empty, "held stages")(t.policy.heldState.stages)
  }

  test("a shuffle that a caching stage reads waits for GC") {
    val t = startTracker("local[2]", AqeKey -> "false")
    val cached = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .groupBy("k").agg(sum("id").as("s")).cache()
    assertResult(7, "rows")(tagged("cache")(rowsOf(cached)).size)
    settle(t)
    val e = executionOf(t, "cache")
    val s = single(ownedBy(t, e), "shuffles the execution owns")
    val mapStage = mapStageOf(t, s).stageId
    assert(t.stagesSubmitted.map(_.stageInfo).exists(st => st.parentIds.contains(mapStage) &&
      st.rddInfos.exists(_.storageLevel != StorageLevel.NONE)),
      s"no stage that reads shuffle $s caches an RDD")
    assert(t.calls.decisionsFor(s).isEmpty, t.calls.all)
    assert(skipped(t, s, e, ShuffleReclaimRule.R5), t.calls.all)
  }

  test("a dropped event disables the reclaim and withdraws every shuffle in flight") {
    val t = startTracker("local[2]", AqeKey -> "false")
    // the counter never falls for a SparkContext, so this case has a session of its own
    t.sc.listenerBus.metrics.metricRegistry.counter(DroppedEventsName).inc()
    tagged("first")(rowsOf(oneShuffle(5)))
    settle(t)
    val calls = t.calls.all
    val disables = t.calls.disables
    assertResult(1, s"disable calls in $calls")(disables.size)
    assert(disables.head.reason.contains(CapacityKey), disables.head.reason)
    val disabledAt = calls.indexWhere(_.isInstanceOf[Disabled])
    assert(!calls.drop(disabledAt).exists(_.isInstanceOf[Dispatched]), calls)
    val inFlight = calls.take(disabledAt).collect { case d: Dispatched => d }
    // the policy reads the counter at a fence's marker, so the first query's shuffle was in flight
    val first = single(ownedBy(t, executionOf(t, "first")), "shuffles the first query owns")
    assertResult(List(first), s"shuffles dispatched before the disable in $calls")(
      inFlight.flatMap(_.shuffles.map(_.shuffleId)))
    inFlight.foreach { d =>
      d.shuffles.foreach { decision =>
        assertResult(List((d.fenceId, Withdrawal)), s"verdicts on shuffle ${decision.shuffleId}")(
          t.handoff.verdictsFor(decision.shuffleId))
      }
    }
    tagged("second")(rowsOf(oneShuffle(3)))
    settle(t)
    val s = single(ownedBy(t, executionOf(t, "second")), "shuffles the second query owns")
    assert(t.calls.decisionsFor(s).isEmpty, t.calls.all)
  }

  test("indeterminate map stages wait for GC, and the determinate shuffle before them is " +
      "reclaimed") {
    // with both checksum keys off, a 4.2 CPU exchange carries no retry flag, as GPU ones never do
    val t = startTracker("local[2]", AqeKey -> "false", PartitionsKey -> "4",
      "spark.sql.execution.sortBeforeRepartition" -> "false",
      RowBasedShuffleChecksumConf.ChecksumEnabledKey -> "false",
      RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey -> "false")
    val df = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .groupBy("k").agg(sum("id").as("s")).repartition(4)
      .groupBy((col("k") % 2).as("p")).agg(sum("s"))
    val firstRows = tagged("first")(rowsOf(df))
    settle(t)
    val e = executionOf(t, "first")
    val owned = ownedBy(t, e)
    assertResult(3, "shuffles the execution owns")(owned.size)
    val levels =
      owned.map(s => mapStageOf(t, s).rddInfos.headOption.map(_.outputDeterministicLevel))
    assertResult(List(Some(DeterministicLevel.DETERMINATE),
      Some(DeterministicLevel.INDETERMINATE), Some(DeterministicLevel.INDETERMINATE)),
      "the deterministic levels of the map stages' own RDDs")(levels)
    val sa = owned.head
    val sr = owned(1)
    val sb = owned(2)
    assertReclaimedOnce(t, sa, e)
    assertResult(List(sa), "dispatched shuffles")(
      t.calls.dispatches.flatMap(_.shuffles.map(_.shuffleId)))
    assert(skipped(t, sr, e, ShuffleReclaimRule.R7), t.calls.all)
    assert(skipped(t, sb, e, ShuffleReclaimRule.R7), t.calls.all)
    Seq(sr, sb).foreach { s =>
      assertResult(Some(mapStageOf(t, s).numTasks), s"available outputs of shuffle $s")(
        TestTrampolineUtil.availableMapOutputs(s))
    }
    val before = t.handled.count
    val secondRows = tagged("again")(rowsOf(df))
    settle(t)
    val again = executionOf(t, "again")
    assertResult(firstRows)(secondRows)
    assertNoFetchFailure(t)
    val laterTasks =
      t.handled.events.drop(before).collect { case end: SparkListenerTaskEnd => end }
    assert(!laterTasks.exists(_.taskType == "ShuffleMapTask"), "the second collect ran a map task")
    assert(!t.calls.all.exists {
      case d: Dispatched => d.shuffles.exists(_.owner == again)
      case k: Skipped => k.executionId == again
      case c: Closed => c.executionId == again
      case _ => false
    }, t.calls.all)
  }

  test("an AQE join with an empty side reclaims the side it reads only after its execution and " +
      "tasks end") {
    val t = startTracker("local[2]", AqeKey -> "true", BroadcastKey -> "-1")
    val left = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id AS v")
      .groupBy("k").agg(sum("v").as("sv"))
    val right = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id AS w").where("w < 0")
    // AQE replaces an outer join whose right side is empty with its left side, which the final
    // stage still reads
    val df = left.join(right, Seq("k"), "left_outer")
    assertResult(7, "rows")(tagged("join")(rowsOf(df)).size)
    val plan = df.queryExecution.executedPlan
    assert(!planNodes(plan).exists(_.isInstanceOf[SortMergeJoinExec]),
      s"AQE kept the join with an empty side:\n$plan")
    // a stage that AQE abandons runs on after the execution ends
    settle(t)
    val e = executionOf(t, "join")
    val owned = ownedBy(t, e).toSet
    assert(owned.nonEmpty, s"execution $e owns no shuffle")
    val closes = t.calls.closes.filter(_.executionId == e)
    assertResult(1, s"closes of execution $e")(closes.size)
    assertResult(owned, "the ids the close carries")(closes.head.ownedIds.toSet)
    val dispatched = t.calls.dispatches.flatMap(_.shuffles).filter(_.owner == e).map(_.shuffleId)
    assert(dispatched.nonEmpty, s"execution $e dispatched no shuffle: ${t.calls.all}")
    val skips = t.calls.skips.filter(_.executionId == e)
    assertResult(owned, "the ids dispatched or skipped")(
      (dispatched ++ skips.map(_.shuffleId)).toSet)
    // nothing reads a shuffle that AQE abandons in a query that succeeds, which is R4's case
    skips.foreach(k => assertResult(ShuffleReclaimRule.R4, s"the rule of $k")(k.rule))
    assertDispatchedAfterEnd(t, e)
  }

  /**
   * Runs `body` with the tracker over the real reclaim manager, built with its primary
   * constructor, whose driver step and event sink go through `hooks`. The manager's tick never
   * runs, and the manager shuts down, with every hold open, once `body` ends.
   */
  private def withManager(isCandidate: Int => Boolean)(
      body: (Tracker[ShuffleCleanupManager], ManagerHooks) => Unit): Unit = {
    val sc = spark.sparkContext
    val hooks = new ManagerHooks(sc)
    val manager = new ShuffleCleanupManager(
      clock = () => System.currentTimeMillis(),
      postEvent = hooks.postEvent,
      driverStep = hooks.driverStep,
      eventLogEnabled = true,
      disabled = new AtomicBoolean(false),
      droppedEvents =
        () => TrampolineUtil.listenerQueueDroppedEvents(sc, ShuffleCleanupListener.QUEUE_NAME),
      cleanupIntervalMs = TimeUnit.HOURS.toMillis(1))
    try {
      body(installTracker(manager, isCandidate = isCandidate), hooks)
    } finally {
      hooks.openAll()
      manager.shutdown()
    }
  }

  /** The requests that a poll from executor `driver`, local mode's only executor, gets. */
  private def pollDriver(t: Tracker[ShuffleCleanupManager]): List[ShuffleReclaimRequest] =
    t.handoff.handleReclaimPoll(SparkContext.DRIVER_IDENTIFIER).requests.toList

  /**
   * Polls as executor `driver`, bounded, until a reply lists `shuffleId`, which only a fence's
   * release allows, and returns that request.
   */
  private def awaitPolled(
      t: Tracker[ShuffleCleanupManager],
      shuffleId: Int): ShuffleReclaimRequest =
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      pollDriver(t).find(_.shuffleId == shuffleId)
        .getOrElse(fail(s"no poll for executor driver listed shuffle $shuffleId"))
    }

  /** Waits, bounded, for the one reclaim event that the manager posts for `execution`. */
  private def awaitReclaimEvent(
      hooks: ManagerHooks,
      execution: Long): SparkRapidsShuffleReclaimEvent =
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      val posted = hooks.reclaimEvents.filter(_.executionId == execution)
      assertResult(1, s"reclaim events of execution $execution")(posted.size)
      posted.head
    }

  /** The policy's verdict on each fence that held `shuffleId`, in the order it gave them. */
  private def verdictsOn(t: Tracker[_], shuffleId: Int): List[(Long, Verdict)] =
    t.calls.all.collect {
      case r: Released if r.shuffleId == shuffleId => (r.fenceId, Release)
      case w: Withdrawn if w.shuffleId == shuffleId => (w.fenceId, Withdrawal)
    }

  /**
   * Checks that the policy disabled the reclaim once, and that one captured line, the WARN,
   * gives the reason.
   */
  private def assertDisabledOnce(t: Tracker[_], logged: Array[String]): Unit = {
    val disables = t.calls.disables
    assertResult(1, s"disable calls in ${t.calls.all}")(disables.size)
    assertResult(1, s"lines that give the reason, in:\n${logged.mkString("\n")}")(
      logged.count(_.contains(disables.head.reason)))
  }

  /** The position on the tracker's queue of the first event that `matches`, which must exist. */
  private def positionOf(t: Tracker[_], what: String)(
      matches: PartialFunction[SparkListenerEvent, Boolean]): Int = {
    val position = t.seen.events.indexWhere(matches.lift(_).contains(true))
    assert(position >= 0, s"the tracker's queue has no $what")
    position
  }

  /** The action whose job submitted the map stage of `shuffleId`. */
  private def actionOfShuffle(t: Tracker[_], shuffleId: Int): Option[String] =
    t.stagesSubmitted.find(_.stageInfo.shuffleDepId.contains(shuffleId))
      .flatMap(stage => actionOf(stage.properties))

  test("a reclaim request carries the largest task id the tracker saw at its decision") {
    startSession("local[2]")
    withManager(_ => true) { (t, hooks) =>
      val step = hooks.newHold()
      val marker = hooks.newHold()
      hooks.holdSteps(_ => Some(step))
      hooks.holdFences(marker)
      tagged("query")(rowsOf(oneShuffle(7)))
      settle(t)
      val e1 = executionOf(t, "query")
      val s = single(ownedBy(t, e1), "shuffles the execution owns")
      val dispatch = eventually(Timeout(Span(WaitSeconds, Seconds))) {
        t.calls.dispatches.find(_.shuffles.exists(_.shuffleId == s))
          .getOrElse(fail(s"no decision for shuffle $s: ${t.calls.all}"))
      }
      // the tracker sees this job's tasks while the driver step and the marker wait
      tagged("outside")(t.sc.parallelize(1 to 8, 8).count())
      settle(t)
      step.open()
      marker.open()
      // a dispatch's seen count covers the events up to the one that caused the decision
      val w = t.seen.events.take(dispatch.seen)
        .collect { case start: SparkListenerTaskStart => start.taskInfo.taskId }
        .foldLeft(-1L)((largest, id) => math.max(largest, id))
      assertResult(List(Set(SparkContext.DRIVER_IDENTIFIER)), s"the targets of shuffle $s")(
        dispatch.shuffles.filter(_.shuffleId == s).map(_.targets))
      assertResult(ShuffleReclaimRequest(s, w), "the request a poll from the target gets")(
        awaitPolled(t, s))
      val starts = t.handled.events.collect { case start: SparkListenerTaskStart => start }
      def taskIds(stages: Set[Int]): List[Long] =
        starts.filter(start => stages.contains(start.stageId)).map(_.taskInfo.taskId)
      val queryTasks = taskIds(stagesOf(t, e1))
      val laterTasks = taskIds(t.jobStarts.filter(j => actionOf(j.properties).contains("outside"))
        .flatMap(_.stageInfos.map(_.stageId)).toSet)
      assert(queryTasks.nonEmpty && queryTasks.forall(_ <= w),
        s"watermark $w and the query's tasks $queryTasks")
      assertResult(8, "tasks of the job outside SQL")(laterTasks.size)
      assert(laterTasks.forall(_ > w), s"watermark $w and the later job's tasks $laterTasks")
    }
  }

  test("a job that reads a shuffle before its fence's marker withdraws the shuffle") {
    startSession("local[2]")
    withManager(_ => true) { (t, hooks) =>
      val step = hooks.newHold()
      hooks.holdSteps(_ => Some(step))
      val df = oneShuffle(7)
      val firstRows = tagged("first")(rowsOf(df))
      assert(step.awaitReached(), "no driver step reached the hold")
      // the clear waits at the hold, so this collect reads the outputs the first one wrote
      val secondRows = tagged("second")(rowsOf(df))
      settle(t)
      val e1 = executionOf(t, "first")
      assertPlannedAdaptively(t, e1)
      val s = single(ownedBy(t, e1), "shuffles the first execution owns")
      assert(!ownedBy(t, executionOf(t, "second")).contains(s),
        s"the second collect ran shuffle $s's map stage")
      step.open()
      val event = awaitReclaimEvent(hooks, e1)
      assertResult(List(Withdrawal), s"verdicts on shuffle $s")(verdictsOn(t, s).map(_._2))
      assert(event.skippedShared.contains(s), event)
      assert(!pollDriver(t).exists(_.shuffleId == s), s"a poll listed shuffle $s")
      assertResult(firstRows)(secondRows)
      assertNoFetchFailure(t)
    }
  }

  test("a read that crosses the driver clear fails its fetch, and its job reruns the map stage") {
    startSession("local[1]", AqeKey -> "false")
    withManager(_ => true) { (t, hooks) =>
      val step = hooks.newHold()
      hooks.holdSteps(_ => Some(step))
      val df = oneShuffle(7)
      val firstRows = tagged("first")(rowsOf(df))
      assert(step.awaitReached(), "no driver step reached the hold")
      settle(t)
      val e1 = executionOf(t, "first")
      val s = single(ownedBy(t, e1), "shuffles the first execution owns")
      resetGate(-1)
      // a one-task job at the gate holds the only core, so the second collect's reduce tasks
      // look up the map outputs after the clear
      val core = startAction("core") {
        t.sc.parallelize(1 to 1, 1).map { x => awaitGate(); x }.count()
      }
      var second: Option[ActionThread[List[String]]] = None
      try {
        assert(awaitEntered(), "the one-task job did not reach the gate")
        second = Some(startAction("second")(rowsOf(df)))
        eventually(Timeout(Span(WaitSeconds, Seconds))) {
          assert(t.jobStarts.exists(j => actionOf(j.properties).contains("second")),
            "the tracker has not handled the second collect's JobStart")
        }
        step.open()
        eventually(Timeout(Span(WaitSeconds, Seconds))) {
          assert(verdictsOn(t, s).exists(_._2 == Withdrawal), s"shuffle $s was not withdrawn")
        }
      } finally {
        step.open()
        openGate()
        core.await()
        second.foreach(_.await())
      }
      assert(core.await().isSuccess, "the one-task job failed")
      val secondRows = second.map(_.await().get)
      settle(t)
      val e2 = executionOf(t, "second")
      assertResult(Some(firstRows))(secondRows)
      val fetchFailures = t.taskEnds.map(_.reason).collect { case f: FetchFailed => f }
      assert(fetchFailures.exists(_.bmAddress == null), s"fetch failures: $fetchFailures")
      assert(ownedBy(t, e2).contains(s), s"the second collect did not rerun shuffle $s's map stage")
      assertResult(List(e1), s"owners of the decisions for shuffle $s")(
        t.calls.decisionsFor(s).map(_._2.owner))
    }
  }

  test("a job that reads a shuffle after its fence's marker reruns its map stage, and the " +
      "rerun is reclaimed again") {
    startSession("local[2]")
    withManager(_ => true) { (t, _) =>
      val df = oneShuffle(7)
      val firstRows = tagged("first")(rowsOf(df))
      settle(t)
      val e1 = executionOf(t, "first")
      assertPlannedAdaptively(t, e1)
      val s = single(ownedBy(t, e1), "shuffles the first execution owns")
      val w1 = awaitPolled(t, s).watermark
      val secondRows = tagged("second")(rowsOf(df))
      settle(t)
      val e2 = executionOf(t, "second")
      assert(ownedBy(t, e2).contains(s), s"the second collect did not rerun shuffle $s's map stage")
      assertNoFetchFailure(t)
      assertResult(firstRows)(secondRows)
      val decisions = t.calls.decisionsFor(s)
      assertResult(List(e1, e2), s"owners of the decisions for shuffle $s")(
        decisions.map(_._2.owner))
      assertResult(w1, "the watermark of the first decision")(decisions.head._2.watermark)
      assert(decisions(1)._2.watermark > w1, s"the second decision's watermark is not above $w1")
      eventually(Timeout(Span(WaitSeconds, Seconds))) {
        assertResult(decisions.map { case (fenceId, _) => (fenceId, Release) },
          s"verdicts on shuffle $s")(verdictsOn(t, s))
      }
    }
  }

  test("a dropped event seen at a fence's marker disables the reclaim and withdraws the " +
      "fence's shuffles") {
    // the counter never falls for a SparkContext, so this case has a session of its own
    startSession("local[2]", AqeKey -> "false")
    withManager(_ => true) { (t, hooks) =>
      val marker = hooks.newHold()
      hooks.holdFences(marker)
      tagged("first")(rowsOf(twoAggregates()))
      // the marker waits at the hold once the driver has cleared both shuffles
      assert(marker.awaitReached(), "no fence marker reached the hold")
      settle(t)
      val e = executionOf(t, "first")
      val owned = ownedBy(t, e)
      assertResult(2, "shuffles the execution owns")(owned.size)
      assertResult(List(owned.toSet), "the shuffles of each dispatch")(
        t.calls.dispatches.map(_.shuffles.map(_.shuffleId).toSet))
      t.sc.listenerBus.metrics.metricRegistry.counter(DroppedEventsName).inc()
      val logged = LogCaptureUtils.captureLogsFrom(Seq(ManagerLogger)) {
        marker.open()
        awaitReclaimEvent(hooks, e)
        // the policy disables the reclaim on the tracker's thread, as it handles the marker
        t.drain()
      }
      assertResult(owned.toSet, s"shuffles execution $e left to GC as shared")(
        awaitReclaimEvent(hooks, e).skippedShared.toSet)
      assert(!pollDriver(t).exists(r => owned.contains(r.shuffleId)), s"a poll listed $owned")
      assertDisabledOnce(t, logged)
      tagged("second")(rowsOf(oneShuffle(5)))
      settle(t)
      val later = single(ownedBy(t, executionOf(t, "second")), "shuffles the second query owns")
      assert(t.calls.decisionsFor(later).isEmpty, t.calls.all)
      val held = t.policy.heldState
      assert(held.stages.isEmpty && held.jobs.isEmpty && held.executions.isEmpty, held)
    }
  }

  test("a tracker handler that throws disables the reclaim and withdraws the shuffle in flight") {
    startSession("local[2]")
    // the one shuffle that the predicate admits once armed, or -1 while it admits every one
    val admitted = new AtomicLong(-1L)
    val isCandidate: Int => Boolean = shuffleId => {
      val only = admitted.get
      if (only >= 0 && shuffleId != only) {
        throw new IllegalStateException(s"injected failure for shuffle $shuffleId")
      }
      true
    }
    withManager(isCandidate) { (t, hooks) =>
      val step = hooks.newHold()
      hooks.holdSteps(_ => Some(step))
      tagged("first")(rowsOf(oneShuffle(7)))
      assert(step.awaitReached(), "no driver step reached the hold")
      settle(t)
      val e1 = executionOf(t, "first")
      val s1 = single(ownedBy(t, e1), "shuffles the first query owns")
      admitted.set(s1)
      val logged = LogCaptureUtils.captureLogsFrom(Seq(ManagerLogger)) {
        // the policy's handler of this collect's JobStart throws
        tagged("second")(rowsOf(oneShuffle(5)))
        step.open()
        awaitReclaimEvent(hooks, e1)
        t.drain()
      }
      assert(awaitReclaimEvent(hooks, e1).skippedShared.contains(s1),
        s"shuffle $s1 is not under skippedShared")
      assert(!pollDriver(t).exists(_.shuffleId == s1), s"a poll listed shuffle $s1")
      assertDisabledOnce(t, logged)
      settle(t)
      val s2 = single(ownedBy(t, executionOf(t, "second")), "shuffles the second query owns")
      assert(t.calls.decisionsFor(s2).isEmpty, t.calls.all)
    }
  }

  test("an execution whose planning fails ends with no start, and the policy stays enabled") {
    val t = startTracker("local[2]")
    val strategy = new FailingStrategy(t.sc)
    val strategies = spark.experimental.extraStrategies
    spark.experimental.extraStrategies = Seq(strategy)
    try {
      strategy.armed = true
      intercept[IllegalStateException](tagged("unplanned")(oneShuffle(7).collect()))
    } finally {
      strategy.armed = false
      spark.experimental.extraStrategies = strategies
    }
    t.drain()
    tagged("planned")(rowsOf(oneShuffle(5)))
    settle(t)
    val failed = strategy.executionIds.distinct
    assertResult(1, "executions whose planning failed")(failed.size)
    val f = failed.head
    val events = t.handled.events
    assert(events.exists {
      case end: SparkListenerSQLExecutionEnd => end.executionId == f
      case _ => false
    }, s"the tracker's queue has no End of execution $f")
    assert(!events.exists {
      case start: SparkListenerSQLExecutionStart => start.executionId == f
      case _ => false
    }, s"the tracker's queue has a start of execution $f")
    assert(!t.policy.heldState.executions.contains(f), t.policy.heldState)
    assert(!t.handoff.isDisabled && t.calls.disables.isEmpty, t.calls.all)
    val e = executionOf(t, "planned")
    val s = single(ownedBy(t, e), "shuffles the planned query owns")
    assertResult(List(e), s"owners of the decisions for shuffle $s")(
      t.calls.decisionsFor(s).map(_._2.owner))
  }

  test("an execution that starts after a later one, and after a job of its own, owns its " +
      "shuffle") {
    val t = startTracker("local[2]")
    val strategy = new LateStartStrategy(t.sc, "A")
    t.handled.onEach {
      case start: SparkListenerJobStart
          if strategy.executionId.exists(a => executionId(start.properties).contains(a)) =>
        strategy.jobSeen.countDown()
      case _ =>
    }
    resetGate(-1)
    val strategies = spark.experimental.extraStrategies
    spark.experimental.extraStrategies = Seq(strategy)
    try {
      val threadA = startAction("A")(rowsOf(oneShuffle(7)))
      try {
        assert(strategy.holding.await(WaitSeconds, TimeUnit.SECONDS), "thread A did not plan")
        tagged("B")(rowsOf(oneShuffle(5)))
        strategy.release.countDown()
        eventually(Timeout(Span(WaitSeconds, Seconds))) {
          assert(t.handled.events.exists {
            case start: SparkListenerSQLExecutionStart =>
              strategy.executionId.contains(start.executionId)
            case _ => false
          }, "the tracker has not handled the start of thread A's execution")
        }
      } finally {
        strategy.release.countDown()
        strategy.jobSeen.countDown()
        openGate()
      }
      strategy.job.foreach(job => Await.result(job, Duration(WaitSeconds, TimeUnit.SECONDS)))
      threadA.await().get
    } finally {
      spark.experimental.extraStrategies = strategies
    }
    settle(t)
    val a = strategy.executionId.getOrElse(fail("thread A planned outside an execution"))
    val b = executionOf(t, "B")
    assert(a < b, s"execution $a took its id after execution $b")
    val jobId = single(strategy.job.toList.flatMap(_.jobIds), "jobs thread A submitted")
    val positions = List(
      positionOf(t, s"start of execution $b") {
        case start: SparkListenerSQLExecutionStart => start.executionId == b
      },
      positionOf(t, s"End of execution $b") {
        case end: SparkListenerSQLExecutionEnd => end.executionId == b
      },
      positionOf(t, s"JobStart of job $jobId under execution $a") {
        case start: SparkListenerJobStart =>
          start.jobId == jobId && executionId(start.properties).contains(a)
      },
      positionOf(t, s"start of execution $a") {
        case start: SparkListenerSQLExecutionStart => start.executionId == a
      },
      positionOf(t, s"JobEnd of job $jobId") {
        case end: SparkListenerJobEnd => end.jobId == jobId
      })
    assertResult(positions.sorted, "positions of B's start and End, then of the job's start, " +
      "A's start and the job's End")(positions)
    val sa = single(ownedBy(t, a), "shuffles execution A owns")
    val sb = single(ownedBy(t, b), "shuffles execution B owns")
    assertReclaimedOnce(t, sa, a)
    assertReclaimedOnce(t, sb, b)
    assert(!t.calls.skips.exists(k => k.shuffleId == sa || k.shuffleId == sb), t.calls.all)
    assert(!t.handoff.isDisabled && t.calls.disables.isEmpty, t.calls.all)
  }

  test("a job of an ended execution whose entry is gone creates no entry and owns no shuffle") {
    val t = startTracker("local[2]")
    tagged("first")(rowsOf(oneShuffle(7)))
    settle(t)
    val e1 = executionOf(t, "first")
    val s1 = single(ownedBy(t, e1), "shuffles the first query owns")
    // the dispatch decides the execution's last shuffle, so the tracker drops its entry
    assertResult(List(e1), s"owners of the decisions for shuffle $s1")(
      t.calls.decisionsFor(s1).map(_._2.owner))
    resetGate(0)
    // a thread that inherits an ended execution's id, as PySpark's serving thread does
    t.sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, e1.toString)
    try {
      val job = tagged("rdd") {
        t.sc.parallelize(1 to 100, 4).map { i => awaitGate(); (i % 4, i) }
          .reduceByKey(_ + _, 2).countAsync()
      }
      try {
        assert(awaitEntered(), "no map task of the RDD job reached the gate")
        eventually(Timeout(Span(WaitSeconds, Seconds))) {
          assert(t.handled.events.exists {
            case start: SparkListenerTaskStart => start.taskInfo.taskId == heldTaskId
            case _ => false
          }, s"the tracker has not handled the start of task $heldTaskId")
        }
        t.drain()
        assert(!t.policy.heldState.executions.contains(e1), t.policy.heldState)
      } finally {
        openGate()
      }
      Await.result(job, Duration(WaitSeconds, TimeUnit.SECONDS))
    } finally {
      t.sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    }
    settle(t)
    assert(!t.policy.heldState.executions.contains(e1), t.policy.heldState)
    val rddShuffle = single(t.jobStarts.filter(j => actionOf(j.properties).contains("rdd"))
      .flatMap(_.stageInfos.flatMap(_.shuffleDepId)).distinct, "shuffles the RDD job writes")
    assert(!t.calls.all.exists {
      case d: Dispatched => d.shuffles.exists(_.shuffleId == rddShuffle)
      case k: Skipped => k.shuffleId == rddShuffle
      case c: Closed => c.ownedIds.contains(rddShuffle)
      case _ => false
    }, t.calls.all)
    assert(!t.handoff.isDisabled && t.calls.disables.isEmpty, t.calls.all)
  }

  test("a loop of fresh queries reclaims each shuffle once, each next query inside the fence " +
      "before it") {
    startSession("local[2]", AqeKey -> "false", BroadcastKey -> "-1", PartitionsKey -> "2")
    withManager(_ => true) { (t, hooks) =>
      spark.range(0, 100, 1, 2).selectExpr("id % 10 AS k", "id AS v")
        .createOrReplaceTempView("lhs")
      spark.range(0, 100, 1, 2).selectExpr("id % 10 AS k", "id * 2 AS w")
        .createOrReplaceTempView("rhs")
      val text = "SELECT lhs.k, count(*) AS n, sum(rhs.w) AS s FROM lhs JOIN rhs " +
        "ON lhs.k = rhs.k GROUP BY lhs.k"
      // iteration k's clears wait until iteration k + 1's collect returns
      val holds = Map("iteration-1" -> hooks.newHold(), "iteration-2" -> hooks.newHold())
      hooks.holdSteps(s => actionOfShuffle(t, s).flatMap(holds.get))
      val plans = (1 to 3).map { k =>
        val df = spark.sql(text)
        tagged(s"iteration-$k")(df.collect())
        holds.get(s"iteration-${k - 1}").foreach(_.open())
        df.queryExecution.executedPlan
      }
      assertResult(2, s"shuffle exchanges in the first iteration's plan:\n${plans.head}")(
        planNodes(plans.head).count(_.isInstanceOf[ShuffleExchangeExec]))
      settle(t)
      val executions = (1 to 3).map(k => executionOf(t, s"iteration-$k"))
      val owned = executions.map(ownedBy(t, _))
      val ids = owned.flatten
      eventually(Timeout(Span(WaitSeconds, Seconds))) {
        val released = t.calls.all.collect { case r: Released => r.shuffleId }.toSet
        assert(ids.forall(released.contains), s"releases of $released, for the shuffles $owned")
      }
      t.drain()
      assertResult(6, s"shuffles the iterations own: $owned")(ids.size)
      assertResult(ids.distinct, s"shuffles the iterations own: $owned")(ids)
      val mapStages = executions.map(e => t.stagesSubmitted.count(stage =>
        stage.stageInfo.shuffleDepId.isDefined && executionId(stage.properties).contains(e)))
      assertResult(Seq.fill(3)(mapStages.head), "map stages each iteration submitted")(mapStages)
      // each next iteration's JobStart reached the tracker between the dispatch before it and
      // that dispatch's marker, which the hold kept back
      (0 until executions.size - 1).foreach { k =>
        val next = positionOf(t, s"JobStart of execution ${executions(k + 1)}") {
          case start: SparkListenerJobStart =>
            executionId(start.properties).contains(executions(k + 1))
        }
        t.calls.dispatches.filter(_.shuffles.exists(_.owner == executions(k))).foreach { d =>
          val marker = positionOf(t, s"marker of fence ${d.fenceId}") {
            case fence: RapidsShuffleReclaimFence => fence.fenceId == d.fenceId
          }
          assert(d.seen - 1 < next && next < marker,
            s"the next JobStart at $next is outside $d, whose marker is at $marker")
        }
      }
      executions.zip(owned).foreach { case (e, shuffles) =>
        shuffles.foreach { s =>
          val decisions = t.calls.decisionsFor(s)
          assertResult(List(e), s"owners of the decisions for shuffle $s")(
            decisions.map(_._2.owner))
          assertResult(decisions.map { case (fenceId, _) => (fenceId, Release) },
            s"verdicts on shuffle $s")(verdictsOn(t, s))
        }
      }
      assert(t.calls.skips.isEmpty, t.calls.all)
    }
  }
}

object UcxReclaimSessionSuite {
  /** How long a case waits for the bus, an action or a task. */
  val WaitSeconds = 30L

  /** How long a held task waits at the gate: longer than any case waits for it. */
  private val GateSeconds = 60L

  /** The local property that names the action whose thread submitted a job. */
  val ActionKey = "ucx.reclaim.test.action"

  private val JobGroup = "ucx-reclaim-held"

  val AqeKey = "spark.sql.adaptive.enabled"
  val PartitionsKey = "spark.sql.shuffle.partitions"
  val BroadcastKey = "spark.sql.autoBroadcastJoinThreshold"

  val DroppedEventsName = s"queue.${ShuffleCleanupListener.QUEUE_NAME}.numDroppedEvents"

  /** The key that the policy's reason for disabling itself after a drop names. */
  val CapacityKey =
    s"spark.scheduler.listenerbus.eventqueue.${ShuffleCleanupListener.QUEUE_NAME}.capacity"

  val PolicyLogger: String = classOf[UcxReclaimPolicy].getName

  /** The reclaim manager's logger, whose WARN says why the reclaim disabled itself. */
  val ManagerLogger: String = classOf[ShuffleCleanupManager].getName

  /** A count of 1 that follows the word "undecided", whatever the live executions' count is. */
  private val OneUndecided = "(?i)\\bundecided\\b[^:]*:\\s*1\\b".r

  /** What `twoAggregates` returns, as sorted row strings. */
  val TwoAggregatesRows: List[String] = (0L until 1000L).groupBy(_ % 7 % 2).toList
    .map { case (p, ids) => s"[$p,${ids.sum}]" }.sorted

  @volatile private var gate = new CountDownLatch(0)
  @volatile private var entered = new CountDownLatch(0)
  @volatile private var heldPartition = -1
  private val heldTask = new AtomicLong(-1L)

  /** Closes the gate on the tasks of `partition`, or of every partition when it is -1. */
  def resetGate(partition: Int): Unit = {
    heldTask.set(-1L)
    heldPartition = partition
    entered = new CountDownLatch(1)
    gate = new CountDownLatch(1)
  }

  def openGate(): Unit = gate.countDown()

  /** Waits, bounded, until a task reaches the closed gate. */
  def awaitEntered(): Boolean = entered.await(WaitSeconds, TimeUnit.SECONDS)

  /** The attempt id of the first task the gate held, or -1. */
  def heldTaskId: Long = heldTask.get

  /** Holds a task of the gate's partition, bounded, until a case opens the gate. */
  def awaitGate(): Unit = {
    val context = TaskContext.get()
    if (heldPartition < 0 || context.partitionId() == heldPartition) {
      heldTask.compareAndSet(-1L, context.taskAttemptId())
      entered.countDown()
      gate.await(GateSeconds, TimeUnit.SECONDS)
    }
  }

  /** Passes its input on, after `awaitGate`. */
  val gated = udf { (x: Long) =>
    awaitGate()
    x
  }

  /** Passes its key on, and fails the task that reads key 3. */
  val failsOnKeyThree = udf { (k: Long) =>
    if (k == 3L) {
      throw new IllegalStateException("injected failure of a reduce task")
    }
    k
  }

  /** A call that the policy made into its handoff. */
  sealed trait HandoffCall {
    /** How many events the tracker's queue had delivered when the policy made the call. */
    def seen: Int
  }

  case class Dispatched(
      fenceId: Long,
      shuffles: List[ShuffleReclaimDecision],
      decidedAtMs: Long,
      seen: Int) extends HandoffCall

  case class Skipped(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule, seen: Int)
    extends HandoffCall

  case class Closed(executionId: Long, endedAtMs: Long, ownedIds: List[Int], seen: Int)
    extends HandoffCall

  case class ExecutorGone(executorId: String, seen: Int) extends HandoffCall

  case class Released(fenceId: Long, shuffleId: Int, seen: Int) extends HandoffCall

  case class Withdrawn(fenceId: Long, shuffleId: Int, seen: Int) extends HandoffCall

  case class Disabled(reason: String, seen: Int) extends HandoffCall

  /** Records each call the policy makes, with the count of events `seen` had, then passes it on. */
  class CallRecorder(handoff: ShuffleReclaimHandoff, seen: EventRecorder)
      extends ShuffleReclaimHandoff {
    private val recorded = new ConcurrentLinkedQueue[HandoffCall]()

    def all: List[HandoffCall] = recorded.asScala.toList

    def dispatches: List[Dispatched] = all.collect { case d: Dispatched => d }

    def skips: List[Skipped] = all.collect { case k: Skipped => k }

    def closes: List[Closed] = all.collect { case c: Closed => c }

    def disables: List[Disabled] = all.collect { case d: Disabled => d }

    /** Each decision dispatched for a shuffle, with the fence it went under. */
    def decisionsFor(shuffleId: Int): List[(Long, ShuffleReclaimDecision)] =
      dispatches.flatMap(d => d.shuffles.filter(_.shuffleId == shuffleId).map(s => (d.fenceId, s)))

    override def dispatch(
        fenceId: Long,
        shuffles: Seq[ShuffleReclaimDecision],
        decidedAtMs: Long): Unit = {
      recorded.add(Dispatched(fenceId, shuffles.toList, decidedAtMs, seen.count))
      handoff.dispatch(fenceId, shuffles, decidedAtMs)
    }

    override def skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule): Unit = {
      recorded.add(Skipped(shuffleId, executionId, rule, seen.count))
      handoff.skip(shuffleId, executionId, rule)
    }

    override def executionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int]): Unit = {
      recorded.add(Closed(executionId, endedAtMs, ownedIds.toList, seen.count))
      handoff.executionClosed(executionId, endedAtMs, ownedIds)
    }

    override def executorRemoved(executorId: String): Unit = {
      recorded.add(ExecutorGone(executorId, seen.count))
      handoff.executorRemoved(executorId)
    }

    override def release(fenceId: Long, shuffleId: Int): Unit = {
      recorded.add(Released(fenceId, shuffleId, seen.count))
      handoff.release(fenceId, shuffleId)
    }

    override def withdraw(fenceId: Long, shuffleId: Int): Unit = {
      recorded.add(Withdrawn(fenceId, shuffleId, seen.count))
      handoff.withdraw(fenceId, shuffleId)
    }

    override def isDisabled: Boolean = handoff.isDisabled

    override def disable(reason: String): Unit = {
      recorded.add(Disabled(reason, seen.count))
      handoff.disable(reason)
    }
  }

  /** Where a shuffle that a fence holds stands in the default sink. */
  sealed trait Verdict

  case object Awaiting extends Verdict

  case object Release extends Verdict

  case object Withdrawal extends Verdict

  /**
   * The manager's stand-in. On the tracker's thread it runs the real driver step and posts the
   * fence, applies each verdict, and keeps a disabled flag of its own whose first set withdraws
   * every shuffle still waiting for a verdict.
   */
  class DefaultSink(sc: SparkContext) extends ShuffleReclaimHandoff {
    private val disabled = new AtomicBoolean(false)
    // each driver step, as the shuffle and whether the driver held its status
    private val steps = mutable.ArrayBuffer[(Int, Boolean)]()
    private val verdicts = mutable.LinkedHashMap[(Long, Int), Verdict]()

    /** Whether each driver step for a shuffle found its status, in order. */
    def stepsFor(shuffleId: Int): List[Boolean] = synchronized {
      steps.toList.collect { case (s, held) if s == shuffleId => held }
    }

    /** The verdict on a shuffle under each fence that held it, in dispatch order. */
    def verdictsFor(shuffleId: Int): List[(Long, Verdict)] = synchronized {
      verdicts.toList.collect { case ((fenceId, s), verdict) if s == shuffleId =>
        (fenceId, verdict)
      }
    }

    override def dispatch(
        fenceId: Long,
        shuffles: Seq[ShuffleReclaimDecision],
        decidedAtMs: Long): Unit = {
      shuffles.foreach { d =>
        val held = TrampolineUtil.clearMapOutputs(d.shuffleId)
        synchronized {
          steps += ((d.shuffleId, held))
          if (held) {
            verdicts((fenceId, d.shuffleId)) = Awaiting
          }
        }
      }
      TrampolineUtil.postEvent(sc, RapidsShuffleReclaimFence(fenceId))
    }

    override def skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule): Unit = {}

    override def executionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int]): Unit = {}

    override def executorRemoved(executorId: String): Unit = {}

    // once disabled, the manager applies a release as a withdrawal
    override def release(fenceId: Long, shuffleId: Int): Unit =
      decide(fenceId, shuffleId, if (disabled.get) Withdrawal else Release)

    override def withdraw(fenceId: Long, shuffleId: Int): Unit =
      decide(fenceId, shuffleId, Withdrawal)

    override def isDisabled: Boolean = disabled.get

    override def disable(reason: String): Unit = {
      if (disabled.compareAndSet(false, true)) {
        synchronized {
          verdicts.keys.toList.foreach { key =>
            if (verdicts(key) == Awaiting) {
              verdicts(key) = Withdrawal
            }
          }
        }
      }
    }

    private def decide(fenceId: Long, shuffleId: Int, verdict: Verdict): Unit = synchronized {
      if (verdicts.get((fenceId, shuffleId)).contains(Awaiting)) {
        verdicts((fenceId, shuffleId)) = verdict
      }
    }
  }

  /** Records every event its queue delivers, in delivery order, and runs a case's hook on each. */
  class EventRecorder extends SparkFirehoseListener {
    private val recorded = mutable.ArrayBuffer[SparkListenerEvent]()
    @volatile private var hook: SparkListenerEvent => Unit = (_ => ())

    override def onEvent(event: SparkListenerEvent): Unit = {
      synchronized {
        recorded += event
      }
      hook(event)
    }

    /** Runs `f` on the queue's thread for each later event, after recording it. */
    def onEach(f: SparkListenerEvent => Unit): Unit = hook = f

    def count: Int = synchronized(recorded.size)

    def events: List[SparkListenerEvent] = synchronized(recorded.toList)
  }

  /**
   * A case's tracker: the policy and its listener on the tracker's queue, the recorders that get
   * each event before (`seen`) and after (`handled`) the policy, every call the policy made, and
   * the handoff behind those calls.
   */
  class Tracker[H](
      val sc: SparkContext,
      val policy: UcxReclaimPolicy,
      val listener: ShuffleCleanupListener,
      val calls: CallRecorder,
      val seen: EventRecorder,
      val handled: EventRecorder,
      val handoff: H) {
    /** Waits, bounded, until every queue on the bus has handled every event posted to it. */
    def drain(): Unit = sc.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(WaitSeconds))

    def jobStarts: List[SparkListenerJobStart] =
      handled.events.collect { case e: SparkListenerJobStart => e }

    def stagesSubmitted: List[SparkListenerStageSubmitted] =
      handled.events.collect { case e: SparkListenerStageSubmitted => e }

    def taskEnds: List[SparkListenerTaskEnd] =
      handled.events.collect { case e: SparkListenerTaskEnd => e }
  }

  /** An action on a daemon thread of its own, whose outcome a case collects. */
  class ActionThread[T](name: String, body: () => T) {
    private val outcome = new AtomicReference[Try[T]]()
    private val thread = new Thread(() => outcome.set(Try(body())), name)
    thread.setDaemon(true)
    thread.start()

    /** How the action ended, or a failure if it has not ended within `WaitSeconds`. */
    def await(): Try[T] = {
      thread.join(TimeUnit.SECONDS.toMillis(WaitSeconds))
      Option(outcome.get).getOrElse(Failure(new IllegalStateException(s"$name has not ended")))
    }

    /** Whether the action has ended, which tells its own failure from `await`'s. */
    def hasEnded: Boolean = outcome.get != null
  }

  /**
   * A point on the reclaim manager's thread that waits, bounded, until a case opens it, and that
   * tells the case when the thread has reached it.
   */
  class Hold {
    private val reached = new CountDownLatch(1)
    private val opened = new CountDownLatch(1)

    /** Waits, bounded, until the manager's thread reaches the hold. */
    def awaitReached(): Boolean = reached.await(WaitSeconds, TimeUnit.SECONDS)

    def open(): Unit = opened.countDown()

    /** Called on the manager's thread: waits, bounded, until a case opens the hold. */
    def pass(): Unit = {
      reached.countDown()
      opened.await(GateSeconds, TimeUnit.SECONDS)
    }
  }

  /**
   * The real reclaim manager's driver step and event sink. The step runs the real clear, and the
   * sink posts to the real bus and keeps each reclaim event. A case can hold either on the
   * manager's thread: a shuffle's step before its clear, or a fence's marker before its post.
   */
  class ManagerHooks(sc: SparkContext) {
    private val holds = new ConcurrentLinkedQueue[Hold]()
    private val events = new ConcurrentLinkedQueue[SparkRapidsShuffleReclaimEvent]()
    @volatile private var stepHold: Int => Option[Hold] = (_ => None)
    @volatile private var markerHold: Option[Hold] = None

    /** A new closed hold, which `openAll` opens. */
    def newHold(): Hold = {
      val hold = new Hold
      holds.add(hold)
      hold
    }

    /** Holds the driver step of each shuffle that `holdOf` gives a hold. */
    def holdSteps(holdOf: Int => Option[Hold]): Unit = stepHold = holdOf

    /** Holds each fence's marker at `hold` before its post. */
    def holdFences(hold: Hold): Unit = markerHold = Some(hold)

    def openAll(): Unit = holds.asScala.foreach(_.open())

    def reclaimEvents: List[SparkRapidsShuffleReclaimEvent] = events.asScala.toList

    def driverStep(shuffleId: Int): Boolean = {
      stepHold(shuffleId).foreach(_.pass())
      TrampolineUtil.clearMapOutputs(shuffleId)
    }

    def postEvent(event: SparkListenerEvent): Unit = {
      event match {
        case _: RapidsShuffleReclaimFence => markerHold.foreach(_.pass())
        case reclaim: SparkRapidsShuffleReclaimEvent => events.add(reclaim)
        case _ =>
      }
      TrampolineUtil.postEvent(sc, event)
    }
  }

  /**
   * A planner strategy that plans nothing. While armed, it records the SQL execution id of the
   * thread that plans, and throws, so that execution's planning fails before Spark posts its
   * start.
   */
  class FailingStrategy(sc: SparkContext) extends SparkStrategy {
    @volatile var armed = false
    private val ids = new ConcurrentLinkedQueue[Long]()

    /** The execution id that each call made while armed found on its thread. */
    def executionIds: List[Long] = ids.asScala.toList

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      if (armed) {
        Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
          .foreach(id => ids.add(id.toLong))
        throw new IllegalStateException("injected failure to plan")
      }
      Nil
    }
  }

  /**
   * A planner strategy that plans nothing. On its first call from the thread whose action is
   * `action`, it records that thread's SQL execution id and holds the planning until `release`.
   * It then submits from that thread a job that the gate holds, and returns once `jobSeen`
   * opens, so the job carries the execution's id and starts before the execution does.
   */
  class LateStartStrategy(sc: SparkContext, action: String) extends SparkStrategy {
    val holding = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val jobSeen = new CountDownLatch(1)
    private val first = new AtomicBoolean(true)
    @volatile var executionId: Option[Long] = None
    @volatile var job: Option[FutureAction[Long]] = None

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      if (Option(sc.getLocalProperty(ActionKey)).contains(action) &&
          first.compareAndSet(true, false)) {
        executionId = Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(_.toLong)
        holding.countDown()
        release.await(WaitSeconds, TimeUnit.SECONDS)
        job = Some(sc.parallelize(1 to 2, 1).map { x => awaitGate(); x }.countAsync())
        jobSeen.await(WaitSeconds, TimeUnit.SECONDS)
      }
      Nil
    }
  }
}
