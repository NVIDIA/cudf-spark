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
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.{DatabricksShimVersion, GpuBuildLeft, GpuBuildRight, LogCaptureUtils, RapidsShuffleReclaimResponseMsg, RowBasedShuffleChecksumConf, ShimLoader, ShuffleCleanupManagerTestUtils, ShuffleReclaimRequest, ShuffleReclaimResult, UcxCatalogReclaimer}
import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{ABSENT, REMOVED}
import com.nvidia.spark.rapids.shims.SparkShimImpl
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkConf, SparkEnv, Success}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{broadcast, col, udf}
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsShuffleInternalManagerBase}

/**
 * What the executor's broadcast cache keeps of a driverless build once the UCX query-end reclaim
 * frees the build's shuffle, in local mode.
 */
class DriverlessBroadcastReclaimSuite extends AnyFunSuite with BeforeAndAfterEach {
  import DriverlessBroadcastReclaimSuite._

  private var spark: SparkSession = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override def afterEach(): Unit = {
    try {
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

  private def start(gateOn: Boolean): FailedTaskListener = {
    assume(!ShimLoader.getShimVersion.isInstanceOf[DatabricksShimVersion],
      "Databricks plans no driverless broadcast")
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.shuffle.manager", ShimLoader.getRapidsShuffleManagerClass)
      .set("spark.rapids.shuffle.mode", "UCX")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
      .set(RowBasedShuffleChecksumConf.ChecksumEnabledKey, "false")
      .set(RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey, "false")
      .set("spark.rapids.shuffle.ucx.cleanupOnQueryEnd.enabled", gateOn.toString)
      .set("spark.rapids.sql.driverlessBroadcast.enabled", "true")
      .set("spark.rapids.sql.join.hashTable.reuse", "false")
    spark = SparkSession.builder().config(conf).getOrCreate()
    val listener = new FailedTaskListener
    spark.sparkContext.addSparkListener(listener)
    listener
  }

  /**
   * A driverless broadcast join, filtered to a few rows. The join runs in the result stage, so
   * each action runs its tasks, and each task looks the build up in the cache.
   */
  private def query(): DataFrame = {
    val build = spark.range(0, BuildRows, 1, 4).selectExpr("id AS bk", "id * 3 AS bv")
    val probe = spark.range(0, ProbeRows, 1, 4)
      .selectExpr(s"(id * 7) % $BuildRows AS pk", "id AS pv")
    probe.join(broadcast(build), col("pk") === col("bk")).where(col("pv") % 1000 === 0)
  }

  private def driverlessJoin(df: DataFrame): GpuDriverlessBroadcastHashJoinExec = {
    val plan = df.queryExecution.executedPlan
    val joins = SparkShimImpl.findOperators(plan,
      _.isInstanceOf[GpuDriverlessBroadcastHashJoinExec])
    assertResult(1, s"driverless joins in:\n$plan")(joins.size)
    joins.head.asInstanceOf[GpuDriverlessBroadcastHashJoinExec]
  }

  private def buildShuffleId(join: GpuDriverlessBroadcastHashJoinExec): Int =
    (join.buildSide match {
      case GpuBuildLeft => join.left
      case GpuBuildRight => join.right
    }) match {
      case shuffle: GpuShuffleExchangeExecBase => shuffle.shuffleDependencyColumnar.shuffleId
      case ReusedExchangeExec(_, shuffle: GpuShuffleExchangeExecBase) =>
        shuffle.shuffleDependencyColumnar.shuffleId
      case other => fail(s"unexpected build plan $other")
    }

  /** The GPU shuffles of the plan, the build's included. */
  private def gpuShuffleIds(df: DataFrame, buildId: Int): Seq[Int] =
    (SparkShimImpl.findOperators(df.queryExecution.executedPlan,
      _.isInstanceOf[GpuShuffleExchangeExecBase])
      .map(_.asInstanceOf[GpuShuffleExchangeExecBase].shuffleDependencyColumnar.shuffleId) :+
      buildId).distinct

  private def awaitReclaimed(shuffleIds: Seq[Int]): Unit = {
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      assertResult(Seq.empty, "shuffles the catalog still holds")(
        shuffleIds.filter(GpuShuffleEnv.getCatalog.hasActiveShuffle))
    }
  }

  /** Waits until the driver has cleared the shuffle's map outputs and opened its request. */
  private def awaitRequest(shuffleId: Int): Unit = {
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      assert(ShuffleCleanupManagerTestUtils.pendingReclaim(shuffleId).isDefined,
        s"no reclaim request for shuffle $shuffleId")
    }
  }

  /** Waits until the local executor has reported its reclaim of the shuffle to the driver. */
  private def awaitAnswered(shuffleId: Int): Unit = {
    val executorId = SparkEnv.get.executorId
    eventually(Timeout(Span(WaitSeconds, Seconds))) {
      assert(ShuffleCleanupManagerTestUtils.pendingReclaim(shuffleId)
        .exists(_._2.contains(executorId)), s"no reclaim of shuffle $shuffleId reported")
    }
  }

  private def drain(): Unit = spark.sparkContext.listenerBus.waitUntilEmpty(WaitSeconds * 1000L)

  /** Builds that the join's tasks made, since the last action reset its metrics. */
  private def builds(join: GpuDriverlessBroadcastHashJoinExec): Long =
    join.metrics("executorBuilds").value

  private def cacheHits(join: GpuDriverlessBroadcastHashJoinExec): Long =
    join.metrics("executorBuildCacheHits").value

  private def cachedBuilds: Int = GpuShuffleEnv.getBroadcastCache.activeEntryCount

  private def rows(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq

  test("with the reclaim gate off, a second collect reuses the cached build, and an ABSENT " +
      "reclaim that detaches the build logs its line at INFO") {
    val listener = start(gateOn = false)
    val df = query()
    val first = rows(df)
    val join = driverlessJoin(df)
    val buildId = buildShuffleId(join)
    assertResult(first, "rows of the second collect")(rows(df))
    assertResult(0L, "builds of the second collect")(builds(join))
    assert(cacheHits(join) > 0, "the second collect hit no cached build")
    assertResult(1, "builds cached")(cachedBuilds)
    assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(buildId),
      s"the catalog dropped shuffle $buildId")
    // what an executor that only read the build shuffle holds: the build, and none of the shuffle
    assertResult(REMOVED)(GpuShuffleEnv.getCatalog.reclaimShuffle(buildId, Long.MaxValue).outcome)
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer(send = result => sent += result)
    val reply =
      RapidsShuffleReclaimResponseMsg(Array(ShuffleReclaimRequest(buildId, Long.MaxValue)))
    val lines = LogCaptureUtils.captureLogsFrom(
        Seq(classOf[RapidsShuffleInternalManagerBase].getName), "INFO") {
      // the second reclaim detaches nothing, so its line stays at DEBUG
      (1 to 2).foreach(_ => reclaimer.poll(SparkEnv.get.executorId, _ => reply, () => false))
    }.filter(_.startsWith(s"Reclaimed shuffle $buildId "))
    assertResult(Seq(ABSENT, ABSENT))(sent.map(_.outcome))
    assertResult(1, lines.toSeq)(lines.length)
    assert(lines.head.contains("ABSENT") && lines.head.contains(" 1 cached broadcast builds"),
      lines.head)
    assertResult(0, "builds cached after the reclaim")(cachedBuilds)
    drain()
    assertResult(Seq.empty, "failed tasks")(listener.failedTasks)
  }

  /** Passes a value through, and once `holding` is set, makes each task wait for `release`. */
  private def holdUdf = udf { (value: Long) =>
    if (DriverlessBroadcastReclaimSuite.holding) {
      DriverlessBroadcastReclaimSuite.entered.countDown()
      DriverlessBroadcastReclaimSuite.release.await(WaitSeconds, TimeUnit.SECONDS)
    }
    value
  }

  test("with the reclaim gate on, a reclaim that arrives after a rerun built its own build drops " +
      "that build too, the rerun builds it again, and the end of the rerun's query drops the " +
      "build from the cache and from the device store") {
    val listener = start(gateOn = true)
    val handlesBefore = SpillFramework.stores.deviceStore.numHandles
    DriverlessBroadcastReclaimSuite.holding = false
    DriverlessBroadcastReclaimSuite.entered = new CountDownLatch(1)
    DriverlessBroadcastReclaimSuite.release = new CountDownLatch(1)
    // the UDF runs above the join, so a held task has borrowed its build
    val df = query().select(col("pk"), holdUdf(col("pv")).as("pv"))
    val rerunRows = new AtomicReference[Seq[String]]()
    val rerunError = new AtomicReference[Throwable]()
    val rerun = new Thread(() => {
      try {
        rerunRows.set(rows(df))
      } catch {
        case NonFatal(e) => rerunError.set(e)
      }
    }, "driverless-reclaim-rerun")
    rerun.setDaemon(true)
    var held = false
    try {
      ShuffleCleanupManagerTestUtils.holdReclaimPolls()
      held = true
      val first = rows(df)
      val join = driverlessJoin(df)
      val buildId = buildShuffleId(join)
      assertResult(1L, "builds of the first collect")(builds(join))
      awaitRequest(buildId)
      DriverlessBroadcastReclaimSuite.holding = true
      rerun.start()
      assert(DriverlessBroadcastReclaimSuite.entered.await(WaitSeconds, TimeUnit.SECONDS),
        "no task of the second collect reached the UDF")
      assertResult(2, "builds cached before the reclaim")(cachedBuilds)
      val lines = LogCaptureUtils.captureLogsFrom(
          Seq(classOf[RapidsShuffleInternalManagerBase].getName), "INFO") {
        ShuffleCleanupManagerTestUtils.releaseReclaimPolls()
        held = false
        awaitAnswered(buildId)
      }.filter(_.startsWith(s"Reclaimed shuffle $buildId "))
      assertResult(1, lines.toSeq)(lines.length)
      // the rerun's writers registered above the watermark, so the catalog kept their output,
      // and the reclaim dropped both the stale build and the rerun's
      assert(lines.head.contains("DEFERRED") && lines.head.contains(" 2 cached broadcast builds"),
        lines.head)
      assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(buildId),
        s"the reclaim freed the rerun's output of shuffle $buildId")
      DriverlessBroadcastReclaimSuite.holding = false
      DriverlessBroadcastReclaimSuite.release.countDown()
      rerun.join(TimeUnit.SECONDS.toMillis(WaitSeconds))
      assert(!rerun.isAlive, "the second collect never ended")
      Option(rerunError.get).foreach(e => fail("the second collect failed", e))
      assertResult(first, "rows of the second collect")(rerunRows.get)
      // the tasks that looked the build up after the reclaim built it once more
      assertResult(2L, "builds of the second collect")(builds(join))
      // the end of the second collect's query reclaims the rest, its build included
      awaitReclaimed(gpuShuffleIds(df, buildId))
      assertResult(0, "builds cached after the second collect's reclaim")(cachedBuilds)
      eventually(Timeout(Span(WaitSeconds, Seconds))) {
        assertResult(handlesBefore, "handles the device store tracks")(
          SpillFramework.stores.deviceStore.numHandles)
      }
      drain()
      assertResult(Seq.empty, "failed tasks")(listener.failedTasks)
    } finally {
      if (held) ShuffleCleanupManagerTestUtils.releaseReclaimPolls()
      DriverlessBroadcastReclaimSuite.holding = false
      DriverlessBroadcastReclaimSuite.release.countDown()
      rerun.join(TimeUnit.SECONDS.toMillis(WaitSeconds))
    }
  }
}

object DriverlessBroadcastReclaimSuite {
  val BuildRows = 100000L
  val ProbeRows = 20000L
  val WaitSeconds = 60L

  /** Makes `holdUdf` wait for `release`, once set. */
  @volatile var holding: Boolean = false

  /** Counted down by each held task as it reaches the UDF. */
  @volatile var entered: CountDownLatch = new CountDownLatch(1)

  /** Lets the held tasks go on. */
  @volatile var release: CountDownLatch = new CountDownLatch(1)

  /** Records each task that did not succeed. */
  class FailedTaskListener extends SparkListener {
    private val failures = new ConcurrentLinkedQueue[String]()

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      if (taskEnd.reason != Success) {
        failures.add(s"task ${taskEnd.taskInfo.taskId}: ${taskEnd.reason}")
      }
    }

    def failedTasks: Seq[String] = failures.asScala.toList
  }
}
