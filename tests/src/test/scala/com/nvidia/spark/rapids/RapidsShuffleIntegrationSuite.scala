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

package com.nvidia.spark.rapids

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{FetchFailed, SparkConf, Success, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent,
  SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.functions.{col, sum, udf}
import org.apache.spark.sql.rapids.{GpuShuffleEnv, GpuShuffleRegistry,
  RapidsShuffleInternalManagerBase, TestTrampolineUtil}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, TrampolineUtil}

/**
 * Listener to capture SparkRapidsShuffleDiskSavingsEvent during tests.
 */
class ShuffleDiskSavingsEventListener extends SparkListener {
  private val events = new ArrayBuffer[SparkRapidsShuffleDiskSavingsEvent]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkRapidsShuffleDiskSavingsEvent =>
        synchronized {
          events += e
        }
      case _ =>
    }
  }

  def getEvents: Seq[SparkRapidsShuffleDiskSavingsEvent] = synchronized {
    events.toSeq
  }

  def clear(): Unit = synchronized {
    events.clear()
  }
}

/**
 * Integration test suite for RAPIDS Shuffle Manager.
 * Tests buffer fallback behavior under different configurations:
 * - Verifies SpillablePartialFileHandle memory-to-disk fallback
 * - Tests different partialFileBufferMaxSize settings
 * - Validates forced file-only mode for finalMergeWriter
 *
 * Uses SparkRapidsShuffleDiskSavingsEvent for verification instead of log messages.
 *
 * Also checks that CACHE_ONLY reads one attempt per map under spark.shuffle.useOldFetchProtocol,
 * and that a local read of a shuffle the executor no longer holds reruns its map stage.
 *
 * For MULTITHREADED with skip-merge, checks that the end of a SQL execution removes its shuffles
 * from the multithreaded catalog, and that a shuffle written outside one stays there through
 * session stop.
 *
 * For UCX, checks what the shuffle reclaim at the end of a SQL execution frees and what it keeps,
 * with its gate on and off.
 */
class RapidsShuffleIntegrationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var eventListener: ShuffleDiskSavingsEventListener = _

  private def shuffleManagerClass: String = {
    val shimVersion = ShimLoader.getShimVersion
    s"com.nvidia.spark.rapids.spark${shimVersion.toString.replace(".", "")}.RapidsShuffleManager"
  }

  /**
   * Create SparkSession with custom configuration.
   * Required because partialFileBufferMaxSize is .startupOnly() config.
   *
   * @param maxBufferSize The max buffer size (e.g., "1m", "6m")
   * @param compressionCodec Compression setting:
   *                         - null: use Spark default (compression enabled, default codec)
   *                         - Some(codec): enable compression with specific codec
   *                         - None: explicitly disable compression
   * @param batchSize GPU batch size (default "5m")
   * @param extraConf Settings applied after all the others
   */
  private def createSessionWithConfig(
      maxBufferSize: String,
      compressionCodec: Option[String] = null,
      batchSize: String = "5m",
      extraConf: Map[String, String] = Map.empty): Unit = {
    if (spark != null) {
      spark.stop()
    }
    SparkSession.clearActiveSession()

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.test.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleDataIO")
      // Enable skipMerge to use MultithreadedShuffleBufferCatalog, which emits
      // SparkRapidsShuffleDiskSavingsEvent that this test suite depends on for verification.
      // skipMerge requires off-heap memory limits to be enabled to prevent OOM.
      .set("spark.rapids.shuffle.multithreaded.skipMerge", "true")
      .set("spark.rapids.memory.host.offHeapLimit.enabled", "true")
      .set("spark.rapids.memory.host.offHeapLimit.size", "10g")

      .set("spark.rapids.memory.host.partialFileBufferInitialSize", "1m")
      .set("spark.rapids.memory.host.partialFileBufferMaxSize", maxBufferSize)
      .set("spark.rapids.sql.batchSizeBytes", batchSize)

    // Set compression configuration
    // null = use Spark default, Some(codec) = specific codec, None = disable compression
    if (compressionCodec != null) {
      compressionCodec match {
        case Some(codec) =>
          conf.set("spark.shuffle.compress", "true")
          conf.set("spark.io.compression.codec", codec)
        case None =>
          conf.set("spark.shuffle.compress", "false")
      }
    }
    // When compressionCodec is null, don't set any compression config (use Spark defaults)

    conf.setAll(extraConf)
    spark = SparkSession.builder().config(conf).getOrCreate()

    // Add event listener to capture shuffle events
    eventListener = new ShuffleDiskSavingsEventListener()
    spark.sparkContext.addSparkListener(eventListener)
  }

  /** Backwards-compatible helper - uses default compression */
  private def createSessionWithBufferConfig(maxBufferSize: String): Unit = {
    createSessionWithConfig(maxBufferSize)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Clean up any existing session before each test
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
      eventListener = null
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterEach()
    }
  }

  /**
   * Aggregate statistics from all captured events.
   */
  private def aggregateStats(): (Long, Long, Int, Int, Int) = {
    val events = eventListener.getEvents
    val bytesFromMemory = events.map(_.bytesFromMemory).sum
    val bytesFromDisk = events.map(_.bytesFromDisk).sum
    val numExpansions = events.map(_.numExpansions).sum
    val numSpills = events.map(_.numSpills).sum
    val numForcedFileOnly = events.map(_.numForcedFileOnly).sum
    (bytesFromMemory, bytesFromDisk, numExpansions, numSpills, numForcedFileOnly)
  }

  test("shuffle with join - 1MB max buffer triggers spill") {
    // Test verifying buffer fallback with small max buffer size
    // Setup: 1MB max buffer, 1MB initial buffer, 5MB GPU batch size
    // Expected: Serialized data (~4MB) exceeds 1MB limit, triggers spill to disk
    createSessionWithBufferConfig("1m")

    val df1 = spark.range(0, 3000000, 1, 3)
      .selectExpr(
        "id as key",
        "id * 2 as value1",
        "concat('left_', cast(id as string)) as left_str"
      )

    val df2 = spark.range(0, 30000000, 1, 3)
      .selectExpr(
        "id as key",
        "id * 3 as value2",
        "concat('right_', cast(id as string)) as right_str"
      )

    val result = df1.join(df2, "key")
      .selectExpr("key", "value1", "value2")
      .collect()

    assert(result.length == 3000000, "Should have 3M joined rows")

    // Wait for events to be posted (async)
    Thread.sleep(2000)

    val (bytesFromMemory, bytesFromDisk, numExpansions, numSpills, numForcedFileOnly) =
      aggregateStats()

    // Verify: buffer cannot expand beyond 1MB, must spill to disk or use file-only
    assert(bytesFromDisk > 0 || numSpills > 0 || numForcedFileOnly > 0,
      s"Expected data from disk or spills/file-only with 1MB max buffer. " +
        s"bytesFromMemory=$bytesFromMemory, bytesFromDisk=$bytesFromDisk, " +
        s"numExpansions=$numExpansions, numSpills=$numSpills, numForcedFileOnly=$numForcedFileOnly")
  }

  test("shuffle with join - 6MB max buffer avoids spill") {
    // Test verifying buffer stays in memory with sufficient max buffer size
    // Setup: 6MB max buffer, 1MB initial buffer, 5MB GPU batch size
    // Expected: Serialized data (~4MB) fits within 6MB limit via buffer expansion
    createSessionWithBufferConfig("6m")

    val df1 = spark.range(0, 3000000, 1, 3)
      .selectExpr(
        "id as key",
        "id * 2 as value1",
        "concat('left_', cast(id as string)) as left_str"
      )

    val df2 = spark.range(0, 30000000, 1, 3)
      .selectExpr(
        "id as key",
        "id * 3 as value2",
        "concat('right_', cast(id as string)) as right_str"
      )

    val result = df1.join(df2, "key")
      .selectExpr("key", "value1", "value2")
      .collect()

    assert(result.length == 3000000, "Should have 3M joined rows")

    // Wait for events to be posted (async)
    Thread.sleep(2000)

    val (bytesFromMemory, bytesFromDisk, numExpansions, numSpills, numForcedFileOnly) =
      aggregateStats()

    // Verify: buffer expands to accommodate data, no spill to disk
    // Note: numForcedFileOnly may be > 0 for final merge operations
    assert(bytesFromMemory > 0 && numExpansions > 0 && numSpills == 0,
      s"Expected data from memory with expansions, no spills with 6MB max buffer. " +
        s"bytesFromMemory=$bytesFromMemory, bytesFromDisk=$bytesFromDisk, " +
        s"numExpansions=$numExpansions, numSpills=$numSpills, numForcedFileOnly=$numForcedFileOnly")
  }

  // ============================================================================
  // Multi-segment compression tests
  // Tests that multiple shuffle segments work correctly with different codecs
  // ============================================================================

  /**
   * Run a shuffle query that generates multiple segments (batches) and verify correctness.
   * Uses small batch size to force multiple batches per task.
   *
   * @param codec Compression codec name (lz4, snappy, zstd) or None for no compression
   */
  private def runMultiSegmentShuffleTest(codec: Option[String]): Unit = {
    // Use small batch size (1MB) to force multiple segments per shuffle task
    // With 6MB max buffer, data will stay in memory
    createSessionWithConfig(
      maxBufferSize = "6m",
      compressionCodec = codec,
      batchSize = "1m"  // Small batch = more segments
    )

    // Assert the compression configuration is as expected
    val conf = spark.sparkContext.getConf
    codec.foreach { expectedCodec =>
      assert(conf.get("spark.shuffle.compress", "true") == "true",
        "Shuffle compression should be enabled")
      val actualCodec = conf.get("spark.io.compression.codec", "lz4")
      assert(actualCodec == expectedCodec,
        s"Expected compression codec $expectedCodec but got $actualCodec")
    }
    if (codec.isEmpty) {
      val compressEnabled = conf.get("spark.shuffle.compress", "true")
      assert(compressEnabled == "false",
        s"Shuffle compression should be disabled, but got $compressEnabled")
    }

    // Create data that will produce multiple batches when shuffled
    val df1 = spark.range(0, 1000000, 1, 4)
      .selectExpr(
        "id as key",
        "id * 2 as value1",
        "concat('str_', cast(id % 1000 as string)) as str_col"
      )

    val df2 = spark.range(0, 1000000, 1, 4)
      .selectExpr(
        "id as key",
        "id * 3 as value2"
      )

    // Join triggers shuffle with multiple segments due to small batch size
    val result = df1.join(df2, "key")
      .groupBy("str_col")
      .count()
      .collect()

    // Verify we got results (correctness check)
    val codecDesc = codec.getOrElse("no compression")
    assert(result.length == 1000, s"Expected 1000 distinct str_col values with $codecDesc")
    assert(result.map(_.getLong(1)).sum == 1000000,
      s"Total count should be 1M with $codecDesc")

    // Wait for events
    Thread.sleep(2000)

    val events = eventListener.getEvents
    assert(events.nonEmpty, s"Should have shuffle events with $codecDesc")
  }

  test("multi-segment shuffle without compression") {
    runMultiSegmentShuffleTest(None)
  }

  test("multi-segment shuffle with lz4 compression") {
    runMultiSegmentShuffleTest(Some("lz4"))
  }

  test("multi-segment shuffle with snappy compression") {
    runMultiSegmentShuffleTest(Some("snappy"))
  }

  test("multi-segment shuffle with zstd compression") {
    runMultiSegmentShuffleTest(Some("zstd"))
  }

  test("the end of a SQL execution removes its shuffles from the multithreaded catalog") {
    // An executor reports a shuffle's disk savings only after removing it from its catalog, so
    // the event shows that the removal has happened.
    createSessionWithConfig("6m")
    val df = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id")
      .repartition(8, col("k")).groupBy((col("k") % 2).as("p")).agg(sum("id"))
    assertResult((0L until 1000L).sum)(df.collect().map(_.getLong(1)).sum)
    val catalog = GpuShuffleEnv.getMultithreadedCatalog.getOrElse(
      fail("the session has no multithreaded catalog"))
    val plan = df.queryExecution.executedPlan
    val shuffleIds = SparkShimImpl.findOperators(plan, _.isInstanceOf[GpuShuffleExchangeExecBase])
      .map(_.asInstanceOf[GpuShuffleExchangeExecBase].shuffleDependencyColumnar.shuffleId)
    assert(shuffleIds.nonEmpty, s"no GPU shuffle in:\n$plan")
    eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
      assertResult(Set.empty, "shuffles with no disk savings event")(
        shuffleIds.toSet -- eventListener.getEvents.map(_.shuffleId))
    }
    assertResult(Seq.empty, "shuffles the multithreaded catalog still holds")(
      shuffleIds.filter(catalog.hasActiveShuffle))
  }

  test("a shuffle written outside any SQL execution stays in the multithreaded catalog " +
      "through session stop") {
    // A job outside any SQL execution gets no SQL-end cleanup, and the executor plugin resets
    // the shuffle env before closing the cleanup endpoint, so the endpoint's final cleanup finds
    // no catalog. Both checksum keys are off, so that on Spark 4.1+ no checksum fallback can send
    // the write to Spark's sort shuffle.
    createSessionWithConfig("6m", extraConf = Map(
      RowBasedShuffleChecksumConf.ChecksumEnabledKey -> "false",
      RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey -> "false"))
    val sqlExecutions = new ConcurrentLinkedQueue[Long]()
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerSQLExecutionStart => sqlExecutions.add(e.executionId)
        case _ =>
      }
    })
    val df = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k").repartition(8, col("k"))
    assertResult(1000L)(df.queryExecution.toRdd.count())
    val catalog = GpuShuffleEnv.getMultithreadedCatalog.getOrElse(
      fail("the session has no multithreaded catalog"))
    val ids = catalog.getActiveShuffleIds
    try {
      assert(ids.nonEmpty, "the job left no shuffle in the multithreaded catalog")
      spark.stop()
      spark = null
      // the stop drained the listener bus, so every SQL execution start has arrived
      assert(sqlExecutions.isEmpty, s"the session ran SQL executions $sqlExecutions")
      // LocalSchedulerBackend.stop only asks its executor to stop. Waiting for the env reset
      // keeps this check independent of the RPC shutdown that drains that request.
      eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
        assert(GpuShuffleEnv.getMultithreadedCatalog.isEmpty, "the executor has not shut down")
      }
      val remaining = catalog.getActiveShuffleIds
      assert(ids.forall(remaining.contains),
        s"held ${ids.sorted} before the stop and ${remaining.sorted} after it")
    } finally {
      // Unregistering closes the file-backed handles that the stop leaves open. Reading df here
      // keeps its shuffle reachable, so GC cannot remove it mid-test.
      ids.foreach(catalog.unregisterShuffle)
      df.hashCode()
    }
  }

  test("with the old fetch protocol, a map retried on the same executor is read once") {
    // spark.shuffle.useOldFetchProtocol makes every attempt of a map write the same blocks, and
    // local mode has one executor, so the retry always runs where the first attempt cached its
    // output. local[2,4] allows four attempts per task. The UDF runs on the CPU, so
    // spark.rapids.sql.test.enabled stays off.
    val conf = new SparkConf()
      .setMaster("local[2,4]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.rapids.shuffle.mode", "CACHE_ONLY")
      .set("spark.shuffle.useOldFetchProtocol", "true")
    spark = SparkSession.builder().config(conf).getOrCreate()
    RapidsShuffleIntegrationSuite.failedTasks.clear()
    // fails the first attempt of map 0 once its shuffle write has returned
    val failAfterWrite = udf { (id: Long) =>
      val tc = TaskContext.get()
      if (tc.partitionId() == 0 && tc.attemptNumber() == 0 &&
          RapidsShuffleIntegrationSuite.failedTasks.add(tc.taskAttemptId())) {
        tc.addTaskCompletionListener[Unit] { _ =>
          throw new IllegalStateException("injected failure after the shuffle write")
        }
      }
      id
    }
    val numRows = 100000L
    val query = spark.range(0, numRows, 1, 4).select(failAfterWrite(col("id")).as("id"))
      .selectExpr("id % 97 AS k").repartition(8, col("k")).groupBy("k").count()
      .agg(sum("count"))
    val total = query.collect().head.getLong(0)
    // Spark's own shuffle also keeps one attempt per map, so the count proves nothing unless the
    // shuffles stayed on the GPU while the UDF fell back to the CPU
    val plan = query.queryExecution.executedPlan
    assert(SparkShimImpl.findOperators(plan, _.isInstanceOf[ShuffleExchangeExec]).isEmpty,
      s"a shuffle fell back to the CPU:\n$plan")
    assert(SparkShimImpl.findOperators(plan, _.isInstanceOf[GpuShuffleExchangeExecBase]).nonEmpty,
      s"no GPU shuffle in:\n$plan")
    assertResult(1, "first attempts that failed after their write")(
      RapidsShuffleIntegrationSuite.failedTasks.size())
    assertResult(numRows)(total)
  }

  test("a local read of an unregistered shuffle reruns the map stage that wrote it") {
    // A local master starts no transport, so every read is local. The fetch failure names
    // executor driver, whose block manager cannot register again on Spark 3.3.2 and later: the
    // session must stop soon after, as afterEach does, and every task result stays far below
    // spark.task.maxDirectResultSize, since a larger one would be stored in that block manager.
    // AQE is off so that both collects run one unchanged physical plan.
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.rapids.shuffle.mode", "UCX")
      .set("spark.sql.adaptive.enabled", "false")
      // Spark 4.1+ removes a classic query's shuffles in testing mode, and Spark 4.2 turns on
      // checksums that the GPU shuffle does not compute
      .set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
      .set(RowBasedShuffleChecksumConf.ChecksumEnabledKey, "false")
      .set(RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey, "false")
    spark = SparkSession.builder().config(conf).getOrCreate()
    val taskEnds = new ConcurrentLinkedQueue[SparkListenerTaskEnd]()
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = taskEnds.add(taskEnd)
    })
    // hash partitioning, since a round-robin repartition can move rows when its maps rerun
    val df = spark.range(0, 1000, 1, 4).selectExpr("id", "id % 7 AS k").repartition(8, col("k"))
    val firstRows = df.collect().map(_.toString).sorted
    val plan = df.queryExecution.executedPlan
    assert(SparkShimImpl.findOperators(plan, _.isInstanceOf[ShuffleExchangeExec]).isEmpty,
      s"the shuffle fell back to the CPU:\n$plan")
    val exchanges = SparkShimImpl.findOperators(plan, _.isInstanceOf[GpuShuffleExchangeExecBase])
    assertResult(1, s"GPU shuffles in:\n$plan")(exchanges.size)
    val shuffleId =
      exchanges.head.asInstanceOf[GpuShuffleExchangeExecBase].shuffleDependencyColumnar.shuffleId
    GpuShuffleEnv.getCatalog.unregisterShuffle(shuffleId)
    taskEnds.clear()
    assertResult(firstRows)(df.collect().map(_.toString).sorted)
    // the rerun's writers registered the shuffle again, on the GPU
    assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(shuffleId))
    eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
      val ends = taskEnds.asScala.toList
      assert(ends.exists(_.reason match {
        case f: FetchFailed => f.shuffleId == shuffleId
        case _ => false
      }), s"no fetch failure of shuffle $shuffleId in ${ends.map(_.reason)}")
      // the first collect ran the map stage as attempt 0, and its events may arrive late
      assert(ends.exists(e => e.taskType == "ShuffleMapTask" && e.stageAttemptId > 0 &&
        e.reason == Success),
        s"the map stage did not run again: " +
          s"${ends.map(e => (e.taskType, e.stageAttemptId, e.reason))}")
    }
  }

  // ============================================================================
  // UCX shuffle reclaim at the end of a SQL execution
  // ============================================================================

  /**
   * Starts a session in UCX mode with the settings of the test of a local read of an
   * unregistered shuffle, and with the UCX shuffle reclaim gate on or off.
   *
   * @param gateOn whether the session turns the reclaim gate on
   * @param extraConf settings applied after all the others
   * @return a listener that records the events the tests read
   */
  private def startUcxReclaimSession(
      gateOn: Boolean,
      extraConf: Map[String, String]): RapidsShuffleIntegrationSuite.ReclaimTestListener = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.rapids.shuffle.mode", "UCX")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
      .set(RowBasedShuffleChecksumConf.ChecksumEnabledKey, "false")
      .set(RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey, "false")
      .set("spark.rapids.shuffle.ucx.cleanupOnQueryEnd.enabled", gateOn.toString)
      .setAll(extraConf)
    spark = SparkSession.builder().config(conf).getOrCreate()
    val listener = new RapidsShuffleIntegrationSuite.ReclaimTestListener
    spark.sparkContext.addSparkListener(listener)
    listener
  }

  /** A DataFrame with one GPU hash shuffle and small results. */
  private def oneShuffleQuery(): DataFrame =
    spark.range(0, 1000, 1, 4).selectExpr("id", "id % 7 AS k").repartition(8, col("k"))

  /** A DataFrame with two GPU hash shuffles and two result rows. */
  private def twoShuffleQuery(): DataFrame = spark.range(0, 1000, 1, 4)
    .selectExpr("id % 7 AS k", "id").repartition(8, col("k"))
    .groupBy((col("k") % 2).as("p")).agg(sum("id"))

  /** The GPU exchanges of a DataFrame's plan, after checking that no shuffle runs on the CPU. */
  private def gpuExchanges(df: DataFrame): Seq[GpuShuffleExchangeExecBase] = {
    val plan = df.queryExecution.executedPlan
    assert(SparkShimImpl.findOperators(plan, _.isInstanceOf[ShuffleExchangeExec]).isEmpty,
      s"a shuffle fell back to the CPU:\n$plan")
    SparkShimImpl.findOperators(plan, _.isInstanceOf[GpuShuffleExchangeExecBase])
      .map(_.asInstanceOf[GpuShuffleExchangeExecBase])
  }

  /** The shuffle id of the one GPU exchange in a DataFrame's plan. */
  private def onlyGpuShuffle(df: DataFrame): Int = {
    val exchanges = gpuExchanges(df)
    assertResult(1, s"GPU shuffles in:\n${df.queryExecution.executedPlan}")(exchanges.size)
    exchanges.head.shuffleDependencyColumnar.shuffleId
  }

  /** The reclaim manager that the driver plugin of a gate-on UCX session starts. */
  private def reclaimManager(): ShuffleCleanupManager = Option(ShuffleCleanupManager.get)
    .getOrElse(fail("the gate-on driver plugin started no reclaim manager"))

  /** Waits until the listener has handled every event posted to the bus before this call. */
  private def drain(listener: RapidsShuffleIntegrationSuite.ReclaimTestListener): Unit = {
    val marker = new RapidsShuffleIntegrationSuite.ListenerDrainMarker
    TrampolineUtil.postEvent(spark.sparkContext, marker)
    eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
      assert(listener.hasHandled(marker), "the listener has not handled the drain marker")
    }
  }

  /** Waits for the reclaim event of the session's `n`-th SQL execution, counting from 1. */
  private def awaitReclaimEvent(
      listener: RapidsShuffleIntegrationSuite.ReclaimTestListener,
      n: Int): SparkRapidsShuffleReclaimEvent = {
    eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
      val executions = listener.executions
      assert(executions.size >= n, s"the session started ${executions.size} SQL executions")
      val executionId = executions(n - 1)
      listener.reclaimEvents.find(_.executionId == executionId).getOrElse(
        fail(s"no reclaim event for SQL execution $executionId"))
    }
  }

  /**
   * Counts a DataFrame's rows once more outside SQL, and checks that the count ran no map task
   * and failed no fetch, so it read the output that an earlier rerun left in the catalog.
   */
  private def assertReadsKeptOutput(
      listener: RapidsShuffleIntegrationSuite.ReclaimTestListener,
      df: DataFrame,
      expectedRows: Long): Unit = {
    drain(listener)
    val taskMark = listener.taskEnds.size
    assertResult(expectedRows, "rows of the read after the reclaim")(
      df.queryExecution.toRdd.count())
    drain(listener)
    val ends = listener.taskEnds.drop(taskMark)
    assertResult(Seq.empty, "map tasks of the read after the reclaim")(
      ends.filter(_.taskType == "ShuffleMapTask").map(_.taskInfo.taskId))
    assertResult(Seq.empty, "fetch failures of the read after the reclaim")(
      ends.map(_.reason).filter(_.isInstanceOf[FetchFailed]))
  }

  test("the end of a SQL execution reclaims its UCX shuffles from the catalog and the driver") {
    val listener = startUcxReclaimSession(gateOn = true, Map.empty)
    val df = twoShuffleQuery()
    val firstRows = df.collect().map(_.toString).sorted
    val shuffleIds = gpuExchanges(df).map(_.shuffleDependencyColumnar.shuffleId)
    assertResult(2, s"GPU shuffles in:\n${df.queryExecution.executedPlan}")(shuffleIds.size)
    val event = awaitReclaimEvent(listener, 1)
    assertResult(Seq.empty, "shuffles the catalog still holds")(
      shuffleIds.filter(GpuShuffleEnv.getCatalog.hasActiveShuffle))
    assertResult(shuffleIds.map(id => id -> Some(0)), "available map outputs on the driver")(
      shuffleIds.map(id => id -> TestTrampolineUtil.availableMapOutputs(id)))
    assertResult(0, s"timed-out executors in $event")(event.executorsTimedOut)
    assert(event.buffersFreed > 0 && event.bytesFreed > 0, s"nothing freed in $event")
    // the driver holds no output of either shuffle, so a second collect reruns both map stages
    drain(listener)
    val stageMark = listener.mapStageSubmissions.size
    val taskMark = listener.taskEnds.size
    assertResult(firstRows)(df.collect().map(_.toString).sorted)
    drain(listener)
    assertResult(shuffleIds.toSet, "shuffles whose map stage the second collect ran")(
      listener.mapStageSubmissions.drop(stageMark).toSet)
    assertResult(Seq.empty, "fetch failures of the second collect")(
      listener.taskEnds.drop(taskMark).map(_.reason).filter(_.isInstanceOf[FetchFailed]))
  }

  test("with the UCX reclaim gate off, a SQL execution's shuffles stay in the catalog") {
    startUcxReclaimSession(gateOn = false, Map.empty)
    val df = twoShuffleQuery()
    df.collect()
    val shuffleIds = gpuExchanges(df).map(_.shuffleDependencyColumnar.shuffleId)
    assertResult(2, s"GPU shuffles in:\n${df.queryExecution.executedPlan}")(shuffleIds.size)
    assertResult(shuffleIds, "shuffles the catalog holds")(
      shuffleIds.filter(GpuShuffleEnv.getCatalog.hasActiveShuffle))
  }

  test("with a row-checksum key on, the GPU shuffle registry admits exactly the shuffles " +
      "whose writers use the catalog") {
    startUcxReclaimSession(gateOn = true,
      Map(RowBasedShuffleChecksumConf.ChecksumEnabledKey -> "true"))
    assert(GpuShuffleEnv.isRowBasedChecksumEnabled,
      "the plugin does not see the row-checksum key")
    val manager = reclaimManager()
    manager.holdReclaimPollsForTesting()
    val shuffleIds = try {
      val df = twoShuffleQuery()
      df.collect()
      val exchanges = gpuExchanges(df)
      assert(exchanges.nonEmpty, s"no GPU shuffle in:\n${df.queryExecution.executedPlan}")
      exchanges.map { exchange =>
        val dep = exchange.shuffleDependencyColumnar
        val admitted = Option(GpuShuffleRegistry.get).exists(_.contains(dep.shuffleId))
        val held = GpuShuffleEnv.getCatalog.hasActiveShuffle(dep.shuffleId)
        val usesCatalog =
          RapidsShuffleInternalManagerBase.usesShuffleBufferCatalog(dep.shuffleHandle)
        assert(admitted == held && held == usesCatalog, s"shuffle ${dep.shuffleId}: " +
          s"admitted $admitted, in the catalog $held, usesShuffleBufferCatalog $usesCatalog")
        dep.shuffleId
      }
    } finally {
      manager.releaseReclaimPollsForTesting()
    }
    eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
      assertResult(Seq.empty, "shuffles the catalog still holds")(
        shuffleIds.filter(GpuShuffleEnv.getCatalog.hasActiveShuffle))
    }
  }

  test("a held UCX reclaim that arrives after a map-stage rerun outside SQL keeps the " +
      "rerun's output") {
    val listener = startUcxReclaimSession(gateOn = true, Map.empty)
    val manager = reclaimManager()
    val df = oneShuffleQuery()
    manager.holdReclaimPollsForTesting()
    val (shuffleId, rerunRows) = try {
      df.collect()
      val id = onlyGpuShuffle(df)
      eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
        assert(manager.isReclaimReleasedForTesting(id), s"no fence released shuffle $id")
      }
      // a job outside SQL reruns the map stage above the watermark and never owns the shuffle
      (id, df.queryExecution.toRdd.count())
    } finally {
      manager.releaseReclaimPollsForTesting()
    }
    val event = awaitReclaimEvent(listener, 1)
    assertResult(1, s"reclaimed shuffles in $event")(event.reclaimed)
    assert(event.buffersFreed > 0, s"nothing freed in $event")
    assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(shuffleId),
      s"the reclaim removed the rerun's registration of shuffle $shuffleId")
    assertReadsKeptOutput(listener, df, rerunRows)
  }

  test("a held UCX reclaim that arrives after a rerun writer registered, and before the " +
      "writer's first add, keeps the writer's output") {
    assertHeldReclaimKeepsRegisteredWriter(Map.empty, deferred = 0)
  }

  test("with the old fetch protocol, a held UCX reclaim that arrives after a rerun writer " +
      "registered defers, and keeps the writer's output") {
    // map ids are partition ids under this protocol, so only the writers' task ids can show
    // that the rerun registered above the watermark
    assertHeldReclaimKeepsRegisteredWriter(
      Map("spark.shuffle.useOldFetchProtocol" -> "true"), deferred = 1)
  }

  /**
   * Holds the executor's reclaim polls until a rerun writer of the reclaimed shuffle has
   * registered and waits in a UDF, then lets the reclaim run before the writer's first add.
   * `deferred` is how many executors the reclaim event says deferred.
   */
  private def assertHeldReclaimKeepsRegisteredWriter(
      extraConf: Map[String, String],
      deferred: Int): Unit = {
    RapidsShuffleIntegrationSuite.holdWriters = false
    RapidsShuffleIntegrationSuite.writerEntered = new CountDownLatch(1)
    RapidsShuffleIntegrationSuite.releaseWriters = new CountDownLatch(1)
    val listener = startUcxReclaimSession(gateOn = true, extraConf)
    val manager = reclaimManager()
    // Spark calls getWriter before the writer reads its first row, so a writer that reaches this
    // UDF has registered the shuffle with its own task id
    val holdWriter = udf { (id: Long) =>
      if (RapidsShuffleIntegrationSuite.holdWriters) {
        RapidsShuffleIntegrationSuite.writerEntered.countDown()
        RapidsShuffleIntegrationSuite.releaseWriters.await(
          RapidsShuffleIntegrationSuite.WaitSeconds, TimeUnit.SECONDS)
      }
      id
    }
    val numRows = 1000L
    val df = spark.range(0, numRows, 1, 4).select(holdWriter(col("id")).as("id"))
      .selectExpr("id", "id % 7 AS k").repartition(8, col("k"))
    val rerunRows = new AtomicLong(-1L)
    val rerunError = new AtomicReference[Throwable]()
    val rerun = new Thread(() => {
      try {
        rerunRows.set(df.queryExecution.toRdd.count())
      } catch {
        case NonFatal(e) => rerunError.set(e)
      }
    }, "ucx-reclaim-rerun")
    rerun.setDaemon(true)
    try {
      manager.holdReclaimPollsForTesting()
      val (shuffleId, taskMark) = try {
        df.collect()
        val shuffleId = onlyGpuShuffle(df)
        eventually(Timeout(Span(RapidsShuffleIntegrationSuite.WaitSeconds, Seconds))) {
          assert(manager.isReclaimReleasedForTesting(shuffleId),
            s"no fence released shuffle $shuffleId")
        }
        drain(listener)
        val mark = listener.taskEnds.size
        RapidsShuffleIntegrationSuite.holdWriters = true
        rerun.start()
        assert(RapidsShuffleIntegrationSuite.writerEntered.await(
          RapidsShuffleIntegrationSuite.WaitSeconds, TimeUnit.SECONDS),
          "no writer of the rerun reached the UDF")
        (shuffleId, mark)
      } finally {
        manager.releaseReclaimPollsForTesting()
      }
      val event = awaitReclaimEvent(listener, 1)
      assertResult(deferred, s"deferred executors in $event")(event.executorsDeferred)
      // the held writer is above the watermark, so its registration stays
      assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(shuffleId),
        s"the reclaim removed shuffle $shuffleId's registration")
      RapidsShuffleIntegrationSuite.releaseWriters.countDown()
      rerun.join(TimeUnit.SECONDS.toMillis(RapidsShuffleIntegrationSuite.JoinSeconds))
      assert(!rerun.isAlive, "the rerun never ended")
      Option(rerunError.get).foreach(e => fail("the rerun failed", e))
      assertResult(numRows, "rows of the rerun")(rerunRows.get)
      drain(listener)
      assertResult(Seq.empty, "failed tasks of the rerun")(
        listener.taskEnds.drop(taskMark).map(_.reason).filter(_ != Success))
      assertReadsKeptOutput(listener, df, numRows)
    } finally {
      RapidsShuffleIntegrationSuite.releaseWriters.countDown()
      RapidsShuffleIntegrationSuite.holdWriters = false
      rerun.join(TimeUnit.SECONDS.toMillis(RapidsShuffleIntegrationSuite.JoinSeconds))
    }
  }

  test("a GPU round-robin shuffle and the shuffle after it wait for GC, and the shuffle before " +
      "it is reclaimed") {
    val listener = startUcxReclaimSession(gateOn = true,
      Map("spark.sql.shuffle.partitions" -> "4"))
    val df = spark.range(0, 1000, 1, 4).selectExpr("id % 7 AS k", "id").groupBy("k")
      .agg(sum("id").as("s")).repartition(4).groupBy((col("k") % 2).as("p")).agg(sum("s"))
    val firstRows = df.collect().map(_.toString).sorted
    val plan = df.queryExecution.executedPlan
    val exchanges = gpuExchanges(df)
    assertResult(3, s"GPU shuffles in:\n$plan")(exchanges.size)
    // findOperators lists the operators of this one-chain plan from the top down
    val Seq(h2, r, h1) = exchanges
    assert(r.outputPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      Seq(h1, h2).forall(_.outputPartitioning.isInstanceOf[HashPartitioning]),
      s"no round-robin GPU shuffle between two hash GPU shuffles in:\n$plan")
    val Seq(h1Id, rId, h2Id) = Seq(h1, r, h2).map(_.shuffleDependencyColumnar.shuffleId)
    val event = awaitReclaimEvent(listener, 1)
    assertResult((3, 1), s"owned and reclaimed shuffles in $event")(
      (event.shuffles, event.reclaimed))
    assertResult(Seq(rId, h2Id).sorted, s"indeterminate shuffles in $event")(
      event.skippedIndeterminate.sorted)
    assertResult(Seq(false, true, true), "whether the catalog holds H1, R and H2")(
      Seq(h1Id, rId, h2Id).map(GpuShuffleEnv.getCatalog.hasActiveShuffle))
    // the driver keeps every output of the two shuffles that wait for GC
    val keptOutputs = Seq(r, h2).map(e => Some(e.shuffleDependencyColumnar.rdd.getNumPartitions))
    assertResult(Some(0) +: keptOutputs, "available map outputs of H1, R and H2 on the driver")(
      Seq(h1Id, rId, h2Id).map(TestTrampolineUtil.availableMapOutputs))
    drain(listener)
    val stageMark = listener.mapStageSubmissions.size
    val taskMark = listener.taskEnds.size
    assertResult(firstRows)(df.collect().map(_.toString).sorted)
    drain(listener)
    assertResult(Seq.empty, "shuffles whose map stage the second collect ran")(
      listener.mapStageSubmissions.drop(stageMark))
    assertResult(Seq.empty, "fetch failures of the second collect")(
      listener.taskEnds.drop(taskMark).map(_.reason).filter(_.isInstanceOf[FetchFailed]))
  }

  test("each SQL execution that reruns a UCX shuffle's map stage reclaims the shuffle again") {
    val listener = startUcxReclaimSession(gateOn = true, Map.empty)
    val df = oneShuffleQuery()
    val firstRows = df.collect().map(_.toString).sorted
    val shuffleId = onlyGpuShuffle(df)
    (1 to 3).foreach { round =>
      if (round > 1) {
        assertResult(firstRows, s"rows of round $round")(df.collect().map(_.toString).sorted)
      }
      val event = awaitReclaimEvent(listener, round)
      assertResult((1, 1), s"owned and reclaimed shuffles in round $round: $event")(
        (event.shuffles, event.reclaimed))
      assert(event.buffersFreed > 0, s"round $round freed nothing: $event")
      assertResult(round, s"map stages of shuffle $shuffleId by the end of round $round")(
        listener.mapStageSubmissions.count(_ == shuffleId))
    }
    assert(!GpuShuffleEnv.getCatalog.hasActiveShuffle(shuffleId),
      s"the catalog still holds shuffle $shuffleId")
    assertResult(Seq.empty, "failed tasks")(listener.taskEnds.map(_.reason).filter(_ != Success))
  }

  test("with the UCX reclaim gate off, repeated collects reuse a shuffle's map output") {
    val listener = startUcxReclaimSession(gateOn = false, Map.empty)
    val df = oneShuffleQuery()
    val firstRows = df.collect().map(_.toString).sorted
    val shuffleId = onlyGpuShuffle(df)
    (2 to 3).foreach { round =>
      assertResult(firstRows, s"rows of round $round")(df.collect().map(_.toString).sorted)
    }
    drain(listener)
    assertResult(1, s"map stages of shuffle $shuffleId")(
      listener.mapStageSubmissions.count(_ == shuffleId))
    assert(GpuShuffleEnv.getCatalog.hasActiveShuffle(shuffleId),
      s"the catalog dropped shuffle $shuffleId")
    assertResult(Seq.empty, "failed tasks")(listener.taskEnds.map(_.reason).filter(_ != Success))
  }

  test("with the UCX reclaim gate off, the driver has no GPU shuffle registry") {
    startUcxReclaimSession(gateOn = false, Map.empty)
    assert(GpuShuffleRegistry.get == null, "a gate-off driver plugin installed the registry")
  }

  test("a gate-on driver plugin keeps a GPU shuffle registry until its session stops") {
    startUcxReclaimSession(gateOn = true, Map.empty)
    assert(GpuShuffleRegistry.get != null, "the gate-on driver plugin installed no registry")
    spark.stop()
    spark = null
    assert(GpuShuffleRegistry.get == null, "the stopped driver plugin left its registry")
  }
}

object RapidsShuffleIntegrationSuite {
  /** How long a test waits for an event, a latch or a condition. */
  val WaitSeconds = 30L

  /** How long a test waits for a thread it started to end. */
  val JoinSeconds = 60L

  /** Task attempts that the injected failure has armed, shared with the session's tasks. */
  val failedTasks: java.util.Set[Long] = ConcurrentHashMap.newKeySet[Long]()

  /** Makes the held-writer test's UDF wait for `releaseWriters`, once set. */
  @volatile var holdWriters: Boolean = false

  /** Counted down by each held writer as it reaches the UDF. */
  @volatile var writerEntered: CountDownLatch = new CountDownLatch(1)

  /** Lets the held writers go on. */
  @volatile var releaseWriters: CountDownLatch = new CountDownLatch(1)

  /** An event that a test posts to learn when a listener has handled every earlier event. */
  class ListenerDrainMarker extends SparkListenerEvent

  /** Records the events of a session that the UCX shuffle reclaim tests read. */
  class ReclaimTestListener extends SparkListener {
    private val executionIds = new ConcurrentLinkedQueue[Long]()
    private val reclaims = new ConcurrentLinkedQueue[SparkRapidsShuffleReclaimEvent]()
    private val mapStages = new ConcurrentLinkedQueue[Int]()
    private val ends = new ConcurrentLinkedQueue[SparkListenerTaskEnd]()
    private val markers = ConcurrentHashMap.newKeySet[ListenerDrainMarker]()

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
      TestTrampolineUtil.shuffleDepId(stageSubmitted.stageInfo).foreach(id => mapStages.add(id))

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = ends.add(taskEnd)

    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionStart => executionIds.add(e.executionId)
      case e: SparkRapidsShuffleReclaimEvent => reclaims.add(e)
      case m: ListenerDrainMarker => markers.add(m)
      case _ =>
    }

    /** The id of each SQL execution that started, in order. */
    def executions: Seq[Long] = executionIds.asScala.toList

    /** Each reclaim event, in order. */
    def reclaimEvents: Seq[SparkRapidsShuffleReclaimEvent] = reclaims.asScala.toList

    /** The shuffle id of each map stage submitted, in order. */
    def mapStageSubmissions: Seq[Int] = mapStages.asScala.toList

    /** Each task end, in order. */
    def taskEnds: Seq[SparkListenerTaskEnd] = ends.asScala.toList

    /** Whether the listener has handled the marker. */
    def hasHandled(marker: ListenerDrainMarker): Boolean = markers.contains(marker)
  }
}
