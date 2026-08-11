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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession

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
 */
class RapidsShuffleIntegrationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var eventListener: ShuffleDiskSavingsEventListener = _

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
   */
  private def createSessionWithConfig(
      maxBufferSize: String,
      compressionCodec: Option[String] = null,
      batchSize: String = "5m"): Unit = {
    if (spark != null) {
      spark.stop()
    }
    SparkSession.clearActiveSession()

    val shimVersion = ShimLoader.getShimVersion
    val shuffleManagerClass = s"com.nvidia.spark.rapids.spark" +
      s"${shimVersion.toString.replace(".", "")}." +
      "RapidsShuffleManager"

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

}
