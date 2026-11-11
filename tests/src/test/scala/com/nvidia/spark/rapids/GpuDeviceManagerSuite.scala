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

import ai.rapids.cudf.{CudaMemInfo, DeviceMemoryBuffer, Rmm}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object TestMemoryChecker extends MemoryChecker {
  private var availMemBytes: Option[Long] = None

  override def getAvailableMemoryBytes(rapidsConf: RapidsConf): Option[Long] = availMemBytes

  def setAvailableMemoryBytes(b: Option[Long]): Unit = availMemBytes = b
}

/**
 * Helper to create CudaMemInfo objects for testing using reflection
 */
object TestCudaMemInfo {
  import ai.rapids.cudf.CudaMemInfo

  def create(total: Long, free: Long): CudaMemInfo = {
    val constructor = classOf[CudaMemInfo].getDeclaredConstructor(classOf[Long], classOf[Long])
    constructor.setAccessible(true)
    constructor.newInstance(free.asInstanceOf[Object], total.asInstanceOf[Object])
  }
}

class GpuDeviceManagerSuite extends AnyFunSuite with BeforeAndAfter {

  before {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  after {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  def toBytes: String => Long = ConfHelper.byteFromString(_, ByteUnit.BYTE)

  Seq(false, true).foreach { smallPool =>
    val suffix = if (smallPool) " with startup allocations in a small pool" else ""
    test(s"RMM pool size$suffix") {
      val poolFraction = 0.1
      val maxPoolFraction = 0.2
      // we need to reduce the minAllocFraction for this test since the
      // pool allocation here is less than the default minimum
      val minPoolFraction = 0.01
      val conf = new SparkConf()
          .set(RapidsConf.RMM_POOL.key, "ARENA")
          .set(RapidsConf.RMM_ALLOC_FRACTION.key, poolFraction.toString)
          .set(RapidsConf.RMM_ALLOC_MIN_FRACTION.key, minPoolFraction.toString)
          .set(RapidsConf.RMM_ALLOC_MAX_FRACTION.key, maxPoolFraction.toString)
          .set(RapidsConf.RMM_ALLOC_RESERVE.key, "0")
      if (smallPool) {
        // Keep a deterministic regression case where startup allocations consume half the pool.
        conf.set(RapidsConf.RMM_EXACT_ALLOC.key, "256m")
            .set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, "32m")
            .set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_COUNT.key, "4")
      }
      TestUtils.withGpuSparkSession(conf) { _ =>
        // Other workers can consume GPU memory before this session initializes its pool.
        // Account for startup allocations, including the chunked-pack bounce buffers.
        val availablePoolSize = GpuDeviceManager.getMemorySize - Rmm.getTotalBytesAllocated
        val allocSize = availablePoolSize * 3 / 4
        assert(allocSize > 0)
        // initial allocation should fit within pool size
        withResource(DeviceMemoryBuffer.allocate(allocSize)) { _ =>
          assertThrows[OutOfMemoryError] {
            // this should exceed the specified pool size
            withResource(DeviceMemoryBuffer.allocate(allocSize)) { _ => () }
          }
        }
      }
    }
  }

  test("RMM reserve larger than max") {
    val rapidsConf = new RapidsConf(Map(RapidsConf.RMM_ALLOC_RESERVE.key -> "200g"))
    assertThrows[IllegalArgumentException] {
      SparkSession.builder().master("local[1]").getOrCreate()
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    }
  }

  test("RMM pool size equals max") {
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_RESERVE.key -> "0",
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.3",
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> "0.3",
      // This test validates that equal allocation and maximum fractions are accepted. Keep the
      // minimum independent of whole-device capacity so the test also works when other test JVMs
      // are using the GPU.
      RapidsConf.RMM_ALLOC_MIN_FRACTION.key -> "0.01"))
    try {
      SparkSession.builder().master("local[1]").getOrCreate()
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    } finally {
      GpuDeviceManager.shutdown()
    }
  }

  test("get host memory limits zero config off heap disabled") {
    val deviceCount = 1
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "false"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)

    assertResult(0)(pinnedSize)
    assertResult(-1)(nonPinnedSize)
  }

  test("get host memory limits zero config") {
    val deviceCount = 1
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)

    val minMem = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = minMem - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits zero config with host mem") {
    val deviceCount = 1
    val pySparkOverheadStr = "2g"
    val sparkOffHeapSizeStr = "1g"
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", sparkOffHeapSizeStr)
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"))
    val availableHostMem = toBytes("16g")
    TestMemoryChecker.setAvailableMemoryBytes(Some(availableHostMem))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    val heapSize = toBytes("1g") // default
    val pySparkOverhead = toBytes(pySparkOverheadStr)
    val sparkOffHeapSize = toBytes(sparkOffHeapSizeStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = (.8 * (availableHostMem - heapSize - pySparkOverhead -
      sparkOffHeapSize)).toLong - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits zero config with host mem - multi device") {
    val deviceCount = 2
    val pySparkOverheadStr = "2g"
    val sparkOffHeapSizeStr = "1g"
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", sparkOffHeapSizeStr)
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"))
    val availableHostMem = toBytes("32g")
    TestMemoryChecker.setAvailableMemoryBytes(Some(availableHostMem))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    val heapSize = toBytes("1g") // default
    val pySparkOverhead = toBytes(pySparkOverheadStr)
    val sparkOffHeapSize = toBytes(sparkOffHeapSizeStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = (.8 * ((availableHostMem / deviceCount) - heapSize - pySparkOverhead -
      sparkOffHeapSize)).toLong - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits off heap configured") {
    val deviceCount = 1
    val offHeapLimitStr = "16g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true",
      RapidsConf.OFF_HEAP_LIMIT_SIZE.key -> offHeapLimitStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
      TestMemoryChecker)

    val offHeapLimit = toBytes(offHeapLimitStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = offHeapLimit - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits memoryOverhead configured") {
    val deviceCount = 1
    val sparkOverheadStr = "8g"
    val sparkConf = new SparkConf()
      .set("spark.executor.memoryOverhead", sparkOverheadStr)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.executor.pyspark.memory", "1g") // should be ignored here
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)

    val sparkOverhead = toBytes(sparkOverheadStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = sparkOverhead - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits pinned config only") {
    val deviceCount = 1
    val pinnedSizeStr = "2g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true",
      RapidsConf.PINNED_POOL_SIZE.key -> pinnedSizeStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)

    val expectedPinned = toBytes(pinnedSizeStr)
    val minMemLimit = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = minMemLimit - totalOverhead - expectedPinned

    assertResult(expectedPinned)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits pinned config above memLimit") {
    val deviceCount = 1
    val pinnedSizeStr = "8g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true",
      RapidsConf.PINNED_POOL_SIZE.key -> pinnedSizeStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)

    val minMemLimit = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedPinned = minMemLimit - totalOverhead

    assertResult(expectedPinned)(pinnedSize)
    assertResult(0)(nonPinnedSize)
  }

  test("get host memory limits zero config with host mem with heap size set") {
    val deviceCount = 1
    val pySparkOverheadStr = "2g"
    val heapSizeStr = "2g"
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.executor.memory", heapSizeStr)
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"))
    val hostBytes = toBytes("16g")
    TestMemoryChecker.setAvailableMemoryBytes(Some(hostBytes))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    val pySparkOverhead = toBytes(pySparkOverheadStr)
    val heapSize = toBytes(heapSizeStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = (.8 * (hostBytes - heapSize - pySparkOverhead)).toLong - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("computeRmmPoolSize with integrated GPU") {
    val totalGpuMem = 16L * 1024 * 1024 * 1024 // 16GB
    val freeGpuMem = 12L * 1024 * 1024 * 1024  // 12GB free
    val integratedGpuFraction = 0.6 // 60% of physical memory allocated to GPU

    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.8",
      RapidsConf.RMM_ALLOC_MIN_FRACTION.key -> "0.01",
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> "0.9",
      RapidsConf.RMM_ALLOC_RESERVE.key -> "0",
      RapidsConf.INTEGRATED_GPU_MEMORY_FRACTION.key -> integratedGpuFraction.toString
    ))

    def truncateToAlignment(x: Long): Long = x & ~511L

    try {
      // Force integrated GPU behavior
      GpuDeviceManager.setForceIntegratedGpuForTesting(true)

      val memInfo = TestCudaMemInfo.create(totalGpuMem, freeGpuMem)
      val effectiveGpuTotal = (totalGpuMem * integratedGpuFraction).toLong
      val reserveAmount = rapidsConf.rmmAllocReserve
      val expectedPoolSize = truncateToAlignment(
        (rapidsConf.rmmAllocFraction * (Math.min(memInfo.free, effectiveGpuTotal) - reserveAmount))
        .toLong)

      val poolSize = GpuDeviceManager.computeRmmPoolSize(rapidsConf, memInfo)

      assertResult(expectedPoolSize)(poolSize)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("computeRmmPoolSize with discrete GPU") {
    val totalGpuMem = 8L * 1024 * 1024 * 1024  // 8GB
    val freeGpuMem = 6L * 1024 * 1024 * 1024   // 6GB free

    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.8",
      RapidsConf.RMM_ALLOC_MIN_FRACTION.key -> "0.01",
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> "0.9",
      RapidsConf.RMM_ALLOC_RESERVE.key -> "0"
    ))

    try {
      // Force discrete GPU behavior
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)

      val memInfo = TestCudaMemInfo.create(totalGpuMem, freeGpuMem)
      val poolSize = GpuDeviceManager.computeRmmPoolSize(rapidsConf, memInfo)

      // For discrete GPU, effective total = total = 8GB
      // Pool allocation = allocFraction * min(free, total) = 0.8 * min(6GB, 8GB) =
      // 0.8 * 6GB = 4.8GB
      // Then truncate to 512-byte alignment
      val rawPoolSize = (0.8 * Math.min(freeGpuMem, totalGpuMem)).toLong
      val expectedPoolSize = rawPoolSize & ~511L

      assertResult(expectedPoolSize)(poolSize)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("computeRmmPoolSize integrated GPU with reserve memory") {
    val totalGpuMem = 16L * 1024 * 1024 * 1024 // 16GB
    val freeGpuMem = 14L * 1024 * 1024 * 1024  // 14GB free
    val reserveMem = 1L * 1024 * 1024 * 1024    // 1GB reserve
    val integratedGpuFraction = 0.5             // 50% of physical memory allocated to GPU

    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.8",
      RapidsConf.RMM_ALLOC_RESERVE.key -> reserveMem.toString,
      RapidsConf.INTEGRATED_GPU_MEMORY_FRACTION.key -> integratedGpuFraction.toString
    ))

    def truncateToAlignment(x: Long): Long = x & ~511L

    try {
      // Force integrated GPU behavior
      GpuDeviceManager.setForceIntegratedGpuForTesting(true)

      val memInfo = TestCudaMemInfo.create(totalGpuMem, freeGpuMem)
      val effectiveGpuTotal = (totalGpuMem * integratedGpuFraction).toLong
      val reserveAmount = rapidsConf.rmmAllocReserve
      val expectedPoolSize = truncateToAlignment(
        (rapidsConf.rmmAllocFraction * (Math.min(memInfo.free, effectiveGpuTotal) - reserveAmount))
        .toLong)

      val poolSize = GpuDeviceManager.computeRmmPoolSize(rapidsConf, memInfo)

      assertResult(expectedPoolSize)(poolSize)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  private val mib = 1024L * 1024

  // by default, two device pools (send and receive) of 40 bounce buffers of 2 MiB: 160 MiB
  private def ucxConf(pool: String, allocFraction: String, maxAllocFraction: String,
      minAllocFraction: String = "0.01",
      mode: RapidsConf.RapidsShuffleManagerMode.Value = RapidsConf.RapidsShuffleManagerMode.UCX,
      reserveMiB: Long = 512, bounceBufferMiB: Long = 2, deviceCount: Int = 40): RapidsConf =
    new RapidsConf(Map(
      RapidsConf.SHUFFLE_MANAGER_MODE.key -> mode.toString,
      RapidsConf.RMM_POOL.key -> pool,
      RapidsConf.RMM_ALLOC_FRACTION.key -> allocFraction,
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> maxAllocFraction,
      RapidsConf.RMM_ALLOC_MIN_FRACTION.key -> minAllocFraction,
      RapidsConf.RMM_ALLOC_RESERVE.key -> (reserveMiB * mib).toString,
      RapidsConf.SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE.key -> (bounceBufferMiB * mib).toString,
      RapidsConf.SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT.key -> deviceCount.toString))

  // With the default bounce buffers, 2 x 32 x 4 MiB, set aside beside a 640 MiB reserve,
  // 0.5 * (12288 - 896) MiB falls below the 0.35 * 16384 MiB minimum. Without them it does not.
  private def minimumBoundaryConf(deviceCount: Int = 32): RapidsConf =
    ucxConf("ASYNC", "0.5", "0.9", "0.35", reserveMiB = 640, bounceBufferMiB = 4,
      deviceCount = deviceCount)

  // the pool size on an executor that can start the UCX transport
  private def executorPoolSize(conf: RapidsConf, info: CudaMemInfo): Long =
    GpuDeviceManager.computeRmmPoolSize(conf, info, ucxTransportPossible = true)

  test("computeRmmPoolSize reserves both UCX device bounce buffer pools under ASYNC") {
    try {
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)
      val memInfo = TestCudaMemInfo.create(total = 16384 * mib, free = 12288 * mib)
      // 0.5 * (12288 MiB free - 512 MiB reserve - 160 MiB of bounce buffers)
      assertResult(5808 * mib)(executorPoolSize(ucxConf("ASYNC", "0.5", "0.9"), memInfo))
      // capped at 0.25 * 16384 MiB total - 512 MiB reserve - 160 MiB of bounce buffers
      assertResult(3424 * mib)(executorPoolSize(ucxConf("ASYNC", "0.25", "0.25"),
        TestCudaMemInfo.create(total = 16384 * mib, free = 15872 * mib)))
      // equal fractions, but too little free memory for the cap: 0.25 * (8192 - 672) MiB
      assertResult(1880 * mib)(executorPoolSize(ucxConf("ASYNC", "0.25", "0.25"),
        TestCudaMemInfo.create(total = 16384 * mib, free = 8192 * mib)))
      // no device bounce buffers, then one of 2 MiB in each of the two pools
      assertResult(5888 * mib)(
        executorPoolSize(ucxConf("ASYNC", "0.5", "0.9", deviceCount = 0), memInfo))
      assertResult(5886 * mib)(
        executorPoolSize(ucxConf("ASYNC", "0.5", "0.9", deviceCount = 1), memInfo))
      // no bounce buffer reserve when they come from the pool, or without the UCX transport
      assertResult(5888 * mib)(executorPoolSize(ucxConf("ARENA", "0.5", "0.9"), memInfo))
      assertResult(5888 * mib)(executorPoolSize(ucxConf("ASYNC", "0.5", "0.9",
        mode = RapidsConf.RapidsShuffleManagerMode.MULTITHREADED), memInfo))
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("computeRmmPoolSize errors report the configured fractions and the UCX bounce buffers") {
    try {
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)
      val memInfo = TestCudaMemInfo.create(total = 16384 * mib, free = 12288 * mib)
      val reserve =
        s"${RapidsConf.RMM_ALLOC_RESERVE}: 512.0 MiB plus 160.0 MiB of UCX bounce buffers"
      // 0.5 * (12288 - 672) MiB is below 0.4 * 16384 MiB
      val belowMin = intercept[IllegalArgumentException] {
        executorPoolSize(ucxConf("ASYNC", "0.5", "0.9", "0.4"), memInfo)
      }
      assert(belowMin.getMessage.contains(reserve), belowMin.getMessage)
      // the 672 MiB reserve exceeds 0.04 * 16384 MiB
      val aboveMax = intercept[IllegalArgumentException] {
        executorPoolSize(ucxConf("ASYNC", "0.03", "0.04"), memInfo)
      }
      assert(aboveMax.getMessage.contains(s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=0.04)"),
        aboveMax.getMessage)
      assert(aboveMax.getMessage.contains(reserve), aboveMax.getMessage)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("computeRmmPoolSize takes no UCX bounce buffer reserve on the driver") {
    try {
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)
      // local mode runs its executor on the driver, which never starts a UCX transport, even
      // with the RAPIDS shuffle manager configured
      SparkSession.builder().master("local[1]")
        .config("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
        .getOrCreate()
      assertResult(5824 * mib)(GpuDeviceManager.computeRmmPoolSize(minimumBoundaryConf(),
        TestCudaMemInfo.create(total = 16384 * mib, free = 12288 * mib)))
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("UCX bounce buffers are reserved only where a UCX transport can start") {
    val rapidsShuffle = new SparkConf()
      .set("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
    assert(GpuDeviceManager.ucxTransportCanStart("1", rapidsShuffle))
    // without the early start, the transport can still start at the first shuffle
    assert(GpuDeviceManager.ucxTransportCanStart("1",
      rapidsShuffle.clone.set(RapidsConf.SHUFFLE_TRANSPORT_EARLY_START.key, "false")))
    assert(!GpuDeviceManager.ucxTransportCanStart("driver", rapidsShuffle))
    assert(!GpuDeviceManager.ucxTransportCanStart("1", new SparkConf()))
    assert(!GpuDeviceManager.ucxTransportCanStart("1",
      new SparkConf().set("spark.shuffle.manager", "sort")))
    try {
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)
      val memInfo = TestCudaMemInfo.create(total = 16384 * mib, free = 12288 * mib)
      assertResult(5824 * mib)(GpuDeviceManager.computeRmmPoolSize(minimumBoundaryConf(),
        memInfo, ucxTransportPossible = false))
      // the old reserve, one 4 MiB buffer per pool, left 0.5 * (12288 - 648) MiB, above the minimum
      assertResult(5820 * mib)(executorPoolSize(minimumBoundaryConf(deviceCount = 1), memInfo))
      assertThrows[IllegalArgumentException](executorPoolSize(minimumBoundaryConf(), memInfo))
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
    }
  }

  test("get host memory limits with integrated GPU") {
    val deviceCount = 1
    val pySparkOverheadStr = "2g"
    val heapSizeStr = "2g"
    val hostMemBytes = 32L * 1024 * 1024 * 1024 // 32GB
    val integratedGpuFraction = 0.6

    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.executor.memory", heapSizeStr)
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true",
      RapidsConf.INTEGRATED_GPU_MEMORY_FRACTION.key -> integratedGpuFraction.toString
    ))

    TestMemoryChecker.setAvailableMemoryBytes(Some(hostMemBytes))

    try {
      // Force integrated GPU behavior
      GpuDeviceManager.setForceIntegratedGpuForTesting(true)

      val (pinnedSize, nonPinnedSize) =
        GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
          TestMemoryChecker)

      val pySparkOverhead = toBytes(pySparkOverheadStr)
      val heapSize = toBytes(heapSizeStr)
      val totalOverhead = toBytes("15m") // default

      // For integrated GPU, available host memory = hostMem * (1 - integratedGpuFraction)
      val expectedAvailableHostMem = (hostMemBytes * (1.0 - integratedGpuFraction)).toLong
      val expectedNonPinned = (.8 * (expectedAvailableHostMem - heapSize - pySparkOverhead))
        .toLong - totalOverhead

      assertResult(0)(pinnedSize)
      assertResult(expectedNonPinned)(nonPinnedSize)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
      TestMemoryChecker.setAvailableMemoryBytes(None)
    }
  }

  test("MT read limit should be 90% of total off heap when off heap limit disabled") {
    val deviceCount = 1
    val availableHostMem = 32L * 1024 * 1024 * 1024  // 32 GiB
    val heapSize = toBytes("1g")  // default spark.executor.memory

    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "false"))

    TestMemoryChecker.setAvailableMemoryBytes(Some(availableHostMem))

    try {
      val mtLimit = GpuDeviceManager.computeMtReadLimit(
        pinnedSize = 0L, nonPinnedLimit = -1L,
        rapidsConf, sparkConf, deviceCount, TestMemoryChecker)

      // When off-heap limit is disabled, the effective total off-heap should be derived
      // from hardware (same 80% formula used in the enabled path), and MT read limit = 90% of that.
      val effectiveOffHeap = (0.8 * (availableHostMem - heapSize)).toLong
      val expectedMtLimit = (0.9 * effectiveOffHeap).toLong
      assertResult(expectedMtLimit)(mtLimit)
    } finally {
      TestMemoryChecker.setAvailableMemoryBytes(None)
    }
  }

  test("get host memory limits with discrete GPU") {
    val deviceCount = 1
    val pySparkOverheadStr = "2g"
    val heapSizeStr = "2g"
    val hostMemBytes = 32L * 1024 * 1024 * 1024 // 32GB

    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.executor.memory", heapSizeStr)
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "true"
    ))

    TestMemoryChecker.setAvailableMemoryBytes(Some(hostMemBytes))

    try {
      // Force discrete GPU behavior
      GpuDeviceManager.setForceIntegratedGpuForTesting(false)

      val (pinnedSize, nonPinnedSize) =
        GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf, deviceCount,
          TestMemoryChecker)

      val pySparkOverhead = toBytes(pySparkOverheadStr)
      val heapSize = toBytes(heapSizeStr)
      val totalOverhead = toBytes("15m") // default

      // For discrete GPU, available host memory = hostMem (no fraction applied)
      val expectedNonPinned = (.8 * (hostMemBytes - heapSize - pySparkOverhead))
        .toLong - totalOverhead

      assertResult(0)(pinnedSize)
      assertResult(expectedNonPinned)(nonPinnedSize)
    } finally {
      GpuDeviceManager.resetForceIntegratedGpuForTesting()
      TestMemoryChecker.setAvailableMemoryBytes(None)
    }
  }
}
