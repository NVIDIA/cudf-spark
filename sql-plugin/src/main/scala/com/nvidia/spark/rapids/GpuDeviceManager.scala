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

import java.util.concurrent.ThreadFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf._
import com.nvidia.spark.rapids.jni.DeviceAttr
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv

sealed trait MemoryState
private case object Initialized extends MemoryState
private case object Uninitialized extends MemoryState
private case object Errored extends MemoryState

object GpuDeviceManager extends Logging {
  // This config controls whether RMM/Pinned memory are initialized from the task
  // or from the executor side plugin. The default is to initialize from the
  // executor plugin.
  // var for testing purposes only!
  var rmmTaskInitEnabled = {
    java.lang.Boolean.getBoolean("com.nvidia.spark.rapids.memory.gpu.rmm.init.task")
  }

  private var numCores = 0

  /**
   * Get an approximate count on the number of cores this executor will use.
   */
  def getNumCores: Int = numCores

  private var memorySize = 0L

  def getMemorySize: Long = memorySize

  // Memory resource used only for cudf::chunked_pack to allocate scratch space
  // during spill to host. This is done to set aside some memory for this operation
  // from the beginning of the job.
  var chunkedPackMemoryResource: Option[RmmPoolMemoryResource[RmmCudaMemoryResource]] = None

  // for testing only
  def setRmmTaskInitEnabled(enabled: Boolean): Unit = {
    rmmTaskInitEnabled = enabled
  }

  private val threadGpuInitialized = new ThreadLocal[Boolean]()
  @volatile private var singletonMemoryInitialized: MemoryState = Uninitialized
  @volatile private var deviceId: Option[Int] = None

  /**
   * Exposes the device id used while initializing the RMM pool
   */
  def getDeviceId(): Option[Int] = deviceId

  // For testing purposes only - allows forcing integrated GPU behavior
  private var forceIntegratedGpu: Option[Boolean] = None

  /**
   * For testing: force integrated GPU behavior regardless of actual hardware
   */
  def setForceIntegratedGpuForTesting(force: Boolean): Unit = {
    forceIntegratedGpu = Some(force)
  }

  /**
   * For testing: reset forced integrated GPU behavior
   */
  def resetForceIntegratedGpuForTesting(): Unit = {
    forceIntegratedGpu = None
  }

  /**
   * Check if we should treat the GPU as integrated (for testing or actual hardware)
   */
  private def isIntegratedGpu: Boolean = {
    forceIntegratedGpu.getOrElse(DeviceAttr.isIntegratedGPU == 1)
  }

  @volatile private var poolSizeLimit = 0L

  // Never split below 100 MiB (but this is really just for testing)
  def getSplitUntilSize: Long = {
    val conf = new RapidsConf(SQLConf.get)
    conf.splitUntilSizeOverride
        .getOrElse(Math.max(poolSizeLimit / 8, 100 * 1024 * 1024))
  }

  // Attempt to set and acquire the gpu, return true if acquired, false otherwise
  def tryToSetGpuDeviceAndAcquire(addr: Int): Boolean = {
    try {
      GpuDeviceManager.setGpuDeviceAndAcquire(addr)
    } catch {
      case NonFatal(e) =>
        logInfo(s"Will not use GPU $addr because of $e")
        // we may have lost a race trying to acquire this addr or GPU is already busy
        return false
    }
    return true
  }

  /**
   * This is just being extra paranoid when we are running on GPUs in exclusive mode. It is not
   * clear exactly what cuda interactions cause us to acquire a device, so this will force us
   * to do an interaction we know will acquire the device.
   */
  private def findGpuAndAcquire(): Int = {
    val deviceCount: Int = Cuda.getDeviceCount()
    // loop multiple times to see if a GPU was released or something unexpected happened that
    // we couldn't acquire on first try
    var numRetries = 2
    val addrsToTry = ArrayBuffer.empty ++= (0 until deviceCount)
    while (numRetries > 0) {
      val addr = addrsToTry.find(tryToSetGpuDeviceAndAcquire)
      if (addr.isDefined) {
        return addr.get
      }
      numRetries -= 1
    }
    throw new IllegalStateException("Could not find a single GPU to use")
  }

  def setGpuDeviceAndAcquire(addr: Int): Int = {
    logDebug(s"Initializing GPU device ID to $addr")
    Cuda.setDevice(addr.toInt)
    // cudaFree(0) to actually allocate the set device - no process exclusive required
    // since we are relying on Spark to schedule it properly and not give it to multiple
    // executors
    Cuda.freeZero()
    addr
  }

  def getGPUAddrFromResources(resources: Map[String, ResourceInformation],
      conf: RapidsConf): Option[Int] = {
    val sparkGpuResourceName = conf.getSparkGpuResourceName
    if (resources.contains(sparkGpuResourceName)) {
      logDebug(s"Spark resources contain: $sparkGpuResourceName")
      val addrs = resources(sparkGpuResourceName).addresses
      if (addrs.length > 1) {
        // Throw an exception since we assume one GPU per executor.
        // If multiple GPUs are allocated by spark, then different tasks could get assigned
        // different GPUs but RMM would only be initialized for 1. We could also just get
        // weird results that are hard to debug.
        throw new IllegalArgumentException("Spark GPU Plugin only supports 1 gpu per executor")
      }
      Some(addrs.head.toInt)
    } else {
      None
    }
  }

  // Initializes the GPU if Spark assigned one.
  // Returns either the GPU addr Spark assigned or None if Spark didn't assign one.
  def initializeGpu(resources: Map[String, ResourceInformation], conf: RapidsConf): Option[Int] = {
    getGPUAddrFromResources(resources, conf).map(setGpuDeviceAndAcquire(_))
  }

  def initializeGpuAndMemory(resources: Map[String, ResourceInformation],
      conf: RapidsConf, numCores: Int): Unit = {
    this.numCores = numCores
    // as long in execute mode initialize everything because we could enable it after startup
    if (conf.isSqlExecuteOnGPU) {
      // Set the GPU before RMM is initialized if spark provided the GPU address so that RMM
      // uses that GPU. We only need to initialize RMM once per Executor because we are relying on
      // only 1 GPU per executor.
      // If Spark didn't provide the address we just use the default GPU.
      val addr = initializeGpu(resources, conf)
      initializeMemory(addr)
    }
  }

  def shutdown(): Unit = synchronized {
    // assume error during shutdown until we complete it
    singletonMemoryInitialized = Errored

    chunkedPackMemoryResource.foreach(_.close)
    chunkedPackMemoryResource = None
    poolSizeLimit = 0L

    SpillFramework.shutdown()

    // If we close RMM resources in the shutdown hook in `MemoryCleaner`, will
    // get segment fault, so close them here before shutdown hook starts.
    // We assume all RMM resources should be closed at this point, or leaks occurs,
    // because all Spark tasks/jobs are done at this point.
    MemoryCleaner.cleanAllRmmBlockers()

    RmmSpark.clearEventHandler()
    Rmm.clearEventHandler()
    GpuShuffleEnv.shutdown()

    Rmm.shutdown()
    singletonMemoryInitialized = Uninitialized
  }

  def getResourcesFromTaskContext: Map[String, ResourceInformation] = {
    val tc = TaskContext.get()
    if (tc == null) Map.empty[String, ResourceInformation] else tc.resources()
  }

  /**
   * Always set the GPU if it was assigned by Spark and initialize the RMM if its configured
   * to do so in the task.
   * We expect the plugin to be run with 1 GPU per executor.
   */
  def initializeFromTask(): Unit = {
    if (threadGpuInitialized.get() == false) {
      val resources = getResourcesFromTaskContext
      val conf = new RapidsConf(SparkEnv.get.conf)
      if (rmmTaskInitEnabled) {
        val numCores = RapidsPluginUtils.estimateCoresOnExec(SparkEnv.get.conf)
        initializeGpuAndMemory(resources, conf, numCores)
      } else {
        // just set the device if provided so task thread uses right GPU
        initializeGpu(resources, conf)
      }
      threadGpuInitialized.set(true)
    }
  }

  private def toMiB(x: Long): Double = x / 1024 / 1024.0

  /**
   * Compute RMM pool size based on configuration and GPU memory info.
   * Visible for testing.
   */
  def computeRmmPoolSize(conf: RapidsConf, info: CudaMemInfo): Long = {
    def truncateToAlignment(x: Long): Long = x & ~511L

    // For integrated GPUs, we treat memory as shared between CPU and GPU
    // The available GPU memory is physical_total * integratedGpuMemoryFraction
    val availableGpuTotal = if (isIntegratedGpu) {
      (info.total * conf.integratedGpuMemoryFraction).toLong
    } else {
      info.total
    }
    logInfo(s"availableGpuTotal: ${toMiB(availableGpuTotal)} MiB")

    // No checks when rmmExactAlloc is given. We are just going to go with the amount requested
    // with the proper alignment (which is a requirement for some allocators). This is because
    // it is for testing and we assume that the tests know what they are doing. If the conf becomes
    // public, then we need to do some more work.
    conf.rmmExactAlloc.map(truncateToAlignment).getOrElse {
      val minAllocation = truncateToAlignment((conf.rmmAllocMinFraction * availableGpuTotal).toLong)
      val maxAllocation = truncateToAlignment((conf.rmmAllocMaxFraction * availableGpuTotal).toLong)
      val reserveAmount =
        if (conf.isUCXShuffleManagerMode && conf.rmmPool.equalsIgnoreCase("ASYNC")) {
          // When using the async allocator, UCX calls `cudaMalloc` directly to allocate the
          // bounce buffers.
          conf.rmmAllocReserve + conf.shuffleUcxBounceBuffersSize * 2
        } else {
          conf.rmmAllocReserve
        }
      val availableFree = Math.min(info.free, availableGpuTotal)
      logInfo(s"availableGpuFree: ${toMiB(availableFree)} MiB")
      var poolAllocation = truncateToAlignment(
        (conf.rmmAllocFraction * (availableFree - reserveAmount)).toLong)
      val errorPhrase = "The pool allocation of " +
        s"${toMiB(poolAllocation)} MiB (gpu.free: ${toMiB(availableFree)}," +
        s"${RapidsConf.RMM_ALLOC_FRACTION}: (=${conf.rmmAllocFraction}," +
        s"${RapidsConf.RMM_ALLOC_RESERVE}: ${reserveAmount} => " +
        s"(gpu.free - reserve) * allocFraction = ${toMiB(poolAllocation)}) was "
      if (poolAllocation < minAllocation) {
        throw new IllegalArgumentException(errorPhrase +
            s"less than allocation of ${toMiB(minAllocation)} MiB (gpu.total: " +
            s"${toMiB(availableGpuTotal)} MiB, ${RapidsConf.RMM_ALLOC_MIN_FRACTION}: " +
            s"${conf.rmmAllocMinFraction} => gpu.total *" +
            s"minAllocFraction = ${toMiB(minAllocation)} MiB). Please ensure that the GPU has " +
            s"enough free memory, or adjust configuration accordingly.")
      }
      if (maxAllocation < poolAllocation) {
        throw new IllegalArgumentException(errorPhrase +
            s"more than allocation of ${toMiB(maxAllocation)} MiB (gpu.total: " +
            s"${toMiB(availableGpuTotal)} MiB, ${RapidsConf.RMM_ALLOC_MAX_FRACTION}: " +
            s"${conf.rmmAllocMaxFraction} => gpu.total *" +
            s"maxAllocFraction = ${toMiB(maxAllocation)} MiB). Please ensure that pool " +
            s"allocation does not exceed maximum allocation and adjust configuration accordingly.")
      }
      if (reserveAmount >= maxAllocation) {
        throw new IllegalArgumentException(s"RMM reserve memory (${toMiB(reserveAmount)} MB) " +
            s"larger than maximum pool size (${toMiB(maxAllocation)} MB). Check the settings for " +
            s"${RapidsConf.RMM_ALLOC_MAX_FRACTION} (=${conf.rmmAllocFraction}) and " +
            s"${RapidsConf.RMM_ALLOC_RESERVE} (=$reserveAmount)")
      }
      val adjustedMaxAllocation = truncateToAlignment(maxAllocation - reserveAmount)
      if (poolAllocation > adjustedMaxAllocation) {
        logWarning(s"RMM pool allocation (${toMiB(poolAllocation)} MB) does not leave enough " +
          s"free memory for reserve memory (${toMiB(reserveAmount)} MB), lowering the pool " +
          s"size to ${toMiB(adjustedMaxAllocation)} MB to " +
          s"accommodate the requested reserve amount.")
        poolAllocation = adjustedMaxAllocation
      }

      poolAllocation
    }
  }

  private var memoryEventHandler: DeviceMemoryEventHandler = _

  private def initializeSpillAndMemoryEvents(conf: RapidsConf): Unit = {
    SpillFramework.initialize(conf)

    memoryEventHandler = new DeviceMemoryEventHandler(
      SpillFramework.stores.deviceStore,
      conf.gpuOomDumpDir,
      conf.gpuOomMaxRetries)

    if (conf.sparkRmmStateEnable) {
      val debugLoc = if (conf.sparkRmmDebugLocation.isEmpty) {
        null
      } else {
        conf.sparkRmmDebugLocation
      }
      // Enable RMM debug mode if retry coverage tracking is enabled, to get onAllocated callbacks
      val enableDebug = AllocationRetryCoverageTracker.ENABLED
      RmmSpark.setEventHandler(memoryEventHandler, debugLoc, enableDebug)
    } else {
      logWarning("SparkRMM retry has been disabled")
      Rmm.setEventHandler(memoryEventHandler)
    }
  }

  /**
   * Parse the rmm allocation mode from the config, and return it as an Int,
   * see "RmmAllocationMode" for supported types.
   * (Visible for tests)
   */
  def rmmModeFromConf(
      conf: RapidsConf,
      features: Option[ArrayBuffer[String]] = None): Int = {
    // Old config warning
    val oldPoolConfKey = "spark.rapids.memory.gpu.pooling.enabled"
    if (conf.rapidsConfMap.containsKey(oldPoolConfKey)) {
      logWarning(s"Found '$oldPoolConfKey' is being used, but it will be ignored " +
        s"since it is completely dropped now.")
    }

    var init = conf.rmmPool match {
      case c if "default".equalsIgnoreCase(c) =>
        if (Cuda.isPtdsEnabled) {
          logWarning("Configuring the DEFAULT allocator with a CUDF built for " +
            "Per-Thread Default Stream (PTDS). This is known to be unstable! " +
            "We recommend you use the ARENA allocator when PTDS is enabled.")
        }
        features.foreach(_ += "POOLED")
        RmmAllocationMode.POOL
      case c if "arena".equalsIgnoreCase(c) =>
        features.foreach(_ += "ARENA")
        RmmAllocationMode.ARENA
      case c if "async".equalsIgnoreCase(c) =>
        features.foreach(_ += "ASYNC")
        RmmAllocationMode.CUDA_ASYNC
      case c if "none".equalsIgnoreCase(c) =>
        // Pooling is disabled.
        RmmAllocationMode.CUDA_DEFAULT
      case c =>
        throw new IllegalArgumentException(s"RMM pool set to '$c' is not supported.")
    }

    if (conf.isUvmEnabled) {
      // Enable managed memory only if async allocator is not used.
      if ((init & RmmAllocationMode.CUDA_ASYNC) == 0) {
        features.foreach(_ += "UVM")
        init = init | RmmAllocationMode.CUDA_MANAGED_MEMORY
      } else {
        throw new IllegalArgumentException(
          "CUDA Unified Memory is not supported in CUDA_ASYNC allocation mode");
      }
    }
    init
  }

  private def initializeRmmGpuPool(gpuId: Int, conf: RapidsConf): Unit = {
    if (!Rmm.isInitialized) {
      val poolSize = conf.chunkedPackPoolSize
      chunkedPackMemoryResource =
        if (poolSize > 0) {
          val chunkedPackPool =
            new RmmPoolMemoryResource(new RmmCudaMemoryResource(), poolSize, poolSize)
          logDebug(
            s"Initialized pool resource for spill operations " +
                s"of ${chunkedPackMemoryResource.map(_.getMaxSize)} Bytes")
          Some(chunkedPackPool)
        } else {
          None
        }

      val info = Cuda.memGetInfo()
      val poolAllocation = computeRmmPoolSize(conf, info)
      memorySize = poolAllocation
      val features = new ArrayBuffer[String](3 /* 1.pool type, 2.uvm mode, 3.log sink */)
      var init = rmmModeFromConf(conf, Some(features))

      val logConf: Rmm.LogConf = conf.rmmDebugLocation match {
        case c if "none".equalsIgnoreCase(c) => null
        case c if "stdout".equalsIgnoreCase(c) =>
          features += "LOG: STDOUT"
          Rmm.logToStdout()
        case c if "stderr".equalsIgnoreCase(c) =>
          features += "LOG: STDERR"
          Rmm.logToStderr()
        case c =>
          logWarning(s"RMM logging set to '$c' is not supported and is being ignored.")
          null
      }

      deviceId = Some(gpuId)

      logInfo(s"Initializing RMM${features.mkString(" ", " ", "")} " +
          s"pool size = ${toMiB(poolAllocation)} MB on gpuId $gpuId")

      if (Cuda.isPtdsEnabled()) {
        logInfo("Using per-thread default stream")
      } else {
        logInfo("Using legacy default stream")
      }

      Cuda.setDevice(gpuId)
      try {
        poolSizeLimit = poolAllocation
        Rmm.initialize(init, logConf, poolAllocation)
      } catch {
        case firstEx: CudfException if ((init & RmmAllocationMode.CUDA_ASYNC) != 0) => {
          logWarning("Failed to initialize RMM with ASYNC allocator. " +
            "Initializing with ARENA allocator as a fallback option.")
          init = init & (~RmmAllocationMode.CUDA_ASYNC) | RmmAllocationMode.ARENA
          try {
            Rmm.initialize(init, logConf, poolAllocation)
          } catch {
            case secondEx: Throwable => {
              logError(
                "Failed to initialize RMM with either ASYNC or ARENA allocators. Exiting...")
              secondEx.addSuppressed(firstEx)
              poolSizeLimit = 0L
              throw secondEx
            }
          }
        }
      }

    }
  }

  private def initializePinnedPoolAndOffHeapLimits(gpuId: Int, conf: RapidsConf,
                                                   sparkConf: SparkConf): Unit = {
    val (pinnedSize, nonPinnedLimit) = getPinnedPoolAndOffHeapLimits(
      conf, sparkConf, Cuda.getDeviceCount)
    // disable the cuDF provided default pinned pool for now
    if (!PinnedMemoryPool.configureDefaultCudfPinnedPoolSize(0L)) {
      // This is OK in tests because they don't unload/reload our shared
      // library, and in prod it would be nice to know about it.
      logWarning("The default cuDF host pool was already configured")
    }
    if (!PinnedMemoryPool.isInitialized && pinnedSize > 0) {
      val initThreads = if (conf.pinnedPoolParallelInitThreads == "all") numCores
        else conf.pinnedPoolParallelInitThreads.toInt
      val logDetails =
        if (initThreads > 1) s" using ${initThreads} parallel pre-touch threads" else ""
      logInfo(s"Initializing pinned memory pool (${pinnedSize / 1024 / 1024.0} MiB)${logDetails}")
      PinnedMemoryPool.initialize(pinnedSize, gpuId, conf.pinnedPoolCuioDefault, initThreads)
    }
    // Host memory limits must be set after the pinned memory pool is initialized
    HostAlloc.initialize(nonPinnedLimit)
    // Fill the MULTITHREAD_READ_MEMORY_LIMIT_SIZE with 90% of the total off-heap memory
    // if it is not set already. When off-heap limit tracking is disabled (nonPinnedLimit == -1),
    // falls back to a hardware-derived estimate via computeEffectiveOffHeapLimit.
    if (conf.multiThreadReadMemoryLimit == 0) {
      sparkConf.set(RapidsConf.MULTITHREAD_READ_MEMORY_LIMIT_SIZE.key,
        computeMtReadLimit(pinnedSize, nonPinnedLimit, conf, sparkConf,
          Cuda.getDeviceCount).toString)
    }
  }

  // visible for testing
  def computeMtReadLimit(pinnedSize: Long, nonPinnedLimit: Long,
      conf: RapidsConf, sparkConf: SparkConf, deviceCount: Int,
      memCheck: MemoryChecker = MemoryCheckerImpl): Long = {
    val totalOffHeap = if (nonPinnedLimit >= 0) {
      pinnedSize + nonPinnedLimit
    } else {
      // nonPinnedLimit == -1 means off-heap limit tracking is disabled (unlimited).
      // Derive a concrete total from hardware so the MT read limit is still meaningful.
      // Note: this feature is off by default
      // (spark.rapids.sql.multiThreadedRead.memoryLimit.enabled defaults to false), so the risk
      // of this approximation affecting users is low. The
      // underlying hardware-derived calculation has known limitations — in particular, it may not
      // account correctly for spark.memory.offHeap.size in all environments (see
      // https://github.com/NVIDIA/spark-rapids/issues/13628). For now we reuse the same
      // best-effort estimate rather than special-casing this path.
      computeEffectiveOffHeapLimit(conf, sparkConf, deviceCount, memCheck)
    }
    val limit = (0.9 * totalOffHeap).toLong
    logInfo(s"Setting MT read memory limit to ${limit / 1024 / 1024.0} MiB " +
      s"(90% of ${totalOffHeap / 1024 / 1024.0} MiB total off-heap)")
    limit
  }

  private def computeEffectiveOffHeapLimit(conf: RapidsConf, sparkConf: SparkConf,
      deviceCount: Int, memCheck: MemoryChecker): Long = {
    val confLimit = conf.offHeapLimit
    // This min limit of 4GB is somewhat arbitrary, but based on some testing which showed
    // that the previous minimum of 15 MB * num cores was too little for certain benchmark
    // queries to complete, whereas this limit was sufficient.
    val minMemoryLimit = 4L * 1024 * 1024 * 1024

    val executorOverheadKey = "spark.executor.memoryOverhead"
    val pysparkOverheadKey = "spark.executor.pyspark.memory"
    val heapSizeKey = "spark.executor.memory"
    val sparkOffHeapEnabledKey = "spark.memory.offHeap.enabled"
    val sparkOffHeapSizeKey = "spark.memory.offHeap.size"

    def toBytes = ConfHelper.byteFromString(_, ByteUnit.BYTE)

    val executorOverhead = sparkConf.getOption(executorOverheadKey).map(toBytes)
    val pysparkOverhead = toBytes(sparkConf.get(pysparkOverheadKey, "0"))
    val heapSize = toBytes(sparkConf.get(heapSizeKey, "1g"))
    val sparkOffHeapEnabled = sparkConf.getBoolean(sparkOffHeapEnabledKey, defaultValue = false)
    val sparkOffHeapSize = if (sparkOffHeapEnabled) {
      toBytes(sparkConf.get(sparkOffHeapSizeKey, "0"))
    } else {
      0L
    }

    if (confLimit.isDefined) {
      if (executorOverhead.isEmpty) {
        logWarning(s"$executorOverheadKey is not set")
      }
      logInfo(s"using configured ${RapidsConf.OFF_HEAP_LIMIT_SIZE} of ${confLimit.get}")
      confLimit.get
    } else {
      // in case we cannot query the host for available memory due to environmental
      // constraints, getAvailableMemoryBytes returns None and we fall back to 0, which causes
      // basedOnHostMemory to go negative and trip the minMemoryLimit floor below.
      lazy val availableHostMemory = if (isIntegratedGpu) {
        (memCheck.getAvailableMemoryBytes(conf).getOrElse(0L) * (1.0 -
        conf.integratedGpuMemoryFraction)).toLong
      } else {
        memCheck.getAvailableMemoryBytes(conf).getOrElse(0L)
      }
      val hostMemUsageFraction = .8
      // Spark calculates the total mem to allocate to the job as
      // val totalMemMiB =
      //      executorMemoryMiB + memoryOverheadMiB + memoryOffHeapMiB + pysparkMemToUseMiB
      // and RAPIDS uses memory from the overhead portion here. Therefore, if the overhead is
      // set we can just use that, otherwise we can infer it from the above as
      // val memoryOverheadMiB =
      //      totalMemMiB - executorMemoryMiB - memoryOffHeapMiB - pysparkMemToUseMiB
      // where totalMemMiB is instead derived from the actual mem limits we can observe
      // directly from the system.
      // Note: subtracting sparkOffHeapSize here is an approximation. In some environments
      // (e.g. Databricks) spark.memory.offHeap.size is set to a large value and RAPIDS is
      // expected to operate within that allocation rather than outside it, so subtracting it
      // double-counts the memory and under-estimates the available limit. Fixing this properly
      // may require hooking into Spark's MemoryManager. See
      // https://github.com/NVIDIA/spark-rapids/issues/13628
      lazy val basedOnHostMemory = (hostMemUsageFraction * ((1.0 * availableHostMemory /
        deviceCount) - heapSize - pysparkOverhead - sparkOffHeapSize)).toLong
      if (executorOverhead.isDefined) {
        val basedOnConfiguredOverhead = executorOverhead.get
        logWarning(s"${RapidsConf.OFF_HEAP_LIMIT_SIZE} is not set; we derived " +
          s"a memory limit from ($executorOverheadKey = ${executorOverhead.get}")
        if (basedOnConfiguredOverhead < minMemoryLimit) {
          logWarning(s"memory limit $basedOnConfiguredOverhead is less than the minimum of " +
            s"$minMemoryLimit; using the latter")
          if (minMemoryLimit > basedOnHostMemory) {
            logWarning(s"the amount of available memory detected on the host is " +
              s"$availableHostMemory, based off of which we computed a limit of " +
              s"$basedOnHostMemory, which is less than the minimum $minMemoryLimit, " +
              s"so we are using the minimum $minMemoryLimit")
          }
          minMemoryLimit
        } else {
          basedOnConfiguredOverhead
        }
      } else {
        logWarning(s"${RapidsConf.OFF_HEAP_LIMIT_SIZE} is not set; we used " +
          s"memory limit derived from ($hostMemUsageFraction * (estimated available " +
          s"host memory / device count) - $heapSizeKey - $pysparkOverheadKey - " +
          s"$sparkOffHeapSizeKey) = ($hostMemUsageFraction * ($availableHostMemory / " +
          s"$deviceCount) - $heapSize - $pysparkOverhead - $sparkOffHeapSize) = " +
          s"$basedOnHostMemory")
        if (basedOnHostMemory < minMemoryLimit) {
          logWarning(s"the memory limit, $basedOnHostMemory, based on the available " +
            s"host memory of $availableHostMemory, is less than the minimum of " +
            s"$minMemoryLimit; using the latter $minMemoryLimit")
          minMemoryLimit
        } else {
          basedOnHostMemory
        }
      }
    }
  }

  // visible for testing
  def getPinnedPoolAndOffHeapLimits(conf: RapidsConf, sparkConf: SparkConf, deviceCount: Int,
      memCheck: MemoryChecker =
      MemoryCheckerImpl): (Long, Long) = {
    val perTaskOverhead = conf.perTaskOverhead
    val totalOverhead = perTaskOverhead * GpuDeviceManager.numCores
    val confPinnedSize = conf.pinnedPoolSize
    val confLimitEnabled = conf.offHeapLimitEnabled

    if (confLimitEnabled) {
      val memoryLimit = computeEffectiveOffHeapLimit(conf, sparkConf, deviceCount, memCheck)

      // Now we need to know the pinned vs non-pinned limits
      val pinnedLimit = if (confPinnedSize + totalOverhead <= memoryLimit) {
        confPinnedSize
      } else {
        val ret = memoryLimit - totalOverhead
        logWarning(s"The configured pinned memory ${confPinnedSize / 1024 / 1024.0} MiB " +
            s"plus the overhead ${totalOverhead / 1024 / 1024.0} MiB " +
            s"is larger than the off heap limit ${memoryLimit / 1024 / 1024.0} MiB " +
            s"dropping pinned memory to ${ret / 1024 / 1024.0} MiB")
        ret
      }
      val nonPinnedLimit = memoryLimit - totalOverhead - pinnedLimit
      logWarning(s"Off Heap Host Memory configured to be " +
          s"${pinnedLimit / 1024 / 1024.0} MiB pinned, " +
          s"${nonPinnedLimit / 1024 / 1024.0} MiB non-pinned, and " +
          s"${totalOverhead / 1024 / 1024.0} MiB of untracked overhead.")
      (pinnedLimit, nonPinnedLimit)

    } else {
      (confPinnedSize, -1L)
    }
  }

  /**
   * Initialize the GPU memory for gpuId according to the settings in rapidsConf.  It is assumed
   * that if gpuId is set then that gpu is already the default device.  If gpuId is not set
   * this will search all available GPUs starting at 0 looking for the appropriate one.
   *
   * @param gpuId the id of the gpu to use
   * @param rapidsConf the config to use.
   */
  def initializeMemory(gpuId: Option[Int], rapidsConf: Option[RapidsConf] = None): Unit = {
    if (singletonMemoryInitialized != Initialized) {
      // Memory or memory related components that only need to be initialized once per executor.
      // This synchronize prevents multiple tasks from trying to initialize these at the same time.
      GpuDeviceManager.synchronized {
        if (singletonMemoryInitialized == Errored) {
          throw new IllegalStateException(
            "Cannot initialize memory due to previous shutdown failing")
        } else if (singletonMemoryInitialized == Uninitialized) {
          val gpu = gpuId.getOrElse(findGpuAndAcquire())
          val sparkConf = SparkEnv.get.conf
          val conf = rapidsConf.getOrElse(new RapidsConf(sparkConf))
          initializePinnedPoolAndOffHeapLimits(gpu, conf, sparkConf)
          initializeRmmGpuPool(gpu, conf)
          // we want to initialize this last because we want to take advantage
          // of pinned memory if it is configured
          initializeSpillAndMemoryEvents(conf)
          GpuShuffleEnv.init(conf)
          singletonMemoryInitialized = Initialized
        }
      }
    }
  }

  /** Wrap a thread factory with one that will set the GPU device on each thread created. */
  def wrapThreadFactory(factory: ThreadFactory,
      before: () => Unit = null,
      after: () => Unit = null): ThreadFactory = new ThreadFactory() {
    private[this] val devId = getDeviceId.getOrElse {
      throw new IllegalStateException("Device ID is not set")
    }

    override def newThread(runnable: Runnable): Thread = {
      factory.newThread(() => {
        Cuda.setDevice(devId)
        try {
          if (before != null) {
            before()
          }
          runnable.run()
        } finally {
          if (after != null) {
            after()
          }
        }
      })
    }
  }
}
