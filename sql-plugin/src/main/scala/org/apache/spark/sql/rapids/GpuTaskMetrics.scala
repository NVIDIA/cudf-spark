/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import java.{lang => jl}
import java.io.ObjectInputStream
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{NvtxId, NvtxRegistry, PerfIO}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{AccumulatorV2, LongAccumulator, Utils}

case class NanoTime(value: java.lang.Long) {
  override def toString: String = {
    val hours = TimeUnit.NANOSECONDS.toHours(value)
    var remaining = value - TimeUnit.HOURS.toNanos(hours)
    val minutes = TimeUnit.NANOSECONDS.toMinutes(remaining)
    remaining = remaining - TimeUnit.MINUTES.toNanos(minutes)
    val seconds = remaining.toDouble / TimeUnit.SECONDS.toNanos(1)
    val locale = Locale.US
    "%02d:%02d:%06.3f".formatLocal(locale, hours, minutes, seconds)
  }
}

// Format example:
//  10.74GB (11534336000 bytes)
//  1.23MB (1289750 bytes)
//  1020.10KB (1044585 bytes)
case class SizeInBytes(value: jl.Long) {
  override def toString: String = {
    var unitVal = value
    var remainVal = 0L
    var unitIndex = 0
    while (unitIndex < SizeInBytes.SizeUnitNames.length && unitVal >= 1024) {
      val nextUnitVal = unitVal >> 10
      remainVal = unitVal - (nextUnitVal << 10)
      unitVal = nextUnitVal
      unitIndex += 1
    }
    val finalVal = "%.2f".format(unitVal + (remainVal.toDouble / 1024))
    s"$finalVal${SizeInBytes.SizeUnitNames(unitIndex)} ($value bytes)"
  }
}

private object SizeInBytes {
  private val SizeUnitNames: Array[String] = Array("B", "KB", "MB", "GB", "TB", "PB", "EB")
}

class NanoSecondAccumulator extends AccumulatorV2[jl.Long, NanoTime] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0


  override def copy(): NanoSecondAccumulator = {
    val newAcc = new NanoSecondAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  def add (v: Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, NanoTime]): Unit = other match {
    case ns: NanoSecondAccumulator =>
      _sum += ns._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: NanoTime = NanoTime(_sum)
}

/**
 * Accumulator to sum up size in bytes, almost identical to LongAccumulator but with
 * a user-friendly representation of the value.
 */
class SizeInBytesAccumulator extends AccumulatorV2[jl.Long, SizeInBytes] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0

  override def copy(): SizeInBytesAccumulator = {
    val newAcc = new SizeInBytesAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, SizeInBytes]): Unit = other match {
    case sb: SizeInBytesAccumulator =>
      _sum += sb._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: SizeInBytes = SizeInBytes(_sum)

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue
}

class HighWatermarkAccumulator extends AccumulatorV2[jl.Long, SizeInBytes] {
  private var _value = 0L
  override def isZero: Boolean = _value == 0

  override def copy(): HighWatermarkAccumulator = {
    val newAcc = new HighWatermarkAccumulator
    newAcc._value = this._value
    newAcc
  }

  override def reset(): Unit = {
    _value = 0
  }

  override def add(v: jl.Long): Unit = {
    _value += v
  }

  override def merge(other: AccumulatorV2[jl.Long, SizeInBytes]): Unit = other match {
    case wa: HighWatermarkAccumulator =>
      _value = _value.max(wa._value)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: SizeInBytes = SizeInBytes(_value)
}

class MaxLongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _v = 0L

  override def isZero: Boolean = _v == 0

  override def copy(): MaxLongAccumulator = {
    val newAcc = new MaxLongAccumulator
    newAcc._v = this._v
    newAcc
  }

  override def reset(): Unit = {
    _v = 0L
  }

  override def add(v: jl.Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  def add(v: Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: MaxLongAccumulator =>
      add(o.value)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Long = _v
}

class AvgLongAccumulator extends AccumulatorV2[jl.Long, jl.Double] {
  private var _sum = 0L
  private var _count = 0L

  override def isZero: Boolean = _count == 0L

  override def copy(): AvgLongAccumulator = {
    val newAcc = new AvgLongAccumulator
    newAcc._sum = this._sum
    newAcc._count = this._count
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: AccumulatorV2[jl.Long, jl.Double]): Unit = other match {
    case o: AvgLongAccumulator =>
      _sum += o._sum
      _count += o._count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = if (_count != 0) {
    1.0 * _sum / _count
  } else 0;
}

class GpuTaskMetrics extends Serializable with Logging {
  private val semaphoreHoldingTime = new NanoSecondAccumulator
  private val semWaitTimeNs = new NanoSecondAccumulator
  private val retryCount = new LongAccumulator
  private val splitAndRetryCount = new LongAccumulator
  private val retryBlockTime = new NanoSecondAccumulator
  private val retryComputationTime = new NanoSecondAccumulator
  // onGpuTask means a task that has data in GPU memory.
  // Since it's not easy to decided if a task has data in GPU memory,
  // We only count the tasks that had held semaphore before,
  // so it's very likely to have data in GPU memory
  private val onGpuTasksInWaitingQueueAvgCount = new AvgLongAccumulator
  private val onGpuTasksInWaitingQueueMaxCount = new MaxLongAccumulator

  // This is used to track the max parallelism of multithreaded readers
  private val multithreadReaderMaxParallelism = new MaxLongAccumulator

  // This is used to track the maximum concurrent tasks in PrioritySemaphore
  private val maxConcurrentGpuTasks = new MaxLongAccumulator

  // Spill
  private val spillToHostTimeNs = new NanoSecondAccumulator
  private val spillToDiskTimeNs = new NanoSecondAccumulator
  private val readSpillFromHostTimeNs = new NanoSecondAccumulator
  private val readSpillFromDiskTimeNs = new NanoSecondAccumulator
  private val spillToHostBytes = new SizeInBytesAccumulator
  private val spillToDiskBytes = new SizeInBytesAccumulator

  private val maxDeviceMemoryBytes = new HighWatermarkAccumulator
  private val maxHostMemoryBytes = new HighWatermarkAccumulator
  private val maxPageableMemoryBytes = new HighWatermarkAccumulator
  private val maxPinnedMemoryBytes = new HighWatermarkAccumulator
  private val maxDiskMemoryBytes = new HighWatermarkAccumulator

  private val maxGpuFootprint = new SizeInBytesAccumulator

  // Disk write savings from SpillablePartialFileHandle
  private val diskWriteSavedBytes = new LongAccumulator

  // PerfIO S3 backend executor counts — each executor contributes 1 to its active backend
  // per stage, making it easy to see from the event log whether PerfIO is enabled on all nodes.
  private val perfioS3NettyExecutors = new LongAccumulator
  private val perfioS3CrtExecutors = new LongAccumulator
  private val perfioS3S3aExecutors = new LongAccumulator
  private val perfioS3IcebergFallbacks = new LongAccumulator

  // GCS PerfIO executor counts split by the connector-repackaged SDK transport.
  private val perfioGcsHttpExecutors = new LongAccumulator
  private val perfioGcsGrpcExecutors = new LongAccumulator

  private var maxHostBytesAllocated: Long = 0
  private var maxPageableBytesAllocated: Long = 0
  private var maxPinnedBytesAllocated: Long = 0

  private var maxDiskBytesAllocated: Long = 0

  def getDiskBytesAllocated: Long = GpuTaskMetrics.diskBytesAllocated.get()

  def getMaxDiskBytesAllocated: Long = maxDiskBytesAllocated

  def getHostBytesAllocated: Long = GpuTaskMetrics.hostBytesAllocated.get()

  def getMaxHostBytesAllocated: Long = maxHostBytesAllocated

  def incHostBytesAllocated(bytes: Long, isPinned: Boolean): Unit = {
    GpuTaskMetrics.incHostBytesAllocated(bytes, isPinned)
    maxHostBytesAllocated = maxHostBytesAllocated.max(GpuTaskMetrics.hostBytesAllocated.get())
    if (isPinned) {
      maxPinnedBytesAllocated =
        maxPinnedBytesAllocated.max(GpuTaskMetrics.pinnedBytesAllocated.get())
    } else {
      maxPageableBytesAllocated = maxPageableBytesAllocated.max(
        GpuTaskMetrics.pageableBytesAllocated.get())
    }
  }

  def decHostBytesAllocated(bytes: Long, isPinned: Boolean): Unit = {
    GpuTaskMetrics.decHostBytesAllocated(bytes, isPinned)
  }

  def incDiskBytesAllocated(bytes: Long): Unit = {
    GpuTaskMetrics.incDiskBytesAllocated(bytes)
    maxDiskBytesAllocated = maxDiskBytesAllocated.max(GpuTaskMetrics.diskBytesAllocated.get())
  }

  def decDiskBytesAllocated(bytes: Long): Unit = {
    GpuTaskMetrics.decDiskBytesAllocated(bytes)
  }

  private val metrics = Map[String, AccumulatorV2[_, _]](
    "gpuTime" -> semaphoreHoldingTime,
    "gpuSemaphoreWait" -> semWaitTimeNs,
    "gpuRetryCount" -> retryCount,
    "gpuSplitAndRetryCount" -> splitAndRetryCount,
    "gpuRetryBlockTime" -> retryBlockTime,
    "gpuRetryComputationTime" -> retryComputationTime,
    "gpuSpillToHostTime" -> spillToHostTimeNs,
    "gpuSpillToDiskTime" -> spillToDiskTimeNs,
    "gpuReadSpillFromHostTime" -> readSpillFromHostTimeNs,
    "gpuReadSpillFromDiskTime" -> readSpillFromDiskTimeNs,
    "gpuSpillToHostBytes" -> spillToHostBytes,
    "gpuSpillToDiskBytes" -> spillToDiskBytes,
    "gpuMaxDeviceMemoryBytes" -> maxDeviceMemoryBytes,
    "gpuMaxHostMemoryBytes" -> maxHostMemoryBytes,
    "gpuMaxDiskMemoryBytes" -> maxDiskMemoryBytes,
    "gpuMaxPageableMemoryBytes" -> maxPageableMemoryBytes,
    "gpuMaxPinnedMemoryBytes" -> maxPinnedMemoryBytes,
    "gpuOnGpuTasksWaitingGPUAvgCount" -> onGpuTasksInWaitingQueueAvgCount,
    "gpuOnGpuTasksWaitingGPUMaxCount" -> onGpuTasksInWaitingQueueMaxCount,
    "gpuMaxTaskFootprint" -> maxGpuFootprint,
    "multithreadReaderMaxParallelism" -> multithreadReaderMaxParallelism,
    "gpuMaxConcurrentGpuTasks" -> maxConcurrentGpuTasks,
    "gpuDiskWriteSavedBytes" -> diskWriteSavedBytes,
    "perfio.s3.netty.executors" -> perfioS3NettyExecutors,
    "perfio.s3.crt.executors" -> perfioS3CrtExecutors,
    "perfio.s3.s3a.executors" -> perfioS3S3aExecutors,
    "perfio.s3.iceberg.fallbacks" -> perfioS3IcebergFallbacks,
    "perfio.gcs.http.executors" -> perfioGcsHttpExecutors,
    "perfio.gcs.grpc.executors" -> perfioGcsGrpcExecutors
  )

  def register(sc: SparkContext): Unit = {
    metrics.foreach { case (k, m) =>
      // This is not a public API, but the only way to get failed task
      // If we run into problems we can use use sc.register(m, k), but
      // it would not allow us to collect metrics for failed tasks.
      m.register(sc, Some(k), true)
    }
  }

  def makeSureRegistered(): Unit = {
    // This is a noop for now, but need to make sure something happens
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    // Now we need to make sure that we are registered with the proper task
    GpuTaskMetrics.registerOnTask(this)
  }

  private def timeIt[A](timer: NanoSecondAccumulator,
      range: String,
      color: NvtxColor,
      f: => A): A = {
    val start = System.nanoTime()
    withResource(new NvtxRange(range, color)) { _ =>
      try {
        f
      } finally {
        timer.add(System.nanoTime() - start)
      }
    }
  }

  private def timeIt[A](timer: NanoSecondAccumulator,
                        range: NvtxId,
                        f: => A): A = {
    val start = System.nanoTime()
    range {
      try {
        f
      } finally {
        timer.add(System.nanoTime() - start)
      }
    }
  }

  def getSemaphoreHoldingTime: Long = semaphoreHoldingTime.value.value

  def addSemaphoreHoldingTime(duration: Long): Unit = semaphoreHoldingTime.add(duration)

  def getSemWaitTime(): Long = semWaitTimeNs.value.value

  def semWaitTime[A](f: => A): A = timeIt(semWaitTimeNs, NvtxRegistry.ACQUIRE_GPU, f)

  def spillToHostTime[A](f: => A): A = {
    timeIt(spillToHostTimeNs, "spillToHostTime", NvtxColor.RED, f)
  }

  def spillToDiskTime[A](f: => A): A = {
    timeIt(spillToDiskTimeNs, "spillToDiskTime", NvtxColor.RED, f)
  }

  def readSpillFromHostTime[A](f: => A): A = {
    timeIt(readSpillFromHostTimeNs, "readSpillFromHostTime", NvtxColor.ORANGE, f)
  }

  def readSpillFromDiskTime[A](f: => A): A = {
    timeIt(readSpillFromDiskTimeNs, "readSpillFromDiskTime", NvtxColor.ORANGE, f)
  }

  def recordSpillToHost(sizeInBytes: Long): Unit = {
    spillToHostBytes.add(sizeInBytes)
  }

  def recordSpillToDisk(sizeInBytes: Long): Unit = {
    spillToDiskBytes.add(sizeInBytes)
  }

  def updateRetry(taskAttemptId: Long): Unit = {
    val rc = RmmSpark.getAndResetNumRetryThrow(taskAttemptId)
    if (rc > 0) {
      retryCount.add(rc)
    }

    val src = RmmSpark.getAndResetNumSplitRetryThrow(taskAttemptId)
    if (src > 0) {
      splitAndRetryCount.add(src)
    }

    val timeNs = RmmSpark.getAndResetBlockTimeNs(taskAttemptId)
    if (timeNs > 0) {
      retryBlockTime.add(timeNs)
    }

    val compNs = RmmSpark.getAndResetComputeTimeLostToRetryNs(taskAttemptId)
    if (compNs > 0) {
      retryComputationTime.add(compNs)
    }
  }

  def updateMaxMemory(taskAttemptId: Long): Unit = {
    val maxMem = RmmSpark.getAndResetGpuMaxMemoryAllocated(taskAttemptId)
    if (maxMem > 0) {
      // These metrics track the max amount of memory that is allocated on the gpu and disk,
      // respectively, during the lifespan of a task. However, this update function only gets called
      // once on task completion, whereas the actual logic tracking of the max value during memory
      // allocations lives in the JNI. Therefore, we can stick the convention here of calling the
      // add method instead of adding a dedicated max method to the accumulator.
      if (maxDeviceMemoryBytes.value.value > 0) {
        logError(s"updateMaxMemory called twice for task $taskAttemptId with maxMem $maxMem")
      }
      maxDeviceMemoryBytes.add(maxMem)
    }
    if (maxHostBytesAllocated > 0) {
      maxHostMemoryBytes.add(maxHostBytesAllocated)
    }
    if (maxPageableBytesAllocated > 0) {
      maxPageableMemoryBytes.add(maxPageableBytesAllocated)
    }
    if (maxPinnedBytesAllocated > 0) {
      maxPinnedMemoryBytes.add(maxPinnedBytesAllocated)
    }
    if (maxDiskBytesAllocated > 0) {
      maxDiskMemoryBytes.add(maxDiskBytesAllocated)
    }
  }

  def updateFootprint(taskAttemptId: Long): Unit = {
    val maxFootprint = RmmSpark.getMaxGpuTaskMemory(taskAttemptId)
    if (maxFootprint > 0) {
      maxGpuFootprint.setValue(maxFootprint)
    }
  }

  def recordOnGpuTasksWaitingNumber(num: Int): Unit = {
    onGpuTasksInWaitingQueueAvgCount.add(num)
    onGpuTasksInWaitingQueueMaxCount.add(num)
  }

  def recordConcurrentGpuTasks(currentConcurrentTasks: Long): Unit = {
    maxConcurrentGpuTasks.add(currentConcurrentTasks)
  }

  def updateMultithreadReaderMaxParallelism(parallelism: Long): Unit = {
    multithreadReaderMaxParallelism.add(parallelism)
  }

  def addDiskWriteSaved(bytes: Long): Unit = {
    diskWriteSavedBytes.add(bytes)
  }

  /**
   * Records this executor's PerfIO S3 backend exactly once per stage (per GpuTaskMetrics
   * instance). Call from task code on any S3 read path. Uses the accumulator ID as a key
   * to prevent double-counting — each new stage creates fresh accumulators with new IDs.
   */
  def recordPerfioS3BackendOnce(): Unit = {
    val acc = PerfIO.s3BackendName match {
      case "netty" => perfioS3NettyExecutors
      case "crt"   => perfioS3CrtExecutors
      case _       => perfioS3S3aExecutors
    }
    try {
      if (PerfIO.reportedBackendAccIds.add(acc.id)) {
        acc.add(1L)
      }
    } catch {
      case _: IllegalArgumentException => // accumulator not yet registered; no-op
    }
  }

  /** Records this executor after a GCS PerfIO read succeeds, once per stage. */
  def recordPerfioGCSBackendOnce(): Unit = {
    val acc = PerfIO.gcsBackendName match {
      case "grpc" => perfioGcsGrpcExecutors
      case _      => perfioGcsHttpExecutors
    }
    try {
      if (PerfIO.reportedBackendAccIds.add(acc.id)) {
        acc.add(1L)
      }
    } catch {
      case _: IllegalArgumentException => // accumulator not yet registered; no-op
    }
  }

  def recordPerfioS3IcebergFallback(): Unit = {
    perfioS3IcebergFallbacks.add(1L)
  }
}

/**
 * Provides task level metrics
 */
object GpuTaskMetrics extends Logging {
  private val taskLevelMetrics = new ConcurrentHashMap[Long, GpuTaskMetrics]()

  private val hostBytesAllocated = new AtomicLong(0)
  private val pageableBytesAllocated = new AtomicLong(0)
  private val pinnedBytesAllocated = new AtomicLong(0)
  private val diskBytesAllocated = new AtomicLong(0)

  private def incHostBytesAllocated(bytes: Long, isPinned: Boolean): Unit = {
    hostBytesAllocated.addAndGet(bytes)
    if (isPinned) {
      pinnedBytesAllocated.addAndGet(bytes)
    } else {
      pageableBytesAllocated.addAndGet(bytes)
    }
  }

  private def decHostBytesAllocated(bytes: Long, isPinned: Boolean): Unit = {
    hostBytesAllocated.addAndGet(-bytes)
    if (isPinned) {
      pinnedBytesAllocated.addAndGet(-bytes)
    } else {
      pageableBytesAllocated.addAndGet(-bytes)
    }
  }

  def incDiskBytesAllocated(bytes: Long): Unit = {
    diskBytesAllocated.addAndGet(bytes)
  }

  def decDiskBytesAllocated(bytes: Long): Unit = {
    diskBytesAllocated.addAndGet(-bytes)
  }

  def registerOnTask(metrics: GpuTaskMetrics): Unit = {
    val tc = TaskContext.get()
    if (tc != null) {
      val id = tc.taskAttemptId()
      // avoid double registering the task metrics...
      if (taskLevelMetrics.putIfAbsent(id, metrics) == null) {
        onTaskCompletion(tc, tc =>
          taskLevelMetrics.remove(tc.taskAttemptId())
        )
      }
    }
  }

  def get: GpuTaskMetrics = {
    val tc = TaskContext.get()
    val metrics = if (tc != null) {
      Option(taskLevelMetrics.get(tc.taskAttemptId()))
    } else {
      None
    }
    // As a backstop better to not have metrics than to crash...
    // Spark does this too for regular task metrics
    metrics.getOrElse(new GpuTaskMetrics)
  }
}
