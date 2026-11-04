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

package com.nvidia.spark.rapids

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.collection.mutable

import ai.rapids.cudf.{DefaultHostMemoryAllocator, HostMemoryAllocator, HostMemoryBuffer, MemoryBuffer, PageableMemoryPool, PinnedMemoryPool}
import com.nvidia.spark.rapids.HostAlloc.bookkeepHostMemoryFree
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{BOOKKEEP_MEMORY, BOOKKEEP_MEMORY_CALLSTACK}
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuTaskMetrics

case class HostAllocResult(buffer: HostMemoryBuffer, isPinned: Boolean)

private class HostAlloc(
    nonPinnedLimit: Long,
    replacePinnedHostAllocWithPageable: Boolean) extends HostMemoryAllocator with Logging {
  private var currentNonPinnedAllocated: Long = 0L
  private val isUnlimited = nonPinnedLimit < 0
  private val isPinnedOnly = nonPinnedLimit == 0
  private val debugSummaryEvery = java.lang.Long.getLong(
    "ai.rapids.cudf.hostalloc.debug.summary.every", 0L)
  private val allocationDecisions = new AtomicLong()
  private val pinnedAttempts = new AtomicLong()
  private val pinnedSuccess = new AtomicLong()
  private val pinnedMiss = new AtomicLong()
  private val pinnedClose = new AtomicLong()
  private val pinnedLiveBytes = new AtomicLong()
  private val pinnedMaxLiveBytes = new AtomicLong()
  private val pageableAttempts = new AtomicLong()
  private val pageableSuccess = new AtomicLong()
  private val pageableMiss = new AtomicLong()
  private val pageableClose = new AtomicLong()
  private val pageableLiveBytes = new AtomicLong()
  private val pageableMaxLiveBytes = new AtomicLong()
  private val rawAttempts = new AtomicLong()
  private val rawSuccess = new AtomicLong()
  private val rawLimitFailure = new AtomicLong()
  private val preferTrueAttempts = new AtomicLong()
  private val preferTrueRequestBytes = new AtomicLong()
  private val preferTrueMaxRequestBytes = new AtomicLong()
  private val preferFalseAttempts = new AtomicLong()
  private val preferFalseRequestBytes = new AtomicLong()
  private val preferFalseMaxRequestBytes = new AtomicLong()
  private val pinnedBytesPreferTrue = new AtomicLong()
  private val pinnedBytesPreferFalse = new AtomicLong()
  private val pageableBytesPreferTrue = new AtomicLong()
  private val pageableBytesPreferFalse = new AtomicLong()
  private val rawBytesPreferTrue = new AtomicLong()
  private val rawBytesPreferFalse = new AtomicLong()
  private val rawLivePreferTrue = new AtomicLong()
  private val rawLivePreferFalse = new AtomicLong()
  private val rawMaxLivePreferTrue = new AtomicLong()
  private val rawMaxLivePreferFalse = new AtomicLong()
  private val spillAttemptsPreferTrue = new AtomicLong()
  private val spillAttemptsPreferFalse = new AtomicLong()
  private val spillRequestedBytesPreferTrue = new AtomicLong()
  private val spillRequestedBytesPreferFalse = new AtomicLong()
  private val spillActualBytesPreferTrue = new AtomicLong()
  private val spillActualBytesPreferFalse = new AtomicLong()
  private val spillZeroResultsPreferTrue = new AtomicLong()
  private val spillZeroResultsPreferFalse = new AtomicLong()

  private def updateMax(target: AtomicLong, value: Long): Unit = {
    var previous = target.get()
    while (value > previous && !target.compareAndSet(previous, value)) {
      previous = target.get()
    }
  }

  private def recordRawSuccess(amount: Long, preferPinned: Boolean): Unit = {
    val (bytes, live, maxLive) = if (preferPinned) {
      (rawBytesPreferTrue, rawLivePreferTrue, rawMaxLivePreferTrue)
    } else {
      (rawBytesPreferFalse, rawLivePreferFalse, rawMaxLivePreferFalse)
    }
    bytes.addAndGet(amount)
    updateMax(maxLive, live.addAndGet(amount))
  }

  private def maybeLogDebugSummary(): Unit = {
    val decisions = allocationDecisions.incrementAndGet()
    if (debugSummaryEvery > 0 && (decisions == 1 || decisions % debugSummaryEvery == 0)) {
      val rawLive = synchronized { currentNonPinnedAllocated }
      logWarning(s"HostAlloc debug summary: decisions=$decisions " +
        s"modeReplacePinned=$replacePinnedHostAllocWithPageable " +
        s"totalLimit=$getTotalLimit nonPinnedLimit=$nonPinnedLimit rawLive=$rawLive " +
        s"pinnedPoolBytes=${PinnedMemoryPool.getTotalPoolSizeBytes} " +
        s"pinnedAttempts=${pinnedAttempts.get} pinnedSuccess=${pinnedSuccess.get} " +
        s"pinnedMiss=${pinnedMiss.get} pinnedClose=${pinnedClose.get} " +
        s"pinnedLive=${pinnedLiveBytes.get} pinnedMaxLive=${pinnedMaxLiveBytes.get} " +
        s"pageablePoolBytes=${PageableMemoryPool.getTotalPoolSizeBytes} " +
        s"pageableAttempts=${pageableAttempts.get} pageableSuccess=${pageableSuccess.get} " +
        s"pageableMiss=${pageableMiss.get} pageableClose=${pageableClose.get} " +
        s"pageableLive=${pageableLiveBytes.get} pageableMaxLive=${pageableMaxLiveBytes.get} " +
        s"rawAttempts=${rawAttempts.get} rawSuccess=${rawSuccess.get} " +
        s"rawLimitFailure=${rawLimitFailure.get} " +
        s"preferTrueAttempts=${preferTrueAttempts.get} " +
        s"preferTrueRequestBytes=${preferTrueRequestBytes.get} " +
        s"preferTrueMaxRequestBytes=${preferTrueMaxRequestBytes.get} " +
        s"preferFalseAttempts=${preferFalseAttempts.get} " +
        s"preferFalseRequestBytes=${preferFalseRequestBytes.get} " +
        s"preferFalseMaxRequestBytes=${preferFalseMaxRequestBytes.get} " +
        s"pinnedBytesPreferTrue=${pinnedBytesPreferTrue.get} " +
        s"pinnedBytesPreferFalse=${pinnedBytesPreferFalse.get} " +
        s"pageableBytesPreferTrue=${pageableBytesPreferTrue.get} " +
        s"pageableBytesPreferFalse=${pageableBytesPreferFalse.get} " +
        s"rawBytesPreferTrue=${rawBytesPreferTrue.get} " +
        s"rawBytesPreferFalse=${rawBytesPreferFalse.get} " +
        s"rawLivePreferTrue=${rawLivePreferTrue.get} " +
        s"rawLivePreferFalse=${rawLivePreferFalse.get} " +
        s"rawMaxLivePreferTrue=${rawMaxLivePreferTrue.get} " +
        s"rawMaxLivePreferFalse=${rawMaxLivePreferFalse.get} " +
        s"spillAttemptsPreferTrue=${spillAttemptsPreferTrue.get} " +
        s"spillAttemptsPreferFalse=${spillAttemptsPreferFalse.get} " +
        s"spillRequestedBytesPreferTrue=${spillRequestedBytesPreferTrue.get} " +
        s"spillRequestedBytesPreferFalse=${spillRequestedBytesPreferFalse.get} " +
        s"spillActualBytesPreferTrue=${spillActualBytesPreferTrue.get} " +
        s"spillActualBytesPreferFalse=${spillActualBytesPreferFalse.get} " +
        s"spillZeroResultsPreferTrue=${spillZeroResultsPreferTrue.get} " +
        s"spillZeroResultsPreferFalse=${spillZeroResultsPreferFalse.get}")
    }
  }

  private class OnPinnedClose(ptr: Long, amount: Long) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        pinnedClose.incrementAndGet()
        pinnedLiveBytes.addAndGet(-amount)
        val metrics = GpuTaskMetrics.get
        metrics.decHostBytesAllocated(amount, isPinned = true)
        logTrace(getHostAllocMetricsLogStr(metrics))
        RmmSpark.cpuDeallocate(ptr, amount)
        if (BOOKKEEP_MEMORY) {
          bookkeepHostMemoryFree(ptr, amount)
        }
      }
    }
  }

  // Expose for usage ratio calculation
  def getCurrentAllocated: Long = synchronized {
    currentNonPinnedAllocated
  }

  def getTotalLimit: Long = {
    if (isUnlimited) Long.MaxValue
    else PinnedMemoryPool.getTotalPoolSizeBytes +
      PageableMemoryPool.getTotalPoolSizeBytes + nonPinnedLimit
  }

  private def getPreferredPoolSize: Long = {
    if (replacePinnedHostAllocWithPageable) {
      PageableMemoryPool.getTotalPoolSizeBytes
    } else {
      PinnedMemoryPool.getTotalPoolSizeBytes
    }
  }

  /**
   * A callback class so we know when a non-pinned host buffer was released
   */
  private class OnCloseCallback(
      ptr: Long,
      amount: Long,
      countsTowardNonPinnedLimit: Boolean,
      preferPinned: Boolean) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        if (!countsTowardNonPinnedLimit) {
          pageableClose.incrementAndGet()
          pageableLiveBytes.addAndGet(-amount)
        } else if (preferPinned) {
          rawLivePreferTrue.addAndGet(-amount)
        } else {
          rawLivePreferFalse.addAndGet(-amount)
        }
        releaseNonPinned(ptr, amount, countsTowardNonPinnedLimit)
        if (BOOKKEEP_MEMORY) {
          bookkeepHostMemoryFree(ptr, amount)
        }
      }
    }
  }

  private def getHostAllocMetricsLogStr(metrics: GpuTaskMetrics): String = {
    Option(TaskContext.get()).map { context =>
      val taskId = context.taskAttemptId()
      val totalSize = metrics.getHostBytesAllocated
      val maxSize = metrics.getMaxHostBytesAllocated
      s"total size for task $taskId is $totalSize, max size is $maxSize"
    }.getOrElse("allocated memory outside of a task context")
  }

  private def releaseNonPinned(
      ptr: Long,
      amount: Long,
      countsTowardNonPinnedLimit: Boolean): Unit = {
    if (countsTowardNonPinnedLimit) {
      synchronized {
        currentNonPinnedAllocated -= amount
      }
    }
    val metrics = GpuTaskMetrics.get
    metrics.decHostBytesAllocated(amount, isPinned = false)
    logTrace(getHostAllocMetricsLogStr(metrics))
    RmmSpark.cpuDeallocate(ptr, amount)
  }

  private def tryAllocPinned(amount: Long, preferPinned: Boolean): Option[HostMemoryBuffer] = {
    pinnedAttempts.incrementAndGet()
    val pinned = Option(PinnedMemoryPool.tryAllocate(amount))
    pinned match {
      case Some(buffer) =>
        pinnedSuccess.incrementAndGet()
        (if (preferPinned) pinnedBytesPreferTrue else pinnedBytesPreferFalse).addAndGet(amount)
        updateMax(pinnedMaxLiveBytes, pinnedLiveBytes.addAndGet(amount))
        HostAlloc.addEventHandler(buffer, new OnPinnedClose(buffer.getAddress, amount))
      case None => pinnedMiss.incrementAndGet()
    }
    pinned
  }

  private def tryAllocPageablePool(amount: Long, preferPinned: Boolean,
      stageDeviceToHost: Boolean): Option[HostMemoryBuffer] = {
    pageableAttempts.incrementAndGet()
    val pageable = Option(if (stageDeviceToHost) {
      PageableMemoryPool.tryAllocateForDeviceToHost(amount)
    } else {
      PageableMemoryPool.tryAllocate(amount)
    })
    pageable match {
      case Some(buffer) =>
        pageableSuccess.incrementAndGet()
        (if (preferPinned) pageableBytesPreferTrue else pageableBytesPreferFalse).addAndGet(amount)
        updateMax(pageableMaxLiveBytes, pageableLiveBytes.addAndGet(amount))
        // The pageable pool's full resident size is reserved from the off-heap limit at startup.
        // Its sub-allocations participate in retry/metrics callbacks but are not charged again.
        HostAlloc.addEventHandler(buffer,
          new OnCloseCallback(buffer.getAddress, amount,
            countsTowardNonPinnedLimit = false, preferPinned))
      case None => pageableMiss.incrementAndGet()
    }
    pageable
  }

  private def tryAllocNonPinned(amount: Long, preferPinned: Boolean,
      stageDeviceToHost: Boolean = false):
      Option[HostMemoryBuffer] = {
    // The pool is reserved from the non-pinned budget at startup. Make that reservation
    // available to all non-pinned allocations before consuming the remaining raw budget.
    val fromPool = tryAllocPageablePool(amount, preferPinned, stageDeviceToHost)
    def allocateRaw(): HostMemoryBuffer = {
      val buffer = try {
        if (stageDeviceToHost) PageableMemoryPool.allocateRawForDeviceToHost(amount)
        else HostMemoryBuffer.allocateRaw(amount)
      } catch {
        case t: Throwable =>
          synchronized { currentNonPinnedAllocated -= amount }
          throw t
      }
      rawSuccess.incrementAndGet()
      recordRawSuccess(amount, preferPinned)
      buffer
    }
    if (fromPool.isEmpty) {
      rawAttempts.incrementAndGet()
    }
    val (ret, countsTowardNonPinnedLimit): (Option[HostMemoryBuffer], Boolean) =
      if (fromPool.isDefined) {
        // The pageable pool's full resident size is reserved from the off-heap limit at startup.
        // Charging sub-allocations here would count the same memory twice.
        (fromPool, false)
      } else if (isUnlimited) {
        synchronized {
          currentNonPinnedAllocated += amount
        }
        (Some(allocateRaw()), true)
      } else {
        synchronized {
          if ((currentNonPinnedAllocated + amount) <= nonPinnedLimit) {
            currentNonPinnedAllocated += amount
            (Some(allocateRaw()), true)
          } else {
            rawLimitFailure.incrementAndGet()
            (None, false)
          }
        }
      }
    if (countsTowardNonPinnedLimit) {
      ret.foreach { b =>
        HostAlloc.addEventHandler(b,
          new OnCloseCallback(b.getAddress, amount,
            countsTowardNonPinnedLimit = true, preferPinned))
      }
    }
    ret
  }

  private def canNeverSucceed(amount: Long, preferPinned: Boolean): Boolean = synchronized {
    val preferredPoolFailed = (isPinnedOnly || preferPinned) && (amount > getPreferredPoolSize)
    val nonPinnedFailed = isPinnedOnly || (amount > nonPinnedLimit)
    val pageablePoolFailed = amount > PageableMemoryPool.getTotalPoolSizeBytes
    !isUnlimited && preferredPoolFailed && nonPinnedFailed && pageablePoolFailed
  }

  private def checkSize(amount: Long, preferPinned: Boolean): Unit = synchronized {
    if (canNeverSucceed(amount, preferPinned)) {
      throw new IllegalArgumentException(s"The amount requested $amount is larger than the " +
          s"maximum pool size $getTotalLimit")
    }
  }

  private def spillAndCheckRetry(
      allocSize: Long,
      retryCount: Long,
      preferPinned: Boolean): Boolean = {
    // check arguments for good measure
    require(allocSize >= 0,
      s"spillAndCheckRetry invoked with invalid allocSize $allocSize")

    require(retryCount >= 0,
      s"spillAndCheckRetry invoked with invalid retryCount $retryCount")

    val store = SpillFramework.stores.hostStore
    val totalSize: Long = synchronized {
      currentNonPinnedAllocated
    }

    val attemptMsg = if (retryCount > 0) {
      s"Attempt $retryCount"
    } else {
      "First attempt"
    }

    val (attempts, requested, actual, zeroResults) = if (preferPinned) {
      (spillAttemptsPreferTrue, spillRequestedBytesPreferTrue,
        spillActualBytesPreferTrue, spillZeroResultsPreferTrue)
    } else {
      (spillAttemptsPreferFalse, spillRequestedBytesPreferFalse,
        spillActualBytesPreferFalse, spillZeroResultsPreferFalse)
    }
    attempts.incrementAndGet()
    requested.addAndGet(allocSize)
    val amountSpilled = store.spill(allocSize)
    actual.addAndGet(amountSpilled)

    if (amountSpilled == 0) {
      zeroResults.incrementAndGet()
      val shouldRetry = store.numHandles > 0
      val exhaustedMsg = s"Host store exhausted, unable to allocate $allocSize bytes. " +
          s"Total host allocated is $totalSize bytes. $attemptMsg."
      if (!shouldRetry) {
        logWarning(exhaustedMsg)
      } else {
        logWarning(s"$exhaustedMsg Attempting a retry.")
      }
      shouldRetry
    } else {
      logInfo(s"Spilled $amountSpilled bytes from the host store")
      true
    }
  }

  private def tryAllocInternal(amount: Long,
      preferPinned: Boolean,
      blocking: Boolean,
      pageableSpillOnly: Boolean = false): (Option[HostMemoryBuffer], Boolean) = {
    if (preferPinned) {
      preferTrueAttempts.incrementAndGet()
      preferTrueRequestBytes.addAndGet(amount)
      updateMax(preferTrueMaxRequestBytes, amount)
    } else {
      preferFalseAttempts.incrementAndGet()
      preferFalseRequestBytes.addAndGet(amount)
      updateMax(preferFalseMaxRequestBytes, amount)
    }
    var retryCount = 0L
    var ret = Option.empty[HostAllocResult]
    var shouldRetry = false
    var shouldRetryInternal = true
    val isRecursive = RmmSpark.preCpuAlloc(amount, blocking)
    var allocAttemptFinishedWithoutException = false
    try {
      do {
        ret = if (pageableSpillOnly) {
          tryAllocNonPinned(amount, preferPinned, stageDeviceToHost = true)
            .map(HostAllocResult(_, isPinned = false))
        } else if (preferPinned) {
          if (replacePinnedHostAllocWithPageable) {
            val result = tryAllocNonPinned(amount, preferPinned = preferPinned)
              .map(HostAllocResult(_, isPinned = false))
            result
          } else {
            val pinned = tryAllocPinned(amount, preferPinned)
            if (pinned.isDefined) {
              pinned.map(HostAllocResult(_, isPinned = true))
            } else {
              val result = tryAllocNonPinned(amount, preferPinned = preferPinned)
                .map(HostAllocResult(_, isPinned = false))
              result
            }
          }
        } else {
          val nonPinned = tryAllocNonPinned(amount, preferPinned = preferPinned)
            .map(HostAllocResult(_, isPinned = false))
          nonPinned.orElse {
            if (replacePinnedHostAllocWithPageable) {
              None
            } else {
              tryAllocPinned(amount, preferPinned)
                .map(HostAllocResult(_, isPinned = true))
            }
          }
        }
        if (ret.isEmpty) {
          // We could not make it work so try and spill enough to make it work
          // Experiment diagnostics only: reuse the existing opt-in debug switch.
          // Pool state is a nearby snapshot; concurrent allocations may change it.
          val diagnose = debugSummaryEvery > 0
          val started = if (diagnose) System.nanoTime() else 0L
          if (diagnose) {
            val rawLive = synchronized { currentNonPinnedAllocated }
            val context = Option(TaskContext.get())
            logWarning(s"HostAlloc spill begin: requestBytes=$amount retry=$retryCount " +
              s"pageableOnly=$pageableSpillOnly preferPinned=$preferPinned " +
              s"stage=${context.map(_.stageId()).getOrElse(-1)} " +
              s"task=${context.map(_.taskAttemptId()).getOrElse(-1L)} " +
              s"rawLive=$rawLive rawLimit=$nonPinnedLimit " +
              s"pinnedLive=${pinnedLiveBytes.get} " +
              s"pinnedPoolBytes=${PinnedMemoryPool.getTotalPoolSizeBytes}")
            PageableMemoryPool.logPoolState()
          }
          shouldRetryInternal = spillAndCheckRetry(amount, retryCount, preferPinned)
          if (diagnose) {
            logWarning(s"HostAlloc spill end: requestBytes=$amount retry=$retryCount " +
              s"pageableOnly=$pageableSpillOnly shouldRetry=$shouldRetryInternal " +
              s"elapsedNs=${System.nanoTime() - started}")
          }
          if (shouldRetryInternal) {
            retryCount += 1
          }
        }
      } while(ret.isEmpty && shouldRetryInternal && retryCount < 10)
      allocAttemptFinishedWithoutException = true
    } finally {
      ret match {
        case Some(HostAllocResult(buffer: HostMemoryBuffer, isPinned: Boolean)) =>
          val metrics = GpuTaskMetrics.get
          metrics.incHostBytesAllocated(amount, isPinned)
          if (BOOKKEEP_MEMORY) {
            HostAlloc.bookkeepHostMemoryAlloc(buffer.getAddress, amount)
          }
          // Check retry coverage for host memory allocation
          AllocationRetryCoverageTracker.checkHostAllocation()
          logTrace(getHostAllocMetricsLogStr(metrics))
          RmmSpark.postCpuAllocSuccess(buffer.getAddress, amount, blocking, isRecursive)
        case None =>
          // shouldRetry should indicate if spill did anything for us and we should try again.
          shouldRetry = RmmSpark.postCpuAllocFailed(allocAttemptFinishedWithoutException,
            blocking, isRecursive)
      }
      maybeLogDebugSummary()
    }
    (ret.map(_.buffer), shouldRetry)
  }

  def tryAlloc(amount: Long, preferPinned: Boolean = true): Option[HostMemoryBuffer] = {
    if (canNeverSucceed(amount, preferPinned)) {
      return None
    }
    var shouldRetry = true
    var ret = Option.empty[HostMemoryBuffer]
    while (shouldRetry) {
      val (r, sr) = tryAllocInternal(amount, preferPinned, blocking = false)
      ret = r
      shouldRetry = sr
    }
    ret
  }

  def tryAllocPageableForDeviceToHost(amount: Long): Option[HostMemoryBuffer] = {
    if (!isUnlimited && amount > PageableMemoryPool.getTotalPoolSizeBytes &&
        amount > nonPinnedLimit) {
      return None
    }
    var shouldRetry = true
    var ret = Option.empty[HostMemoryBuffer]
    while (shouldRetry) {
      val (r, sr) = tryAllocInternal(amount, preferPinned = true,
        blocking = false, pageableSpillOnly = true)
      ret = r
      shouldRetry = sr
    }
    ret
  }

  def allocPageableForDeviceToHost(amount: Long): HostMemoryBuffer = {
    require(isUnlimited || amount <= PageableMemoryPool.getTotalPoolSizeBytes ||
      amount <= nonPinnedLimit,
      s"Device-to-host allocation $amount exceeds both pageable pool and raw limits")
    var ret = Option.empty[HostMemoryBuffer]
    var count = 0
    while (ret.isEmpty && count < 1000) {
      val (r, _) = tryAllocInternal(amount, preferPinned = true,
        blocking = true, pageableSpillOnly = true)
      ret = r
      count += 1
    }
    ret.getOrElse(throw new CpuRetryOOM(
      "Could not allocate pageable device-to-host buffer after 1000 retries"))
  }

  def alloc(amount: Long, preferPinned: Boolean = true): HostMemoryBuffer = {
    checkSize(amount, preferPinned)
    var ret = Option.empty[HostMemoryBuffer]
    var count = 0
    while (ret.isEmpty && count < 1000) {
      val (r, _) = tryAllocInternal(amount, preferPinned, blocking = true)
      ret = r
      count += 1
    }
    if (ret.isEmpty) {
      // This can happen if someone broke the rules and not all host memory is
      // spillable when doing an allocation, like if not all of the code has
      // been updated yet.
      throw new CpuRetryOOM("Could not complete allocation after 1000 retries")
    }
    ret.get
  }

  override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer =
    alloc(amount, preferPinned)

  override def allocate(amount: Long): HostMemoryBuffer =
    alloc(amount)
}

/**
 * A new API for host memory allocation. This can be used to limit the amount of host memory.
 */
object HostAlloc extends Logging {
  private var singleton: HostAlloc = new HostAlloc(-1, replacePinnedHostAllocWithPageable = false)

  val pageableDeviceToHostAllocator: HostMemoryAllocator = new HostMemoryAllocator {
    override def allocate(amount: Long): HostMemoryBuffer =
      allocPageableForDeviceToHost(amount)

    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer =
      allocPageableForDeviceToHost(amount)
  }

  private def getSingleton: HostAlloc = synchronized {
    singleton
  }

  def initialize(
      nonPinnedLimit: Long,
      replacePinnedHostAllocWithPageable: Boolean = false): Unit = synchronized {
    singleton = new HostAlloc(nonPinnedLimit, replacePinnedHostAllocWithPageable)
    DefaultHostMemoryAllocator.set(singleton)
  }

  def tryAlloc(amount: Long, preferPinned: Boolean = true): Option[HostMemoryBuffer] = {
    getSingleton.tryAlloc(amount, preferPinned)
  }

  def tryAllocPageableForDeviceToHost(amount: Long): Option[HostMemoryBuffer] = {
    getSingleton.tryAllocPageableForDeviceToHost(amount)
  }

  def allocPageableForDeviceToHost(amount: Long): HostMemoryBuffer = {
    getSingleton.allocPageableForDeviceToHost(amount)
  }

  def alloc(amount: Long, preferPinned: Boolean = true): HostMemoryBuffer = {
    getSingleton.alloc(amount, preferPinned)
  }

  /**
   * Get current host memory usage ratio (0.0 to 1.0).
   * Returns current allocated / limit.
   */
  def getUsageRatio(): Double = {
    val alloc = getSingleton
    val currentAllocated = alloc.getCurrentAllocated
    val totalLimit = alloc.getTotalLimit
    if (totalLimit == Long.MaxValue) {
      0.0  // Unlimited, consider as 0% used
    } else {
      currentAllocated.toDouble / totalLimit.toDouble
    }
  }

  /**
   * Check if host memory usage is below the given threshold (0.0 to 1.0).
   */
  def isUsageBelowThreshold(threshold: Double): Boolean = {
    getUsageRatio() < threshold
  }

  def addEventHandler(buff: HostMemoryBuffer,
                      handler: MemoryBuffer.EventHandler): HostMemoryBuffer = {
    buff.synchronized {
      val previous = Option(buff.getEventHandler)
      val handlerToSet = previous.map { p =>
        MultiEventHandler(p, handler)
      }.getOrElse {
        handler
      }
      buff.setEventHandler(handlerToSet)
      buff
    }
  }

  private def removeEventHandlerFrom(
      multiEventHandler: MultiEventHandler,
      handler: MemoryBuffer.EventHandler): MemoryBuffer.EventHandler = {
    if (multiEventHandler.a == handler) {
      multiEventHandler.b
    } else if (multiEventHandler.b == handler) {
      multiEventHandler.a
    } else multiEventHandler.a match {
      case oldA: MultiEventHandler =>
        // From how the MultiEventHandler is set up we know that b cannot be one
        val newA = removeEventHandlerFrom(oldA, handler)
        MultiEventHandler(newA, multiEventHandler.b)
      case _ =>
        multiEventHandler
    }
  }

  def removeEventHandler(buff: HostMemoryBuffer,
                         handler: MemoryBuffer.EventHandler): HostMemoryBuffer = {
    buff.synchronized {
      val newHandler = buff.getEventHandler match {
        case multi: MultiEventHandler =>
          removeEventHandlerFrom(multi, handler)
        case other =>
          if (other == handler) null else other
      }
      buff.setEventHandler(newHandler)
      buff
    }
  }

  private def findEventHandlerInternal[K](handler: MemoryBuffer.EventHandler,
    eh: PartialFunction[MemoryBuffer.EventHandler, K]): Option[K] = handler match {
    case multi: MultiEventHandler =>
      findEventHandlerInternal(multi.a, eh)
        .orElse(findEventHandlerInternal(multi.b, eh))
    case other =>
      eh.lift(other)
  }

  def findEventHandler[K](buff: HostMemoryBuffer)(
    eh: PartialFunction[MemoryBuffer.EventHandler, K]): Option[K] = {
    buff.synchronized {
      findEventHandlerInternal(buff.getEventHandler, eh)
    }
  }

  private case class MultiEventHandler(a: MemoryBuffer.EventHandler,
                                       b: MemoryBuffer.EventHandler)
    extends MemoryBuffer.EventHandler {
    override def onClosed(i: Int): Unit = {
      var t: Option[Throwable] = None
      try {
        a.onClosed(i)
      } catch {
        case e: Throwable =>
          t = Some(e)
      }
      try {
        b.onClosed(i)
      } catch {
        case e: Throwable =>
          t match {
            case Some(previousError) =>
              previousError.addSuppressed(e)
            case None =>
              t = Some(e)
          }
      }
      t.foreach { error =>
        throw error
      }
    }
  }


  /**
   * For bookkeeping host memory usage per thread
   */
  private trait PerThreadMemoryUsage {
    def add(addr: Long, amount: Long, callstack: String): Unit
    def remove(addr: Long, amount: Long): Unit
  }
  private class SimplePerThreadMemoryUsage extends PerThreadMemoryUsage {
    private val totalMem: LongAdder = new LongAdder()
    override def toString: String = s"${totalMem.sum()} bytes in total"
    override def add(addr: Long, amount: Long, callstack: String): Unit = {
      totalMem.add(amount)
    }
    override def remove(addr: Long, amount: Long): Unit = totalMem.add(-amount)
  }
  private case class MemoryUsageDetail(addr: Long, amount: Long, callStack: String) {
    override def toString: String = s"$amount bytes behind address $addr at $callStack"
  }

  private class PerThreadMemoryUsageInDetails extends PerThreadMemoryUsage {
    private val details: mutable.Map[Long, MemoryUsageDetail] = mutable.Map()
    override def toString: String =
      s"Total ${details.values.map(_.amount).sum} bytes from below callstacks:\n" +
        s"${details.values.mkString("\n")}"

    override def add(addr: Long, amount: Long, callstack: String): Unit =
      details.put(addr, MemoryUsageDetail(addr, amount, callstack))

    override def remove(addr: Long, amount: Long): Unit =
      details.remove(addr)
  }
  private val muPerThreads = new ConcurrentHashMap[Long, PerThreadMemoryUsage]()
  private val addr2threadId = new ConcurrentHashMap[Long, java.lang.Long]()

  private def bookkeepHostMemoryAlloc(addr: Long, amount: Long): Unit = {
    val threadId = Thread.currentThread().getId
    HostAlloc.addr2threadId.put(addr, threadId)
    if (BOOKKEEP_MEMORY_CALLSTACK) {
      val mu = muPerThreads.computeIfAbsent(threadId, _ => new PerThreadMemoryUsageInDetails)
      val callstack = Thread.currentThread().getStackTrace.mkString(" at ")
      mu.add(addr, amount, callstack)
    } else {
      val mu = muPerThreads.computeIfAbsent(threadId, _ => new SimplePerThreadMemoryUsage)
      mu.add(addr, amount, null)
    }
  }

  private def bookkeepHostMemoryFree(ptr: Long, amount: Long) = {
    val threadId = HostAlloc.addr2threadId.get(ptr)
    if (threadId != null) {
      val mu = HostAlloc.muPerThreads.get(threadId)
      if (mu != null) {
        mu.remove(ptr, amount)
      } else {
        logWarning(s"Could not find MemoryUsage for thread $threadId from address $ptr, " +
          s"bytes: $amount")
      }
      HostAlloc.addr2threadId.remove(ptr)
    } else {
      logWarning(s"Could not find thread id for address $ptr, bytes: $amount")
    }
  }

  def getHostAllocBookkeepSummary(): String = {
    val sb = new StringBuilder
    sb.append("<<Host Memory Bookkeeping>>\n")
    muPerThreads.forEach((threadId, mu) => {
      sb.append(s"Thread with ID $threadId memory usage: ${mu.toString}\n\n")
    })
    sb.toString()
  }
}
