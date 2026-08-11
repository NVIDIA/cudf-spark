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
import java.util.concurrent.atomic.LongAdder

import scala.collection.mutable

import ai.rapids.cudf.{DefaultHostMemoryAllocator, HostMemoryAllocator, HostMemoryBuffer, MemoryBuffer, PinnedMemoryPool}
import com.nvidia.spark.rapids.HostAlloc.bookkeepHostMemoryFree
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{BOOKKEEP_MEMORY, BOOKKEEP_MEMORY_CALLSTACK}
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuTaskMetrics

case class HostAllocResult(buffer: HostMemoryBuffer, isPinned: Boolean)

private class HostAlloc(nonPinnedLimit: Long) extends HostMemoryAllocator with Logging {
  private var currentNonPinnedAllocated: Long = 0L
  private val pinnedLimit: Long = PinnedMemoryPool.getTotalPoolSizeBytes
  // For now we are going to assume that we are the only ones calling into the pinned pool
  // That is not really true, but should be okay.
  private var currentPinnedAllocated: Long = 0L
  private val isUnlimited = nonPinnedLimit < 0
  private val isPinnedOnly = nonPinnedLimit == 0

  // Expose for usage ratio calculation
  def getCurrentAllocated: Long = synchronized {
    currentNonPinnedAllocated + currentPinnedAllocated
  }

  def getTotalLimit: Long = {
    if (isUnlimited) Long.MaxValue
    else pinnedLimit + nonPinnedLimit
  }

  /**
   * A callback class so we know when a non-pinned host buffer was released
   */
  private class OnCloseCallback(ptr: Long, amount: Long) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        releaseNonPinned(ptr, amount)
        if (BOOKKEEP_MEMORY) {
          bookkeepHostMemoryFree(ptr, amount)
        }
      }
    }
  }

  /**
   * A callback so we know when a pinned host buffer was released.
   */
  private class OnPinnedCloseCallback(ptr: Long, amount: Long) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        releasePinned(ptr, amount)
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

  private def releasePinned(ptr: Long, amount: Long): Unit = {
    synchronized {
      currentPinnedAllocated -= amount
    }
    val metrics = GpuTaskMetrics.get
    metrics.decHostBytesAllocated(amount, isPinned = true)
    logTrace(getHostAllocMetricsLogStr(metrics))
    RmmSpark.cpuDeallocate(ptr, amount)
  }

  private def releaseNonPinned(ptr: Long, amount: Long): Unit = {
    synchronized {
      currentNonPinnedAllocated -= amount
    }
    val metrics = GpuTaskMetrics.get
    metrics.decHostBytesAllocated(amount, isPinned = false)
    logTrace(getHostAllocMetricsLogStr(metrics))
    RmmSpark.cpuDeallocate(ptr, amount)
  }

  private def tryAllocPinned(amount: Long): Option[HostMemoryBuffer] = {
    val ret = Option(PinnedMemoryPool.tryAllocate(amount))
    ret.foreach { b =>
      synchronized {
        currentPinnedAllocated += amount
      }
      HostAlloc.addEventHandler(b, new OnPinnedCloseCallback(b.getAddress, amount))
    }
    ret
  }

  private def tryAllocNonPinned(amount: Long): Option[HostMemoryBuffer] = {
    val ret = if (isUnlimited) {
      synchronized {
        currentNonPinnedAllocated += amount
      }
      Some(HostMemoryBuffer.allocateRaw(amount))
    } else {
      synchronized {
        if ((currentNonPinnedAllocated + amount) <= nonPinnedLimit) {
          currentNonPinnedAllocated += amount
          Some(HostMemoryBuffer.allocateRaw(amount))
        } else {
          None
        }
      }
    }
    ret.foreach { b =>
      HostAlloc.addEventHandler(b, new OnCloseCallback(b.getAddress, amount))
    }
    ret
  }

  private def canNeverSucceed(amount: Long, preferPinned: Boolean): Boolean = synchronized {
    val pinnedFailed = (isPinnedOnly || preferPinned) && (amount > pinnedLimit)
    val nonPinnedFailed = isPinnedOnly || (amount > nonPinnedLimit)
    !isUnlimited && pinnedFailed && nonPinnedFailed
  }

  private def checkSize(amount: Long, preferPinned: Boolean): Unit = synchronized {
    if (canNeverSucceed(amount, preferPinned)) {
      throw new IllegalArgumentException(s"The amount requested $amount is larger than the " +
          s"maximum pool size ${math.max(pinnedLimit, nonPinnedLimit)}")
    }
  }

  private def spillAndCheckRetry(allocSize: Long, retryCount: Long): Boolean = {
    // check arguments for good measure
    require(allocSize >= 0,
      s"spillAndCheckRetry invoked with invalid allocSize $allocSize")

    require(retryCount >= 0,
      s"spillAndCheckRetry invoked with invalid retryCount $retryCount")

    val store = SpillFramework.stores.hostStore
    val totalSize: Long = synchronized {
      currentPinnedAllocated + currentNonPinnedAllocated
    }

    val attemptMsg = if (retryCount > 0) {
      s"Attempt $retryCount"
    } else {
      "First attempt"
    }

    val amountSpilled = store.spill(allocSize)

    if (amountSpilled == 0) {
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
      blocking: Boolean): (Option[HostMemoryBuffer], Boolean) = {
    var retryCount = 0L
    var ret = Option.empty[HostAllocResult]
    var shouldRetry = false
    var shouldRetryInternal = true
    val isRecursive = RmmSpark.preCpuAlloc(amount, blocking)
    var allocAttemptFinishedWithoutException = false
    try {
      do {
        ret = (
          if (preferPinned) {
            tryAllocPinned(amount).map(HostAllocResult(_, isPinned = true))
          } else {
            tryAllocNonPinned(amount).map(HostAllocResult(_, isPinned = false))
          }).orElse {
          if (preferPinned) {
            tryAllocNonPinned(amount).map(HostAllocResult(_, isPinned = false))
          } else {
            tryAllocPinned(amount).map(HostAllocResult(_, isPinned = true))
          }
        }
        if (ret.isEmpty) {
          // We could not make it work so try and spill enough to make it work
          shouldRetryInternal = spillAndCheckRetry(amount, retryCount)
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
  private var singleton: HostAlloc = new HostAlloc(-1)

  private def getSingleton: HostAlloc = synchronized {
    singleton
  }

  def initialize(nonPinnedLimit: Long): Unit = synchronized {
    singleton = new HostAlloc(nonPinnedLimit)
    DefaultHostMemoryAllocator.set(singleton)
  }

  def tryAlloc(amount: Long, preferPinned: Boolean = true): Option[HostMemoryBuffer] = {
    getSingleton.tryAlloc(amount, preferPinned)
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
