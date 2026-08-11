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

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong

import ai.rapids.cudf.{Cuda, Rmm, RmmEventHandler}
import com.nvidia.spark.rapids.spill.SpillableDeviceStore
import com.sun.management.HotSpotDiagnosticMXBean

import org.apache.spark.internal.Logging

/**
 * RMM event handler to trigger spilling from the device memory store.
 * @param store device memory store that will be triggered to spill
 * @param oomDumpDir local directory to create heap dumps on GPU OOM
 * @param maxFailedOOMRetries maximum number of retries for OOMs after
 *                            depleting the device store
 */
class DeviceMemoryEventHandler(
    store: SpillableDeviceStore,
    oomDumpDir: Option[String],
    maxFailedOOMRetries: Int) extends RmmEventHandler with Logging {

  // Flag that ensures we dump stack traces once and not for every allocation
  // failure. The assumption is that unhandled allocations will be fatal
  // to the process at this stage, so we only need this once before we exit.
  private var dumpStackTracesOnFailureToHandleOOM = true

  // Thread local used to the number of times a sync has been attempted, when
  // handling OOMs after depleting the device store.
  private val oomRetryState =
    ThreadLocal.withInitial[OOMRetryState](() => new OOMRetryState)

  /**
   * A small helper class that helps keep track of retry counts as we trigger
   * synchronizes on a completely spilled store.
   */
  class OOMRetryState {
    private var synchronizeAttempts = 0
    private var retryCountLastSynced = 0

    def getRetriesSoFar: Int = synchronizeAttempts

    private def reset(): Unit = {
      synchronizeAttempts = 0
      retryCountLastSynced = 0
    }

    /**
     * If we have synchronized less times than `maxFailedOOMRetries` we allow
     * this retry to proceed, and track the `retryCount` provided by cuDF. If we
     * are above our counter, we reset our state.
     */
    def shouldTrySynchronizing(retryCount: Int): Boolean = {
      if (synchronizeAttempts < maxFailedOOMRetries) {
        retryCountLastSynced = retryCount
        synchronizeAttempts += 1
        true
      } else {
        reset()
        false
      }
    }

    /**
     * We reset our counters if `storeSpillableSize` is non-zero (as we only track when all
     * spillable buffers are removed from the store), or `retryCount` is less than or equal
     * to what we had previously recorded in `shouldTrySynchronizing`.
     * We do this to detect that the new failures are for a separate allocation (we need to
     * give this new attempt a new set of retries.)
     *
     * For example, if an allocation fails and we spill all eligible buffers, `retryCountLastSynced`
     * is set to the last `retryCount` sent to us by cuDF as we keep allowing retries
     * from cuDF. If we succeed, cuDF resets `retryCount`, and so the new count sent to us
     * must be <= than what we saw last, so we can reset our tracking.
     */
    def resetIfNeeded(retryCount: Int, couldSpill: Boolean): Unit = {
      if (couldSpill || retryCount <= retryCountLastSynced) {
        reset()
      }
    }
  }

  /**
   * Handles RMM allocation failures by spilling buffers from device memory.
   * @param allocSize the byte amount that RMM failed to allocate
   * @param retryCount the number of times this allocation has been retried after failure
   * @return true if allocation should be reattempted or false if it should fail
   */
  override def onAllocFailure(allocSize: Long, retryCount: Int): Boolean = {
    // check arguments for good measure
    require(allocSize >= 0,
      s"onAllocFailure invoked with invalid allocSize $allocSize")

    require(retryCount >= 0,
      s"onAllocFailure invoked with invalid retryCount $retryCount")

    try {
      val attemptMsg = if (retryCount > 0) {
        s"Attempt ${retryCount}. "
      } else {
        "First attempt. "
      }

      val retryState = oomRetryState.get()

      val amountSpilled = store.spill(allocSize)
      retryState.resetIfNeeded(retryCount, amountSpilled > 0)
      logInfo(s"Device allocation of $allocSize bytes failed. " +
        s"Device store spilled $amountSpilled bytes. $attemptMsg" +
        s"Total RMM allocated is ${Rmm.getTotalBytesAllocated} bytes.")
      if (amountSpilled == 0) {
        if (retryState.shouldTrySynchronizing(retryCount)) {
          Cuda.deviceSynchronize()
          logWarning(s"[RETRY ${retryState.getRetriesSoFar}] " +
            s"Retrying allocation of $allocSize after a synchronize. " +
            s"Total RMM allocated is ${Rmm.getTotalBytesAllocated} bytes.")
          true
        } else {
          logWarning(s"Device store exhausted, unable to allocate $allocSize bytes. " +
            s"Total RMM allocated is ${Rmm.getTotalBytesAllocated} bytes.")
          synchronized {
            if (dumpStackTracesOnFailureToHandleOOM) {
              dumpStackTracesOnFailureToHandleOOM = false
              GpuSemaphore.dumpActiveStackTracesToLog()
            }
          }
          oomDumpDir.foreach(heapDump)
          false
        }
      } else {
        true
      }
    } catch {
      case t: Throwable =>
        logError(s"Error handling allocation failure", t)
        false
    }
  }

  override def getAllocThresholds: Array[Long] = null

  override def getDeallocThresholds: Array[Long] = null

  override def onAllocThreshold(totalAllocated: Long): Unit = {
  }

  /**
   * Called after every device memory allocation when RMM debug mode is enabled.
   * Used for retry coverage tracking.
   */
  override def onAllocated(size: Long): Unit = {
    AllocationRetryCoverageTracker.checkDeviceAllocation()
  }

  override def onDeallocThreshold(totalAllocated: Long): Unit = {
  }

  private def heapDump(dumpDir: String): Unit = {
    val dumpPath = getDumpPath(dumpDir)
    logWarning(s"Dumping heap to $dumpPath")
    val server = ManagementFactory.getPlatformMBeanServer
    val mxBean = ManagementFactory.newPlatformMXBeanProxy(server,
      "com.sun.management:type=HotSpotDiagnostic", classOf[HotSpotDiagnosticMXBean])
    mxBean.dumpHeap(dumpPath, false)
  }

  private val dumps: AtomicLong = new AtomicLong(0)
  private def getDumpPath(dumpDir: String): String = {
    // pid is typically before the '@' character in the name
    val pid = ManagementFactory.getRuntimeMXBean.getName.split('@').head
    new File(dumpDir, s"gpu-oom-$pid-${dumps.incrementAndGet}.hprof").toString
  }
}
