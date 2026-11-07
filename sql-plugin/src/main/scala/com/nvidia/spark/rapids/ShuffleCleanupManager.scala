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

import java.util.{Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, Executors, RejectedExecutionException,
  ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.rapids.{RapidsShuffleReclaimFence, ShuffleCleanupListener,
  ShuffleReclaimDecision, ShuffleReclaimHandoff, ShuffleReclaimRule}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Manager for coordinating shuffle cleanup between driver and executors.
 *
 * In MULTITHREADED mode, when a shuffle is unregistered on the driver, this manager:
 * 1. Records the shuffle ID for cleanup
 * 2. Responds to executor polls with the list of shuffles to clean up
 * 3. Posts SparkRapidsShuffleDiskSavingsEvent immediately when executor reports stats
 *
 * This solves the problem where shuffle data is stored in executor-side catalogs,
 * but unregisterShuffle is called on the driver. By using a polling mechanism,
 * executors can learn about shuffles that need cleanup and report their statistics.
 *
 * For information on aggregating SparkRapidsShuffleDiskSavingsEvent from eventlogs,
 * see docs/dev/shuffle-metrics.md.
 *
 * == Design Rationale: Why Pull (Polling) Instead of Push ==
 *
 * This implementation uses a pull model (executors poll driver) rather than a push model
 * (driver pushes to executors). Here's why:
 *
 * '''1. Spark Plugin API Limitation'''
 *
 * The Spark Plugin API only supports executor-to-driver communication:
 * {{{
 * // PluginContext (executor side)
 * def send(message: Any): Unit      // executor -> driver (fire-and-forget)
 * def ask(message: Any): AnyRef     // executor -> driver (request-response)
 *
 * // DriverPlugin (driver side)
 * def receive(message: Any): AnyRef // can only respond to executor requests
 * }}}
 *
 * There is NO `driver.sendToExecutor(executorId, message)` API available.
 *
 * '''2. Alternative: Spark's RemoveShuffle Mechanism'''
 *
 * Spark has a built-in RemoveShuffle mechanism triggered by GC:
 * {{{
 * ContextCleaner.doCleanupShuffle(shuffleId)
 *   -> mapOutputTrackerMaster.unregisterShuffle(shuffleId)  // clears map output info
 *   -> shuffleDriverComponents.removeShuffle(shuffleId)     // sends RemoveShuffle to executors
 * }}}
 *
 * This calls `shuffleManager.unregisterShuffle(shuffleId)` on each executor. However:
 * - It's GC-triggered, so timing is unpredictable (often too late for short jobs)
 * - Calling it manually risks clearing map output info too early, causing FetchFailedException
 * - It's one-way (no return value), so we'd still need Plugin RPC to report stats back
 *
 * '''3. Benefits of Pull Model'''
 *
 * - Uses standard Plugin API for every message between the driver and the executors
 * - Safe timing: we control when cleanup happens (after SQL execution ends)
 * - Naturally handles executor failures (failed executors just stop polling)
 * - MULTITHREADED needs no executor list on the driver side
 * - 1-second polling overhead is negligible
 *
 * == UCX reclaim ==
 *
 * In UCX mode the manager reclaims the GPU shuffles that `UcxReclaimPolicy` hands over. It
 * clears each shuffle's map outputs on the driver, keeps its request from the executors until
 * the policy's verdict at the fence's marker, and posts one `SparkRapidsShuffleReclaimEvent` per
 * SQL execution once each shuffle the execution owned has an outcome. Unlike MULTITHREADED's
 * cleanup, it uses Spark internals: it clears map outputs through `MapOutputTrackerMaster` and
 * keeps each shuffle's status, so a later job reruns the map stage. It also tracks the executors
 * that each request waits for.
 *
 * @param clock the current time in milliseconds
 * @param postEvent posts an event to the listener bus
 * @param driverStep clears a shuffle's map outputs on the driver, and returns true when the
 *                   shuffle's status existed. It may throw.
 * @param eventLogEnabled whether each SQL execution's reclaim event is posted
 * @param disabled set once the UCX reclaim is disabled for the rest of the application
 * @param droppedEvents how many events the UCX policy's listener queue has dropped
 * @param staleEntryMaxAgeMs maximum age for pending entries before they are removed
 * @param cleanupIntervalMs interval for running stale entry cleanup
 */
class ShuffleCleanupManager(
    clock: () => Long,
    postEvent: SparkListenerEvent => Unit,
    driverStep: Int => Boolean,
    eventLogEnabled: Boolean,
    disabled: AtomicBoolean,
    droppedEvents: () => Long,
    staleEntryMaxAgeMs: Long = 300000,  // 5 minutes
    cleanupIntervalMs: Long = 60000     // 1 minute
) extends ShuffleReclaimHandoff with Logging {

  import ShuffleCleanupManager._

  /**
   * Builds the manager that the driver plugin uses, from its SparkContext.
   *
   * @param sc SparkContext whose listener bus the manager posts events to
   */
  def this(sc: SparkContext) = this(
    clock = () => System.currentTimeMillis(),
    postEvent = event => TrampolineUtil.postEvent(sc, event),
    driverStep = TrampolineUtil.clearMapOutputs,
    eventLogEnabled = new RapidsConf(sc.getConf).shuffleUcxCleanupOnQueryEndEventLogEnabled,
    disabled = new AtomicBoolean(false),
    droppedEvents =
      () => TrampolineUtil.listenerQueueDroppedEvents(sc, ShuffleCleanupListener.QUEUE_NAME))

  /**
   * Shuffles pending cleanup. Maps shuffleId -> timestamp when unregister was called.
   */
  private val pendingCleanup = new ConcurrentHashMap[Int, Long]()

  /**
   * Track which executors have reported stats for each shuffle.
   * Maps shuffleId -> Set of executorIds that have reported.
   * Used to avoid sending cleanup requests multiple times to the same executor.
   */
  private val reportedExecutors =
    new ConcurrentHashMap[Int, JSet[String]]()

  // The UCX reclaim's state. The policy's listener thread, the manager's thread, the plugin
  // endpoint's thread and the stopping thread change it only under `lock`, which none of them
  // holds across a driver step, an event post, a log call or an RPC. So no thread waits on the
  // lock for another thread's post or log line, and each caller writes its own after it.
  private val lock = new Object
  private var stopped = false
  private var pollsHeld = false
  // each dispatch's requests that wait for their driver step or their verdict, by fence id
  private val fences = new mutable.HashMap[Long, Fence]()
  // the released request of each shuffle, until each of its targets has an outcome
  private val openRequests = new mutable.HashMap[Int, ReclaimRequest]()
  // the shuffles of the released requests that each executor has not answered
  private val openIds = new mutable.HashMap[String, mutable.HashSet[Int]]()
  // when each executor in `openIds` last polled or sent a result
  private val lastHeard = new mutable.HashMap[String, Long]()
  // what the event of each SQL execution counts, until the event posts
  private val executions = new mutable.HashMap[Long, ExecutionAccount]()
  // the executions whose accounts the withdraw-all dropped. The policy's handler in flight at
  // the disable can still name one, and must not give it an account that never completes.
  private val withdrawnExecutions = new mutable.HashSet[Long]()

  /**
   * Scheduled executor for periodic stale entry cleanup.
   */
  private val cleanupExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("shuffle-cleanup-manager")
        .setDaemon(true)
        .build())

  // Start periodic cleanup
  cleanupExecutor.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = runTick()
    },
    cleanupIntervalMs,
    cleanupIntervalMs,
    TimeUnit.MILLISECONDS)

  /**
   * Register a shuffle for cleanup. Called from driver when unregisterShuffle is invoked.
   *
   * @param shuffleId the shuffle ID to clean up
   */
  def registerForCleanup(shuffleId: Int): Unit = {
    logDebug(s"Registering shuffle $shuffleId for cleanup")
    pendingCleanup.put(shuffleId, System.currentTimeMillis())
    reportedExecutors.put(shuffleId, ConcurrentHashMap.newKeySet[String]())
  }

  /**
   * Handle poll from executor asking for shuffles to clean up.
   *
   * @param executorId the executor polling for work
   * @return list of shuffle IDs that need cleanup
   */
  def handlePoll(executorId: String): RapidsShuffleCleanupResponseMsg = {
    val shuffleIds = new ArrayBuffer[Int]()
    val iter = pendingCleanup.keySet().iterator()
    while (iter.hasNext) {
      val shuffleId = iter.next()
      val reported = reportedExecutors.get(shuffleId)
      // Only send to executors that haven't reported yet
      if (reported != null && !reported.contains(executorId)) {
        shuffleIds += shuffleId
      }
    }
    if (shuffleIds.nonEmpty) {
      logDebug(s"Sending ${shuffleIds.size} shuffles to executor $executorId for cleanup: " +
        s"${shuffleIds.mkString(", ")}")
    }
    RapidsShuffleCleanupResponseMsg(shuffleIds.toArray)
  }

  /**
   * Handle cleanup statistics from executor.
   * Immediately posts a SparkRapidsShuffleDiskSavingsEvent for each non-zero stat.
   *
   * @param executorId the executor reporting stats
   * @param stats cleanup statistics
   */
  def handleStats(executorId: String, stats: Array[ShuffleCleanupStats]): Unit = {
    stats.foreach { stat =>
      val shuffleId = stat.shuffleId
      logDebug(s"Received cleanup stats from executor $executorId for shuffle $shuffleId: " +
        s"bytesFromMemory=${stat.bytesFromMemory}, bytesFromDisk=${stat.bytesFromDisk}")

      // Mark this executor as having reported for this shuffle
      val reported = reportedExecutors.get(shuffleId)
      if (reported != null) {
        reported.add(executorId)
      }

      // Immediately emit event if there are non-zero bytes
      if (stat.bytesFromMemory > 0 || stat.bytesFromDisk > 0) {
        logDebug(s"Emitting SparkRapidsShuffleDiskSavingsEvent for shuffle $shuffleId " +
          s"from executor $executorId: " +
          s"bytesFromMemory=${stat.bytesFromMemory}, bytesFromDisk=${stat.bytesFromDisk}")

        try {
          postEvent(
            SparkRapidsShuffleDiskSavingsEvent(shuffleId, stat.bytesFromMemory, stat.bytesFromDisk,
              stat.numExpansions, stat.numSpills, stat.numForcedFileOnly))
        } catch {
          case e: Exception =>
            logWarning(s"Failed to post shuffle disk savings event for shuffle $shuffleId", e)
        }
      }
    }
  }

  /**
   * Cleanup stale entries that haven't been processed.
   * Called periodically to prevent memory leaks.
   *
   * @param maxAgeMs maximum age in milliseconds before an entry is considered stale
   */
  def cleanupStaleEntries(maxAgeMs: Long = 300000): Unit = {
    val now = System.currentTimeMillis()
    val iter = pendingCleanup.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val shuffleId = entry.getKey
      val timestamp = entry.getValue
      if (now - timestamp > maxAgeMs) {
        val reported = reportedExecutors.get(shuffleId)
        val numReported = if (reported != null) reported.size() else 0
        if (numReported > 0) {
          // At least some executors reported cleanup - this is normal, just metadata cleanup
          logDebug(s"Removing stale shuffle cleanup entry for shuffle $shuffleId " +
            s"(registered ${now - timestamp}ms ago, $numReported executor(s) reported cleanup)")
        } else {
          // No executor ever reported cleanup - potential resource leak
          logWarning(s"Removing stale shuffle cleanup entry for shuffle $shuffleId " +
            s"with NO executor cleanup reported (registered ${now - timestamp}ms ago). " +
            s"Shuffle resources may not have been cleaned up properly.")
        }
        iter.remove()
        reportedExecutors.remove(shuffleId)
      }
    }
  }

  /** The body of the periodic tick: MULTITHREADED's stale entries, then the UCX timeouts. */
  private[rapids] def runTick(): Unit = {
    try {
      cleanupStaleEntries(staleEntryMaxAgeMs)
    } catch {
      case e: Exception =>
        logWarning("Error during stale entry cleanup", e)
    }
    try {
      checkReclaimTimeouts()
    } catch {
      // a fixed-delay task that throws never runs again, and the timeouts need every tick
      case NonFatal(e) =>
        logWarning("Error during the UCX shuffle reclaim's timeout check", e)
    }
  }

  override def dispatch(
      fenceId: Long,
      shuffles: Seq[ShuffleReclaimDecision],
      decidedAtMs: Long): Unit = {
    // each request is recorded before its driver step, so that a skip, close or executor
    // removal the policy sends meanwhile finds it, and its execution's event waits for it
    val requests = lock.synchronized {
      if (stopped) {
        None
      } else {
        val fence = new Fence()
        val recorded = shuffles.map(new ReclaimRequest(fenceId, _, decidedAtMs))
        recorded.foreach { request =>
          fence.requests(request.shuffleId) = request
          if (eventLogEnabled) {
            accountOf(request.owner).foreach(_.dispatched(request.shuffleId, decidedAtMs))
          }
        }
        fences(fenceId) = fence
        Some(recorded)
      }
    }
    requests.foreach(recorded => onManagerThread(clearAndFence(fenceId, recorded)))
  }

  override def skip(shuffleId: Int, executionId: Long, rule: ShuffleReclaimRule): Unit = {
    val out = new Outbox()
    locked {
      if (eventLogEnabled) {
        accountOf(executionId).foreach(_.decided += shuffleId)
        settleShuffle(executionId, shuffleId, new Skipped(rule), skippedDetail(rule), None, out)
      }
    }
    out.run()
  }

  override def executionClosed(executionId: Long, endedAtMs: Long, ownedIds: Seq[Int]): Unit = {
    val out = new Outbox()
    locked {
      if (eventLogEnabled) {
        accountOf(executionId).foreach { account =>
          account.close(endedAtMs, ownedIds)
          postIfComplete(executionId, account, out)
        }
      }
    }
    out.run()
  }

  override def executorRemoved(executorId: String): Unit = {
    val out = new Outbox()
    locked {
      fences.values.foreach(fence => fence.requests.values.foreach(_.targets.remove(executorId)))
      val completed = new ArrayBuffer[ReclaimRequest]()
      openRequests.values.foreach { request =>
        request.targets.remove(executorId) match {
          case Some(target) if !target.answered =>
            request.unanswered -= 1
            if (request.unanswered == 0) {
              completed += request
            }
          case _ =>
        }
      }
      openIds.remove(executorId)
      lastHeard.remove(executorId)
      completed.foreach(settle(_, Reclaimed, "reclaimed", out))
    }
    out.run()
  }

  override def release(fenceId: Long, shuffleId: Int): Unit =
    onManagerThread(applyRelease(fenceId, shuffleId))

  override def withdraw(fenceId: Long, shuffleId: Int): Unit =
    onManagerThread(applyWithdrawal(fenceId, shuffleId))

  override def isDisabled: Boolean = disabled.get()

  override def disable(reason: String): Unit = {
    // the policy's thread and the manager's thread can both disable, and only the one that
    // sets the flag warns and withdraws
    if (!isStopped && disabled.compareAndSet(false, true)) {
      logWarning(s"UCX shuffle reclaim is disabled for the rest of the application: $reason")
      // queued behind each driver step handed over before it, so it also withdraws what those
      // steps fence
      onManagerThread(withdrawAll())
    }
  }

  /**
   * Handle a UCX executor's poll for shuffles to reclaim.
   *
   * @param executorId the executor polling for work
   * @return the released requests that the executor has not answered
   */
  def handleReclaimPoll(executorId: String): RapidsShuffleReclaimResponseMsg = {
    val out = new Outbox()
    val requests = lock.synchronized {
      if (stopped || pollsHeld) {
        Array.empty[ShuffleReclaimRequest]
      } else {
        openIds.get(executorId).fold(Array.empty[ShuffleReclaimRequest])(
          handOut(executorId, _, out))
      }
    }
    out.run()
    RapidsShuffleReclaimResponseMsg(requests)
  }

  /**
   * Handle the results of a UCX executor's reclaims, on the calling thread.
   *
   * @param executorId the executor reporting results
   * @param results the result of each reclaim
   */
  def handleReclaimResult(executorId: String, results: Array[ShuffleReclaimResult]): Unit = {
    val out = new Outbox()
    locked {
      if (openIds.contains(executorId)) {
        lastHeard(executorId) = clock()
      }
      results.foreach { result =>
        openRequests.get(result.shuffleId) match {
          // only a target that holds this watermark with no outcome takes the result
          case Some(request) if request.watermark == result.watermark &&
              request.targets.get(executorId).exists(!_.answered) =>
            answer(request, executorId, request.targets(executorId), result, out)
          case _ =>
            out.info(s"$IGNORED_RESULT_INFO_PREFIX executor $executorId, shuffle " +
              s"${result.shuffleId}, watermark ${result.watermark}, ${result.outcome}, " +
              s"${result.buffers} buffers, ${result.bytes} bytes")
        }
      }
    }
    out.run()
  }

  /** The body of a release, which `release` runs on the manager's thread. */
  private[rapids] def applyRelease(fenceId: Long, shuffleId: Int): Unit = {
    val out = new Outbox()
    locked {
      fencedRequest(fenceId, shuffleId).foreach { request =>
        if (disabled.get()) {
          settle(request, LeftToGc, "withdrawn, released after the reclaim was disabled", out)
        } else {
          openRequest(request, out)
        }
      }
    }
    out.run()
  }

  /** The body of a withdrawal, which `withdraw` runs on the manager's thread. */
  private[rapids] def applyWithdrawal(fenceId: Long, shuffleId: Int): Unit = {
    val out = new Outbox()
    locked {
      fencedRequest(fenceId, shuffleId).foreach { request =>
        settle(request, LeftToGc, "withdrawn, a job touched it before its fence's marker", out)
      }
    }
    out.run()
  }

  /** Withdraws each fence 10 minutes past its clear, and times out each silent target. */
  private[rapids] def checkReclaimTimeouts(): Unit = {
    val out = new Outbox()
    locked {
      val now = clock()
      fences.toList.foreach { case (fenceId, fence) =>
        if (fence.clearedAtMs.exists(now - _ >= RECLAIM_TIMEOUT_MS)) {
          val requests = fence.requests.values.toList
          requests.foreach(settle(_, LeftToGc, "withdrawn, its fence timed out", out))
          out.warn(s"$FENCE_TIMEOUT_WARN_PREFIX fence $fenceId, shuffles " +
            s"${requests.map(_.shuffleId).sorted.mkString(", ")} left to GC")
        }
      }
      openIds.toList.foreach { case (executorId, ids) =>
        val heard = lastHeard.get(executorId)
        // a target's silence counts from its release, or from a later poll or result
        val silent = ids.toList.filter { shuffleId =>
          val releasedAtMs = openRequests(shuffleId).releasedAtMs
          now - heard.fold(releasedAtMs)(math.max(_, releasedAtMs)) >= RECLAIM_TIMEOUT_MS
        }
        timeOut(executorId, silent, out)
      }
    }
    out.run()
  }

  /** Makes each UCX poll leave every request out, until the release below. */
  private[rapids] def holdReclaimPollsForTesting(): Unit = lock.synchronized {
    pollsHeld = true
  }

  /** Ends the hold that `holdReclaimPollsForTesting` set. */
  private[rapids] def releaseReclaimPollsForTesting(): Unit = lock.synchronized {
    pollsHeld = false
  }

  /** Whether a fence released the shuffle while its request is open. */
  private[rapids] def isReclaimReleasedForTesting(shuffleId: Int): Boolean =
    lock.synchronized(openRequests.contains(shuffleId))

  /** Runs the driver step of each shuffle of one dispatch, then posts the dispatch's marker. */
  private def clearAndFence(fenceId: Long, requests: Seq[ReclaimRequest]): Unit = {
    try {
      requests.foreach { request =>
        if (!isStopped) {
          clearOnDriver(request)
        }
      }
    } finally {
      // runs after a fatal error in a step too, so the fence's timeout still settles what the
      // error left unfenced
      val running = lock.synchronized {
        if (!stopped) {
          val now = clock()
          fences.get(fenceId).foreach(fence => fence.clearedAtMs = Some(now))
        }
        !stopped
      }
      // posted even when nothing was fenced, since the policy holds each shuffle until a marker
      if (running) {
        postFence(fenceId)
      }
    }
  }

  /**
   * Clears one shuffle's map outputs on the driver and fences its request, or settles the
   * request when the driver must leave the shuffle to GC or finds it already removed.
   */
  private def clearOnDriver(request: ReclaimRequest): Unit = {
    val out = new Outbox()
    try {
      clearRefusal() match {
        case Some(detail) => locked(settle(request, LeftToGc, detail, out))
        case None =>
          if (driverStep(request.shuffleId)) {
            locked {
              request.fenced = true
            }
          } else {
            locked(settle(request, AlreadyRemoved, "already removed", out))
          }
      }
    } catch {
      // without this catch the throwable stays in the future that `execute` discards, and the
      // shuffle never gets an outcome
      case NonFatal(e) =>
        logWarning(s"$DRIVER_STEP_WARN_PREFIX shuffle ${request.shuffleId} of SQL execution " +
          s"${request.owner} is left to GC", e)
        locked(settle(request, LeftToGc, "left to GC, its driver step failed", out))
    }
    out.run()
  }

  /**
   * Why the driver must leave a shuffle to GC with no clear: the reclaim is disabled, or the
   * policy's listener queue dropped an event, which disables it. None when it may clear.
   */
  private def clearRefusal(): Option[String] = {
    if (disabled.get()) {
      Some("left to GC, the reclaim is disabled")
    } else {
      val dropped = droppedEvents()
      if (dropped == 0) {
        None
      } else {
        val queue = ShuffleCleanupListener.QUEUE_NAME
        disable(s"the listener bus queue $queue dropped $dropped events, so the reclaim cannot " +
          s"see every job. Raising spark.scheduler.listenerbus.eventqueue.$queue.capacity " +
          "avoids this")
        Some("left to GC, the policy's listener queue dropped an event")
      }
    }
  }

  /** Posts a dispatch's marker, or withdraws the dispatch's fence when the post fails. */
  private def postFence(fenceId: Long): Unit = {
    try {
      postEvent(RapidsShuffleReclaimFence(fenceId))
    } catch {
      case NonFatal(e) =>
        val out = new Outbox()
        val withdrawn = new ArrayBuffer[Int]()
        locked {
          fences.get(fenceId).foreach { fence =>
            fence.requests.values.toList.foreach { request =>
              withdrawn += request.shuffleId
              settle(request, LeftToGc, "withdrawn, its fence's marker could not be posted", out)
            }
          }
        }
        logWarning(s"UCX shuffle reclaim could not post the marker of fence $fenceId, so it " +
          s"left shuffles ${withdrawn.sorted.mkString(", ")} to GC", e)
        out.run()
    }
  }

  /**
   * Hands a fenced request to its targets' polls. A released request for the same shuffle from
   * an earlier owner settles for that owner, and its targets join this request, which asks each
   * of them again with its own watermark.
   */
  private def openRequest(request: ReclaimRequest, out: Outbox): Unit = {
    detach(request)
    openRequests.get(request.shuffleId).foreach { older =>
      older.targets.foreach { case (executorId, target) =>
        val joined = request.targets.getOrElseUpdate(executorId, new ReclaimTarget())
        joined.handed = joined.handed || target.handed
      }
      settle(older, Reclaimed, s"reclaimed, replaced by the request of SQL execution " +
        s"${request.owner}", out)
    }
    request.released = true
    request.releasedAtMs = clock()
    request.unanswered = request.targets.size
    openRequests(request.shuffleId) = request
    request.targets.keys.foreach { executorId =>
      openIds.getOrElseUpdate(executorId, new mutable.HashSet[Int]()) += request.shuffleId
    }
    if (request.unanswered == 0) {
      settle(request, Reclaimed, "reclaimed, with no target left", out)
    }
  }

  /**
   * Withdraws every fenced request once the reclaim is disabled, and drops the events of the
   * executions that cannot complete, since the policy closes and decides nothing more.
   */
  private def withdrawAll(): Unit = {
    val out = new Outbox()
    locked {
      val fenced = fences.values.flatMap(_.requests.values).filter(_.fenced).toList
      fenced.foreach(settle(_, LeftToGc, "withdrawn, the reclaim was disabled", out))
      val stranded = executions.filter { case (_, account) => !account.canComplete }.keys.toList
      stranded.foreach { executionId =>
        executions.remove(executionId)
        withdrawnExecutions += executionId
      }
      out.info(s"UCX shuffle reclaim left ${fenced.size} fenced shuffles to GC and dropped the " +
        s"events of ${stranded.size} SQL executions that cannot complete")
    }
    out.run()
  }

  /** The poll reply for an executor with unanswered requests, which counts each hand-out. */
  private def handOut(
      executorId: String,
      ids: mutable.HashSet[Int],
      out: Outbox): Array[ShuffleReclaimRequest] = {
    lastHeard(executorId) = clock()
    val reply = new ArrayBuffer[ShuffleReclaimRequest]()
    val again = new ArrayBuffer[Int]()
    val silent = new ArrayBuffer[Int]()
    ids.foreach { shuffleId =>
      val request = openRequests(shuffleId)
      val target = request.targets(executorId)
      // a second hand-out covers a lost reply, reclaim or result, and a third times out
      if (target.handOuts < MAX_HAND_OUTS) {
        if (target.handOuts > 0) {
          again += shuffleId
        }
        target.handOuts += 1
        target.handed = true
        reply += ShuffleReclaimRequest(shuffleId, request.watermark)
      } else {
        silent += shuffleId
      }
    }
    if (again.nonEmpty) {
      out.info(s"UCX shuffle reclaim handed executor $executorId shuffles " +
        s"${again.sorted.mkString(", ")} again, since a reply, a reclaim or a result was lost")
    }
    timeOut(executorId, silent, out)
    reply.toArray
  }

  /** Counts one target's result, and closes the target. */
  private def answer(
      request: ReclaimRequest,
      executorId: String,
      target: ReclaimTarget,
      result: ShuffleReclaimResult,
      out: Outbox): Unit = {
    result.outcome match {
      case ShuffleReclaimOutcome.REMOVED => request.removed += 1
      case ShuffleReclaimOutcome.ABSENT => request.absent += 1
      case ShuffleReclaimOutcome.DEFERRED => request.deferred += 1
      case ShuffleReclaimOutcome.FAILED =>
        request.failed += 1
        out.warn(s"$FAILED_WARN_PREFIX shuffle ${request.shuffleId}, watermark " +
          s"${request.watermark}, executor $executorId: ${result.error.getOrElse("no error")}")
    }
    request.buffers += result.buffers
    request.bytes += result.bytes
    request.maxElapsedNanos = math.max(request.maxElapsedNanos, result.elapsedNanos)
    closeTarget(request, executorId, target, out)
  }

  /** Times out an executor's targets of the given shuffles, with one WARN that lists them. */
  private def timeOut(executorId: String, shuffleIds: Iterable[Int], out: Outbox): Unit = {
    if (shuffleIds.nonEmpty) {
      val neverPolled = new ArrayBuffer[Int]()
      val polled = new ArrayBuffer[Int]()
      shuffleIds.toList.sorted.foreach { shuffleId =>
        val request = openRequests(shuffleId)
        val target = request.targets(executorId)
        // the reason is whether the executor was handed the shuffle under any watermark
        val reason = if (target.handed) {
          polled += shuffleId
          POLLED_NO_RESULT
        } else {
          neverPolled += shuffleId
          request.neverPolled += 1
          NEVER_POLLED
        }
        request.timeouts += s"$executorId ($reason)"
        closeTarget(request, executorId, target, out)
      }
      val byReason = Seq(NEVER_POLLED -> neverPolled, POLLED_NO_RESULT -> polled).collect {
        case (reason, ids) if ids.nonEmpty => s"$reason [${ids.mkString(", ")}]"
      }
      out.warn(s"$TIMEOUT_WARN_PREFIX $executorId, ${byReason.mkString(", ")}")
    }
  }

  /** Closes one target of a released request, and settles the request with its last target. */
  private def closeTarget(
      request: ReclaimRequest,
      executorId: String,
      target: ReclaimTarget,
      out: Outbox): Unit = {
    target.answered = true
    openIds.get(executorId).foreach { ids =>
      ids -= request.shuffleId
      // an executor's silence counts only while it has a request to answer
      if (ids.isEmpty) {
        openIds.remove(executorId)
        lastHeard.remove(executorId)
      }
    }
    request.unanswered -= 1
    if (request.unanswered == 0) {
      settle(request, Reclaimed, "reclaimed", out)
    }
  }

  /** Ends a request with its outcome, which counts for the request's owner. */
  private def settle(
      request: ReclaimRequest,
      outcome: Outcome,
      detail: String,
      out: Outbox): Unit = {
    detach(request)
    settleShuffle(request.owner, request.shuffleId, outcome, detail, Some(request), out)
  }

  /**
   * Logs a shuffle's outcome and counts it once in its owner's event, which posts when it was
   * the owner's last shuffle with no outcome. With the event key off nothing is counted.
   */
  private def settleShuffle(
      owner: Long,
      shuffleId: Int,
      outcome: Outcome,
      detail: String,
      request: Option[ReclaimRequest],
      out: Outbox): Unit = {
    val account = executions.get(owner)
    if (!account.exists(_.settled.contains(shuffleId))) {
      val now = clock()
      out.debug(settledLine(owner, shuffleId, detail, request, account, now))
      account.foreach { a =>
        a.record(shuffleId, outcome, request, now)
        postIfComplete(owner, a, out)
      }
    }
  }

  /** Posts an execution's event once the execution is closed and each shuffle has an outcome. */
  private def postIfComplete(executionId: Long, account: ExecutionAccount, out: Outbox): Unit = {
    if (account.isComplete) {
      executions.remove(executionId)
      out.post(account.toEvent(executionId))
    }
  }

  /** Takes a request out of its fence, or out of the released requests. */
  private def detach(request: ReclaimRequest): Unit = {
    if (request.released) {
      openRequests.remove(request.shuffleId)
    } else {
      fences.get(request.fenceId).foreach { fence =>
        fence.requests.remove(request.shuffleId)
        if (fence.requests.isEmpty) {
          fences.remove(request.fenceId)
        }
      }
    }
  }

  /** The request that a fence holds for a shuffle until the fence's verdict on it. */
  private def fencedRequest(fenceId: Long, shuffleId: Int): Option[ReclaimRequest] =
    fences.get(fenceId).flatMap(_.requests.get(shuffleId)).filter(_.fenced)

  /** An execution's account, made on first use, or None once the withdraw-all dropped it. */
  private def accountOf(executionId: Long): Option[ExecutionAccount] =
    if (withdrawnExecutions.contains(executionId)) {
      None
    } else {
      Some(executions.getOrElseUpdate(executionId, new ExecutionAccount()))
    }

  /** The DEBUG line for a shuffle whose outcome is settled. */
  private def settledLine(
      owner: Long,
      shuffleId: Int,
      detail: String,
      request: Option[ReclaimRequest],
      account: Option[ExecutionAccount],
      now: Long): String = {
    val line = s"UCX shuffle reclaim settled shuffle $shuffleId of SQL execution $owner: $detail"
    request.fold(line) { r =>
      val waited = account.flatMap(_.endedAt).fold("") { endedAt =>
        s", decided ${r.decidedAtMs - endedAt} ms after the execution ended"
      }
      s"$line$waited, settled ${now - r.decidedAtMs} ms after the decision, executors " +
        s"removed ${r.removed}, absent ${r.absent}, deferred ${r.deferred}, failed " +
        s"${r.failed}, timed out ${r.timeouts.size} [${r.timeouts.mkString(", ")}], " +
        s"${r.buffers} buffers, ${r.bytes} bytes, longest executor reclaim " +
        s"${TimeUnit.NANOSECONDS.toMillis(r.maxElapsedNanos)} ms"
    }
  }

  private def isStopped: Boolean = lock.synchronized(stopped)

  /** Runs `body` under the lock, unless the manager has stopped. */
  private def locked(body: => Unit): Unit = lock.synchronized {
    if (!stopped) {
      body
    }
  }

  /** Runs `body` on the manager's thread, after everything handed to that thread before it. */
  private def onManagerThread(body: => Unit): Unit = {
    try {
      cleanupExecutor.execute(() => body)
    } catch {
      // the manager has stopped, and drops what it is handed
      case _: RejectedExecutionException =>
    }
  }

  private def postReclaimEvent(event: SparkRapidsShuffleReclaimEvent): Unit = {
    try {
      postEvent(event)
    } catch {
      // a failed post must not fail the policy's or the endpoint's call that settled the event
      case NonFatal(e) =>
        logWarning(s"Failed to post the shuffle reclaim event of SQL execution " +
          s"${event.executionId}", e)
    }
  }

  /**
   * The lines and events that a call collects under the lock, logged and posted in order once
   * the call has released it.
   */
  private final class Outbox {
    private var actions: List[() => Unit] = Nil

    def debug(line: String): Unit = add(() => logDebug(line))

    def info(line: String): Unit = add(() => logInfo(line))

    def warn(line: String): Unit = add(() => logWarning(line))

    def post(event: SparkRapidsShuffleReclaimEvent): Unit = add(() => postReclaimEvent(event))

    def run(): Unit = actions.reverse.foreach(action => action())

    private def add(action: () => Unit): Unit = {
      actions = action :: actions
    }
  }

  /**
   * Shutdown the manager.
   */
  def shutdown(): Unit = {
    logInfo("Shutting down ShuffleCleanupManager")
    // every UCX entry point does nothing from here, so the manager's thread ends its task
    lock.synchronized {
      stopped = true
    }

    // Stop the cleanup executor
    cleanupExecutor.shutdown()
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        cleanupExecutor.shutdownNow()
        Thread.currentThread().interrupt()
    }

    pendingCleanup.clear()
    reportedExecutors.clear()
    dropReclaimState()
  }

  /** Drops what the UCX reclaim still holds once the manager has stopped, with one count. */
  private def dropReclaimState(): Unit = {
    val (requests, fenceCount, events) = lock.synchronized {
      val counts = (fences.values.map(_.requests.size).sum + openRequests.size, fences.size,
        executions.size)
      fences.clear()
      openRequests.clear()
      openIds.clear()
      lastHeard.clear()
      executions.clear()
      withdrawnExecutions.clear()
      counts
    }
    if (requests + fenceCount + events > 0) {
      logInfo(s"Dropped $requests UCX shuffle reclaim requests, $fenceCount fences and the " +
        s"events of $events SQL executions at shutdown")
    }
  }
}

object ShuffleCleanupManager {
  @volatile private var instance: ShuffleCleanupManager = _
  // the context that built `instance`. A context whose plugin init failed never shuts its
  // plugins down, so the next context replaces the manager it left.
  private var instanceContext: SparkContext = _

  def init(sc: SparkContext): Unit = synchronized {
    if (instance != null && (instanceContext ne sc)) {
      instance.shutdown()
      instance = null
    }
    if (instance == null) {
      instance = new ShuffleCleanupManager(sc)
      instanceContext = sc
    }
  }

  def get: ShuffleCleanupManager = instance

  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
      instanceContext = null
    }
  }

  /** The start of the one WARN for an executor that reports a failed reclaim. */
  private[rapids] val FAILED_WARN_PREFIX: String = "UCX shuffle reclaim failed on an executor:"

  /** The start of the one WARN for a driver step that threw. */
  private[rapids] val DRIVER_STEP_WARN_PREFIX: String =
    "UCX shuffle reclaim could not clear a shuffle on the driver:"

  /** The start of the one WARN for a fence with no verdict 10 minutes after its clear. */
  private[rapids] val FENCE_TIMEOUT_WARN_PREFIX: String =
    "UCX shuffle reclaim withdrew a fence with no verdict:"

  /** The start of the one WARN that lists an executor's timed-out shuffles by reason. */
  private[rapids] val TIMEOUT_WARN_PREFIX: String =
    "UCX shuffle reclaim timed out on an executor:"

  /** The start of the INFO line for a result that settles no target. */
  private[rapids] val IGNORED_RESULT_INFO_PREFIX: String =
    "UCX shuffle reclaim ignored a result that settles nothing:"

  /** How long a fence waits for its verdict, and a target for its executor. */
  private val RECLAIM_TIMEOUT_MS: Long = TimeUnit.MINUTES.toMillis(10)

  /** How many polls hand a request to a target before the target times out. */
  private val MAX_HAND_OUTS = 2

  /** The timeout reason of a target whose executor was never handed the shuffle. */
  private val NEVER_POLLED = "never polled"

  /** The timeout reason of a target whose executor was handed the shuffle. */
  private val POLLED_NO_RESULT = "polled, no result"

  /** Where a settled shuffle counts in its execution's event. */
  private sealed trait Outcome

  private case object Reclaimed extends Outcome

  private case object AlreadyRemoved extends Outcome

  /** Left to GC, and listed in the event under the rule. */
  private final class Skipped(val rule: ShuffleReclaimRule) extends Outcome

  // the event lists what the fence or the driver step leaves to GC with R2's shared shuffles
  private val LeftToGc: Outcome = new Skipped(ShuffleReclaimRule.R2)

  /** The DEBUG line's outcome for a shuffle that a rule leaves to GC. */
  private def skippedDetail(rule: ShuffleReclaimRule): String = rule match {
    case ShuffleReclaimRule.R2 => "skipped, shared (R2)"
    case ShuffleReclaimRule.R4 => "skipped, unconsumed (R4)"
    case ShuffleReclaimRule.R5 => "skipped, cached (R5)"
    case ShuffleReclaimRule.R7 => "skipped, indeterminate (R7)"
  }

  /** One executor that a reclaim request waits for. */
  private final class ReclaimTarget {
    // the polls that handed the executor the current request
    var handOuts = 0
    // whether a poll handed the executor the shuffle under any watermark
    var handed = false
    var answered = false
  }

  /**
   * A request to reclaim one shuffle for its owner, from its dispatch until each target has an
   * outcome. The manager's lock guards its state.
   */
  private final class ReclaimRequest(
      val fenceId: Long,
      decision: ShuffleReclaimDecision,
      val decidedAtMs: Long) {
    val shuffleId: Int = decision.shuffleId
    val owner: Long = decision.owner
    val watermark: Long = decision.watermark
    // each target, answered or not, until its executor's removal drops it
    val targets = new mutable.HashMap[String, ReclaimTarget]()
    decision.targets.foreach(executorId => targets(executorId) = new ReclaimTarget())
    // set once the driver step cleared the shuffle, so that the fence's verdict decides it
    var fenced = false
    // set once the fence released the request to its targets' polls
    var released = false
    var releasedAtMs = 0L
    var unanswered = 0
    var removed = 0
    var absent = 0
    var deferred = 0
    var failed = 0
    var neverPolled = 0
    var buffers = 0L
    var bytes = 0L
    var maxElapsedNanos = 0L
    // each target that timed out, with its reason
    val timeouts = new ArrayBuffer[String]()
  }

  /** The requests of one dispatch that wait for their driver step or their verdict. */
  private final class Fence {
    val requests = new mutable.HashMap[Int, ReclaimRequest]()
    // when the dispatch's driver steps ended, which starts the wait for verdicts
    var clearedAtMs: Option[Long] = None
  }

  /** What one SQL execution's reclaim event counts, until the event posts. */
  private final class ExecutionAccount {
    // the shuffles that the policy dispatched or skipped, and those with an outcome
    val decided = new mutable.HashSet[Int]()
    val settled = new mutable.HashSet[Int]()
    private var closed = false
    private var endedAtMs = 0L
    private var ownedIds: Seq[Int] = Nil
    private var latestDecisionMs: Option[Long] = None
    private var completedAtMs = 0L
    private var reclaimed = 0
    private var alreadyRemoved = 0
    private val skippedBy = new mutable.HashMap[ShuffleReclaimRule, ArrayBuffer[Int]]()
    private var executorsFailed = 0
    private var executorsTimedOut = 0
    private var executorsNeverPolled = 0
    private var executorsDeferred = 0
    private var buffersFreed = 0L
    private var bytesFreed = 0L
    private var maxRemoveMs = 0L

    /** When the execution ended, once the policy closed it. */
    def endedAt: Option[Long] = if (closed) Some(endedAtMs) else None

    /** Whether the event can still post: the execution is closed and each shuffle decided. */
    def canComplete: Boolean = closed && ownedIds.forall(decided.contains)

    /** Whether the event is ready: the execution is closed and each shuffle has an outcome. */
    def isComplete: Boolean = closed && ownedIds.forall(settled.contains)

    def close(atMs: Long, owned: Seq[Int]): Unit = {
      closed = true
      endedAtMs = atMs
      ownedIds = owned.toList
    }

    def dispatched(shuffleId: Int, decidedAtMs: Long): Unit = {
      decided += shuffleId
      latestDecisionMs = Some(latestDecisionMs.fold(decidedAtMs)(math.max(_, decidedAtMs)))
    }

    def record(
        shuffleId: Int,
        outcome: Outcome,
        request: Option[ReclaimRequest],
        now: Long): Unit = {
      settled += shuffleId
      outcome match {
        case Reclaimed => reclaimed += 1
        case AlreadyRemoved => alreadyRemoved += 1
        case skipped: Skipped =>
          skippedBy.getOrElseUpdate(skipped.rule, new ArrayBuffer[Int]()) += shuffleId
      }
      request.foreach { r =>
        executorsFailed += r.failed
        executorsTimedOut += r.timeouts.size
        executorsNeverPolled += r.neverPolled
        executorsDeferred += r.deferred
        buffersFreed += r.buffers
        bytesFreed += r.bytes
        maxRemoveMs = math.max(maxRemoveMs, now - r.decidedAtMs)
      }
      completedAtMs = math.max(completedAtMs, now)
    }

    def toEvent(executionId: Long): SparkRapidsShuffleReclaimEvent =
      SparkRapidsShuffleReclaimEvent(
        executionId = executionId,
        endedAtMs = endedAtMs,
        // a shuffle that a rule settled before the execution's end adds no cleanup time
        completedAtMs = math.max(completedAtMs, endedAtMs),
        shuffles = ownedIds.size,
        reclaimed = reclaimed,
        alreadyRemoved = alreadyRemoved,
        skippedShared = skippedIds(ShuffleReclaimRule.R2),
        skippedUnconsumed = skippedIds(ShuffleReclaimRule.R4),
        skippedCached = skippedIds(ShuffleReclaimRule.R5),
        skippedIndeterminate = skippedIds(ShuffleReclaimRule.R7),
        executorsFailed = executorsFailed,
        executorsTimedOut = executorsTimedOut,
        executorsNeverPolled = executorsNeverPolled,
        executorsDeferred = executorsDeferred,
        buffersFreed = buffersFreed,
        bytesFreed = bytesFreed,
        maxWaitMs = latestDecisionMs.fold(0L)(latest => math.max(0L, latest - endedAtMs)),
        maxRemoveMs = maxRemoveMs)

    private def skippedIds(rule: ShuffleReclaimRule): List[Int] =
      skippedBy.get(rule).fold(List.empty[Int])(_.toList)
  }
}
