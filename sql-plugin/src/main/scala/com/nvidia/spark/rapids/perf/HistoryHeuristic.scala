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

package com.nvidia.spark.rapids.perf

import java.time.Duration
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.history.{MetricStore, MetricStores}
import com.nvidia.spark.rapids.{HistoryMetricsManager, RapidsConf}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * A planning decision that reads history at planning time and records what the query did when
 * it ends.
 *
 * @param store the installed store; `MetricStores.current()` in production
 */
abstract class HistoryHeuristic(store: () => MetricStore) extends Logging {

  type Ctx
  type Decision

  def name: String

  /**
   * Whether this heuristic's own `spark.rapids.sql.history.<name>.enabled` switch is on. Read once
   * at driver plugin init, and only consulted when a history provider is requested.
   */
  def enabled(conf: RapidsConf): Boolean

  /** Families this heuristic reads and writes. */
  def metrics: Seq[HistoryMetric]

  private lazy val histories: Map[HistoryMetric, MetricHistory] =
    metrics.map(m => m -> new MetricHistory(m, store)).toMap

  /** Set while history-backed planning is on; otherwise the static decision stands. */
  @volatile private var active: Option[HistoryPolicy] = None

  final def enable(policy: HistoryPolicy): Unit = active = Some(policy)

  final def disable(): Unit = active = None

  final def isEnabled: Boolean = active.isDefined

  /** Declares every family up front. */
  final def declare(): Unit = active.foreach { policy =>
    histories.values.foreach(_.declare(policy))
  }

  /** Dimension value for `ctx` in `metric`. */
  protected def keyFor(metric: HistoryMetric, ctx: Ctx): String

  /** The decision without history. */
  protected def staticDecision(ctx: Ctx): Decision

  /** Called only when `sufficient` holds. */
  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: Ctx): Decision

  protected def constrain(raw: Decision, ctx: Ctx): Decision = raw

  /** What the query did; called after it ends. */
  protected def observe(ctx: Ctx): Map[HistoryMetric, Double]

  /** Default: every family answered. */
  protected def sufficient(observed: Map[HistoryMetric, Double]): Boolean =
    observed.size == metrics.size

  protected def shouldObserve(ctx: Ctx): Boolean = true

  /**
   * How old a reading may be for this decision; None keeps each family's declared planning age.
   * A lookup window only: it never changes what is declared to the store.
   */
  protected def planningMaxAge(ctx: Ctx): Option[Duration] = None

  /**
   * History-based or static, never a blend. Abstaining families are absent from `observed`;
   * `sufficient` decides whether the rest is enough.
   */
  final def decide(ctx: Ctx, nowMs: Long): Decision = active match {
    case None => staticDecision(ctx)
    case Some(policy) =>
      val observed = metrics.flatMap { m =>
        histories(m).latest(keyFor(m, ctx), nowMs, policy, planningMaxAge(ctx))
          .map(value => m -> value)
      }.toMap
      if (sufficient(observed)) constrain(decideFrom(observed, ctx), ctx)
      else staticDecision(ctx)
  }

  /** Queues `ctx` for observation at execution end; untracked outside a SQL execution. */
  final def register(executionId: Option[Long], ctx: Ctx): Unit = {
    if (isEnabled && shouldObserve(ctx)) {
      executionId match {
        case None => logDebug(s"$name planned outside a SQL execution; not tracked for history")
        case Some(id) => HistoryObservations.register(id, () => recordAll(ctx))
      }
    }
  }

  /** Records what `ctx` observed; called at execution end. */
  private[perf] final def recordAll(ctx: Ctx): Unit = active.foreach { policy =>
    val atMs = System.currentTimeMillis()
    observe(ctx).foreach { case (m, value) =>
      histories(m).record(keyFor(m, ctx), value, atMs, policy)
    }
  }

  /** For tests. */
  private[perf] final def historyOf(metric: HistoryMetric): MetricHistory = histories(metric)
}

/**
 * Holds planned contexts per SQL execution id and records them when the execution ends, once
 * accumulators are merged. Shared by all heuristics.
 */
object HistoryObservations extends Logging {

  /** Ended execution ids remembered, so late registrations are dropped. */
  private val ENDED_MEMORY = 1024

  @volatile private var active: Boolean = false

  /** Callbacks per running execution. Guarded by `this`. */
  private val executions = mutable.HashMap.empty[Long, ArrayBuffer[() => Unit]]

  /** Recently ended executions, oldest first. Guarded by `this`. */
  private val ended = mutable.LinkedHashSet.empty[Long]

  def start(): Unit = active = true

  def shutdown(): Unit = synchronized {
    active = false
    executions.clear()
    ended.clear()
  }

  def listener: SparkListener = new ObservationListener

  /** Executions holding callbacks, for tests. */
  private[perf] def trackedCount: Int = synchronized(executions.size)

  /** The calling thread's SQL execution id, if any. */
  def currentExecutionId(sc: SparkContext): Option[Long] =
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).flatMap { id =>
      try Some(id.toLong) catch { case _: NumberFormatException => None }
    }

  private[perf] def register(executionId: Long, record: () => Unit): Unit = synchronized {
    // A registration for an execution that already ended would never drain.
    if (active && !ended.contains(executionId)) {
      executions.getOrElseUpdate(executionId, ArrayBuffer.empty) += record
    }
  }

  private def executionEnded(executionId: Long): Unit = {
    val callbacks = synchronized {
      ended += executionId
      if (ended.size > ENDED_MEMORY) {
        ended -= ended.head
      }
      executions.remove(executionId)
    }
    if (active) {
      callbacks.foreach(_.foreach { record =>
        try {
          record()
        } catch {
          case t: Throwable if MetricHistory.isContained(t) =>
            logDebug(s"Observation skipped for execution $executionId: ${t.getClass.getName}")
        }
      })
    }
  }

  private class ObservationListener extends SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionEnd => executionEnded(e.executionId)
      case _ =>
    }
  }
}

/**
 * Starts and stops the history-backed heuristics. External dependencies are injected for tests.
 *
 * @param currentStore the installed store; `MetricStores.current()` in production
 */
private[perf] class HistoryLifecycle(
    heuristics: Seq[HistoryHeuristic],
    currentStore: () => MetricStore) extends Logging {

  /** Store at `start`; None while stopped. */
  private var storeAtStart: Option[MetricStore] = None
  /** Heuristics enabled at `start`. */
  private var started: Seq[HistoryHeuristic] = Nil

  /**
   * Enables the heuristics that `isOn` selects if a provider is requested. Stops any earlier
   * start first so nothing leaks across SparkContexts.
   *
   * @return whether any heuristic is enabled
   */
  def start(provider: String, policy: HistoryPolicy, addListener: SparkListener => Unit,
      isOn: HistoryHeuristic => Boolean = _ => true): Boolean = synchronized {
    stop()
    val requested = Option(provider).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    started = heuristics.filter(isOn)
    if (requested.isEmpty || requested == HistoryMetricsManager.NO_PROVIDER || started.isEmpty) {
      false
    } else {
      try {
        storeAtStart = Some(currentStore())
        HistoryObservations.start()
        // SparkContext-level: sees every session and streaming micro-batch.
        addListener(HistoryObservations.listener)
        started.foreach(_.enable(policy))
        logInfo(s"History-backed planning requested for ${started.map(_.name)}; it activates " +
          "once a history metrics provider is installed")
        true
      } catch {
        case t: Throwable if MetricHistory.isContained(t) =>
          logWarning(s"History-backed planning could not start (${t.getClass.getName}); " +
            "planning stays static")
          stop()
          false
      }
    }
  }

  /**
   * Called after provider selection. Stays on only if a provider store replaced the one seen at
   * `start`, then declares every family.
   */
  def activate(): Unit = synchronized {
    storeAtStart.foreach { before =>
      if (currentStore() eq before) {
        logInfo(s"History-backed planning for ${started.map(_.name)} is off: no history " +
          "metrics provider was installed")
        stop()
      } else {
        started.foreach(_.declare())
        logInfo(s"History-backed planning is active for ${started.map(_.name)}")
      }
    }
  }

  def stop(): Unit = synchronized {
    heuristics.foreach(_.disable())
    HistoryObservations.shutdown()
    storeAtStart = None
    started = Nil
  }
}

/** The application's history-backed heuristics, driven by the driver plugin. */
object HistoryHeuristics {

  private val lifecycle = new HistoryLifecycle(
    heuristics = Seq(ScanSplitHeuristic),
    currentStore = () => MetricStores.current())

  /** At driver plugin init. */
  def start(sc: SparkContext, conf: RapidsConf): Unit =
    lifecycle.start(conf.historyMetricsProvider,
      HistoryPolicy(Duration.ofMillis(conf.historyPlanningTimeoutMs),
        Duration.ofMillis(conf.historyDeclareTimeoutMs)),
      sc.addSparkListener,
      _.enabled(conf))

  /** After the provider manager ran. */
  def activate(): Unit = lifecycle.activate()

  /**
   * After the history manager's listener drain, so executions that ended last still record. This
   * never touches the store, so it is safe once the manager has closed it.
   */
  def stop(): Unit = lifecycle.stop()
}
