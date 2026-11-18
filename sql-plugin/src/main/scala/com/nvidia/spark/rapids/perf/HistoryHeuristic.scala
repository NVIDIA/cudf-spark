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

import com.nvidia.spark.history.{HistoryMetricCatalog, MetricStore, MetricStores}
import com.nvidia.spark.rapids.{HistoryMetricsManager, RapidsConf}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Runs the callbacks registered for a SQL execution when it ends, once its accumulators are
 * merged. A callback registered after its execution ended is dropped.
 */
object ExecutionEndCallbacks extends Logging {

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

  def listener: SparkListener = new ExecutionEndListener

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

  private class ExecutionEndListener extends SparkListener {
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
    heuristics: Seq[ScanSplitHeuristic],
    currentStore: () => MetricStore) extends Logging {

  /** Store at `start`; None while stopped. */
  private var storeAtStart: Option[MetricStore] = None
  /** Heuristics enabled at `start`. */
  private var started: Seq[ScanSplitHeuristic] = Nil

  /**
   * Every family these heuristics record, whether or not a heuristic is enabled, so the ids a
   * provider admits do not depend on configuration.
   */
  def catalog: HistoryMetricCatalog = HistoryMetric.catalogOf(heuristics.flatMap(_.metrics))

  /**
   * Enables the heuristics that `isOn` selects if a provider is requested. Stops any earlier
   * start first so nothing leaks across SparkContexts.
   *
   * @return whether any heuristic is enabled
   */
  def start(provider: String, policy: HistoryPolicy, addListener: SparkListener => Unit,
      isOn: ScanSplitHeuristic => Boolean = _ => true): Boolean = synchronized {
    stop()
    val requested = Option(provider).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    started = heuristics.filter(isOn)
    if (requested.isEmpty || requested == HistoryMetricsManager.NO_PROVIDER || started.isEmpty) {
      false
    } else {
      try {
        storeAtStart = Some(currentStore())
        ExecutionEndCallbacks.start()
        // SparkContext-level: sees every session and streaming micro-batch.
        addListener(ExecutionEndCallbacks.listener)
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
    ExecutionEndCallbacks.shutdown()
    storeAtStart = None
    started = Nil
  }
}

/** The application's history-backed heuristics, driven by the driver plugin. */
object HistoryHeuristics {

  private val lifecycle = new HistoryLifecycle(
    heuristics = Seq(ScanSplitHeuristic.instance),
    currentStore = () => MetricStores.current())

  /** At driver plugin init. */
  def start(sc: SparkContext, conf: RapidsConf): Unit =
    lifecycle.start(conf.historyMetricsProvider,
      HistoryPolicy(Duration.ofMillis(conf.historyPlanningTimeoutMs),
        Duration.ofMillis(conf.historyDeclareTimeoutMs)),
      sc.addSparkListener,
      _.enabled(conf))

  /** The catalog to open the history provider with. */
  def catalog: HistoryMetricCatalog = lifecycle.catalog

  /** After the provider manager ran. */
  def activate(): Unit = lifecycle.activate()

  /**
   * After the history manager's listener drain, so executions that ended last still record. This
   * never touches the store, so it is safe once the manager has closed it.
   */
  def stop(): Unit = lifecycle.stop()
}
