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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.history.MetricStore
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Starting, activating and stopping history-backed planning, and the observation registry,
 * with everything outside the package injected. Nothing here starts Spark.
 */
class HistoryLifecycleSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val policy = HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5))

  private var preProvider: FakeMetricStore = _
  private var current: MetricStore = _
  private var heuristic: ScanSplitHeuristic = _
  private var listeners: ArrayBuffer[SparkListener] = _
  private var lifecycle: HistoryLifecycle = _

  override def beforeEach(): Unit = {
    preProvider = new FakeMetricStore
    current = preProvider
    heuristic = new ScanSplitHeuristic(() => current)
    listeners = ArrayBuffer.empty
    lifecycle = new HistoryLifecycle(Seq(heuristic), () => current)
  }

  override def afterEach(): Unit = lifecycle.stop()

  private def start(provider: String): Boolean =
    lifecycle.start(provider, policy, l => listeners += l)

  test("no provider requested: nothing starts") {
    Seq("none", "NONE", " None ", "", "   ", null).foreach { provider =>
      assert(!start(provider), s"'$provider'")
      assert(!heuristic.isEnabled)
      assert(listeners.isEmpty, s"'$provider'")
    }
  }

  test("a named provider enables planning and the listener") {
    assert(start("local"))
    assert(heuristic.isEnabled)
    assert(listeners.size == 1)
  }

  test("no heuristic switched on: nothing starts") {
    assert(!lifecycle.start("local", policy, l => listeners += l, _ => false))
    assert(!heuristic.isEnabled)
    assert(listeners.isEmpty)
  }

  test("activation keeps planning only if a provider store was installed") {
    assert(start("local"))
    lifecycle.activate()
    assert(!heuristic.isEnabled, "the store did not change: no provider was installed")

    assert(start("local"))
    val provided = new FakeMetricStore
    current = provided
    lifecycle.activate()
    assert(heuristic.isEnabled)
    assert(provided.declareCalls.size == 1, "families are declared on activation")
    assert(preProvider.declareCalls.isEmpty)
  }

  test("a restart resets everything an earlier start left") {
    assert(start("local"))
    HistoryObservations.register(5L, () => ())
    assert(HistoryObservations.trackedCount == 1)
    assert(!start("none"))
    assert(!heuristic.isEnabled)
    assert(HistoryObservations.trackedCount == 0)
    assert(start("local"))
    assert(heuristic.isEnabled && listeners.size == 2)
  }

  test("the registry forgets an execution at its end and drops late registrations") {
    assert(start("local"))
    HistoryObservations.register(9L, () => ())
    assert(HistoryObservations.trackedCount == 1)
    listeners.head.onOtherEvent(SparkListenerSQLExecutionEnd(9L, 0L))
    assert(HistoryObservations.trackedCount == 0)
    HistoryObservations.register(9L, () => ())
    assert(HistoryObservations.trackedCount == 0)
  }
}
