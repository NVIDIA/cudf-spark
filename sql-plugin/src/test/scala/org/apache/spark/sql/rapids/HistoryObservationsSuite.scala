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

package org.apache.spark.sql.rapids

import java.time.Duration

import com.nvidia.spark.rapids.perf.{FakeMetricStore, HistoryObservations, HistoryPolicy,
  ScanContext, ScanSplitHeuristic}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/** Observations drain on SQL execution end, driven by hand-built listener events. */
class HistoryObservationsSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val MiB = 1024L * 1024
  private val listener = HistoryObservations.listener
  private var store: FakeMetricStore = _
  private var heuristic: ScanSplitHeuristic = _

  override def beforeEach(): Unit = {
    store = new FakeMetricStore
    heuristic = new ScanSplitHeuristic(() => store)
    heuristic.enable(HistoryPolicy(Duration.ofMillis(100), Duration.ofSeconds(5)))
    HistoryObservations.start()
  }

  override def afterEach(): Unit = HistoryObservations.shutdown()

  /** A one-split scan of `cat.ns.events` that decoded 4x its 100 MiB on disk. */
  private def scan: ScanContext =
    ScanContext("cat.ns.events", 0L, 0L, 0L, 0L, 1, () => Seq((0, 400 * MiB, 100 * MiB)))

  test("records when the execution ends, once") {
    heuristic.register(Some(41L), scan)
    heuristic.register(None, scan) // planned outside an execution: not tracked
    assert(store.offered.isEmpty, "nothing is recorded before the execution ends")
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(41L, 0L))
    assert(store.stored.map(_.value()) == Seq(4.0))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(41L, 0L))
    assert(store.stored.size == 1)
  }

  test("nothing is held while observations are stopped") {
    HistoryObservations.shutdown()
    heuristic.register(Some(45L), scan)
    HistoryObservations.start()
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(45L, 0L))
    assert(store.offered.isEmpty)
  }
}
