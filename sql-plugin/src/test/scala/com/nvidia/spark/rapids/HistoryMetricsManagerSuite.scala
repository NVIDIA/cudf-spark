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

import java.time.Duration
import java.util

import com.nvidia.spark.history._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkContext

class HistoryMetricsManagerSuite extends AnyFunSuite {
  test("none keeps the no-op store without discovering providers") {
    val initial = MetricStores.current()
    val manager = new HistoryMetricsManager(() =>
      throw new AssertionError("provider discovery should not run"))

    manager.initialize(null, "none")

    assert(MetricStores.current() eq initial)
    manager.shutdown()
  }

  test("selected provider is installed and shut down") {
    val initial = MetricStores.current()
    val provider = new TestProvider("local", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(provider))

    manager.initialize(null, "LOCAL")

    assert(provider.openCalls == 1)
    assert(MetricStores.current() eq provider.store)
    manager.shutdown()
    assert(provider.shutdownCalls == 1)
    assert(provider.shutdownTimeout == HistoryMetricsManager.PROVIDER_SHUTDOWN_TIMEOUT)
    assert(MetricStores.current() eq initial)
  }

  test("missing provider keeps the no-op store") {
    val initial = MetricStores.current()
    val provider = new TestProvider("database", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(provider))

    manager.initialize(null, "local")

    assert(provider.openCalls == 0)
    assert(MetricStores.current() eq initial)
    manager.shutdown()
    assert(provider.shutdownCalls == 0)
  }

  test("failed provider is cleaned up and leaves the no-op store") {
    val initial = MetricStores.current()
    val provider = new TestProvider("local", new DelegatingStore(initial), failOpen = true)
    val manager = new HistoryMetricsManager(() => Seq(provider))

    manager.initialize(null, "local")

    assert(provider.openCalls == 1)
    assert(provider.shutdownCalls == 1)
    assert(provider.shutdownTimeout == HistoryMetricsManager.PROVIDER_SHUTDOWN_TIMEOUT)
    assert(MetricStores.current() eq initial)
    manager.shutdown()
    assert(provider.shutdownCalls == 1)
  }

  test("duplicate provider names are rejected without opening either provider") {
    val initial = MetricStores.current()
    val first = new TestProvider("local", new DelegatingStore(initial))
    val second = new TestProvider("LOCAL", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(first, second))

    manager.initialize(null, "local")

    assert(first.openCalls == 0)
    assert(second.openCalls == 0)
    assert(MetricStores.current() eq initial)
    manager.shutdown()
  }

  private class TestProvider(
      providerName: String,
      val store: MetricStore,
      failOpen: Boolean = false,
      shutdownResult: Boolean = true) extends HistoryMetricsProvider {
    var openCalls = 0
    var shutdownCalls = 0
    var shutdownTimeout: Duration = _

    override def name(): String = providerName

    override def open(sparkContext: SparkContext): MetricStore = {
      openCalls += 1
      if (failOpen) {
        throw new IllegalStateException("injected open failure")
      }
      store
    }

    override def shutdown(timeout: Duration): Boolean = {
      shutdownCalls += 1
      shutdownTimeout = timeout
      shutdownResult
    }
  }

  private class DelegatingStore(delegate: MetricStore) extends MetricStore {
    override def declare(
        schemas: util.List[MetricSchema],
        timeout: Duration): util.List[SchemaStatus] = delegate.declare(schemas, timeout)

    override def record(observation: Observation): Unit = delegate.record(observation)

    override def summarize(
        requests: util.List[SummaryRequest],
        timeout: Duration): util.List[SummaryResponse] = delegate.summarize(requests, timeout)

    override def info(): BackendInfo = delegate.info()
  }
}
