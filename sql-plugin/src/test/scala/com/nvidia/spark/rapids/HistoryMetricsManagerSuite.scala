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

import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util
import java.util.concurrent.{CountDownLatch, Executors, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.nvidia.spark.history._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class HistoryMetricsManagerSuite extends AnyFunSuite {
  import Arm.withResource
  private val configuration = util.Collections.singletonMap("spark.test.key", "value")
  private val applicationId = "application-1"
  private val applicationAttemptId = "attempt-2"
  private val producerVersion = "26.10.0-test"

  private def initialize(manager: HistoryMetricsManager, providerName: String): Unit = {
    manager.initialize(
      configuration, applicationId, applicationAttemptId, producerVersion, providerName)
  }

  test("none keeps the no-op store without discovering providers") {
    val initial = MetricStores.current()
    val manager = new HistoryMetricsManager(() =>
      throw new AssertionError("provider discovery should not run"))

    initialize(manager, "none")

    assert(MetricStores.current() eq initial)
    manager.shutdown(() => throw new AssertionError("disabled history should not wait"))
  }

  test("selected provider is installed and shut down") {
    val initial = MetricStores.current()
    val provider = new TestProvider("local", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(provider))

    try {
      initialize(manager, "LOCAL")

      assert(provider.openCalls == 1)
      assert(provider.configuration ne configuration)
      assert(provider.configuration.get("spark.test.key") == "value")
      intercept[UnsupportedOperationException] {
        provider.configuration.put("spark.test.key", "changed")
      }
      assert(provider.applicationId == applicationId)
      assert(provider.applicationAttemptId == applicationAttemptId)
      assert(provider.producerVersion == producerVersion)
      assert(MetricStores.current() eq provider.store)
    } finally {
      manager.shutdown()
    }
    assert(provider.shutdownCalls == 1)
    assert(provider.shutdownTimeout == HistoryMetricsManager.PROVIDER_SHUTDOWN_TIMEOUT)
    assert(MetricStores.current() eq initial)
  }

  test("shutdown waits for an in-flight Spark listener before removing the store") {
    val initial = MetricStores.current()
    val recorded = new AtomicBoolean()
    val observation = new Observation(new MetricVersionId(1, 1),
      util.Collections.emptyMap[String, DimValue](), 1.0, System.currentTimeMillis())
    val store = new DelegatingStore(initial) {
      override def record(observation: Observation): Unit = recorded.set(true)
    }
    val provider = new TestProvider("local", store)
    val manager = new HistoryMetricsManager(() => Seq(provider))
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    val executor = Executors.newSingleThreadExecutor()
    val sc = new SparkContext(new SparkConf(false)
      .setMaster("local[1]")
      .setAppName("history-metrics-shutdown")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1"))
    try {
      initialize(manager, "local")
      sc.addSparkListener(new SparkListener {
        override def onJobEnd(event: SparkListenerJobEnd): Unit = {
          entered.countDown()
          if (release.await(10, TimeUnit.SECONDS)) {
            MetricStores.current().record(observation)
          }
        }
      })
      TrampolineUtil.postEvent(sc, SparkListenerJobEnd(1, System.currentTimeMillis(), JobSucceeded))
      assert(entered.await(10, TimeUnit.SECONDS))
      val shutdown = executor.submit(new Runnable {
        override def run(): Unit = manager.shutdown(() => {
          waiting.countDown()
          TrampolineUtil.waitForListenerEvents(sc,
            HistoryMetricsManager.LISTENER_SHUTDOWN_TIMEOUT.toMillis)
        })
      })
      assert(waiting.await(10, TimeUnit.SECONDS))
      assert(!shutdown.isDone)
      assert(MetricStores.current() eq store)
      release.countDown()
      shutdown.get(10, TimeUnit.SECONDS)
      assert(recorded.get())
      assert(provider.shutdownCalls == 1)
      assert(MetricStores.current() eq initial)
      manager.shutdown(() => throw new AssertionError("already shut down"))
    } finally {
      release.countDown()
      executor.shutdown()
      try {
        assert(executor.awaitTermination(10, TimeUnit.SECONDS))
      } finally {
        manager.shutdown()
        sc.stop()
      }
    }
  }

  test("listener timeout still removes the store and shuts down the provider") {
    val initial = MetricStores.current()
    val provider = new TestProvider("local", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(provider))
    try {
      initialize(manager, "local")
      manager.shutdown(() => {
        assert(MetricStores.current() eq provider.store)
        assert(provider.shutdownCalls == 0)
        throw new TimeoutException("listener did not finish")
      })
      assert(provider.shutdownCalls == 1)
      assert(MetricStores.current() eq initial)
    } finally {
      manager.shutdown()
    }
  }

  test("missing provider keeps the no-op store") {
    val initial = MetricStores.current()
    val provider = new TestProvider("database", new DelegatingStore(initial))
    val manager = new HistoryMetricsManager(() => Seq(provider))

    initialize(manager, "local")

    assert(provider.openCalls == 0)
    assert(MetricStores.current() eq initial)
    manager.shutdown()
    assert(provider.shutdownCalls == 0)
  }

  test("failed provider is cleaned up and leaves the no-op store") {
    val initial = MetricStores.current()
    val provider = new TestProvider("local", new DelegatingStore(initial), failOpen = true)
    val manager = new HistoryMetricsManager(() => Seq(provider))

    initialize(manager, "local")

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

    initialize(manager, "local")

    assert(first.openCalls == 0)
    assert(second.openCalls == 0)
    assert(MetricStores.current() eq initial)
    manager.shutdown()
  }

  test("discovery finds application providers in a child context loader") {
    val apiClass = classOf[HistoryMetricsProvider]
    val providerClass = classOf[HistoryMetricsDiscoveryTestProvider]
    val directory = Files.createTempDirectory("history-metrics-services")
    val services = Files.createDirectories(directory.resolve("META-INF/services"))
    val serviceFile = services.resolve(apiClass.getName)
    try {
      Files.write(serviceFile, providerClass.getName.getBytes(StandardCharsets.UTF_8))
      val urls = Array(directory.toUri.toURL,
        providerClass.getProtectionDomain.getCodeSource.getLocation)
      withResource(new URLClassLoader(urls, apiClass.getClassLoader) {
        override def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
          if (name == providerClass.getName) {
            val loaded: Class[_] = Option(findLoadedClass(name)).getOrElse(findClass(name))
            if (resolve) {
              resolveClass(loaded)
            }
            loaded
          } else {
            super.loadClass(name, resolve)
          }
        }
      }) { loader =>
        withContextLoader(loader) {
          val provider = HistoryMetricsManager.discover()
            .find(_.getClass.getName == providerClass.getName).get
          assert(provider.getClass.getClassLoader eq loader)
          assert(apiClass.isInstance(provider))
        }
      }
    } finally {
      Files.deleteIfExists(serviceFile)
      Files.deleteIfExists(services)
      Files.deleteIfExists(directory.resolve("META-INF"))
      Files.deleteIfExists(directory)
    }
  }

  test("discovery falls back when the context loader is absent") {
    withContextLoader(null) {
      assert(HistoryMetricsManager.discover().exists(_.name() == "local"))
    }
  }

  test("discovery falls back when the context loader has a separate history API") {
    val apiClass = classOf[HistoryMetricsProvider]
    val urls = Array(apiClass.getProtectionDomain.getCodeSource.getLocation)
    withResource(new URLClassLoader(urls, null)) { loader =>
      assert(loader.loadClass(apiClass.getName) ne apiClass)
      withContextLoader(loader) {
        assert(HistoryMetricsManager.discover().exists(_.name() == "local"))
      }
    }
  }

  private def withContextLoader[T](loader: ClassLoader)(body: => T): T = {
    val thread = Thread.currentThread()
    val previous = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(loader)
      body
    } finally {
      thread.setContextClassLoader(previous)
    }
  }

  private class TestProvider(
      providerName: String,
      val store: MetricStore,
      failOpen: Boolean = false,
      shutdownResult: Boolean = true) extends HistoryMetricsProvider {
    var openCalls = 0
    var shutdownCalls = 0
    var shutdownTimeout: Duration = _
    var configuration: util.Map[String, String] = _
    var applicationId: String = _
    var applicationAttemptId: String = _
    var producerVersion: String = _

    override def name(): String = providerName

    override def open(
        configuration: util.Map[String, String],
        applicationId: String,
        applicationAttemptId: String,
        producerVersion: String): MetricStore = {
      openCalls += 1
      this.configuration = configuration
      this.applicationId = applicationId
      this.applicationAttemptId = applicationAttemptId
      this.producerVersion = producerVersion
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

// Public zero-argument service fixture, loaded by the application's child classloader.
class HistoryMetricsDiscoveryTestProvider extends HistoryMetricsProvider {
  override def name(): String = "test-child"

  override def open(
      configuration: util.Map[String, String],
      applicationId: String,
      applicationAttemptId: String,
      producerVersion: String): MetricStore = MetricStores.current()

  override def shutdown(timeout: Duration): Boolean = true
}
