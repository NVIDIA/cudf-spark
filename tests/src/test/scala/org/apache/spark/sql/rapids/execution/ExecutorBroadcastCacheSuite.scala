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
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.{Callable, CountDownLatch, Executors, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.TaskKilledException
import org.apache.spark.sql.rapids.GpuShuffleEnv

class ExecutorBroadcastCacheSuite extends AnyFunSuite {
  private class Value {
    private val refs = new AtomicInteger(1)
    def retain(): Value = {
      assert(refs.getAndIncrement() > 0, "Borrowed a released build")
      this
    }
    def release(): Unit = assert(refs.decrementAndGet() >= 0)
    def references: Int = refs.get()
  }

  private val noAction: () => Unit = () => ()

  test("first use needs no startup opt-in and retains the same cache owner") {
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String]))
    val expected = new IllegalStateException("expected build failure")
    val cache = env.broadcastCache
    assert(env.broadcastCache eq cache)
    try {
      val failure = intercept[IllegalStateException] {
        cache.getOrBuild[Int](1, () => throw expected, noAction, noAction)(_ => 0)
      }
      assert(failure eq expected)
    } finally {
      cache.close()
    }
  }

  test("cleanup before first build prevents late use from recreating state") {
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String]))
    env.broadcastCache.invalidate(1)
    try {
      intercept[IllegalStateException] {
        env.broadcastCache.getOrBuild[Int](1, () => fail("Retired build must not run"),
          noAction, noAction)(_ => 0)
      }
      env.broadcastCache.close()
      intercept[IllegalArgumentException] {
        env.broadcastCache.getOrBuild[Int](2, () => fail("Closed cache must not build"),
          noAction, noAction)(_ => 0)
      }
    } finally {
      env.broadcastCache.close()
    }
  }

  test("retired ordinary shuffles keep only compact IDs and reject late builders") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    try {
      (0 until 100000).foreach(cache.invalidate)
      assert(cache.activeEntryCount == 0)
      assert(cache.retiredShuffleCount == 100000)
      assert(cache.retiredStateBytes < 64 * 1024)
      Seq(0, 50000, 99999).foreach { id =>
        intercept[IllegalStateException] {
          cache.getOrBuild(id, () => fail("Retired ID must not build"),
            noAction, noAction)(_.retain())
        }
      }
      cache.close()
      assert(cache.retiredShuffleCount == 0)
    } finally {
      cache.close()
    }
  }

  test("a new consumer stage attempt builds a fresh replica and cleanup releases both") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val first = new Value
    val retry = new Value
    val originalGeneration = ExecutorBroadcastCache.generation(7, 0)
    val retryGeneration = ExecutorBroadcastCache.generation(7, 1)
    assert(originalGeneration != retryGeneration)
    assert(retryGeneration != ExecutorBroadcastCache.generation(8, 0))
    try {
      val originalBorrow = cache.getOrBuild(42, () => first, noAction, noAction,
        generation = originalGeneration)(_.retain())
      val retryBorrow = cache.getOrBuild(42, () => retry, noAction, noAction,
        generation = retryGeneration)(_.retain())
      assert(originalBorrow eq first)
      assert(retryBorrow eq retry)
      assert(cache.activeEntryCount == 2)
      cache.invalidate(42)
      assert(cache.activeEntryCount == 0)
      assert(first.references == 1 && retry.references == 1)
      originalBorrow.release()
      retryBorrow.release()
      assert(first.references == 0 && retry.references == 0)
    } finally {
      cache.close()
    }
  }

  test("consumers share one build and keep independent references after invalidation") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val value = new Value
    val builds = new AtomicInteger()
    def read(): Value = cache.getOrBuild(7, () => {
      builds.incrementAndGet()
      value
    }, noAction, noAction)(_.retain())
    try {
      val first = read()
      val second = read()
      assert(builds.get() == 1)
      assert(value.references == 3)
      cache.invalidate(7)
      assert(value.references == 2)
      intercept[IllegalStateException](read())
      assert(builds.get() == 1)
      first.release()
      second.release()
      assert(value.references == 0)
    } finally {
      cache.close()
    }
  }

  test("invalidation racing build publication releases the unpublished value") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val value = new Value
    try {
      val result = pool.submit(new Callable[Boolean] {
        override def call(): Boolean = {
          intercept[IllegalStateException] {
            cache.getOrBuild(8, () => {
              started.countDown()
              assert(finish.await(10, TimeUnit.SECONDS))
              value
            }, noAction, noAction)(_.retain())
          }
          true
        }
      })
      assert(started.await(10, TimeUnit.SECONDS))
      cache.invalidate(8)
      finish.countDown()
      assert(result.get(10, TimeUnit.SECONDS))
      assert(value.references == 0)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

  test("concurrent consumers wait for one builder and borrow independent references") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newFixedThreadPool(2)
    val started = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val value = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild(12, () => {
          started.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          value
        }, noAction, noAction)(_.retain())
      })
      assert(started.await(10, TimeUnit.SECONDS))
      val waiter = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild(12,
          () => fail("Concurrent consumer must reuse the build"),
          () => waiting.countDown(), noAction)(_.retain())
      })
      assert(waiting.await(10, TimeUnit.SECONDS))
      finish.countDown()
      val first = builder.get(10, TimeUnit.SECONDS)
      val second = waiter.get(10, TimeUnit.SECONDS)
      assert(value.references == 3)
      cache.invalidate(12)
      first.release()
      second.release()
      assert(value.references == 0)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

  test("application shutdown rejects late publication and new consumers") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val value = new Value
    intercept[IllegalStateException] {
      cache.getOrBuild(13, () => {
        cache.close()
        value
      }, noAction, noAction)(_.retain())
    }
    assert(value.references == 0)
    intercept[IllegalArgumentException] {
      cache.getOrBuild(14, () => fail("Closed cache must not build"),
        noAction, noAction)(_.retain())
    }
  }

  test("a cancelled waiter releases task resources and does not cancel another task's build") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val released = new AtomicInteger()
    val value = new Value
    try {
      val result = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild(9, () => {
          started.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          value
        }, noAction, noAction)(_.retain())
      })
      assert(started.await(10, TimeUnit.SECONDS))
      intercept[InterruptedException] {
        cache.getOrBuild(9, () => fail("A waiter must not build"),
          () => { released.incrementAndGet(); () },
          () => if (released.get() > 0) throw new InterruptedException)(_.retain())
      }
      assert(released.get() == 1)
      finish.countDown()
      val borrowed = result.get(10, TimeUnit.SECONDS)
      assert(value.references == 2)
      cache.close()
      assert(value.references == 1)
      borrowed.release()
      assert(value.references == 0)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

  test("a failed build can be retried but an unregistered shuffle cannot be recreated") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    try {
      intercept[IllegalArgumentException] {
        cache.getOrBuild(10, () => throw new IllegalArgumentException,
          noAction, noAction)(_.retain())
      }
      val value = cache.getOrBuild(10, () => new Value, noAction, noAction)(_.retain())
      value.release()
      cache.invalidate(11)
      intercept[IllegalStateException] {
        cache.getOrBuild(11, () => fail("Invalidated shuffle must not build"),
          noAction, noAction)(_.retain())
      }
      cache.close()
      assert(value.references == 0)
    } finally {
      cache.close()
    }
  }

  Seq[Throwable](new InterruptedException("builder cancelled"),
    new TaskKilledException("builder cancelled")).foreach { cancellation =>
    test(s"a healthy waiter rebuilds after ${cancellation.getClass.getSimpleName}") {
      val cache = new ExecutorBroadcastCache[Value](_.release())
      val pool = Executors.newFixedThreadPool(2)
      val building = new CountDownLatch(1)
      val waiting = new CountDownLatch(1)
      val failBuilder = new CountDownLatch(1)
      val builds = new AtomicInteger()
      val value = new Value
      try {
        val builder = pool.submit(new Callable[Value] {
          override def call(): Value = cache.getOrBuild(77, () => {
            builds.incrementAndGet()
            building.countDown()
            assert(failBuilder.await(10, TimeUnit.SECONDS))
            throw cancellation
          }, noAction, noAction)(_.retain())
        })
        assert(building.await(10, TimeUnit.SECONDS))
        val waiter = pool.submit(new Callable[Value] {
          override def call(): Value = cache.getOrBuild(77, () => {
            builds.incrementAndGet()
            value
          }, () => waiting.countDown(), noAction)(_.retain())
        })
        assert(waiting.await(10, TimeUnit.SECONDS))
        failBuilder.countDown()
        val failure = intercept[ExecutionException] {
          builder.get(10, TimeUnit.SECONDS)
        }
        assert(failure.getCause eq cancellation)
        val borrowed = waiter.get(10, TimeUnit.SECONDS)
        assert(borrowed eq value)
        assert(builds.get() == 2)
        borrowed.release()
        cache.invalidate(77)
        assert(value.references == 0)
      } finally {
        failBuilder.countDown()
        pool.shutdownNow()
        cache.close()
      }
    }
  }

  test("invalidation does not wait for materialization and defers disposal until borrow ends") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newFixedThreadPool(2)
    val reading = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val value = new Value
    try {
      val borrower = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild(78, () => value,
          noAction, noAction) { owned =>
          reading.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          assert(owned.references == 1)
          owned.retain()
        }
      })
      assert(reading.await(10, TimeUnit.SECONDS))
      val invalidation = pool.submit(new Callable[Unit] {
        override def call(): Unit = cache.invalidate(78)
      })
      invalidation.get(5, TimeUnit.SECONDS)
      assert(value.references == 1)
      intercept[IllegalStateException] {
        cache.getOrBuild(78, () => fail("Retired build must not run"),
          noAction, noAction)(_.retain())
      }
      finish.countDown()
      val borrowed = borrower.get(10, TimeUnit.SECONDS)
      assert(value.references == 1)
      borrowed.release()
      assert(value.references == 0)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

}
