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

import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.{RapidsConf, SpillableColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskKilledException
import org.apache.spark.sql.rapids.{GpuShuffleEnv, ShuffleReadSnapshot}
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

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

  test("unused feature cleanup creates no cache and shutdown prevents first use") {
    val creations = new AtomicInteger(0)
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String])) {
      override protected def createBroadcastCache():
          ExecutorBroadcastCache[SpillableColumnarBatch] = {
        creations.incrementAndGet()
        super.createBroadcastCache()
      }
    }
    var cleanups = 0
    assert(!env.isBroadcastCacheInitialized)
    (0 until 100).foreach { id =>
      env.invalidateBroadcastAndThen(id) { cleanups += 1 }
    }
    assert(cleanups == 100)
    assert(!env.isBroadcastCacheInitialized && creations.get() == 0)
    env.closeBroadcastCacheAndThen { cleanups += 1 }
    assert(cleanups == 101 && creations.get() == 0)
    val error = intercept[IllegalArgumentException] { env.broadcastCache }
    assert(error.getMessage.contains("closed"))
    assert(!env.isBroadcastCacheInitialized && creations.get() == 0)
    env.closeBroadcastCacheAndThen { cleanups += 1 }
    assert(cleanups == 102 && creations.get() == 0)
  }

  test("first use needs no startup opt-in and retains the same cache owner") {
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String]))
    val expected = new IllegalStateException("expected build failure")
    assert(!env.isBroadcastCacheInitialized)
    env.invalidateBroadcastAndThen(1) { () }
    assert(!env.isBroadcastCacheInitialized)
    val cache = env.broadcastCache
    assert(env.isBroadcastCacheInitialized)
    assert(env.broadcastCache eq cache)
    try {
      val failure = intercept[IllegalStateException] {
        cache.getOrBuild[Int](1, () => throw expected, noAction, noAction)(_ => 0, _ => ())
      }
      assert(failure eq expected)
    } finally {
      env.closeBroadcastCacheAndThen { () }
    }
    assert(!env.isBroadcastCacheInitialized)
    intercept[IllegalArgumentException] { env.broadcastCache }
  }

  test("cleanup without a cached build retains no state and allows a later build") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    try {
      (0 until 100000).foreach(cache.invalidate)
      assert(cache.activeEntryCount == 0)
      val value = cache.getOrBuild[Value](1, () => new Value,
        noAction, noAction)(_.retain(), _.release())
      assert(cache.activeEntryCount == 1)
      value.release()
      cache.close()
      assert(value.references == 0)
      intercept[IllegalArgumentException] {
        cache.getOrBuild[Value](1, () => fail("Closed cache must not build"),
          noAction, noAction)(_.retain(), _.release())
      }
    } finally {
      cache.close()
    }
  }

  test("output identity is independent of address and block enumeration order") {
    val a = BlockManagerId("a", "host-a", 1000)
    val b = BlockManagerId("b", "host-b", 1001)
    val first = ShuffleReadSnapshot(Vector(
      a -> Vector((ShuffleBlockId(42, 1001L, 0), 10L, 0),
        (ShuffleBlockId(42, 1003L, 0), 30L, 2)),
      b -> Vector((ShuffleBlockId(42, 1002L, 0), 20L, 1))))
    val reordered = ShuffleReadSnapshot(first.blocks.reverse.map { case (address, blocks) =>
      address -> blocks.reverse
    })
    assert(first.buildIdentity(42) == reordered.buildIdentity(42))
    assert(first.buildIdentity(42).hashCode() == reordered.buildIdentity(42).hashCode())
    assert(first.buildIdentity(42).outputs.map(_.mapIndex) == Vector(0, 1, 2))
    intercept[IllegalArgumentException] {
      ShuffleReadSnapshot(first.blocks ++ first.blocks).buildIdentity(42)
    }
    intercept[IllegalArgumentException](first.buildIdentity(43))
    assert(ShuffleReadSnapshot(Vector.empty).buildIdentity(42) == BroadcastBuildIdentity.empty)
  }

  test("unchanged outputs share a replica and recomputed outputs build a new one") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val address = BlockManagerId("a", "host-a", 1000)
    def identity(attempt: Long): BroadcastBuildIdentity = BroadcastBuildIdentity(Seq(
      BroadcastBuildIdentity.MapOutput(0, attempt, address)))
    val first = new Value
    val retry = new Value
    try {
      val originalBorrow = cache.getOrBuild[Value](42, () => first, noAction, noAction,
        identity = identity(1001))( _.retain(), _.release())
      val repeatedBorrow = cache.getOrBuild[Value](42, () => fail("Outputs have not changed"),
        noAction, noAction, identity = identity(1001))(_.retain(), _.release())
      repeatedBorrow.release()
      val retryBorrow = cache.getOrBuild[Value](42, () => retry, noAction, noAction,
        identity = identity(1101))(_.retain(), _.release())
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
    def read(): Value = cache.getOrBuild[Value](7, () => {
      builds.incrementAndGet()
      value
    }, noAction, noAction)(_.retain(), _.release())
    try {
      val first = read()
      val second = read()
      assert(builds.get() == 1)
      assert(value.references == 3)
      cache.invalidate(7)
      assert(value.references == 2)
      val replacement = cache.getOrBuild[Value](7, () => new Value, noAction, noAction)(
        _.retain(), _.release())
      assert(replacement ne value)
      replacement.release()
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
            cache.getOrBuild[Value](8, () => {
              started.countDown()
              assert(finish.await(10, TimeUnit.SECONDS))
              value
            }, noAction, noAction)(_.retain(), _.release())
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
        override def call(): Value = cache.getOrBuild[Value](12, () => {
          started.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          value
        }, noAction, noAction)(_.retain(), _.release())
      })
      assert(started.await(10, TimeUnit.SECONDS))
      val waiter = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](12,
          () => fail("Concurrent consumer must reuse the build"),
          () => waiting.countDown(), noAction)(_.retain(), _.release())
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

  test("an invalidated builder cannot publish over or remove a same-key replacement") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val old = new Value
    val replacement = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](9, () => {
          started.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          old
        }, noAction, noAction)(_.retain(), _.release())
      })
      assert(started.await(10, TimeUnit.SECONDS))
      cache.invalidate(9)
      val borrowed = cache.getOrBuild[Value](9, () => replacement,
        noAction, noAction)(_.retain(), _.release())
      borrowed.release()
      finish.countDown()
      assert(intercept[ExecutionException](builder.get(10, TimeUnit.SECONDS))
        .getCause.isInstanceOf[IllegalStateException])
      assert(old.references == 0)
      val reused = cache.getOrBuild[Value](9, () => fail("Replacement must survive"),
        noAction, noAction)(_.retain(), _.release())
      assert(reused eq replacement)
      reused.release()
      assert(cache.activeEntryCount == 1)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
    assert(replacement.references == 0)
  }

  test("application shutdown rejects late publication and new consumers") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val value = new Value
    intercept[IllegalStateException] {
      cache.getOrBuild[Value](13, () => {
        cache.close()
        value
      }, noAction, noAction)(_.retain(), _.release())
    }
    assert(value.references == 0)
    intercept[IllegalArgumentException] {
      cache.getOrBuild[Value](14, () => fail("Closed cache must not build"),
        noAction, noAction)(_.retain(), _.release())
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
        override def call(): Value = cache.getOrBuild[Value](9, () => {
          started.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          value
        }, noAction, noAction)(_.retain(), _.release())
      })
      assert(started.await(10, TimeUnit.SECONDS))
      intercept[InterruptedException] {
        cache.getOrBuild[Value](9, () => fail("A waiter must not build"),
          () => { released.incrementAndGet(); () },
          () => if (released.get() > 0) throw new InterruptedException)(_.retain(), _.release())
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

  test("failed and invalidated builds can be recreated while the cache is open") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    try {
      intercept[IllegalArgumentException] {
        cache.getOrBuild[Value](10, () => throw new IllegalArgumentException,
          noAction, noAction)(_.retain(), _.release())
      }
      val value = cache.getOrBuild[Value](10, () => new Value, noAction, noAction)(
        _.retain(), _.release())
      value.release()
      cache.invalidate(11)
      val rebuilt = cache.getOrBuild[Value](11, () => new Value,
        noAction, noAction)(_.retain(), _.release())
      rebuilt.release()
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
          override def call(): Value = cache.getOrBuild[Value](77, () => {
            builds.incrementAndGet()
            building.countDown()
            assert(failBuilder.await(10, TimeUnit.SECONDS))
            throw cancellation
          }, noAction, noAction)(_.retain(), _.release())
        })
        assert(building.await(10, TimeUnit.SECONDS))
        val waiter = pool.submit(new Callable[Value] {
          override def call(): Value = cache.getOrBuild[Value](77, () => {
            builds.incrementAndGet()
            value
          }, () => waiting.countDown(), noAction)(_.retain(), _.release())
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
        override def call(): Value = cache.getOrBuild[Value](78, () => value,
          noAction, noAction)({ owned =>
          reading.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          assert(owned.references == 1)
          owned.retain()
        }, _.release())
      })
      assert(reading.await(10, TimeUnit.SECONDS))
      val invalidation = pool.submit(new Callable[Unit] {
        override def call(): Unit = cache.invalidate(78)
      })
      invalidation.get(5, TimeUnit.SECONDS)
      assert(value.references == 1)
      val replacement = cache.getOrBuild[Value](78, () => new Value,
        noAction, noAction)(_.retain(), _.release())
      replacement.release()
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

  test("retirement failure preserves the original borrow failure") {
    val value = new Value
    val primary = new IllegalStateException("borrow failed")
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    try {
      val error = intercept[IllegalStateException] {
        cache.getOrBuild[Value](79, () => value, noAction, noAction)({ _ =>
          cache.invalidate(79)
          throw primary
        }, _.release())
      }
      assert(error eq primary)
      assert(error.getSuppressed.toSeq == Seq(cleanup))
      assert(value.references == 0)
      assert(cache.activeEntryCount == 0)
    } finally {
      cache.close()
    }
  }

  test("retirement failure releases a result that cannot reach its caller") {
    val value = new Value
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    try {
      val error = intercept[IllegalArgumentException] {
        cache.getOrBuild[Value](80, () => value, noAction, noAction)({ owned =>
          val borrowed = owned.retain()
          cache.invalidate(80)
          borrowed
        }, _.release())
      }
      assert(error eq cleanup)
      assert(value.references == 0)
      assert(cache.activeEntryCount == 0)
    } finally {
      cache.close()
    }
  }

  test("borrowed-result cleanup failure is suppressed under the retirement failure") {
    val value = new Value
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val borrowedCleanup = new IllegalStateException("borrowed disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    try {
      val error = intercept[IllegalArgumentException] {
        cache.getOrBuild[Value](81, () => value, noAction, noAction)({ owned =>
          val borrowed = owned.retain()
          cache.invalidate(81)
          borrowed
        }, borrowed => {
          borrowed.release()
          throw borrowedCleanup
        })
      }
      assert(error eq cleanup)
      assert(error.getSuppressed.toSeq == Seq(borrowedCleanup))
      assert(value.references == 0)
    } finally {
      cache.close()
    }
  }

  test("suppressed interruption during retirement restores the interrupt status") {
    val value = new Value
    val primary = new IllegalStateException("borrow failed")
    val cleanup = new InterruptedException("owner disposal interrupted")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    try {
      val error = intercept[IllegalStateException] {
        cache.getOrBuild[Value](82, () => value, noAction, noAction)({ _ =>
          cache.invalidate(82)
          throw primary
        }, _.release())
      }
      assert(error eq primary)
      assert(error.getSuppressed.toSeq == Seq(cleanup))
      assert(Thread.currentThread().isInterrupted)
      assert(value.references == 0)
    } finally {
      Thread.interrupted()
      cache.close()
    }
  }

  test("rejected publication retains its invalidation error when disposal fails") {
    val value = new Value
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    try {
      val error = intercept[IllegalStateException] {
        cache.getOrBuild[Value](83, () => {
          cache.invalidate(83)
          value
        }, noAction, noAction)(_.retain(), _.release())
      }
      assert(error.getMessage.contains("invalidated"))
      assert(error.getSuppressed.toSeq == Seq(cleanup))
      assert(value.references == 0)
    } finally {
      cache.close()
    }
  }

  test("shutdown attempts every owner even when disposers throw the same exception") {
    val values = Seq(new Value, new Value)
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw cleanup
    })
    values.zipWithIndex.foreach { case (value, index) =>
      val borrowed = cache.getOrBuild[Value](index, () => value,
        noAction, noAction)(_.retain(), _.release())
      borrowed.release()
    }
    val error = intercept[IllegalArgumentException](cache.close())
    assert(error eq cleanup)
    assert(error.getSuppressed.isEmpty)
    assert(values.forall(_.references == 0))
    assert(cache.activeEntryCount == 0)
  }

}
