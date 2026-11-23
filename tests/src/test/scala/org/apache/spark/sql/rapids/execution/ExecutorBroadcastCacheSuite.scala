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

  private val buildBytes = 64L

  /** An identity with one output per map index, which carries the map id given for it. */
  private def outputs(mapIds: Long*): BroadcastBuildIdentity = {
    val address = BlockManagerId("a", "host-a", 1000)
    BroadcastBuildIdentity(mapIds.zipWithIndex.map { case (mapId, mapIndex) =>
      BroadcastBuildIdentity.MapOutput(mapIndex, mapId, address)
    })
  }

  /** Publishes a build, which the cache's reference then holds alone. */
  private def publish(
      cache: ExecutorBroadcastCache[Value],
      id: Int,
      identity: BroadcastBuildIdentity,
      value: Value): Unit =
    cache.getOrBuild[Value](id, () => value, noAction, noAction, identity = identity)(
      _.retain(), _.release()).release()

  private def reclaim(
      cache: ExecutorBroadcastCache[Value],
      id: Int,
      isStopped: () => Boolean = () => false): (Int, Long) =
    cache.reclaim(id, isStopped, _ => buildBytes)

  private def countingCache(disposals: AtomicInteger): ExecutorBroadcastCache[Value] =
    new ExecutorBroadcastCache[Value](owned => {
      disposals.incrementAndGet()
      owned.release()
    })

  test("a reclaim detaches every published build of its shuffle, a rerun's included, keeps " +
      "every other shuffle's builds, and a second reclaim finds nothing") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val detached = Seq.fill(3)(new Value)
    val otherShuffle = new Value
    try {
      publish(cache, 1, outputs(99L, 100L), detached(0))
      publish(cache, 1, outputs(101L), detached(1))
      publish(cache, 1, BroadcastBuildIdentity.empty, detached(2))
      publish(cache, 2, outputs(1L), otherShuffle)
      assertResult((3, 3 * buildBytes))(reclaim(cache, 1))
      assertResult(Seq(0, 0, 0))(detached.map(_.references))
      assertResult(1)(otherShuffle.references)
      assertResult(1)(cache.activeEntryCount)
      assertResult((0, 0L))(reclaim(cache, 1))
    } finally {
      cache.close()
    }
    assertResult(0)(otherShuffle.references)
  }

  test("a reclaim keeps a build in flight, which then publishes to its waiter") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val pool = Executors.newFixedThreadPool(2)
    val building = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val value = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](4, () => {
          building.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          value
        }, noAction, noAction)(_.retain(), _.release())
      })
      assert(building.await(10, TimeUnit.SECONDS))
      val waiter = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](4,
          () => fail("A waiter must not build"),
          () => waiting.countDown(), noAction)(_.retain(), _.release())
      })
      assert(waiting.await(10, TimeUnit.SECONDS))
      assertResult((0, 0L))(reclaim(cache, 4))
      finish.countDown()
      val borrowed = Seq(builder.get(10, TimeUnit.SECONDS), waiter.get(10, TimeUnit.SECONDS))
      assert(borrowed.forall(_ eq value))
      borrowed.foreach(_.release())
      assertResult(1)(cache.activeEntryCount)
      assertResult(1)(value.references)
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
    assertResult(0)(value.references)
  }

  test("a builder and a reader that a reclaim meets between lookup and borrow each borrow the " +
      "build, built once, and the last to let go disposes it once") {
    val disposals = new AtomicInteger()
    val cache = countingCache(disposals)
    val pool = Executors.newFixedThreadPool(2)
    val published = new CountDownLatch(1)
    val waiting = new CountDownLatch(1)
    val builderGo = new CountDownLatch(1)
    val readerGo = new CountDownLatch(1)
    val builds = new AtomicInteger()
    val value = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](5, () => {
          builds.incrementAndGet()
          value
        }, noAction, noAction, beforeBorrow = () => {
          published.countDown()
          assert(builderGo.await(10, TimeUnit.SECONDS))
        })(_.retain(), _.release())
      })
      assert(published.await(10, TimeUnit.SECONDS))
      val reader = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](5,
          () => fail("The build is published"), noAction, noAction, beforeBorrow = () => {
            waiting.countDown()
            assert(readerGo.await(10, TimeUnit.SECONDS))
          })(_.retain(), _.release())
      })
      assert(waiting.await(10, TimeUnit.SECONDS))
      assertResult((1, buildBytes))(reclaim(cache, 5))
      assertResult(0)(cache.activeEntryCount)
      assertResult((0, 1))((disposals.get, value.references))
      builderGo.countDown()
      val built = builder.get(10, TimeUnit.SECONDS)
      // the reader's pin still holds the build
      assertResult((0, 2))((disposals.get, value.references))
      readerGo.countDown()
      val read = reader.get(10, TimeUnit.SECONDS)
      assert((built eq value) && (read eq value))
      assertResult(1)(builds.get)
      // the reader let go last, so it disposed of the cache's reference
      assertResult((1, 2))((disposals.get, value.references))
      built.release()
      read.release()
    } finally {
      builderGo.countDown()
      readerGo.countDown()
      pool.shutdownNow()
      cache.close()
    }
    assertResult(0)(value.references)
  }

  test("a borrower inside borrow when a reclaim lands keeps the build until it returns, and the " +
      "build is disposed once after") {
    val disposals = new AtomicInteger()
    val cache = countingCache(disposals)
    val pool = Executors.newSingleThreadExecutor()
    val reading = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val value = new Value
    try {
      val borrower = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](7, () => value,
          noAction, noAction)({ owned =>
          reading.countDown()
          assert(finish.await(10, TimeUnit.SECONDS))
          owned.retain()
        }, _.release())
      })
      assert(reading.await(10, TimeUnit.SECONDS))
      assertResult((1, buildBytes))(reclaim(cache, 7))
      assertResult((0, 1))((disposals.get, value.references))
      finish.countDown()
      val borrowed = borrower.get(10, TimeUnit.SECONDS)
      assertResult((1, 1))((disposals.get, value.references))
      borrowed.release()
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
    assertResult(0)(value.references)
  }

  test("a waiter cancelled before the publish leaves no hold on the build, which a reclaim " +
      "disposes once") {
    val disposals = new AtomicInteger()
    val cache = countingCache(disposals)
    val pool = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val released = new AtomicInteger()
    val value = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
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
      finish.countDown()
      builder.get(10, TimeUnit.SECONDS).release()
      assertResult((1, buildBytes))(reclaim(cache, 9))
      assertResult((1, 0))((disposals.get, value.references))
    } finally {
      finish.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

  test("a reader cancelled after its lookup and a builder whose beforeBorrow throws after its " +
      "publish each let go of their pin, and the last one's disposal failure joins its error") {
    val disposals = new AtomicInteger()
    val cleanup = new IllegalArgumentException("owner disposal failed")
    val cache = new ExecutorBroadcastCache[Value](owned => {
      disposals.incrementAndGet()
      owned.release()
      throw cleanup
    })
    val pool = Executors.newFixedThreadPool(2)
    val published = new CountDownLatch(1)
    val looked = new CountDownLatch(1)
    val builderGo = new CountDownLatch(1)
    val readerGo = new CountDownLatch(1)
    val checks = new AtomicInteger()
    val acquireFailure = new IllegalStateException("semaphore acquire failed")
    val value = new Value
    try {
      val builder = pool.submit(new Callable[Value] {
        override def call(): Value = cache.getOrBuild[Value](10, () => value, noAction, noAction,
          beforeBorrow = () => {
            published.countDown()
            assert(builderGo.await(10, TimeUnit.SECONDS))
            throw acquireFailure
          })(_.retain(), _.release())
      })
      assert(published.await(10, TimeUnit.SECONDS))
      val reader = pool.submit(new Callable[Value] {
        // the first check precedes the lookup, and the next one throws while the pin is held
        override def call(): Value = cache.getOrBuild[Value](10,
          () => fail("The build is published"), noAction, () => if (checks.incrementAndGet() > 1) {
            looked.countDown()
            assert(readerGo.await(10, TimeUnit.SECONDS))
            throw new TaskKilledException("reader cancelled")
          })(_.retain(), _.release())
      })
      assert(looked.await(10, TimeUnit.SECONDS))
      assertResult((1, buildBytes))(reclaim(cache, 10))
      assertResult((0, 1))((disposals.get, value.references))
      readerGo.countDown()
      val cancelled = intercept[ExecutionException](reader.get(10, TimeUnit.SECONDS)).getCause
      assert(cancelled.isInstanceOf[TaskKilledException], cancelled)
      assertResult(Seq.empty)(cancelled.getSuppressed.toSeq)
      // the builder's pin still holds the build
      assertResult((0, 1))((disposals.get, value.references))
      builderGo.countDown()
      val failed = intercept[ExecutionException](builder.get(10, TimeUnit.SECONDS)).getCause
      assert(failed eq acquireFailure)
      assertResult(Seq(cleanup))(failed.getSuppressed.toSeq)
      assertResult((1, 0))((disposals.get, value.references))
    } finally {
      builderGo.countDown()
      readerGo.countDown()
      pool.shutdownNow()
      cache.close()
    }
  }

  Seq[(String, ExecutorBroadcastCache[Value] => Unit)](
    ("invalidation", (target: ExecutorBroadcastCache[Value]) => target.invalidate(12)),
    ("close", (target: ExecutorBroadcastCache[Value]) => target.close())
  ).foreach { case (removal, remove) =>
    test(s"$removal disposes a build at zero borrowers though a reader holds a pin, and the " +
        "reader throws") {
      val disposals = new AtomicInteger()
      val cache = countingCache(disposals)
      val pool = Executors.newSingleThreadExecutor()
      val waiting = new CountDownLatch(1)
      val proceed = new CountDownLatch(1)
      val value = new Value
      try {
        publish(cache, 12, BroadcastBuildIdentity.empty, value)
        val reader = pool.submit(new Callable[Value] {
          override def call(): Value = cache.getOrBuild[Value](12,
            () => fail("The build is published"), noAction, noAction, beforeBorrow = () => {
              waiting.countDown()
              assert(proceed.await(10, TimeUnit.SECONDS))
            })(_.retain(), _.release())
        })
        assert(waiting.await(10, TimeUnit.SECONDS))
        remove(cache)
        assertResult((1, 0))((disposals.get, value.references))
        proceed.countDown()
        assert(intercept[ExecutionException](reader.get(10, TimeUnit.SECONDS))
          .getCause.isInstanceOf[IllegalStateException])
        assertResult(1)(disposals.get)
      } finally {
        proceed.countDown()
        pool.shutdownNow()
        cache.close()
      }
    }
  }

  test("a reclaim whose stop check reads true detaches nothing") {
    val cache = new ExecutorBroadcastCache[Value](_.release())
    val value = new Value
    try {
      publish(cache, 15, outputs(5L), value)
      assertResult((0, 0L))(reclaim(cache, 15, () => true))
      assertResult(1)(cache.activeEntryCount)
      assertResult(1)(value.references)
    } finally {
      cache.close()
    }
    assertResult(0)(value.references)
  }

  test("a reclaim whose stop check turns true after a disposal disposes no further build") {
    val disposals = new AtomicInteger()
    val cache = countingCache(disposals)
    val values = Seq(new Value, new Value)
    try {
      publish(cache, 16, outputs(1L), values(0))
      publish(cache, 16, outputs(2L), values(1))
      assertResult((2, 2 * buildBytes))(reclaim(cache, 16, () => disposals.get > 0))
      assertResult(0)(cache.activeEntryCount)
      assertResult(1)(disposals.get)
      assertResult(Seq(0, 1))(values.map(_.references).sorted)
    } finally {
      cache.close()
    }
  }

  test("a reclaim's disposal failure is thrown after every other disposal ran") {
    val values = Seq(new Value, new Value)
    val cache = new ExecutorBroadcastCache[Value](owned => {
      owned.release()
      throw new IllegalArgumentException("owner disposal failed")
    })
    try {
      publish(cache, 17, outputs(1L), values(0))
      publish(cache, 17, outputs(2L), values(1))
      val error = intercept[IllegalArgumentException](reclaim(cache, 17))
      assertResult(1)(error.getSuppressed.length)
      assertResult(Seq(0, 0))(values.map(_.references))
      assertResult(0)(cache.activeEntryCount)
    } finally {
      cache.close()
    }
  }
}
