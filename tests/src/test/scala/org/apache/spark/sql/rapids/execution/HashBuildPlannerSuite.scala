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

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.{GpuBoundReference, GpuBuildLeft, GpuBuildRight, GpuBuildSide}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType,
  FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

class HashBuildPlannerSuite extends AnyFunSuite {
  private class TestArtifact extends HashArtifact {
    @volatile var ready = true
    val closeCount = new AtomicInteger()

    override val stats: JoinBuildSideStats =
      JoinBuildSideStats(streamMagnificationFactor = 1.0, isDistinct = true)
    override def isReady: Boolean = ready
    override def backend(
        buildSide: GpuBuildSide,
        metrics: HashBuildMetrics,
        onRebuild: () => Unit): HashProbeBackend =
      throw new UnsupportedOperationException("not needed by this test")
    override def close(): Unit = closeCount.incrementAndGet()
  }

  private def keyExpression(ordinal: Int, dataType: org.apache.spark.sql.types.DataType) = {
    GpuBoundReference(ordinal, dataType, nullable = false)(ExprId(ordinal), s"key_$ordinal")
  }

  private val key = HashBuildKey(
    sourceProjection = Seq.empty,
    projectedKeys = Seq(keyExpression(0, IntegerType)),
    compareNullsEqual = false,
    filterOutNulls = false)
  private val demandId = HashBuildDemandId(joinId = 1, stageId = 2, stageAttempt = 0)

  test("hash-build cost classifies numeric and non-numeric key families") {
    val numericTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType)
    numericTypes.zipWithIndex.foreach { case (dataType, ordinal) =>
      assert(HashBuildCost.hasNumericKeys(Seq(keyExpression(ordinal, dataType))))
    }
    assert(HashBuildCost.hasNumericKeys(Seq(
      keyExpression(0, LongType), keyExpression(1, DoubleType))))

    val nonNumericTypes = Seq(
      DecimalType(10, 2), DateType, TimestampType, StringType,
      StructType(Seq(StructField("child", IntegerType))))
    nonNumericTypes.zipWithIndex.foreach { case (dataType, ordinal) =>
      assert(!HashBuildCost.hasNumericKeys(Seq(keyExpression(ordinal, dataType))))
    }
    assert(!HashBuildCost.hasNumericKeys(Seq(
      keyExpression(0, IntegerType), keyExpression(1, StringType))))
    assert(!HashBuildCost.hasNumericKeys(Seq.empty))
    assertResult(0.4)(HashBuildCost.NumericProbeCost)
    assertResult(1.0)(HashBuildCost.NonNumericProbeCost)
  }

  test("AUTO prefers a compatible resident build") {
    assertResult(GpuBuildRight) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = 1,
        rightRowCount = 100,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Ready,
        numericKeys = true,
        demand = BuildDemand.Empty)
    }
  }

  test("AUTO does not prefer a cold or evicted build without enough observed demand") {
    Seq(BuildStatus.Cold, BuildStatus.Evicted).foreach { status =>
      assertResult(GpuBuildLeft) {
        HashBuildPlanner.select(
          JoinBuildSideSelection.AUTO,
          GpuBuildRight,
          leftRowCount = 1,
          rightRowCount = 100,
          offeredSide = GpuBuildRight,
          status = status,
          numericKeys = true,
          demand = BuildDemand.Empty)
      }
    }
  }

  test("AUTO admits a cold numeric build at the observed production-shape boundary") {
    val streamRows = 70959L
    val broadcastRows = 1700000L

    assertResult(GpuBuildLeft) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = streamRows,
        rightRowCount = broadcastRows,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Cold,
        numericKeys = true,
        demand = BuildDemand(probeCount = 2L, probeRows = 2L * streamRows))
    }
    assertResult(GpuBuildRight) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = streamRows,
        rightRowCount = broadcastRows,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Cold,
        numericKeys = true,
        demand = BuildDemand(probeCount = 3L, probeRows = 3L * streamRows))
    }
  }

  test("AUTO buys at the cold non-numeric rent-or-buy equality boundary") {
    val streamRows = 70959L
    val broadcastRows = 1700000L

    assertResult(GpuBuildRight) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = streamRows,
        rightRowCount = broadcastRows,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Cold,
        numericKeys = false,
        demand = BuildDemand(probeCount = 1L, probeRows = streamRows))
    }
    assertResult(GpuBuildRight) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = streamRows,
        rightRowCount = broadcastRows,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Cold,
        numericKeys = false,
        demand = BuildDemand(probeCount = 2L, probeRows = 2L * streamRows))
    }
  }

  test("AUTO joins an in-flight build once another task has chosen to buy") {
    assertResult(GpuBuildRight) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.AUTO,
        GpuBuildRight,
        leftRowCount = 1,
        rightRowCount = 100,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Building,
        numericKeys = false,
        demand = BuildDemand(probeCount = 100L, probeRows = 100L))
    }
  }

  test("FIXED and SMALLEST ignore resident-build preference") {
    assertResult(GpuBuildLeft) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.FIXED,
        GpuBuildLeft,
        leftRowCount = 100,
        rightRowCount = 1,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Ready,
        numericKeys = true,
        demand = BuildDemand(probeCount = 10L, probeRows = 10L))
    }
    assertResult(GpuBuildLeft) {
      HashBuildPlanner.select(
        JoinBuildSideSelection.SMALLEST,
        GpuBuildRight,
        leftRowCount = 1,
        rightRowCount = 100,
        offeredSide = GpuBuildRight,
        status = BuildStatus.Ready,
        numericKeys = true,
        demand = BuildDemand(probeCount = 10L, probeRows = 10L))
    }
  }

  test("hash-build cache accumulates executor-local probe demand") {
    val cache = new HashBuildCache
    try {
      assertResult(BuildDemand.Empty)(cache.demand(key, demandId))
      assertResult(BuildDemand.Empty)(cache.observeProbe(key, demandId, 0L))
      assertResult(BuildDemand(1L, 70959L))(cache.observeProbe(key, demandId, 70959L))
      assertResult(BuildDemand(2L, 141918L))(cache.observeProbe(key, demandId, 70959L))
      assertResult(BuildDemand(2L, 141918L))(cache.demand(key, demandId))
      val nextJoin = demandId.copy(stageId = demandId.stageId + 1)
      assertResult(BuildDemand.Empty)(cache.demand(key, nextJoin))
      assertResult(BuildDemand(1L, 10L))(cache.observeProbe(key, nextJoin, 10L))
      assertResult(BuildDemand(2L, 141918L))(cache.demand(key, demandId))
      cache.resetDemands(key)
      assertResult(BuildDemand.Empty)(cache.demand(key, demandId))
      assertResult(BuildDemand.Empty)(cache.demand(key, nextJoin))
    } finally {
      cache.close()
    }
  }

  test("hash-build cache publishes one artifact to concurrent consumers") {
    val cache = new HashBuildCache
    val artifact = new TestArtifact
    val createCount = new AtomicInteger()
    val buildStarted = new CountDownLatch(1)
    val allowBuild = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(2)

    try {
      val futures = (0 until 2).map { _ =>
        pool.submit(new Callable[HashArtifactLookup] {
          override def call(): HashArtifactLookup = cache.getOrBuild(key, HashBuildMetrics()) {
            createCount.incrementAndGet()
            buildStarted.countDown()
            assert(allowBuild.await(30, TimeUnit.SECONDS))
            artifact
          }
        })
      }

      assert(buildStarted.await(30, TimeUnit.SECONDS))
      assertResult(BuildStatus.Building)(cache.status(key))
      cache.observeProbe(key, demandId, 100L)
      allowBuild.countDown()

      val results = futures.map(_.get(30, TimeUnit.SECONDS))
      assert(results.forall(_.artifact eq artifact))
      assertResult(1)(results.count(_.reused))
      assertResult(1)(createCount.get())
      assertResult(BuildStatus.Ready)(cache.status(key))
      assertResult(BuildDemand.Empty)(cache.demand(key, demandId))

      artifact.ready = false
      assertResult(BuildStatus.Evicted)(cache.status(key))
    } finally {
      allowBuild.countDown()
      pool.shutdownNow()
      assert(pool.awaitTermination(30, TimeUnit.SECONDS))
      cache.close()
    }

    assertResult(1)(artifact.closeCount.get())
  }

  test("hash-build cache close waits for an in-flight build") {
    val cache = new HashBuildCache
    val artifact = new TestArtifact
    val buildStarted = new CountDownLatch(1)
    val allowBuild = new CountDownLatch(1)
    val closeStarted = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(2)

    try {
      val build = pool.submit(new Callable[HashArtifactLookup] {
        override def call(): HashArtifactLookup = cache.getOrBuild(key, HashBuildMetrics()) {
          buildStarted.countDown()
          assert(allowBuild.await(30, TimeUnit.SECONDS))
          artifact
        }
      })
      assert(buildStarted.await(30, TimeUnit.SECONDS))

      val close = pool.submit(new Callable[Unit] {
        override def call(): Unit = {
          closeStarted.countDown()
          cache.close()
        }
      })
      assert(closeStarted.await(30, TimeUnit.SECONDS))
      assert(!close.isDone)
      allowBuild.countDown()

      close.get(30, TimeUnit.SECONDS)
      intercept[java.util.concurrent.ExecutionException] {
        build.get(30, TimeUnit.SECONDS)
      }
      assertResult(1)(artifact.closeCount.get())
    } finally {
      allowBuild.countDown()
      pool.shutdownNow()
      assert(pool.awaitTermination(30, TimeUnit.SECONDS))
      cache.close()
    }
  }

  test("failed build does not poison the cache") {
    val cache = new HashBuildCache
    val artifact = new TestArtifact
    try {
      intercept[IllegalStateException] {
        cache.getOrBuild(key, HashBuildMetrics()) {
          throw new IllegalStateException("expected failure")
        }
      }
      assertResult(BuildStatus.Cold)(cache.status(key))
      assert(cache.getOrBuild(key, HashBuildMetrics())(artifact).artifact eq artifact)
      assertResult(BuildStatus.Ready)(cache.status(key))
    } finally {
      cache.close()
    }
    assertResult(1)(artifact.closeCount.get())
  }
}
