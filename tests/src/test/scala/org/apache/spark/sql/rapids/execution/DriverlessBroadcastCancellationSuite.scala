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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.nvidia.spark.rapids.{GpuRangeExec, GpuSinglePartitioning, GpuTransitionOverrides, JustRowsColumnarBatch, RapidsConf, ShuffleBufferCatalog, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ENSURE_REQUIREMENTS, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch

object DriverlessBroadcastCancellationSuite {
  case class EmptyColumnarPlan(override val output: Seq[Attribute] = Seq.empty)
      extends LeafExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = sparkContext.emptyRDD
  }
}

class DriverlessBroadcastCancellationSuite extends AnyFunSuite with BeforeAndAfterAll {
  import DriverlessBroadcastCancellationSuite.EmptyColumnarPlan

  private var session: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    session = SparkSession.builder().master("local[2]")
      .appName(getClass.getSimpleName).config("spark.ui.enabled", "false").getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (session != null) session.stop()
    } finally {
      super.afterAll()
    }
  }

  private class ControlledExchange(
      childPlan: SparkPlan,
      cpu: BroadcastExchangeExec,
      initial: GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]],
      started: () => Unit = () => ())
      extends GpuBroadcastExchangeExec(IdentityBroadcastMode, childPlan)(cpu) {
    override protected lazy val materializationFuture = {
      started()
      initial
    }
  }

  Seq(false, true).foreach { failCatalog =>
    test(s"shuffle unregister continues after cache disposal fails, catalog failure=$failCatalog") {
      session.withActive {
        val primary = new IllegalStateException("cache disposal failed")
        val secondary = new IllegalArgumentException("catalog cleanup failed")
        val references = new AtomicInteger(1)
        val cache = new ExecutorBroadcastCache[SpillableColumnarBatch](owned => {
          withResource(owned)(_ => ())
          references.decrementAndGet()
          throw primary
        })
        cache.getOrBuild[Unit](91, () => new JustRowsColumnarBatch(1),
          () => (), () => ())(_ => (), _ => ())
        val catalog = new ShuffleBufferCatalog {
          override def unregisterShuffle(id: Int): Unit = {
            super.unregisterShuffle(id)
            if (failCatalog) throw secondary
          }
        }
        catalog.registerShuffle(91)
        val conf = new RapidsConf(Map("spark.rapids.shuffle.mode" -> "CACHE_ONLY"))
        val testEnv = new GpuShuffleEnv(conf) {
          override private[rapids] lazy val broadcastCache = cache
          override def getCatalog: ShuffleBufferCatalog = catalog
        }
        try {
          val error = intercept[IllegalStateException] {
            testEnv.invalidateBroadcastAndThen(91) {
              catalog.unregisterShuffle(91)
            }
          }
          assert(error eq primary)
          assert(!catalog.hasActiveShuffle(91))
          assert(cache.activeEntryCount == 0 && references.get() == 0)
          assert(error.getSuppressed.contains(secondary) == failCatalog)
        } finally {
          cache.close()
        }
      }
    }
  }

  Seq(false, true).foreach { failTeardown =>
    test(s"cache disposal failure still runs remaining teardown, teardown failure=$failTeardown") {
      session.withActive {
        val primary = new IllegalStateException("cache close failed")
        val secondary = new IllegalArgumentException("remaining teardown failed")
        val references = new AtomicInteger(1)
        val cache = new ExecutorBroadcastCache[SpillableColumnarBatch](owned => {
          withResource(owned)(_ => ())
          references.decrementAndGet()
          throw primary
        })
        cache.getOrBuild[Unit](92, () => new JustRowsColumnarBatch(1),
          () => (), () => ())(_ => (), _ => ())
        val testEnv = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String])) {
          override private[rapids] lazy val broadcastCache = cache
        }
        val service = Executors.newSingleThreadExecutor()
        try {
          val error = intercept[IllegalStateException] {
            testEnv.closeBroadcastCacheAndThen {
              service.shutdown()
              if (failTeardown) throw secondary
            }
          }
          assert(error eq primary)
          assert(service.isShutdown)
          assert(cache.activeEntryCount == 0 && references.get() == 0)
          assert(error.getSuppressed.contains(secondary) == failTeardown)
        } finally {
          service.shutdownNow()
          cache.close()
        }
      }
    }
  }

  test("pure GPU consumers can share a reused AQE build with distinct output attributes") {
    session.withActive {
      val key = AttributeReference("key", LongType)()
      val probe = EmptyColumnarPlan(Seq(key))
      val child = EmptyColumnarPlan(Seq(key.newInstance()))
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val exchange = GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu)
      val stage = BroadcastQueryStageExec(1, exchange, cpu.canonicalized)
      val reused = stage.newReuseInstance(2, Seq(child.output.head.newInstance()))
      val first = DriverlessBroadcastTestShim.join(probe, stage)
      val second = DriverlessBroadcastTestShim.join(probe, reused)
      assert(stage.output.head.exprId != reused.output.head.exprId)
      assert(GpuDriverlessBroadcastHashJoinExec.sharedBuilds(
        UnionExec(Seq(first, second))).isEmpty)
    }
  }

  test("a CPU consumer keeps an equivalent shared GPU build on standard broadcast") {
    session.withActive {
      val key = AttributeReference("key", LongType)()
      val probe = EmptyColumnarPlan(Seq(key))
      val child = EmptyColumnarPlan(Seq(key.newInstance()))
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val exchange = GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu)
      val gpu = DriverlessBroadcastTestShim.join(probe, exchange)
      val reused = ReusedExchangeExec(Seq(child.output.head.newInstance()), cpu)
      val standard = BroadcastHashJoinExec(Seq(key), reused.output, Inner, BuildRight,
        None, probe, reused, false)
      Seq(Seq(gpu, standard), Seq(standard, gpu)).foreach { consumers =>
        val blocked = GpuDriverlessBroadcastHashJoinExec.sharedBuilds(UnionExec(consumers))
        assert(blocked.exists(_.fastEquals(cpu.canonicalized)))
      }
    }
  }

  test("GPU validation descends through reuse and still rejects a CPU subtree") {
    session.withActive {
      val key = AttributeReference("key", LongType)()
      val gpu = GpuRangeExec(0, 4, 1, 1, Seq(key), 1024)
      val shuffle = GpuShuffleExchangeExec(GpuSinglePartitioning, gpu,
        ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
      val conf = new RapidsConf(Map.empty[String, String])
      (new GpuTransitionOverrides).assertIsOnTheGpu(
        ReusedExchangeExec(Seq(key.newInstance()), shuffle), conf)
      val cpu = session.range(4).queryExecution.sparkPlan
      val invalid = GpuShuffleExchangeExec(GpuSinglePartitioning, cpu,
        ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
      val error = intercept[IllegalArgumentException] {
        (new GpuTransitionOverrides).assertIsOnTheGpu(ReusedExchangeExec(cpu.output, invalid), conf)
      }
      assert(error.getMessage.contains("Part of the plan is not columnar"))
    }
  }

  test("AQE cancellation retains the standard incomplete-broadcast behavior") {
    session.withActive {
      val child = EmptyColumnarPlan()
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val pending = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val exchange = new ControlledExchange(child, cpu, pending)
      assert(exchange.relationFuture eq pending)
      DriverlessBroadcastTestShim.cancel(
        BroadcastQueryStageExec(1, exchange, cpu.canonicalized))
      assert(pending.isCancelled)
    }
  }

  test("AQE cancellation reaches a late standard fallback after token completion") {
    session.withActive {
      val child = EmptyColumnarPlan()
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val token = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val exchange = new ControlledExchange(child, cpu, token)
      val pending = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val fallbackStarted = new CountDownLatch(1)
      val fallback = new ControlledExchange(child, cpu, pending,
        () => fallbackStarted.countDown())
      val stage = new DriverlessBroadcastStage(exchange, session.sparkContext) {
        override protected val standard: GpuBroadcastExchangeExec = fallback
      }
      val tokenValue = stage.materialize().get
      token.complete(tokenValue)
      assert(stage.isAdmitted)
      // Initialize the real exchange's lazy stage slot, then install a controlled stage
      // without requiring GPU allocation to exercise Spark's cancellation contract.
      assert(exchange.admittedDriverlessBuild.isEmpty)
      val stageField = classOf[GpuBroadcastExchangeExec].getDeclaredFields
        .find(_.getName == "driverlessStage").get
      stageField.setAccessible(true)
      stageField.set(exchange, Some(stage))
      assert(exchange.relationFuture eq token)
      val failure = new AtomicReference[Throwable]()
      val waiter = new Thread(new Runnable {
        override def run(): Unit = session.withActive {
          try {
            stage.standardBroadcast()
          } catch {
            case error: Throwable => failure.set(error)
          }
        }
      })
      try {
        waiter.start()
        assert(fallbackStarted.await(5, TimeUnit.SECONDS))
        assert(exchange.relationFuture eq pending)
        assert(!pending.isDone && waiter.isAlive, Option(failure.get()).toString)
        DriverlessBroadcastTestShim.cancel(
          BroadcastQueryStageExec(1, exchange, cpu.canonicalized))
        waiter.join(5000)
        assert(pending.isCancelled)
        assert(!waiter.isAlive && failure.get() != null)
        assert(token.isDone && !token.isCancelled)
      } finally {
        pending.cancel(true)
        waiter.join(5000)
        tokenValue.destroy()
      }
    }
  }
}
