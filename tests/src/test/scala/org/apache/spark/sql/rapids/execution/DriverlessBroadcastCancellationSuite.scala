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

import com.nvidia.spark.rapids.{GpuCoalesceBatches, GpuMetric, GpuRangeExec, GpuSinglePartitioning, GpuTransitionOverrides, JustRowsColumnarBatch, RapidsConf, RowBasedShuffleChecksumConf, ShimLoader, ShuffleBufferCatalog, SpillableColumnarBatch, TargetSize}
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
import org.apache.spark.sql.execution.{DriverlessBroadcastPlanInfoTestSupport, InputAdapter, LeafExecNode, SparkPlan, UnionExec, WholeStageCodegenExec}
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
      .appName(getClass.getSimpleName).config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.shuffle.manager", ShimLoader.getRapidsShuffleManagerClass)
      .config("spark.rapids.shuffle.mode", "UCX")
      .config("spark.rapids.sql.join.hashTable.reuse", "false")
      .config(RowBasedShuffleChecksumConf.ChecksumEnabledKey, "false")
      .config(RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey, "false")
      .getOrCreate()
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

  test("reused codegen stages validate their permitted CPU leaves") {
    session.withActive {
      val cpu = session.range(4).queryExecution.executedPlan
      assert(cpu.isInstanceOf[WholeStageCodegenExec])
      val validator = new GpuTransitionOverrides(session)
      Seq(cpu, InputAdapter(cpu)).foreach { child =>
        val shuffle = GpuShuffleExchangeExec(GpuSinglePartitioning, child,
          ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
        val reused = ReusedExchangeExec(shuffle.output, shuffle)
        val allowed = new RapidsConf(Map("spark.rapids.sql.test.allowedNonGpu" -> "RangeExec"))
        validator.assertIsOnTheGpu(reused, allowed)
        val error = intercept[IllegalArgumentException] {
          validator.assertIsOnTheGpu(reused, new RapidsConf(Map.empty[String, String]))
        }
        assert(error.getMessage.contains("RangeExec"))
      }
    }
  }

  test("broadcast exchanges register driverless metrics only with opt-in") {
    session.withActive {
      val key = "spark.rapids.sql.driverlessBroadcast.enabled"
      val previous = session.conf.getOption(key)
      val driverless = Set("driverlessBuildBytes", "fallbackBroadcastBytes",
        "driverlessStageTime", "driverlessStageFallbacks")
      try {
        val exchanges = Seq(false, true, false).map { enabled =>
          session.conf.set(key, enabled.toString)
          val child = EmptyColumnarPlan()
          val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
          val exchange = GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu)
          enabled -> exchange
        }
        session.conf.set(key, "false")
        exchanges.foreach { case (enabled, exchange) =>
          val names = exchange.additionalMetrics.keySet
          assert(Set("dataSize", GpuMetric.COLLECT_TIME, GpuMetric.BUILD_TIME,
            "broadcastTime").subsetOf(names))
          assert(names.intersect(driverless) == (if (enabled) driverless else Set.empty))
        }
      } finally {
        previous match {
          case Some(value) => session.conf.set(key, value)
          case None => session.conf.unset(key)
        }
      }
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
      val stage = DriverlessBroadcastTestShim.stage(exchange, cpu.canonicalized)
      val reused = DriverlessBroadcastTestShim.reuse(stage, Seq(child.output.head.newInstance()))
      val first = DriverlessBroadcastTestShim.join(probe, stage)
      val second = DriverlessBroadcastTestShim.join(probe, reused)
      assert(stage.output.head.exprId != reused.output.head.exprId)
      assert(DriverlessBroadcastEligibility.sharedBuilds(
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
        val blocked = DriverlessBroadcastEligibility.sharedBuilds(UnionExec(consumers))
        assert(blocked.exists(_.fastEquals(cpu.canonicalized)))
      }
    }
  }

  test("enabled rewrite preserves CPU joins while converting an eligible GPU join") {
    session.withActive {
      val key = AttributeReference("key", LongType)()
      val probe = GpuRangeExec(0, 4, 1, 1, Seq(key), 1024)
      val child = GpuRangeExec(0, 2, 1, 1, Seq(key.newInstance()), 1024)
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val gpu = DriverlessBroadcastTestShim.join(probe,
        GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu))
      val standard = BroadcastHashJoinExec(Seq(key), cpu.output, Inner, BuildRight,
        None, probe, cpu, false)
      val conf = new RapidsConf(Map(
        "spark.rapids.sql.driverlessBroadcast.enabled" -> "true",
        "spark.rapids.shuffle.mode" -> "UCX",
        "spark.rapids.sql.join.hashTable.reuse" -> "false"))
      assert(DriverlessBroadcastEligibility.planRejections(gpu, conf).isEmpty)
      assert(GpuDriverlessBroadcastHashJoinExec.rewrite(gpu, conf)
        .isInstanceOf[GpuDriverlessBroadcastHashJoinExec])
      assert(GpuDriverlessBroadcastHashJoinExec.rewrite(standard, conf) eq standard)
    }
  }

  test("driverless policy follows each row-based checksum flag and its reset") {
    session.withActive {
      val conf = new RapidsConf(Map("spark.rapids.shuffle.mode" -> "UCX"))
      val keys = Seq(RowBasedShuffleChecksumConf.ChecksumEnabledKey,
        RowBasedShuffleChecksumConf.ChecksumMismatchFullRetryKey)
      assert(DriverlessBroadcastPolicy.rejections(conf).isEmpty)
      keys.foreach { key =>
        session.conf.set(key, "true")
        try {
          assert(DriverlessBroadcastPolicy.rejections(conf)
            .exists(_.contains("row-based shuffle checksums")))
        } finally {
          session.conf.set(key, "false")
        }
      }
      assert(DriverlessBroadcastPolicy.rejections(conf).isEmpty)
    }
  }

  test("AQE build reuse preserves the executed producer and its published read metric") {
    session.withActive {
      val enabled = "spark.rapids.sql.driverlessBroadcast.enabled"
      val adaptive = "spark.sql.adaptive.enabled"
      val previous = Seq(enabled, adaptive).map(key => key -> session.conf.getOption(key))
      session.conf.set(enabled, "true")
      session.conf.set(adaptive, "true")
      try {
        val key = AttributeReference("key", LongType)()
        val child = EmptyColumnarPlan(Seq(key.newInstance()))
        val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
        val exchange = GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu)
        exchange.setTagValue(DriverlessBroadcastStage.eligible, true)
        val token = exchange.relationFuture.get(30, TimeUnit.SECONDS)
        try {
          val producer = exchange.admittedDriverlessShuffle.get
          val probe = GpuRangeExec(0, 4, 1, 1, Seq(key), 1024)
          val stage = DriverlessBroadcastTestShim.stage(exchange, cpu.canonicalized)
          val join = DriverlessBroadcastTestShim.join(probe, stage)
          val conf = new RapidsConf(session.conf.getAll)
          val native = GpuDriverlessBroadcastHashJoinExec.rewrite(join, conf)
            .asInstanceOf[GpuDriverlessBroadcastHashJoinExec]
          val build = native.right.asInstanceOf[ReusedExchangeExec]
          assert(build.child eq producer)

          var rewrittenProducers = 0
          val transformed = native.transformUp {
            case shuffle: GpuShuffleExchangeExec =>
              rewrittenProducers += 1
              shuffle.withNewChildren(Seq(GpuCoalesceBatches(shuffle.child, TargetSize(1024))))
          }.asInstanceOf[GpuDriverlessBroadcastHashJoinExec]
          assert(rewrittenProducers == 0)
          assert(transformed.right.asInstanceOf[ReusedExchangeExec].child eq producer)

          val readMetric = producer.getOpTimeNewMetric.get
          readMetric.set(17)
          assert(transformed.getDescendantOpTimeMetrics.contains(readMetric))
          assert(transformed.getDescendantOpTimeMetrics.find(_ eq readMetric).get.value == 17)
          val metricId = producer.metrics(GpuMetric.OP_TIME_NEW_SHUFFLE_READ).id
          assert(DriverlessBroadcastPlanInfoTestSupport.metricIds(transformed).contains(metricId))
        } finally {
          token.destroy()
        }
      } finally {
        previous.foreach {
          case (key, Some(value)) => session.conf.set(key, value)
          case (key, None) => session.conf.unset(key)
        }
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

  test("AQE rejects oversized materialized row counts before publishing a build token") {
    val enabledSession = session.newSession()
    enabledSession.conf.set("spark.rapids.sql.driverlessBroadcast.enabled", "true")
    enabledSession.withActive {
      val child = EmptyColumnarPlan()
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val exchange = GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(cpu)
      val stage = new DriverlessBroadcastStage(exchange, enabledSession.sparkContext)
      stage.shuffle.metrics(GpuMetric.NUM_OUTPUT_ROWS).set(Int.MaxValue.toLong + 1)
      val error = intercept[IllegalArgumentException] { stage.materialize() }
      assert(error.getMessage.contains("single cuDF batch"))
      assert(!stage.isAdmitted && stage.mapStatistics.isEmpty)
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
        DriverlessBroadcastTestShim.stage(exchange, cpu.canonicalized))
      assert(pending.isCancelled)
    }
  }

  test("AQE cancellation reaches a late standard fallback after token completion") {
    val enabledSession = session.newSession()
    enabledSession.conf.set("spark.rapids.sql.driverlessBroadcast.enabled", "true")
    enabledSession.withActive {
      val child = EmptyColumnarPlan()
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val token = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val exchange = new ControlledExchange(child, cpu, token)
      val pending = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val fallbackStarted = new CountDownLatch(1)
      val fallback = new ControlledExchange(child, cpu, pending,
        () => fallbackStarted.countDown())
      val stage = new DriverlessBroadcastStage(exchange, enabledSession.sparkContext) {
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
        override def run(): Unit = enabledSession.withActive {
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
          DriverlessBroadcastTestShim.stage(exchange, cpu.canonicalized))
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
