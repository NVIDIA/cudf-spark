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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
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
      initial: GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]])
      extends GpuBroadcastExchangeExec(IdentityBroadcastMode, childPlan)(cpu) {
    override protected lazy val materializationFuture = initial
  }

  test("AQE cancellation retains the standard incomplete-broadcast behavior") {
    session.withActive {
      val child = EmptyColumnarPlan()
      val cpu = BroadcastExchangeExec(IdentityBroadcastMode, child)
      val pending = new GpuBroadcastExchangeExecBase.BroadcastFuture[Broadcast[Any]]
      val exchange = new ControlledExchange(child, cpu, pending)
      assert(exchange.relationFuture eq pending)
      BroadcastQueryStageExec(1, exchange, cpu.canonicalized).cancel()
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
      val fallback = new ControlledExchange(child, cpu, pending)
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
      val started = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val waiter = new Thread(new Runnable {
        override def run(): Unit = session.withActive {
          started.countDown()
          try {
            stage.standardBroadcast()
          } catch {
            case error: Throwable => failure.set(error)
          }
        }
      })
      try {
        waiter.start()
        assert(started.await(5, TimeUnit.SECONDS))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while ((exchange.relationFuture ne pending) && waiter.isAlive &&
            System.nanoTime() < deadline) {
          Thread.sleep(10)
        }
        assert(exchange.relationFuture eq pending)
        assert(!pending.isDone && waiter.isAlive, Option(failure.get()).toString)
        BroadcastQueryStageExec(1, exchange, cpu.canonicalized).cancel()
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
