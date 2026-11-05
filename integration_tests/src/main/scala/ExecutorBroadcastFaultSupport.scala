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
package org.apache.spark.sql.rapids.testing

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, RpcTimeout, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockManagerAdded, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.ThreadUtils

/** Test-only missing-output injection for the two-executor shuffle integration suite. */
object ExecutorBroadcastFaultSupport {
  private case class BuildStarted(stageId: Int)
  private case object ProbeEntered

  /** Holds the first probe row until cancellation; the Python test owns and closes this fixture. */
  class CancellationProbe(session: SparkSession) extends AutoCloseable {
    private val context = session.sparkContext
    private val rpc = SparkEnv.get.rpcEnv
    private val entered = new CountDownLatch(1)
    private val lock = new Object
    private val activeTasks = mutable.Set.empty[Long]
    private val listener = new SparkListener {
      override def onTaskStart(event: SparkListenerTaskStart): Unit = lock.synchronized {
        activeTasks += event.taskInfo.taskId
      }
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = lock.synchronized {
        activeTasks -= event.taskInfo.taskId
        lock.notifyAll()
      }
      override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
        lock.synchronized { lock.notifyAll() }
    }
    val functionName: String = s"driverless_cancel_probe_${System.nanoTime()}"
    private val gate = rpc.setupEndpoint(functionName, new ThreadSafeRpcEndpoint {
      override val rpcEnv: RpcEnv = rpc
      private val waiting = mutable.ArrayBuffer.empty[RpcCallContext]
      override def receiveAndReply(call: RpcCallContext): PartialFunction[Any, Unit] = {
        case ProbeEntered =>
          waiting += call
          entered.countDown()
      }
      override def onStop(): Unit = waiting.foreach(_.sendFailure(
        new IllegalStateException("Cancellation probe fixture closed")))
    })
    context.addSparkListener(listener)
    try {
      // Capture only the address/name; the executor resolves its own RPC environment.
      val target = gate.address
      val name = gate.name
      session.udf.register(functionName, (value: Long) => {
        if (value == 0L) {
          SparkEnv.get.rpcEnv.setupEndpointRef(target, name)
            .askSync[Boolean](ProbeEntered, new RpcTimeout(300.seconds,
              "driverless cancellation probe timeout"))
        }
        value
      })
    } catch {
      case error: Throwable =>
        rpc.stop(gate)
        context.removeSparkListener(listener)
        throw error
    }

    def awaitEntered(seconds: Long): Boolean = entered.await(seconds, TimeUnit.SECONDS)

    def awaitMetric(metric: SQLMetric, seconds: Long): Boolean = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
      lock.synchronized {
        var remaining = deadline - System.nanoTime()
        while (metric.value <= 0 && remaining > 0) {
          lock.wait(math.max(1L, remaining / 1000000L))
          remaining = deadline - System.nanoTime()
        }
        metric.value > 0
      }
    }

    def awaitIdle(seconds: Long): Unit = {
      context.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(seconds))
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
      lock.synchronized {
        while (activeTasks.nonEmpty) {
          val remaining = deadline - System.nanoTime()
          require(remaining > 0, s"Cancelled query still has active tasks: $activeTasks")
          lock.wait(math.max(1L, remaining / 1000000L))
        }
      }
      context.listenerBus.waitUntilEmpty(TimeUnit.SECONDS.toMillis(seconds))
    }

    override def close(): Unit = {
      try rpc.stop(gate) finally {
        context.removeSparkListener(listener)
        session.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $functionName")
      }
    }
  }

  def cancellationProbe(session: SparkSession): CancellationProbe = new CancellationProbe(session)

  private class ParallelBuildGate(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {
    private val waiting = mutable.Map.empty[Int, RpcCallContext]
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case BuildStarted(stageId) =>
        if (waiting.contains(stageId)) {
          context.sendFailure(new IllegalStateException("Expected one task per build stage"))
        } else {
          waiting(stageId) = context
          if (waiting.size == 2) waiting.values.foreach(_.reply(true))
        }
    }
    override def onStop(): Unit = {
      if (waiting.size < 2) {
        waiting.values.foreach(_.sendFailure(
          new IllegalStateException("Parallel build test ended before both builds started")))
      }
    }
  }

  private def awaitParallelBuildSlots(session: SparkSession): Unit = {
    val context = session.sparkContext
    val taskCores = context.getConf.getInt("spark.task.cpus", 1)
    if (context.isLocal) {
      require(context.defaultParallelism >= 2 * taskCores,
        "Parallel build test requires two local CPU task slots")
    } else {
      val executorCores = context.getConf.getInt("spark.executor.cores", 1)
      val required = (2 * taskCores + executorCores - 1) / executorCores
      val lock = new Object
      val listener = new SparkListener {
        override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit =
          lock.synchronized { lock.notifyAll() }
        override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit =
          lock.synchronized { lock.notifyAll() }
      }
      def registered: Int = SparkEnv.get.blockManager.master.getMemoryStatus.keysIterator
        .map(_.executorId).filter(_ != "driver").toSet.size
      context.addSparkListener(listener)
      val deadline = System.nanoTime() + 60.seconds.toNanos
      try lock.synchronized {
        while (registered < required) {
          val remaining = deadline - System.nanoTime()
          require(remaining > 0,
            s"Parallel build test environment not ready: need $required registered executors")
          lock.wait(math.max(1L, remaining / 1000000L))
        }
      } finally {
        context.removeSparkListener(listener)
      }
    }
  }

  /**
   * The Python overlap test supplies two one-partition builds calling the temporary
   * driverless_parallel_build_gate UDF. Two CPU task slots must be ready before use;
   * both task bodies must enter the gate before either can finish. No GPU work may
   * precede the gate in a shared executor, so the fixture uses CPU range sources.
   */
  def collectWithParallelBuilds(session: SparkSession, sql: String): Array[String] = {
    awaitParallelBuildSlots(session)
    val context = session.sparkContext
    val lock = new Object
    val tasks = mutable.Map.empty[Long, (Int, Long, Long)]
    val listener = new SparkListener {
      override def onTaskStart(event: SparkListenerTaskStart): Unit = lock.synchronized {
        tasks(event.taskInfo.taskId) = (event.stageId, event.taskInfo.launchTime, Long.MaxValue)
      }
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = lock.synchronized {
        tasks(event.taskInfo.taskId) =
          (event.stageId, event.taskInfo.launchTime, event.taskInfo.finishTime)
      }
    }
    context.addSparkListener(listener)
    val rpc = SparkEnv.get.rpcEnv
    val gate = rpc.setupEndpoint(s"driverless-build-gate-${System.nanoTime()}",
      new ParallelBuildGate(rpc))
    val gateAddress = gate.address
    val gateName = gate.name
    val timeout = new RpcTimeout(60.seconds, "driverless parallel-build test timeout")
    val function = "driverless_parallel_build_gate"
    val group = "driverless-parallel-builds"
    context.setJobGroup(group, "Verify parallel build upstreams")
    try {
      session.udf.register(function, (value: Long) => {
        // SQL UDF deserialization does not install an executor RPC environment on a captured ref.
        val endpoint = SparkEnv.get.rpcEnv.setupEndpointRef(gateAddress, gateName)
        endpoint.askSync[Boolean](BuildStarted(TaskContext.get().stageId()), timeout)
        value
      })
      val frame = session.sql(sql)
      require(frame.schema.fields.map(_.dataType).toSeq == Seq(LongType, LongType, LongType),
        "Parallel build fixture must return exactly three long columns")
      val nativeJoins = frame.queryExecution.executedPlan.collect {
        case node if node.getClass.getSimpleName == "GpuDriverlessBroadcastHashJoinExec" => node
      }
      require(nativeJoins.size == 2, "Expected two driverless joins in the overlap fixture")
      val result = frame.queryExecution.toRdd
      result.getNumPartitions
      context.listenerBus.waitUntilEmpty(30000)
      require(context.statusTracker.getJobIdsForGroup(group).isEmpty,
        "Constructing the join RDD must not submit or wait for a build job")
      val rows = frame.collect().map { row =>
        s"${row.getLong(0)}:${row.getLong(1)}:${row.getLong(2)}"
      }
      context.listenerBus.waitUntilEmpty(30000)
      val singleTaskStages = lock.synchronized {
        tasks.values.toSeq.groupBy(_._1).values.filter(_.size == 1).map(_.head).toSeq
      }
      require(singleTaskStages.size == 2,
        s"Expected two single-task build stages: $singleTaskStages")
      val Seq(first, second) = singleTaskStages.sortBy(_._2)
      require(second._2 < first._3 && first._2 < second._3,
        s"Independent build tasks did not overlap: $singleTaskStages")
      rows
    } finally {
      try {
        context.cancelJobGroup(group)
      } finally {
        rpc.stop(gate)
        context.removeSparkListener(listener)
        context.clearJobGroup()
        session.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function")
      }
    }
  }

  def collectAfterRemovingBuildOutputs(
      session: SparkSession,
      frame: Dataset[Row],
      slots: Int): Array[String] = session.withActive {
    SQLExecution.withNewExecutionId(frame.queryExecution, Some("lost-broadcast-build-output")) {
      collectWithMissingBuild(session, frame, slots)
    }
  }

  private def collectWithMissingBuild(
      session: SparkSession,
      frame: Dataset[Row],
      slots: Int): Array[String] = {
    require(frame.schema.fields.map(_.dataType).toSeq == Seq(LongType, LongType),
      "This fixture checks complete rows with exactly two long columns")
    val executed = frame.queryExecution.executedPlan
    // Constructing the adaptive output RDD finalizes its plan without running result tasks.
    val result = executed.execute()
    val finalPlan = executed match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
      case other => other
    }
    val joins = finalPlan.collect {
      case node if node.getClass.getSimpleName == "GpuDriverlessBroadcastHashJoinExec" => node
    }
    require(joins.size == 1, s"Expected one executor broadcast join, found ${joins.size}")
    val join = joins.head
    // Construct the real output RDD without executing the result stage. Materialize only
    // its build dependency so deletion happens before any consumer can populate its cache.
    val build = join.getClass.getMethod("buildPlan").invoke(join).asInstanceOf[SparkPlan]
    val dependency = build.getClass.getMethod("shuffleDependencyColumnar").invoke(build)
      .asInstanceOf[ShuffleDependency[Any, Any, Any]]
    ThreadUtils.awaitResult(session.sparkContext.submitMapStage(dependency), 30.seconds)
    val shuffleId = build.getClass.getMethod("shuffleId").invoke(build).asInstanceOf[Int]
    val observations = session.sparkContext.parallelize(0 until slots, slots).barrier()
      .mapPartitions { _ =>
        // Barrier scheduling reserves all slots, and the driver verifies that both executors
        // were visited. No barrier synchronization request is needed to remove these outputs.
        // Resolve the catalog through the active shim loader, as the product does. This helper
        // depends only on Spark APIs and does not add a product dependency on testing code.
        val shim = Class.forName("com.nvidia.spark.rapids.ShimLoader$")
        val shimModule = shim.getField("MODULE$").get(null)
        val loader = shim.getMethod("getShimClassLoader").invoke(shimModule)
          .asInstanceOf[ClassLoader]
        val env = Class.forName("org.apache.spark.sql.rapids.GpuShuffleEnv$", true, loader)
        val module = env.getField("MODULE$").get(null)
        val catalog = env.getMethod("getCatalog").invoke(module)
        val active = catalog.getClass.getMethod("hasActiveShuffle", classOf[Int])
        val hadOutput = active.invoke(catalog, Int.box(shuffleId)).asInstanceOf[Boolean]
        // Delete only catalog output. Keep Spark's map registrations and broadcast cache intact
        // so the subsequent read must discover the missing output and trigger map recovery.
        catalog.getClass.getMethod("unregisterShuffle", classOf[Int])
          .invoke(catalog, Int.box(shuffleId))
        require(!active.invoke(catalog, Int.box(shuffleId)).asInstanceOf[Boolean])
        Iterator((SparkEnv.get.executorId, hadOutput))
      }.collect()
    require(observations.map(_._1).distinct.length == 2,
      s"Injection did not visit two executors: ${observations.toSeq}")
    require(observations.exists(_._2), "The injected shuffle had no registered output")
    val rows = result.map(row => s"${row.getLong(0)}:${row.getLong(1)}").collect()
    Array(s"shuffle=$shuffleId,observations=${observations.toSeq}") ++ rows
  }
}
