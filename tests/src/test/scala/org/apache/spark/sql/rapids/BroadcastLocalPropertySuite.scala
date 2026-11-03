/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.UUID
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}

import scala.concurrent.Await
import scala.concurrent.duration._

import com.nvidia.spark.rapids.{PlanUtils, SparkQueryCompareTestSuite}
import org.scalatest.concurrent.{Eventually, Signaler, ThreadSignaler, TimeLimits}

import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{BaseSubqueryExec, FilterExec, LeafExecNode, ScalarSubquery, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEPropagateEmptyRelation, QueryStageExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuBroadcastExchangeExecBase}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

class BroadcastLocalPropertySuite
    extends SparkQueryCompareTestSuite with TimeLimits with Eventually {

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  test("Propagating local properties to broadcast exec") {
    withGpuSparkSession(spark => withSingleBroadcastThread {
      withSQLConf("spark.rapids.sql.test.enabled" -> "false") {
        withTable(spark, "a", "b") {
          val confKey = "spark.sql.y"
          import spark.implicits._
          def generateBroadcastDataFrame(confKey: String, confValue: String): Dataset[String] = {
            val df = spark.range(1).mapPartitions { _ =>
              Iterator(TaskContext.get.getLocalProperty(confKey))
            }.filter($"value".contains(confValue)).as("c")
            df.hint("broadcast")
          }

          val confValue1 = UUID.randomUUID().toString()
          Seq((confValue1, "1")).toDF("key", "value")
            .write
            .format("parquet")
            .partitionBy("key")
            .mode("overwrite")
            .saveAsTable("a")
          val df1 = spark.table("a")

          // // set local property and assert
          val df2 = generateBroadcastDataFrame(confKey, confValue1)
          spark.sparkContext.setLocalProperty(confKey, confValue1)
          val checkDF = df1.join(df2).where($"a.key" === $"c.value").select($"a.key", $"c.value")
          val checks = checkDF.collect()
          assert(checks.forall(_.toSeq == Seq(confValue1, confValue1)))

          // change local property and re-assert
          val confValue2 = UUID.randomUUID().toString()
          Seq((confValue2, "1")).toDF("key", "value")
            .write
            .format("parquet")
            .partitionBy("key")
            .mode("overwrite")
            .saveAsTable("b")

          val df3 = spark.table("b")
          val df4 = generateBroadcastDataFrame(confKey, confValue2)
          spark.sparkContext.setLocalProperty(confKey, confValue2)
          val checks2DF = df3.join(df4).where($"b.key" === $"c.value").select($"b.key", $"c.value")
          val checks2 = checks2DF.collect()
          assert(checks2.forall(_.toSeq == Seq(confValue2, confValue2)))
          assert(checks2.nonEmpty)
        }
      }
  })
  }

  private def withSingleBroadcastThread(f: => Unit): Unit = {
    // Change the effective executor even when an earlier suite initialized the global pool.
    val executor = GpuBroadcastExchangeExecBase.broadcastExecutor
    val maxThreads = executor.getMaximumPoolSize
    val coreThreads = executor.getCorePoolSize
    executor.setCorePoolSize(1)
    executor.setMaximumPoolSize(1)
    try {
      assert(executor.getMaximumPoolSize == 1)
      assert(executor.getCorePoolSize == 1)
      eventually(timeout(10.seconds)) { assert(executor.getPoolSize <= 1) }
      implicit val signaler: Signaler = ThreadSignaler
      failAfter(60.seconds) { f }
    } finally {
      executor.setMaximumPoolSize(maxThreads)
      executor.setCorePoolSize(coreThreads)
    }
  }

  private def withPreparationThreads(threads: Int)(f: => Unit): Unit = {
    val executor = GpuBroadcastExchangeExecBase.preparationExecutor
    val maxThreads = executor.getMaximumPoolSize
    val coreThreads = executor.getCorePoolSize
    // Both pools capture the same static threshold when their containing object initializes.
    assert(maxThreads == GpuBroadcastExchangeExecBase.broadcastExecutor.getMaximumPoolSize)
    executor.setCorePoolSize(math.min(coreThreads, threads))
    executor.setMaximumPoolSize(threads)
    try {
      eventually(timeout(10.seconds)) {
        assert(executor.getActiveCount == 0)
        assert(executor.getPoolSize <= threads)
      }
      f
    } finally {
      executor.setMaximumPoolSize(maxThreads)
      executor.setCorePoolSize(coreThreads)
    }
  }

  for (cpuFilter <- Seq(false, true)) {
    test(s"nested scalar subqueries do not starve broadcast pool (CPU filter: $cpuFilter)") {
      def query(spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
        spark.sql(
          """with v as (
            |  select c1, c2, rand() c3 from t
            |)
            |select * from v except
            |select * from v where c1 = (
            |  with v2 as (
            |    select c1, c2, rand() c3 from t
            |  )
            |  select count(*) from v where c2 not in (
            |    select c2 from v2 where c3 not in (select c3 from v2)
            |  )
            |)
            |""".stripMargin)
      }

      val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key, AQEPropagateEmptyRelation.ruleName)
        .set(SQLConf.BROADCAST_TIMEOUT.key, "30")
        .set("spark.rapids.sql.exec.FilterExec", (!cpuFilter).toString)
        .set("spark.rapids.sql.test.enabled", "false")
      val expected = withCpuSparkSession(spark => {
        try {
          query(spark).collect().toSeq
        } finally {
          spark.catalog.dropTempView("t")
        }
      }, conf)
      assert(expected.isEmpty)

      withGpuSparkSession(spark => withPreparationThreads(1) {
        withSingleBroadcastThread {
          withSQLConf("spark.rapids.sql.test.enabled" -> "false") {
            try {
              val result = query(spark)
              assert(result.collect().toSeq == expected)
              ExecutionPlanCaptureCallback.assertContains(result, "GpuBroadcastExchangeExec")
              if (cpuFilter) {
                val executed = ExecutionPlanCaptureCallback.extractExecutedPlan(
                  result.queryExecution.executedPlan)
                // Spark 4 wraps the final plan in a ResultQueryStageExec.
                val root = executed match {
                  case stage: QueryStageExec => stage.plan
                  case plan => plan
                }
                val broadcasts = PlanUtils.findOperators(root,
                  _.isInstanceOf[GpuBroadcastExchangeExecBase])
                val filters = broadcasts.flatMap { broadcast =>
                  PlanUtils.findOperators(broadcast.children.head, _.isInstanceOf[FilterExec])
                }
                withClue(s"${result.queryExecution.executedPlan}\n") {
                  assert(filters.exists(_.expressions.exists(_.exists {
                    case _: ScalarSubquery => true
                    case _ => false
                  })), "a CPU filter below a GPU broadcast must own a scalar subquery")
                }
              } else {
                ExecutionPlanCaptureCallback.assertContains(result, "GpuScalarSubquery")
              }
            } finally {
              spark.catalog.dropTempView("t")
            }
          }
        }
      }, conf)
    }
  }

  private def controlledBroadcast(
      entered: CountDownLatch,
      release: CountDownLatch,
      collected: CountDownLatch,
      failure: Option[Throwable] = None,
      beforeResult: () => Unit = () => ()): GpuBroadcastExchangeExec = {
    val subquery = ControlledBroadcastSubquery(entered, release, failure, beforeResult)
    val child = ControlledBroadcastChild(ScalarSubquery(subquery, ExprId(0)), collected)
    GpuBroadcastExchangeExec(IdentityBroadcastMode, child)(
      BroadcastExchangeExec(IdentityBroadcastMode, child))
  }

  test("saturated preparation pool completes nested broadcasts within its thread bound") {
    withGpuSparkSession(_ => withPreparationThreads(2) {
      withSingleBroadcastThread {
        val executor = GpuBroadcastExchangeExecBase.preparationExecutor
        val entered = new CountDownLatch(2)
        val release = new CountDownLatch(1)
        val nestedEntered = new CountDownLatch(2)
        val nestedRelease = new CountDownLatch(1)
        val collected = new CountDownLatch(4)
        val exchanges = Seq.fill(2) {
          controlledBroadcast(entered, release, collected, beforeResult = () => {
            val parentThread = Thread.currentThread()
            val nested = controlledBroadcast(nestedEntered, nestedRelease, collected,
              beforeResult = () => assert(Thread.currentThread() eq parentThread))
            try {
              nested.prepare()
              nested.relationFuture.get(10, TimeUnit.SECONDS)
            } finally {
              nested.relationFuture.cancel(true)
            }
          })
        }
        try {
          exchanges.foreach(_.prepare())
          assert(entered.await(10, TimeUnit.SECONDS))
          assert(executor.getActiveCount == 2)
          assert(collected.getCount == 4)
          // Nested broadcasts must be able to use the sole materialization worker while waiting.
          val worker = GpuBroadcastExchangeExecBase.executionContext.submit(new Runnable {
            override def run(): Unit = {}
          })
          worker.get(10, TimeUnit.SECONDS)
          release.countDown()
          assert(nestedEntered.await(10, TimeUnit.SECONDS))
          assert(executor.getPoolSize == 2)
          assert(executor.getMaximumPoolSize == 2)
          assert(executor.getQueue.isEmpty)
          nestedRelease.countDown()
          exchanges.foreach(_.relationFuture.get(10, TimeUnit.SECONDS))
          assert(collected.getCount == 0)
        } finally {
          release.countDown()
          nestedRelease.countDown()
          exchanges.foreach(_.relationFuture.cancel(true))
        }
      }
    })
  }

  test("subquery preparation errors complete both broadcast futures") {
    withGpuSparkSession(_ => {
      val collected = new CountDownLatch(1)
      val error = new IllegalStateException("subquery preparation failed")
      val exchange = controlledBroadcast(new CountDownLatch(1), new CountDownLatch(0),
        collected, Some(error))
      exchange.prepare()
      assert(intercept[ExecutionException] {
        exchange.relationFuture.get(10, TimeUnit.SECONDS)
      }.getCause eq error)
      assert(intercept[IllegalStateException] {
        Await.result(exchange.completionFuture, 10.seconds)
      } eq error)
      assert(collected.getCount == 1)
    })
  }

  for (cancelByTimeout <- Seq(false, true)) {
    test(s"cancellation interrupts saturated nested preparation (timeout: $cancelByTimeout)") {
      withGpuSparkSession(_ => withPreparationThreads(1) {
        withSQLConf(SQLConf.BROADCAST_TIMEOUT.key -> "1") {
          val entered = new CountDownLatch(1)
          val release = new CountDownLatch(1)
          val collected = new CountDownLatch(1)
          val nested = controlledBroadcast(entered, release, collected)
          val exchange = controlledBroadcast(new CountDownLatch(0), new CountDownLatch(0),
            collected, beforeResult = () => {
              nested.prepare()
              nested.relationFuture.get(10, TimeUnit.SECONDS)
            })
          try {
            exchange.prepare()
            assert(entered.await(10, TimeUnit.SECONDS))
            if (cancelByTimeout) {
              intercept[SparkException] { exchange.executeColumnarBroadcast[Any]() }
            } else {
              assert(exchange.relationFuture.cancel(true))
            }
            assert(exchange.relationFuture.isCancelled)
            assert(exchange.completionFuture.isCompleted)
            val subquery = nested.child.asInstanceOf[ControlledBroadcastChild].subquery.plan
              .asInstanceOf[ControlledBroadcastSubquery]
            assert(subquery.finished.await(10, TimeUnit.SECONDS), "preparation must be interrupted")
            release.countDown()
            assert(!collected.await(100, TimeUnit.MILLISECONDS))
          } finally {
            release.countDown()
            exchange.relationFuture.cancel(true)
            nested.relationFuture.cancel(true)
          }
        }
      })
    }
  }
}

// These driver-only plans control the actual Spark prepare/wait boundary without GPU job timing.
private case class ControlledBroadcastSubquery(
    entered: CountDownLatch,
    release: CountDownLatch,
    failure: Option[Throwable],
    beforeResult: () => Unit) extends BaseSubqueryExec with LeafExecNode {
  override def name: String = "controlled broadcast subquery"
  override val child: SparkPlan = ControlledBroadcastInput()
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
  val finished = new CountDownLatch(1)
  override def executeCollect(): Array[InternalRow] = {
    entered.countDown()
    try {
      require(release.await(10, TimeUnit.SECONDS), "subquery was not released")
      beforeResult()
      failure.foreach(throw _)
      Array(InternalRow(1L))
    } finally {
      finished.countDown()
    }
  }
}

private case class ControlledBroadcastChild(
    subquery: ScalarSubquery,
    collected: CountDownLatch) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def supportsColumnar: Boolean = true
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    collected.countDown()
    sparkContext.emptyRDD[ColumnarBatch]
  }
}


private case class ControlledBroadcastInput() extends LeafExecNode {
  override val output: Seq[Attribute] = Seq(AttributeReference("value", LongType)())
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
}
