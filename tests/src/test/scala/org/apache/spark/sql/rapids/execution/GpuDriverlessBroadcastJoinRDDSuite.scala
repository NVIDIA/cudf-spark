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

import com.nvidia.spark.rapids.{GpuMetric, GpuOpTimeTrackingRDD}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{HashPartitioner, NarrowDependency, ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.execution.metric.SQLMetrics

private object BroadcastReplicaRetryState {
  @volatile var firstBuilt = new CountDownLatch(1)
  @volatile var caches = Array.fill(2)(new ExecutorBroadcastCache[Vector[Long]](_ => ()))
}

class GpuDriverlessBroadcastJoinRDDSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var context: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    context = new SparkContext(new SparkConf(false)
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false"))
  }

  override def afterAll(): Unit = {
    try {
      if (context != null) {
        context.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  test("the scheduler materializes the build shuffle before running its consumers") {
    val stream = context.parallelize(Seq(1, 2, 3, 4), 2)
    val build = context.parallelize(Seq(10, 20), 2).repartition(1)
    val joined = new GpuDriverlessBroadcastJoinRDD[Int, Int, Int](stream, build,
      (input, openBuild) => {
        val values = openBuild().toArray
        input.flatMap(value => values.iterator.map(value + _))
      })
    assert(joined.dependencies.size == 2)
    assert(joined.dependencies(0).asInstanceOf[NarrowDependency[Int]].getParents(1) == Seq(1))
    assert(joined.dependencies(1).asInstanceOf[NarrowDependency[Int]].getParents(1) == Seq(0))
    assert(joined.collect().sorted.sameElements(Array(11, 12, 13, 14, 21, 22, 23, 24)))
  }

  test("a consumer that reuses its build does not open the build iterator") {
    val reads = context.longAccumulator("build-reader-opens")
    val stream = context.parallelize(Seq(1, 2, 3, 4), 2)
    val build = context.parallelize(Seq(10, 20), 1).mapPartitions { input =>
      reads.add(1)
      input
    }
    val reused = new GpuDriverlessBroadcastJoinRDD[Int, Int, Int](stream, build,
      (input, _) => input.map(_ + 10))
    assert(reused.collect().sorted.sameElements(Array(11, 12, 13, 14)))
    assert(reads.value == 0L)
  }

  test("multiple build partitions are rejected") {
    val stream = context.parallelize(Seq(1), 1)
    val build = context.parallelize(Seq(10, 20), 2)
    intercept[IllegalArgumentException] {
      new GpuDriverlessBroadcastJoinRDD[Int, Int, Int](stream, build,
        (input, _) => input)
    }
  }

  test("a build fetch failure resubmits the build dependency and completes the join") {
    val stream = context.parallelize(Seq(1, 2, 3, 4), 2)
    val shuffled = context.parallelize(Seq(0 -> 10, 1 -> 20), 2)
      .partitionBy(new HashPartitioner(1))
    val shuffleId = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    val build = shuffled.values
    val joined = new GpuDriverlessBroadcastJoinRDD[Int, Int, (Int, Int)](stream, build,
      (input, openBuild) => {
        val values = openBuild().toArray
        val task = TaskContext.get()
        if (task.partitionId() == 0 && task.stageAttemptNumber() == 0) {
          throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
            shuffleId, 0L, 0, 0, "Injected build fetch failure")
        }
        input.flatMap(value => values.iterator.map(sum => (value + sum, task.stageAttemptNumber())))
      })
    val result = joined.collect()
    assert(result.map(_._1).sorted.sameElements(Array(11, 12, 13, 14, 21, 22, 23, 24)))
    assert(result.filter(row => Set(11, 12, 21, 22).contains(row._1)).forall(_._2 > 0))
  }

  test("a stage retry cannot mix an earlier replica with a recomputed build") {
    BroadcastReplicaRetryState.firstBuilt = new CountDownLatch(1)
    BroadcastReplicaRetryState.caches = Array.fill(2)(
      new ExecutorBroadcastCache[Vector[Long]](_ => ()))
    try {
      // Changing map attempt IDs expose stale cache reuse after map output loss.
      val source = context.parallelize(0 until 2, 2).mapPartitionsWithIndex {
        case (index, _) => Iterator(index -> TaskContext.get().taskAttemptId())
      }
      val shuffled = source.partitionBy(new HashPartitioner(1))
      val shuffleId = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
      val joined = new GpuDriverlessBroadcastJoinRDD[Int, Long, (Vector[Long], Int)](
        context.parallelize(0 until 2, 2), shuffled.values, (input, openBuild) => {
          val task = TaskContext.get()
          val consumer = task.partitionId()
          if (task.stageAttemptNumber() == 0 && consumer == 1) {
            assert(BroadcastReplicaRetryState.firstBuilt.await(10, TimeUnit.SECONDS))
            throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
              shuffleId, 0L, 0, 0, "Injected loss before the second replica reads")
          }
          val value = BroadcastReplicaRetryState.caches(consumer).getOrBuild[Vector[Long]](shuffleId,
            () => openBuild().toVector.sorted, () => (), () => task.killTaskIfInterrupted(),
            generation = ExecutorBroadcastCache.generation(
              task.stageId(), task.stageAttemptNumber()))(identity, _ => ())
          if (task.stageAttemptNumber() == 0 && consumer == 0) {
            BroadcastReplicaRetryState.firstBuilt.countDown()
            // Hold the first result until the fetch failure cancels this attempt.
            while (!task.isInterrupted()) {
              Thread.sleep(10)
            }
            task.killTaskIfInterrupted()
          }
          input.map(_ => (value, task.stageAttemptNumber()))
        })
      val result = joined.collect()
      assert(result.length == 2)
      assert(result.forall(_._2 > 0))
      assert(result.map(_._1).distinct.length == 1,
        "Both consumers must use the recomputed build, not a cached earlier attempt")
    } finally {
      BroadcastReplicaRetryState.caches.foreach(_.close())
    }
  }


  test("operator timing includes initialization before the first output batch") {
    val opTime = GpuMetric.wrap(SQLMetrics.createNanoTimingMetric(context, "join op time"))
    val initializationTime = context.longAccumulator("join initialization")
    val joined = new GpuDriverlessBroadcastJoinRDD[Int, Int, Int](
      context.parallelize(Seq(1), 1), context.parallelize(Seq(2), 1),
      (input, openBuild) => {
        val start = System.nanoTime()
        Thread.sleep(100)
        val values = openBuild().toVector
        initializationTime.add(System.nanoTime() - start)
        input.map(_ + values.head)
      })
    val tracked = new GpuOpTimeTrackingRDD(joined, opTime, Seq.empty)
    assert(tracked.collect().toSeq == Seq(3))
    assert(initializationTime.value > 0)
    assert(opTime.value >= initializationTime.value,
      "Build/cache initialization must execute inside the iterator timing boundary")
  }

}
