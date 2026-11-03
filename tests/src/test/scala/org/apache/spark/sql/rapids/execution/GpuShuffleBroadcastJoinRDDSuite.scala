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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{HashPartitioner, NarrowDependency, ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.shuffle.FetchFailedException

class GpuShuffleBroadcastJoinRDDSuite extends AnyFunSuite with BeforeAndAfterAll {
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
    val joined = new GpuShuffleBroadcastJoinRDD[Int, Int, Int](stream, build,
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
    val reused = new GpuShuffleBroadcastJoinRDD[Int, Int, Int](stream, build,
      (input, _) => input.map(_ + 10))
    assert(reused.collect().sorted.sameElements(Array(11, 12, 13, 14)))
    assert(reads.value == 0L)
  }

  test("multiple build partitions are rejected") {
    val stream = context.parallelize(Seq(1), 1)
    val build = context.parallelize(Seq(10, 20), 2)
    intercept[IllegalArgumentException] {
      new GpuShuffleBroadcastJoinRDD[Int, Int, Int](stream, build,
        (input, _) => input)
    }
  }

  test("a build fetch failure resubmits the build dependency and completes the join") {
    val stream = context.parallelize(Seq(1, 2, 3, 4), 2)
    val shuffled = context.parallelize(Seq(0 -> 10, 1 -> 20), 2)
      .partitionBy(new HashPartitioner(1))
    val shuffleId = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    val build = shuffled.values
    val joined = new GpuShuffleBroadcastJoinRDD[Int, Int, (Int, Int)](stream, build,
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
}
