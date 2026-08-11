/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.rapids

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}

object LocationPreservingMapPartitionsRDD {
  def apply[U: ClassTag, T: ClassTag](prev: RDD[T],
      preservesPartitioning: Boolean = false)
      (f: Iterator[T] => Iterator[U]):  RDD[U] = prev.withScope {
    new LocationPreservingMapPartitionsRDD(
      prev,
      (_: TaskContext, _: Int, iter: Iterator[T]) => f(iter),
      preservesPartitioning)
  }
}

/**
 * Used for a map partitions where we want to be sure that the location information is not lost
 */
class LocationPreservingMapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
    extends MapPartitionsRDD[U, T](
      prev,
      f,
      preservesPartitioning = preservesPartitioning,
      isFromBarrier = isFromBarrier,
      isOrderSensitive = isOrderSensitive) {

  override def getPreferredLocations(split: Partition): Seq[String] =
    prev.preferredLocations(split)
}
