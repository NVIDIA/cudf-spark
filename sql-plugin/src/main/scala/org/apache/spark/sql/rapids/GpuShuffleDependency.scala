/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

/** Driver state shared by a round-robin writer and all readers of its dependency. */
private[rapids] class GpuRoundRobinShuffleRetry extends Serializable {
  @transient private var readerSubmitted = false

  def hasSubmittedReader: Boolean = readerSubmitted

  def markReaderSubmitted(): Unit = {
    readerSubmitted = true
  }
}

class GpuShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    val sparkTypes: Array[DataType],
    serializer: Serializer = SparkEnv.get.serializer,
    keyOrdering: Option[Ordering[K]] = None,
    aggregator: Option[Aggregator[K, V, C]] = None,
    mapSideCombine: Boolean = false,
    shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor,
    val useGPUShuffle: Boolean,
    val useMultiThreadedShuffle: Boolean,
    val metrics: Map[String, SQLMetric] = Map.empty)
  extends ShuffleDependency[K, V, C](rdd, partitioner, serializer, keyOrdering,
    aggregator, mapSideCombine, shuffleWriterProcessor) {

  // Assigned before this dependency can be submitted. Reader history lives as long as the
  // dependency: take/toLocalIterator may keep results after an individual job has finished.
  private[rapids] var roundRobinRetryGuard: Option[GpuRoundRobinShuffleRetry] = None

  // Set by RapidsShuffleInternalManagerBase.registerShuffle to record the
  // registration-time checksum fallback decision so writer and reader use
  // the same path even if SQLConf changes between calls.
  var checksumFallback: Boolean = false

  override def toString: String = "GPU Shuffle Dependency"
}
