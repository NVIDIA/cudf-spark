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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.{Partition, ShuffleDependency, TaskContext}
import org.apache.spark.rapids.shims.ShuffledBatchRDDUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.rapids.{GpuShuffleEnv, ShuffleReadSnapshotProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Keeps shuffle lineage while opening the payload reader only for an executor-cache miss. */
private[execution] class DriverlessBroadcastBuildRDD(
    buildDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    exchangeMetrics: Map[String, SQLMetric],
    schema: StructType,
    output: Seq[Attribute],
    joinMetrics: Map[String, GpuMetric],
    conf: RapidsConf) extends ShuffledBatchRDD(buildDependency, exchangeMetrics) {

  private val maxExecutors = conf.driverlessBroadcastMaxExecutors
  private val codecConfigs = TableCompressionCodec.makeCodecConfig(conf)
  private val maxDecompressBatchMemory = conf.shuffleCompressionMaxBatchMemory
  private val shuffleId = buildDependency.shuffleId

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    // Defer metadata lookup and cache acquisition until the join consumes this iterator.
    new Iterator[ColumnarBatch] {
      private var pending = true

      override def hasNext: Boolean = pending

      override def next(): ColumnarBatch = {
        if (!pending) throw new NoSuchElementException("Driverless build already consumed")
        context.killTaskIfInterrupted()
        val reporter = new SQLShuffleReadMetricsReporter(
          context.taskMetrics().createTempShuffleReadMetrics(), exchangeMetrics)
        val (reader, partitionSize) = ShuffledBatchRDDUtil.getReaderAndPartSize(
          split, context, DriverlessBroadcastBuildRDD.this.dependency, reporter)
        val snapshot = reader match {
          case provider: ShuffleReadSnapshotProvider => provider.snapshot
          case _ => throw new IllegalStateException(
            "Driverless broadcast requires a GPU shuffle reader with output identities")
        }
        val identity = snapshot.buildIdentity(shuffleId)
        var builtHere = false
        val result = GpuShuffleEnv.getBroadcastCache.getOrBuild[ColumnarBatch](shuffleId, () => {
          DriverlessBroadcastPolicy.checkExecutorLimit(maxExecutors)
          builtHere = true
          joinMetrics("executorBuilds") += 1
          exchangeMetrics(GpuMetric.NUM_PARTITIONS).add(1)
          exchangeMetrics(GpuMetric.PARTITION_SIZE).add(partitionSize)
          val input = reader.read().asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]].map(_._2)
          val batch = GpuDriverlessBroadcastHelper.assemble(input, schema, output, joinMetrics,
            codecConfigs, maxDecompressBatchMemory)
          closeOnExcept(batch)(
            SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }, () => GpuSemaphore.releaseIfNecessary(context),
          () => context.killTaskIfInterrupted(),
          () => GpuSemaphore.acquireIfNecessary(context), identity)(
          GpuDriverlessBroadcastHelper.borrow, batch => withResource(batch)(_ => ()))
        if (!builtHere) joinMetrics("executorBuildCacheHits") += 1
        pending = false
        result
      }
    }
  }
}
