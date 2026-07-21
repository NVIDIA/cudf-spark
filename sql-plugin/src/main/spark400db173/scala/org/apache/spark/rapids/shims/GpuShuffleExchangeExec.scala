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
/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import com.databricks.sql.transaction.tahoe.perf.DeltaOptimizedWritePartitioning
import com.nvidia.spark.rapids.{GpuMetric, GpuPartitioning, GpuRoundRobinPartitioning,
  GpuSinglePartitioning}
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, GpuRangePartitioning}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveRepartitioningStatus
import org.apache.spark.sql.execution.exchange.{DELTA_OPTIMIZED_WRITE, ShuffleExchangeLike,
  ShuffleOrigin}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.createAdditionalExchangeMetrics
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDD

case class GpuShuffleExchangeExec(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin,
    adaptiveRepartitioningStatus: AdaptiveRepartitioningStatus =
      AdaptiveRepartitioningStatus.DEFAULT_STATUS)(
    override val targetOutputPartitioning: Partitioning)
  extends GpuDatabricksShuffleExchangeExecBase(gpuOutputPartitioning, child, shuffleOrigin)(
    targetOutputPartitioning.getPhysicalPartitioning) {

  override def otherCopyArgs: Seq[AnyRef] = targetOutputPartitioning :: Nil

  override lazy val additionalMetrics: Map[String, GpuMetric] = {
    createAdditionalExchangeMetrics(this) ++
      GpuMetric.wrap(readMetrics) ++
      GpuMetric.wrap(
        SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)) ++
      // DBR 17.3 specific metrics from ShuffleExchangeLike's parent traits
      GpuMetric.wrap(skewMetrics) ++
      GpuMetric.wrap(spillFallbackMetrics) ++
      GpuMetric.wrap(ensReqDPMetrics) ++
      GpuMetric.wrap(adpMetrics) ++
      GpuMetric.wrap(aosMetrics)
  }

  // Databricks 17.3: Added stageShuffleCount parameter
  override def getShuffleRDD(
      partitionSpecs: Array[ShufflePartitionSpec],
      lazyFetching: Boolean,
      stageShuffleCount: Int): RDD[_] = {
    new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics, partitionSpecs)
  }

  // DBR keeps the partitioning requested by the optimizer separate from the physical
  // partitioning advertised by the exchange. Delta optimized write uses an unevaluable marker
  // as the target while its physical output is a hash partitioning with a zero-partition sentinel.
  // DBR uses numPartitions == 0 in DeltaOptimizedWritePartitioning as a sentinel. Its CPU
  // ShuffleExchangeExec resolves the physical partition count from the number of input
  // partitions immediately before constructing the shuffle dependency. Do the same for the GPU
  // dependency while retaining the native DBR marker as the target contract.
  override protected def gpuOutputPartitioningForShuffle(
      inputNumPartitions: Int): GpuPartitioning = {
    (targetOutputPartitioning, gpuOutputPartitioning) match {
      case (delta: DeltaOptimizedWritePartitioning, hash: GpuHashPartitioning)
          if shuffleOrigin == DELTA_OPTIMIZED_WRITE =>
        hash.copy(numPartitions =
          delta.createDynamicPhysicalPartitioning(inputNumPartitions).numPartitions)
      case _ => gpuOutputPartitioning
    }
  }

  private def gpuPartitioningWithNumPartitions(numPartitions: Int): GpuPartitioning = {
    gpuOutputPartitioning match {
      case hash: GpuHashPartitioning => hash.copy(numPartitions = numPartitions)
      case range: GpuRangePartitioning => range.copy(numPartitions = numPartitions)
      case roundRobin: GpuRoundRobinPartitioning =>
        roundRobin.copy(numPartitions = numPartitions)
      case GpuSinglePartitioning if numPartitions == 1 => GpuSinglePartitioning
      case other =>
        throw new IllegalStateException(
          s"Cannot resize ${other.getClass.getName} to $numPartitions partitions")
    }
  }

  override def withNewNumPartitions(numPartitions: Int): ShuffleExchangeLike = {
    val newTargetPartitioning = outputPartitioning.withNewNumPartitions(numPartitions)
    val newExec = copy(gpuPartitioningWithNumPartitions(numPartitions), child, shuffleOrigin,
      adaptiveRepartitioningStatus)(newTargetPartitioning)
    newExec.copyTagsFrom(this)
    newExec
  }

  def repartition(numPartitions: Int,
      updatedRepartitioningStatus: AdaptiveRepartitioningStatus):
      ShuffleExchangeLike = {
    val newTargetPartitioning = outputPartitioning.withNewNumPartitions(numPartitions)
    copy(gpuPartitioningWithNumPartitions(numPartitions), child, shuffleOrigin,
      updatedRepartitioningStatus)(newTargetPartitioning)
  }

  // not sure how it is used, so try to return one at first.
  // For more details, refer to https://github.com/NVIDIA/spark-rapids/issues/13242.
  override val ensReqDPMetricTag: TreeNodeTag[Int] = TreeNodeTag[Int]("GpuShuffleExchangeExec")
}
