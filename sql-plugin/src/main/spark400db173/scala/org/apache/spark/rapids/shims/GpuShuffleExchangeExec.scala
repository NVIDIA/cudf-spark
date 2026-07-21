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
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.{GpuMetric, GpuPartitioning}
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

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
    cpuOutputPartitioning: Partitioning)
  extends GpuDatabricksShuffleExchangeExecBase(gpuOutputPartitioning, child, shuffleOrigin)(
    cpuOutputPartitioning) {

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

  // In Databricks ShuffleExchangeExec, targetOutputPartitioning is the first
  // constructor parameter (the CPU partitioning). Our GPU version stores this as
  // cpuOutputPartitioning.
  override def targetOutputPartitioning: Partitioning = cpuOutputPartitioning

  // DBR uses numPartitions == 0 in DeltaOptimizedWritePartitioning as a sentinel. Its CPU
  // ShuffleExchangeExec resolves the physical partition count from the number of input
  // partitions immediately before constructing the shuffle dependency. Do the same for the GPU
  // dependency while retaining the native DBR partitioning as the exchange output contract.
  override protected def gpuOutputPartitioningForShuffle(
      inputNumPartitions: Int): GpuPartitioning = {
    gpuOutputPartitioning match {
      case hash: GpuHashPartitioning if shuffleOrigin == DELTA_OPTIMIZED_WRITE =>
        val numPartitions = cpuOutputPartitioning match {
          case delta: DeltaOptimizedWritePartitioning =>
            delta.createDynamicPhysicalPartitioning(inputNumPartitions).numPartitions
          case _ =>
            // AQE can replace the marker partitioning with its zero-partition physical hash
            // partitioning before this dependency is materialized. Reproduce DBR's dynamic
            // calculation from DeltaOptimizedWritePartitioning in that case.
            val targetBlocks = child.conf.getConf(
              DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS)
            val blocksPerPartition =
              if (inputNumPartitions > 0) targetBlocks / inputNumPartitions else 0
            math.min(
              math.max(blocksPerPartition, 1),
              child.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))
        }
        hash.copy(numPartitions = numPartitions)
      case _ => gpuOutputPartitioning
    }
  }

  override def withNewNumPartitions(numPartitions: Int): ShuffleExchangeLike = {
    val newCpuPartitioning = cpuOutputPartitioning.withNewNumPartitions(numPartitions)
    val newExec = copy(gpuOutputPartitioning, child, shuffleOrigin,
      adaptiveRepartitioningStatus)(newCpuPartitioning)
    newExec.copyTagsFrom(this)
    newExec
  }

  def repartition(numPartitions: Int,
      updatedRepartitioningStatus: AdaptiveRepartitioningStatus):
      ShuffleExchangeLike = {
    val newCpuPartitioning = cpuOutputPartitioning.withNewNumPartitions(numPartitions)
    copy(gpuOutputPartitioning, child, shuffleOrigin,
      updatedRepartitioningStatus)(newCpuPartitioning)
  }

  // not sure how it is used, so try to return one at first.
  // For more details, refer to https://github.com/NVIDIA/spark-rapids/issues/13242.
  override val ensReqDPMetricTag: TreeNodeTag[Int] = TreeNodeTag[Int]("GpuShuffleExchangeExec")
}
