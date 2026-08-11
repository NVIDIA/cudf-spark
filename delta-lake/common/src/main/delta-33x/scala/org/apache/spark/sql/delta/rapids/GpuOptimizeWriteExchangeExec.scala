/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * This file was derived from OptimizeWriteExchange.scala
 * in the Delta Lake project at https://github.com/delta-io/delta
 * (pending at https://github.com/delta-io/delta/pull/1198).
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.rapids.delta

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import com.nvidia.spark.rapids.{GpuColumnarBatchSerializer, GpuExec, GpuMetric, GpuPartitioning, GpuRoundRobinPartitioning, RapidsConf}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.delta.RapidsDeltaSQLConf
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, ShuffledBatchRDD}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.createAdditionalExchangeMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Execution plan to repartition and rebalance the data for Optimize Write.
 * The plan will be added on top of the query plan of each write job to avoid any interference
 * from Adaptive Query Execution.
 *
 * @param partitioning Partitioning to use data exchange.
 * @param child Input plan of write job.
 */
case class GpuOptimizeWriteExchangeExec(
    partitioning: GpuPartitioning,
    override val child: SparkPlan,
    @transient deltaLog: DeltaLog) extends Exchange with GpuExec with DeltaLogging {

  // Use 150% of target file size hint config considering parquet compression.
  // Still the result file can be smaller/larger than the config due to data skew or
  // variable compression ratio for each data type.
  final val PARQUET_COMPRESSION_RATIO = 1.5

  // Dummy partitioning because:
  // 1) The exact output partitioning is determined at query runtime
  // 2) optimizeWrite is always placed right after the top node(DeltaInvariantChecker),
  //    there is no parent plan to refer to outputPartitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(partitioning.numPartitions)

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  override lazy val additionalMetrics : Map[String, GpuMetric] = {
    createAdditionalExchangeMetrics(this) ++
      GpuMetric.wrap(readMetrics) ++
      GpuMetric.wrap(writeMetrics)
  }

  override lazy val allMetrics: Map[String, GpuMetric] = {
    Map(
      OP_TIME_NEW_SHUFFLE_WRITE ->
        createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW_SHUFFLE_WRITE),
      OP_TIME_NEW_SHUFFLE_READ ->
        createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW_SHUFFLE_READ),
      PARTITION_SIZE -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_PARTITION_SIZE),
      NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
      NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
      NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES)
    ) ++ additionalMetrics
  }

  override def getOpTimeNewMetric: Option[GpuMetric] = allMetrics.get(OP_TIME_NEW_SHUFFLE_READ)

  private lazy val serializer: Serializer =
    new GpuColumnarBatchSerializer(allMetrics,
      child.output.map(_.dataType).toArray,
      RapidsConf.ShuffleKudoMode.withName(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.get(child.conf)),
      RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.get(child.conf),
      RapidsConf.SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED.get(child.conf))

  @transient lazy val inputRDD: RDD[ColumnarBatch] = child.executeColumnar()

  @transient private lazy val childNumPartitions = inputRDD.getNumPartitions

  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (childNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  @transient private lazy val actualNumPartitions: Int = {
    if (childNumPartitions == 0) {
      0
    } else {
      val targetShuffleBlocks = conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS)
      math.min(
        math.max(targetShuffleBlocks / childNumPartitions, 1),
        conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS))
    }
  }

  // The actual partitioning to use for the shuffle exchange. The input partition count can be
  // adjusted based on the number of partitions in the input RDD and the target number of shuffle
  // blocks.
  @transient private lazy val actualPartitioning: GpuPartitioning = partitioning match {
    // Currently only hash and round-robin partitioning are supported.
    // See DeltaShufflePartitionsUtil.partitioningForRebalance() for more details.
    case p: GpuHashPartitioning => p.copy(numPartitions = actualNumPartitions)
    case p: GpuRoundRobinPartitioning => p.copy(numPartitions = actualNumPartitions)
  }

  @transient lazy val shuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // Get OP_TIME_NEW metrics from all descendants for exclusion (with deduplication)
    val descendantOpTimeMetrics = getDescendantOpTimeMetrics
    val opTimeNewShuffleWriteMetric = allMetrics.get(OP_TIME_NEW_SHUFFLE_WRITE)
    
    val dep = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputRDD,
      child.output,
      actualPartitioning,
      child.output.map(_.dataType).toArray,
      serializer,
      useGPUShuffle=actualPartitioning.usesGPUShuffle,
      useMultiThreadedShuffle=actualPartitioning.usesMultiThreadedShuffle,
      metrics=allMetrics,
      writeMetrics=writeMetrics,
      additionalMetrics=additionalMetrics,
      opTimeNewShuffleWrite=opTimeNewShuffleWriteMetric,
      descendantOpTimeMetrics=descendantOpTimeMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // Collect execution statistics, these will be used to adjust/decide how to split files
    val stats = ThreadUtils.awaitResult(mapOutputStatisticsFuture, Duration.Inf)

    if (stats == null) {
      new ShuffledBatchRDD(shuffleDependency, metrics)
    } else {
      try {
        val partitionSpecs = rebalancePartitions(stats)

        recordDeltaEvent(deltaLog,
          "delta.optimizeWrite.planned",
          data = Map(
            "originalPartitions" -> childNumPartitions,
            "outputPartitions" -> partitionSpecs.length,
            "shufflePartitions" -> actualNumPartitions,
            "numShuffleBlocks" -> conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS),
            "binSize" -> conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE),
            "maxShufflePartitions" ->
              conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS)
          )
        )

        new ShuffledBatchRDD(shuffleDependency, metrics, partitionSpecs.toArray)
      } catch {
        case e: Throwable =>
          logWarning("Failed to apply OptimizeWrite.", e)
          new ShuffledBatchRDD(shuffleDependency, metrics)
      }
    }
  }

  private def rebalancePartitions(stats: MapOutputStatistics): Seq[ShufflePartitionSpec] = {
    val binSize = ByteUnit.BYTE.convertFrom(
      conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE), ByteUnit.MiB)
    val smallPartitionFactor =
      conf.getConf(RapidsDeltaSQLConf.OPTIMIZE_WRITE_SMALL_PARTITION_FACTOR)
    val mergedPartitionFactor =
      conf.getConf(RapidsDeltaSQLConf.OPTIMIZE_WRITE_MERGED_PARTITION_FACTOR)
    val bytesByPartitionId = stats.bytesByPartitionId
    val targetPartitionSize = (binSize * PARQUET_COMPRESSION_RATIO).toLong

    val splitPartitions = if (actualPartitioning.isInstanceOf[GpuRoundRobinPartitioning]) {
      DeltaShufflePartitionsUtil.splitSizeListByTargetSize(
        bytesByPartitionId,
        targetPartitionSize,
        smallPartitionFactor,
        mergedPartitionFactor)
    } else {
      // For partitioned data, do not coalesce small partitions as it will hurt parallelism.
      // Eg. a partition containing 100 partition keys => a task will write 100 files.
      Seq.range(0, bytesByPartitionId.length).toArray
    }

    def optimizeSkewedPartition(reduceIndex: Int): Seq[ShufflePartitionSpec] = {
      val partitionSize = bytesByPartitionId(reduceIndex)
      if (partitionSize > targetPartitionSize) {
        val shuffleId = shuffleDependency.shuffleId
        val newPartitionSpec = DeltaShufflePartitionsUtil.createSkewPartitionSpecs(
          shuffleId,
          reduceIndex,
          targetPartitionSize,
          smallPartitionFactor,
          mergedPartitionFactor)

        if (newPartitionSpec.isEmpty) {
          CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
        } else {
          logDebug(s"[OptimizeWrite] Partition $reduceIndex is skew, " +
              s"split it into ${newPartitionSpec.get.size} parts.")
          newPartitionSpec.get
        }
      } else if (partitionSize > 0) {
        CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
      } else {
        Nil
      }
    }

    // Transform the partitions to the ranges.
    // e.g. [0, 3, 6, 7, 10] -> [[0, 3), [3, 6), [6, 7), [7, 10)]
    (splitPartitions :+ stats.bytesByPartitionId.length).sliding(2).flatMap { k =>
      if (k.head == k.last - 1) {
        // If not a merged partition, split it if needed.
        optimizeSkewedPartition(k.head)
      } else {
        CoalescedPartitionSpec(k.head, k.last) :: Nil
      }
    }.toList
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): GpuOptimizeWriteExchangeExec = {
    copy(child = newChild)
  }
}
