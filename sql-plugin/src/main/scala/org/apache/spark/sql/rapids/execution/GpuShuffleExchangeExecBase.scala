/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import scala.collection.AbstractIterator
import scala.concurrent.Future

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric.{DEBUG_LEVEL, ESSENTIAL_LEVEL, MODERATE_LEVEL}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, GpuRangePartitioning, ShimUnaryExecNode, ShuffleOriginUtil, SparkShimImpl}

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.createAdditionalExchangeMetrics
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

abstract class GpuShuffleMetaBase(
    shuffle: ShuffleExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ShuffleExchangeExec](shuffle, conf, parent, rule) {
  // Some kinds of Partitioning are a type of expression, but Partitioning itself is not
  // so don't let them leak through as expressions
  override val childExprs: scala.Seq[ExprMeta[_]] = Seq.empty
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(shuffle.outputPartitioning, conf, Some(this)))

  // Propagate possible type conversions on the output attributes of map-side plans to
  // reduce-side counterparts. We can pass through the outputs of child because Shuffle will
  // not change the data schema. And we need to pass through because Shuffle itself and
  // reduce-side plans may failed to pass the type check for tagging CPU data types rather
  // than their GPU counterparts.
  //
  // Taking AggregateExec with TypedImperativeAggregate function as example:
  //    Assume I have a query: SELECT a, COLLECT_LIST(b) FROM table GROUP BY a, which physical plan
  //    looks like:
  //    ObjectHashAggregate(keys=[a#10], functions=[collect_list(b#11, 0, 0)],
  //                        output=[a#10, collect_list(b)#17])
  //      +- Exchange hashpartitioning(a#10, 200), true, [id=#13]
  //        +- ObjectHashAggregate(keys=[a#10], functions=[partial_collect_list(b#11, 0, 0)],
  //                               output=[a#10, buf#21])
  //          +- LocalTableScan [a#10, b#11]
  //
  //    We will override the data type of buf#21 in GpuNoHashAggregateMeta. Otherwise, the partial
  //    Aggregate will fall back to CPU because buf#21 produce a GPU-unsupported type: BinaryType.
  //    Just like the partial Aggregate, the ShuffleExchange will also fall back to CPU unless we
  //    apply the same type overriding as its child plan: the partial Aggregate.
  override protected val useOutputAttributesOfChild: Boolean = true

  // For transparent plan like ShuffleExchange, the accessibility of runtime data transition is
  // depended on the next non-transparent plan. So, we need to trace back.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

  override def tagPlanForGpu(): Unit = {

    if (!ShuffleOriginUtil.isSupported(shuffle.shuffleOrigin)) {
      willNotWorkOnGpu(s"${shuffle.shuffleOrigin} not supported on GPU")
    }

    shuffle.outputPartitioning match {
      case _: RoundRobinPartitioning
        if SparkSessionUtils.sessionFromPlan(shuffle).sessionState.conf
            .sortBeforeRepartition =>
        val orderableTypes = GpuOverrides.pluginSupportedOrderableSig +
            TypeSig.ARRAY.nested(GpuOverrides.gpuCommonTypes)

        shuffle.output.map(_.dataType)
            .filterNot(orderableTypes.isSupportedByPlugin)
            .foreach { dataType =>
              willNotWorkOnGpu(s"round-robin partitioning cannot sort $dataType to run " +
                  s"this on the GPU set ${SQLConf.SORT_BEFORE_REPARTITION.key} to false")
            }
      case _ =>
    }

    // When AQE is enabled, we need to preserve meta data as outputAttributes and
    // availableRuntimeDataTransition to the spark plan for the subsequent query stages.
    // These meta data will be fetched in the SparkPlanMeta of CustomShuffleReaderExec.
    if (wrapped.getTagValue(GpuShuffleMetaBase.shuffleExOutputAttributes).isEmpty) {
      wrapped.setTagValue(GpuShuffleMetaBase.shuffleExOutputAttributes, outputAttributes)
    }
    if (wrapped.getTagValue(GpuShuffleMetaBase.availableRuntimeDataTransition).isEmpty) {
      wrapped.setTagValue(GpuShuffleMetaBase.availableRuntimeDataTransition,
        availableRuntimeDataTransition)
    }
  }

  override final def convertToGpu(): GpuExec = {
    // Any AQE child node should be marked columnar if we're columnar.
    val newChild = childPlans.head.wrapped match {
      case adaptive: AdaptiveSparkPlanExec =>
        val goal = TargetSize(conf.gpuTargetBatchSizeBytes)
        SparkShimImpl.columnarAdaptivePlan(adaptive, goal)
      case _ => childPlans.head.convertIfNeeded()
    }
    val gpuShuffle = convertShuffleToGpu(newChild)
    GpuTransitionOverrides.copyAqeQueryStageExchangeTag(shuffle, gpuShuffle)
    gpuShuffle
  }

  protected def convertShuffleToGpu(newChild: SparkPlan): GpuExec = {
    GpuShuffleExchangeExec(
      childParts.head.convertToGpu(),
      newChild,
      shuffle.shuffleOrigin
    )(shuffle.outputPartitioning)
  }
}

object GpuShuffleMetaBase {

  val shuffleExOutputAttributes = TreeNodeTag[Seq[Attribute]](
    "rapids.gpu.shuffleExOutputAttributes")

  val availableRuntimeDataTransition = TreeNodeTag[Boolean](
    "rapids.gpu.availableRuntimeDataTransition")

}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
abstract class GpuShuffleExchangeExecBaseWithMetrics(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan) extends GpuShuffleExchangeExecBase(gpuOutputPartitioning, child) {

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputBatchRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependencyColumnar)
    }
  }
}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
abstract class GpuShuffleExchangeExecBase(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan) extends Exchange with ShimUnaryExecNode with GpuExec {
  import GpuMetric._

  private lazy val kudoMode = RapidsConf.ShuffleKudoMode.withName(
    RapidsConf.SHUFFLE_KUDO_WRITE_MODE.get(child.conf))
  private lazy val useKudo = RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.get(child.conf)
  private lazy val kudoBufferCopyMeasurementEnabled = RapidsConf
    .SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED
    .get(child.conf)

  private lazy val useGPUShuffle = {
    gpuOutputPartitioning match {
      case gpuPartitioning: GpuPartitioning => gpuPartitioning.usesGPUShuffle
      case _ => false
    }
  }

  private lazy val useMultiThreadedShuffle = {
    gpuOutputPartitioning match {
      case gpuPartitioning: GpuPartitioning => gpuPartitioning.usesMultiThreadedShuffle
      case _ => false
    }
  }

  // Shuffle produces a lot of small output batches that should be coalesced together.
  // This coalesce occurs on the GPU and should always be done when using RAPIDS shuffle,
  // when it is under UCX or CACHE_ONLY modes.
  // Normal shuffle performs the coalesce on the CPU to optimize the transfers to the GPU.
  override def coalesceAfter: Boolean = useGPUShuffle

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val additionalMetrics : Map[String, GpuMetric] = {
    createAdditionalExchangeMetrics(this) ++
      GpuMetric.wrap(readMetrics) ++
      GpuMetric.wrap(writeMetrics)
  }

  // Spark doesn't report totalTime for this operator so we override metrics
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_NEW_SHUFFLE_READ ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW_SHUFFLE_READ),
    OP_TIME_NEW_SHUFFLE_WRITE ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW_SHUFFLE_WRITE),
    PARTITION_SIZE -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_PARTITION_SIZE),
    NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
    NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES),
    COPY_TO_HOST_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_COPY_TO_HOST_TIME)
  ) ++ additionalMetrics

  override def getOpTimeNewMetric: Option[GpuMetric] = allMetrics.get(OP_TIME_NEW_SHUFFLE_READ)

  override def nodeName: String = "GpuColumnarExchange"

  def shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    throw new IllegalStateException()
  }

  private lazy val sparkTypes: Array[DataType] = child.output.map(_.dataType).toArray

  // This value must be lazy because the child's output may not have been resolved
  // yet in all cases.
  private lazy val serializer: Serializer = new GpuColumnarBatchSerializer(
    allMetrics, sparkTypes, kudoMode, useKudo, kudoBufferCopyMeasurementEnabled)

  @transient lazy val inputBatchRDD: RDD[ColumnarBatch] = child.executeColumnar()

  /**
   * A `ShuffleDependency` that will partition columnar batches of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependencyColumnar : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val descendantOpTimeMetrics = getDescendantOpTimeMetrics
    val opTimeNewShuffleWrite = allMetrics.get(OP_TIME_NEW_SHUFFLE_WRITE)
    
    GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputBatchRDD,
      child.output,
      gpuOutputPartitioning,
      sparkTypes,
      serializer,
      useGPUShuffle,
      useMultiThreadedShuffle,
      allMetrics,
      writeMetrics,
      additionalMetrics,
      opTimeNewShuffleWrite,
      descendantOpTimeMetrics,
      enableOpTimeTrackingRdd)
  }

  /**
   * Caches the created ShuffleBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: RDD[ColumnarBatch] = null

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = SparkShimImpl
    .attachTreeIfSupported(this, "execute") {
      // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
      if (cachedShuffleRDD == null) {
        cachedShuffleRDD = new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics)
      }
      cachedShuffleRDD
    }
}

object GpuShuffleExchangeExecBase {
  val METRIC_DATA_SIZE = "dataSize"
  val METRIC_DESC_DATA_SIZE = "data size"
  val METRIC_DATA_READ_SIZE = "dataReadSize"
  val METRIC_DESC_DATA_READ_SIZE = "data read size"
  val METRIC_SHUFFLE_SER_STREAM_TIME = "rapidsShuffleSerializationStreamTime"
  val METRIC_DESC_SHUFFLE_SER_STREAM_TIME = "RAPIDS shuffle serialization to output stream time"
  val METRIC_SHUFFLE_DESERIALIZATION_TIME = "rapidsShuffleDeserializationTime"
  val METRIC_DESC_SHUFFLE_DESERIALIZATION_TIME = "RAPIDS shuffle deserialization time"
  val METRIC_SHUFFLE_DESER_STREAM_TIME = "rapidsShuffleDeserializationStreamTime"
  val METRIC_DESC_SHUFFLE_DESER_STREAM_TIME =
    "RAPIDS shuffle deserialization from input stream time"
  val METRIC_SHUFFLE_PARTITION_TIME = "rapidsShufflePartitionTime"
  val METRIC_DESC_SHUFFLE_PARTITION_TIME = "RAPIDS shuffle partition time"
  val METRIC_SHUFFLE_READ_TIME = "rapidsShuffleReadTime"
  val METRIC_DESC_SHUFFLE_READ_TIME = "RAPIDS shuffle shuffle read time"
  val METRIC_SHUFFLE_SER_COPY_BUFFER_TIME = "rapidsShuffleSerializationCopyBufferTime"
  val METRIC_DESC_SHUFFLE_SER_COPY_BUFFER_TIME = "RAPIDS shuffle serialization copy buffer time"
  val METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM = "rapidsShuffleStalledByInputStream"
  val METRIC_DESC_SHUFFLE_STALLED_BY_INPUT_STREAM =
    "RAPIDS shuffle time stalled by input stream operations"
  val METRIC_THREADED_WRITER_LIMITER_WAIT_TIME = "rapidsThreadedWriterLimiterWaitTime"
  val METRIC_DESC_THREADED_WRITER_LIMITER_WAIT_TIME =
    "threaded writer limiter wait time"
  val METRIC_THREADED_WRITER_SERIALIZATION_WAIT_TIME =
    "rapidsThreadedWriterSerializationWaitTime"
  val METRIC_DESC_THREADED_WRITER_SERIALIZATION_WAIT_TIME =
    "threaded writer serialization wait time"
  val METRIC_THREADED_WRITER_INPUT_FETCH_TIME = "rapidsThreadedWriterInputFetchTime"
  val METRIC_DESC_THREADED_WRITER_INPUT_FETCH_TIME =
    "threaded writer input fetch time (records.hasNext/next)"

  // New metrics for shuffle read wall time breakdown
  val METRIC_THREADED_READER_IO_WAIT_TIME = "rapidsThreadedReaderIoWaitTime"
  val METRIC_DESC_THREADED_READER_IO_WAIT_TIME =
    "threaded reader time waiting for IO (fetcherIterator.next)"
  val METRIC_THREADED_READER_DESER_WAIT_TIME = "rapidsThreadedReaderDeserWaitTime"
  val METRIC_DESC_THREADED_READER_DESER_WAIT_TIME =
    "threaded reader time waiting for background deserialization (future.get + queued.take)"
  val METRIC_THREADED_READER_LIMITER_ACQUIRE_COUNT =
    "rapidsThreadedReaderLimiterAcquireCount"
  val METRIC_DESC_THREADED_READER_LIMITER_ACQUIRE_COUNT =
    "threaded reader total number of limiter.acquire() calls"
  val METRIC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT =
    "rapidsThreadedReaderLimiterAcquireFailCount"
  val METRIC_DESC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT =
    "threaded reader number of times limiter.acquire() returned false"
  val METRIC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT =
    "rapidsThreadedReaderLimiterPendingBlockCount"
  val METRIC_DESC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT =
    "threaded reader number of blocks deferred due to limiter being full"

  def createAdditionalExchangeMetrics(gpu: GpuExec): Map[String, GpuMetric] = Map(
    // dataSize and dataReadSize are uncompressed, one is on write and the other on read
    METRIC_DATA_SIZE -> gpu.createSizeMetric(ESSENTIAL_LEVEL, METRIC_DESC_DATA_SIZE),
    METRIC_DATA_READ_SIZE -> gpu.createSizeMetric(MODERATE_LEVEL, METRIC_DESC_DATA_READ_SIZE),
    METRIC_SHUFFLE_SER_STREAM_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_SER_STREAM_TIME),
    METRIC_SHUFFLE_DESERIALIZATION_TIME  ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_DESERIALIZATION_TIME),
    METRIC_SHUFFLE_DESER_STREAM_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_DESER_STREAM_TIME),
    METRIC_SHUFFLE_PARTITION_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_PARTITION_TIME),
    METRIC_SHUFFLE_READ_TIME ->
        gpu.createNanoTimingMetric(ESSENTIAL_LEVEL, METRIC_DESC_SHUFFLE_READ_TIME),
    METRIC_SHUFFLE_SER_COPY_BUFFER_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_SER_COPY_BUFFER_TIME),
    METRIC_SHUFFLE_STALLED_BY_INPUT_STREAM ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL, METRIC_DESC_SHUFFLE_STALLED_BY_INPUT_STREAM),
    METRIC_THREADED_WRITER_LIMITER_WAIT_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_WRITER_LIMITER_WAIT_TIME),
    METRIC_THREADED_WRITER_SERIALIZATION_WAIT_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_WRITER_SERIALIZATION_WAIT_TIME),
    METRIC_THREADED_WRITER_INPUT_FETCH_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_WRITER_INPUT_FETCH_TIME),
    METRIC_THREADED_READER_IO_WAIT_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_READER_IO_WAIT_TIME),
    METRIC_THREADED_READER_DESER_WAIT_TIME ->
        gpu.createNanoTimingMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_READER_DESER_WAIT_TIME),
    METRIC_THREADED_READER_LIMITER_ACQUIRE_COUNT ->
        gpu.createMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_READER_LIMITER_ACQUIRE_COUNT),
    METRIC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT ->
        gpu.createMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_READER_LIMITER_ACQUIRE_FAIL_COUNT),
    METRIC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT ->
        gpu.createMetric(DEBUG_LEVEL,
          METRIC_DESC_THREADED_READER_LIMITER_PENDING_BLOCK_COUNT)
  )

  def prepareBatchShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: GpuPartitioning,
      sparkTypes: Array[DataType],
      serializer: Serializer,
      useGPUShuffle: Boolean,
      useMultiThreadedShuffle: Boolean,
      metrics: Map[String, GpuMetric],
      writeMetrics: Map[String, SQLMetric],
      additionalMetrics: Map[String, GpuMetric],
      opTimeNewShuffleWrite: Option[GpuMetric] = None,
      descendantOpTimeMetrics: Seq[GpuMetric] = Seq.empty,
      enableOpTimeTrackingRdd: Boolean = true)
  : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val isRoundRobin = newPartitioning match {
      case _: GpuRoundRobinPartitioning => true
      case _ => false
    }

    /**
     * Analogous to how spark config for sorting before repartition works, the GPU implementation
     * here sorts on the GPU over all columns of the table in ascending order and nulls as smallest.
     * This essentially means the results are not one to one w.r.t. the row hashcode based sort.
     * As long as we don't mix and match repartition() between CPU and GPU this should not be a
     * concern. Latest spark behavior does not even require this sort as it fails the upstream
     * task when indeterminate tasks re-run.
     */
    val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
      val boundReferences = outputAttributes.zipWithIndex.map { case (attr, index) =>
        SortOrder(GpuBoundReference(index, attr.dataType,
          attr.nullable)(attr.exprId, attr.name), Ascending)
        // Force the sequence to materialize so we don't have issues with serializing too much
      }.toArray.toSeq
      val sorter = new GpuSorter(boundReferences, outputAttributes, metrics)
      rdd.mapPartitions { cbIter =>
        GpuSortEachBatchIterator(cbIter, sorter, false)
      }
    } else {
      rdd
    }
    val partitioner: GpuExpression = getPartitioner(newRdd, outputAttributes,
      newPartitioning, metrics)
    // Inject debugging subMetrics, such as D2HTime before SliceOnCpu
    // The injected metrics will be serialized as the members of GpuPartitioning
    partitioner match {
      case pt: GpuPartitioning => pt.setupDebugMetrics(metrics)
      case _ =>
    }
    val partitionTime: GpuMetric = metrics(METRIC_SHUFFLE_PARTITION_TIME)
    def getPartitioned: ColumnarBatch => Any = {
      batch => partitionTime.ns {
        partitioner.columnarEvalAny(batch)
      }
    }
    val rddWithPartitionIds: RDD[Product2[Int, ColumnarBatch]] = {
      newRdd.mapPartitions { iter =>
        val getParts = getPartitioned
        new AbstractIterator[Product2[Int, ColumnarBatch]] {
          private var partitionedIter: Iterator[Array[(ColumnarBatch, Int)]] =
            Iterator.empty
          private var partitioned: Array[(ColumnarBatch, Int)] = _
          private var at = 0
          private val mutablePair = new MutablePair[Int, ColumnarBatch]()
          private def partNextBatch(): Unit = {
            if (partitioned != null) {
              partitioned.map(_._1).safeClose()
              partitioned = null
              at = 0
            }
            // Try to fill partitionedIter from iter if it's empty
            if (!partitionedIter.hasNext && iter.hasNext) {
              var batch = iter.next()
              while (batch.numRows == 0 && iter.hasNext) {
                batch.close()
                batch = iter.next()
              }
              // Get a non-empty batch or the last batch. So still need to
              // check if it is empty for the later case.
              if (batch.numRows > 0) {
                val spillableBatch = SpillableColumnarBatch(batch,
                    ACTIVE_ON_DECK_PRIORITY)
                partitionedIter = withRetry(spillableBatch,
                    splitSpillableInHalfByRows) { spillable =>
                  val cb = spillable.getColumnarBatch()
                  getParts(cb).asInstanceOf[Array[(ColumnarBatch, Int)]]
                }
              } else {
                batch.close()
              }
            }
            // Process the next partitioned array from the iterator
            if (partitionedIter.hasNext) {
              partitioned = partitionedIter.next()
              partitioned.foreach(batches => {
                metrics(GpuMetric.NUM_OUTPUT_ROWS) += batches._1.numRows()
              })
              metrics(GpuMetric.NUM_OUTPUT_BATCHES) += partitioned.length
              at = 0
            }
          }

          override def hasNext: Boolean = {
            if (partitioned == null || at >= partitioned.length) {
              partNextBatch()
            }

            partitioned != null && at < partitioned.length
          }

          override def next(): Product2[Int, ColumnarBatch] = {
            if (partitioned == null || at >= partitioned.length) {
              partNextBatch()
            }
            if (partitioned == null || at >= partitioned.length) {
              throw new NoSuchElementException("Walked off of the end...")
            }
            val tup = partitioned(at)
            mutablePair.update(tup._2, tup._1)
            at += 1
            mutablePair
          }
        }
      }
    }

    // Apply OP_TIME_NEW tracking to rddWithPartitionIds if opTimeNewMetric is provided
    val finalRddWithPartitionIds = opTimeNewShuffleWrite match {
      case Some(opTimeMetric) if enableOpTimeTrackingRdd =>
        new GpuOpTimeTrackingRDD[Product2[Int, ColumnarBatch]](
          rddWithPartitionIds, opTimeMetric, descendantOpTimeMetrics)
      case _ => rddWithPartitionIds
    }

    // Now, we manually create a GpuShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    // We do a GPU version because it allows us to know that the data is on the GPU so we can
    // detect it and do further processing if needed.
    val dependency =
    new GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      finalRddWithPartitionIds,
      new BatchPartitionIdPassthrough(newPartitioning.numPartitions),
      sparkTypes,
      serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
      useGPUShuffle = useGPUShuffle,
      useMultiThreadedShuffle = useMultiThreadedShuffle,
      metrics = GpuMetric.unwrap(additionalMetrics))

    dependency
  }

  private def getPartitioner(
    rdd: RDD[ColumnarBatch],
    outputAttributes: Seq[Attribute],
    newPartitioning: GpuPartitioning,
    metrics: Map[String, GpuMetric]): GpuExpression with GpuPartitioning = {
    newPartitioning match {
      case h: GpuHashPartitioning =>
        GpuBindReferences.bindReference(h, outputAttributes, metrics)
      case r: GpuRangePartitioning =>
        val sorter = new GpuSorter(r.gpuOrdering, outputAttributes, metrics)
        val bounds = GpuRangePartitioner.createRangeBounds(r.numPartitions, sorter,
          rdd, SQLConf.get.rangeExchangeSampleSizePerPartition)
        // No need to bind arguments for the GpuRangePartitioner. The Sorter has already done it
        new GpuRangePartitioner(bounds, sorter)
      case GpuSinglePartitioning =>
        GpuSinglePartitioning
      case rrp: GpuRoundRobinPartitioning =>
        GpuBindReferences.bindReference(rrp, outputAttributes, metrics)
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
  }
}
