/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.{mutable, BitSet}

import ai.rapids.cudf.{ContiguousTable, HostMemoryBuffer}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.GpuShuffledSizedHashJoinExec.JoinInfo
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.kudo.KudoTableHeader
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.rapids.{GpuHashExpression, GpuMurmur3Hash}
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuShuffledSizedHashJoinExec {
  def useSizedJoin(
      conf: RapidsConf,
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Boolean = {
    if (conf.useShuffledSymmetricHashJoin) {
      joinType match {
        case Inner | FullOuter => true
        case LeftOuter | RightOuter if conf.useShuffledAsymmetricHashJoin =>
          // currently cannot handle the case where the outer join becomes the build side
          // and there are nullable struct children in the join keys
          !GpuHashJoin.anyNullableStructChild(leftKeys) &&
            !GpuHashJoin.anyNullableStructChild(rightKeys)
        case _ => false
      }
    } else {
      false
    }
  }

  /** Utility class to track bound expressions and expression metadata related to a join. */
  case class BoundJoinExprs(
      boundBuildKeys: Seq[GpuExpression],
      buildTypes: Array[DataType],
      buildOutput: Seq[Attribute],
      boundStreamKeys: Seq[GpuExpression],
      streamTypes: Array[DataType],
      streamOutput: Seq[Attribute],
      // Condition bound to leftOutput ++ rightOutput
      boundCondition: Option[GpuExpression],
      numLeftColumns: Int,
      numRightColumns: Int,
      compareNullsEqual: Boolean,
      buildSideNeedsNullFilter: Boolean) {

    /**
     * Create a LazyCompiledCondition for this join's condition, if one exists.
     * The caller takes ownership and is responsible for closing the returned condition.
     */
    def createLazyCompiledCondition(): Option[LazyCompiledCondition] = {
      boundCondition.map { cond =>
        LazyCompiledCondition(cond, numLeftColumns, numRightColumns)
      }
    }

    def flipped(
        joinType: JoinType,
        buildSide: GpuBuildSide,
        condition: Option[Expression],
        leftOutput: Seq[Attribute],
        rightOutput: Seq[Attribute],
        metrics: Map[String, GpuMetric]): BoundJoinExprs = {
      // Bind condition to leftOutput ++ rightOutput
      val flippedCondition = condition.map { c =>
        GpuBindReferences.bindGpuReference(c, leftOutput ++ rightOutput, metrics)
      }

      val treatNullsEqual = GpuHashJoin.compareNullsEqual(joinType, boundStreamKeys)
      val needNullFilter = GpuHashJoin.buildSideNeedsNullFilter(
        joinType, treatNullsEqual, buildSide, boundBuildKeys)

      BoundJoinExprs(boundStreamKeys, streamTypes, streamOutput,
        boundBuildKeys, buildTypes, buildOutput,
        flippedCondition, leftOutput.size, rightOutput.size, treatNullsEqual, needNullFilter)
    }
  }

  object BoundJoinExprs {
    /**
     * Utility to bind join expressions and produce a BoundJoinExprs result. Note that this should
     * be called with the build side that was dynamically determined after probing the join inputs.
     */
    def bind(
        joinType: JoinType,
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        condition: Option[Expression],
        buildSide: GpuBuildSide,
        metrics: Map[String, GpuMetric]): BoundJoinExprs = {
      val leftTypes = leftOutput.map(_.dataType).toArray
      val rightTypes = rightOutput.map(_.dataType).toArray
      val boundLeftKeys = GpuBindReferences.bindGpuReferences(leftKeys, leftOutput, metrics)
      val boundRightKeys = GpuBindReferences.bindGpuReferences(rightKeys, rightOutput, metrics)
      
      val (boundBuildKeys, buildTypes, buildOutput, boundStreamKeys, streamTypes, streamOutput) =
        buildSide match {
          case GpuBuildRight =>
            (boundRightKeys, rightTypes, rightOutput, boundLeftKeys, leftTypes, leftOutput)
          case GpuBuildLeft =>
            (boundLeftKeys, leftTypes, leftOutput, boundRightKeys, rightTypes, rightOutput)
      }
      
      // Bind condition to leftOutput ++ rightOutput.
      // LazyCompiledCondition will transform it for BuildLeft by rewriting ordinals.
      val boundCondition = condition.map { c =>
        GpuBindReferences.bindGpuReference(c, leftOutput ++ rightOutput, metrics)
      }

      val treatNullsEqual = GpuHashJoin.compareNullsEqual(joinType, boundBuildKeys)
      val needNullFilter = GpuHashJoin.buildSideNeedsNullFilter(
        joinType, treatNullsEqual, buildSide, boundBuildKeys)

      BoundJoinExprs(boundBuildKeys, buildTypes, buildOutput,
        boundStreamKeys, streamTypes, streamOutput,
        boundCondition, leftOutput.size, rightOutput.size, 
        treatNullsEqual, needNullFilter)
    }
  }

  /** Utility class to track information related to a join. */
  case class JoinInfo(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      buildIter: Iterator[ColumnarBatch],
      buildSize: Long,
      buildStats: Option[JoinBuildSideStats],
      streamIter: Iterator[ColumnarBatch],
      exprs: BoundJoinExprs)

  /**
   * Trait to house common code for determining the ideal build/stream
   * assignments for symmetric joins.
   */
  trait JoinSizer[T <: AutoCloseable] {
    /** Wrap, if necessary, an iterator in preparation for probing the size before a join. */
    def setupForProbe(iter: Iterator[ColumnarBatch]): Iterator[T]

    /**
     * Build an iterator in preparation for using it for sub-joins.
     *
     * @param queue a possibly empty queue of data that has already been fetched from the underlying
     *              iterator as part of probing sizes of the join inputs
     * @param remainingIter the data remaining to be fetched from the original iterator. Iterating
     *                      the queue followed by this iterator reconstructs the iteration order of
     *                      the original input iterator.
     * @param batchTypes the schema of the data
     * @param gpuBatchSizeBytes target GPU batch size in bytes
     * @param metrics metrics to update (e.g.: if coalescing batches)
     * @return iterator of columnar batches to use in sub-joins
     */
    def setupForJoin(
        queue: mutable.Queue[T],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch]

    /** Get the row count of a batch of data */
    def getProbeBatchRowCount(batch: T): Long

    /** Get the data size in bytes of a batch of data */
    def getProbeBatchDataSize(batch: T): Long

    /**
     * Probe the left and right join inputs to determine which side should be used as the build
     * side and which should be used as the stream side.
     *
     * @param joinType type of join to perform
     * @param leftKeys join keys for the left table
     * @param leftOutput schema of the left table
     * @param rawLeftIter iterator of batches for the left table
     * @param rightKeys join keys for the right table
     * @param rightOutput schema of the right table
     * @param rawRightIter iterator of batches for the right table
     * @param condition inequality portions of the join condition
     * @param gpuBatchSizeBytes target GPU batch size
     * @param metrics map of metrics to update
     * @return join information including build side, bound expressions, etc.
     */
    def getJoinInfo(
        joinType: JoinType,
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rawLeftIter: Iterator[ColumnarBatch],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        rawRightIter: Iterator[ColumnarBatch],
        condition: Option[Expression],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): JoinInfo
  }

  /**
   * Join sizer when both the left and right table are coming directly from a shuffle and
   * the data will be on the host. Caches shuffle batches in host memory while probing without
   * grabbing the GPU semaphore.
   */
  trait HostHostJoinSizer extends JoinSizer[SpillableHostConcatResult] {
    def readOption: CoalesceReadOption

    override def setupForProbe(
        iter: Iterator[ColumnarBatch]): Iterator[SpillableHostConcatResult] = {
      new SpillableHostConcatResultFromColumnarBatchIterator(iter)
    }

    override def setupForJoin(
        queue: mutable.Queue[SpillableHostConcatResult],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      val concatMetrics = getConcatMetrics(metrics)
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
        new HostQueueBatchIterator(queue, remainingIter),
        gpuBatchSizeBytes,
        batchTypes,
        readOption,
        concatMetrics,
        prefetchFirstBatch = true)
    }

    override def getProbeBatchRowCount(batch: SpillableHostConcatResult): Long = {
      batch.getNumRows
    }

    override def getProbeBatchDataSize(batch: SpillableHostConcatResult): Long = {
      batch.getDataLen
    }
  }

  /**
   * Very similar to the HostHostJoinSizer except it does not support host spillable
   * data. This should only be used when the amount of data being probed is the
   * target batch size or less, which matches the behavior of normal shuffle processing
   * today. Ideally we should be using HostHostJoinSizer, but this saves the overhead
   * of registering and unregistering all of the shuffle buffers with the spill framework.
   * See https://github.com/NVIDIA/spark-rapids/issues/11322.
   */
  trait HostHostUnspillableJoinSizer extends JoinSizer[ColumnarBatch] {
    def readOption: CoalesceReadOption

    override def setupForProbe(
        iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = iter

    override def setupForJoin(
        queue: mutable.Queue[ColumnarBatch],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      val concatMetrics = getConcatMetrics(metrics)
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
        queue.iterator ++ remainingIter,
        gpuBatchSizeBytes,
        batchTypes,
        readOption,
        concatMetrics,
        prefetchFirstBatch = true)
    }

    override def getProbeBatchRowCount(batch: ColumnarBatch): Long = batch.numRows()

    override def getProbeBatchDataSize(batch: ColumnarBatch): Long = {
      GpuShuffleCoalesceUtils.getSerializedBufferSize(batch)
    }
  }

  /**
   * Join sizer to use when at least one side of the join is coming from another GPU exec node
   * such that the GPU semaphore is already held. Caches input batches on the GPU.
   */
  trait SpillableColumnarBatchJoinSizer extends JoinSizer[SpillableColumnarBatch] {
    override def setupForProbe(iter: Iterator[ColumnarBatch]): Iterator[SpillableColumnarBatch] = {
      iter.map(batch => SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
    }

    override def setupForJoin(
        queue: mutable.Queue[SpillableColumnarBatch],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      new SpillableColumnarBatchQueueIterator(queue, remainingIter)
    }

    override def getProbeBatchRowCount(batch: SpillableColumnarBatch): Long = batch.numRows()

    override def getProbeBatchDataSize(batch: SpillableColumnarBatch): Long = batch.sizeInBytes
  }

  def getConcatMetrics(metrics: Map[String, GpuMetric]): Map[String, GpuMetric] = {
    // Use a filtered metrics map to avoid output batch counts and other unrelated metric updates
    Map(
      OP_TIME_LEGACY -> metrics(OP_TIME_LEGACY),
      CONCAT_TIME -> metrics(CONCAT_TIME),
    ).withDefaultValue(NoopMetric)
  }

  def createJoinIterator(
      info: JoinInfo,
      spillableBuiltBatch: LazySpillableColumnarBatch,
      lazyStream: Iterator[LazySpillableColumnarBatch],
      joinOptions: JoinOptions,
      conditionForLogging: Option[Expression],
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
    info.joinType match {
      case FullOuter =>
        // Create a LazyCompiledCondition for this iterator (it takes ownership)
        val lazyCond = info.exprs.createLazyCompiledCondition()
        new HashOuterJoinIterator(FullOuter, spillableBuiltBatch, info.exprs.boundBuildKeys,
          info.buildStats, None, lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
          lazyCond, joinOptions, info.buildSide,
          info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
      case LeftOuter if info.buildSide == GpuBuildLeft =>
        val lazyCond = info.exprs.createLazyCompiledCondition()
        new HashOuterJoinIterator(LeftOuter, spillableBuiltBatch, info.exprs.boundBuildKeys,
          info.buildStats, None, lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
          lazyCond, joinOptions, info.buildSide,
          info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
      case RightOuter if info.buildSide == GpuBuildRight =>
        val lazyCond = info.exprs.createLazyCompiledCondition()
        new HashOuterJoinIterator(RightOuter, spillableBuiltBatch, info.exprs.boundBuildKeys,
          info.buildStats, None, lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
          lazyCond, joinOptions, info.buildSide,
          info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
      case _ if info.exprs.boundCondition.isDefined =>
        // ConditionalHashJoinIterator will close the LazyCompiledCondition
        val lazyCond = info.exprs.createLazyCompiledCondition().get
        new ConditionalHashJoinIterator(spillableBuiltBatch, info.exprs.boundBuildKeys,
          info.buildStats, lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
          lazyCond, joinOptions, info.joinType,
          info.buildSide, info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
      case _ =>
        new HashJoinIterator(spillableBuiltBatch, info.exprs.boundBuildKeys, info.buildStats,
          lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
          joinOptions, info.joinType, info.buildSide,
          info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
    }
  }
}

abstract class GpuShuffledSizedHashJoinExec[HOST_BATCH_TYPE <: AutoCloseable] extends GpuJoinExec {
  import GpuShuffledSizedHashJoinExec._

  def left: SparkPlan
  def right: SparkPlan
  def isGpuShuffle: Boolean
  def gpuBatchSizeBytes: Long
  def partitionNumAmplification: Double
  def isSkewJoin: Boolean
  def cpuLeftKeys: Seq[Expression]
  def cpuRightKeys: Seq[Expression]
  def readOption: CoalesceReadOption

  protected def createHostHostSizer(
      readOption: CoalesceReadOption): JoinSizer[HOST_BATCH_TYPE]

  protected def createSpillableColumnarBatchSizer(
      startWithLeftSide: Boolean): JoinSizer[SpillableColumnarBatch]

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    SMALL_JOIN_COUNT -> createMetric(DEBUG_LEVEL, DESCRIPTION_SMALL_JOIN_COUNT),
    BIG_JOIN_COUNT -> createMetric(DEBUG_LEVEL, DESCRIPTION_BIG_JOIN_COUNT),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(GpuHashPartitioning.getDistribution(cpuLeftKeys),
      GpuHashPartitioning.getDistribution(cpuRightKeys))

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
      GpuHashJoin.output(joinType, left.output, right.output)
    case x =>
      throw new IllegalArgumentException(s"unsupported join type: $x")
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException(s"${this.getClass} does not support row-based execution")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val localJoinType = joinType
    val localLeftKeys = leftKeys
    val leftOutput = left.output
    val isLeftHost = isHostBatchProducer(left)
    val localRightKeys = rightKeys
    val rightOutput = right.output
    val isRightHost = isHostBatchProducer(right)
    val localCondition = condition
    val localGpuBatchSizeBytes = gpuBatchSizeBytes
    val localJoinOptions = RapidsConf.getJoinOptions(conf, localGpuBatchSizeBytes)
    val localMetrics = allMetrics.withDefaultValue(NoopMetric)
    val localReadOption = readOption
    left.executeColumnar().zipPartitions(right.executeColumnar()) { case (leftIter, rightIter) =>
      val joinInfo = (isLeftHost, isRightHost) match {
        case (true, true) =>
          getHostHostJoinInfo(localJoinType, localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter, localCondition,
            localGpuBatchSizeBytes, localReadOption, localMetrics)
        case (true, false) =>
          getHostGpuJoinInfo(localJoinType, localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter, localCondition,
            localGpuBatchSizeBytes, localReadOption, localMetrics)
        case (false, true) =>
          getGpuHostJoinInfo(localJoinType, localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter, localCondition,
            localGpuBatchSizeBytes, localReadOption, localMetrics)
        case (false, false) =>
          getGpuGpuJoinInfo(localJoinType, localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter,
            localCondition, localGpuBatchSizeBytes, localMetrics)
      }
      val joinIterator = if (joinInfo.buildSize <= localGpuBatchSizeBytes) {
        localMetrics(SMALL_JOIN_COUNT) += 1
        if (localJoinType.isInstanceOf[InnerLike] && joinInfo.buildSize == 0) {
          Iterator.empty
        } else {
          doSmallBuildJoin(joinInfo, localJoinOptions, localMetrics)
        }
      } else {
        localMetrics(BIG_JOIN_COUNT) += 1
        doBigBuildJoin(joinInfo, localJoinOptions,
          partitionNumAmplification, localMetrics)
      }
      val numOutputRows = localMetrics(NUM_OUTPUT_ROWS)
      val numOutputBatches = localMetrics(NUM_OUTPUT_BATCHES)
      joinIterator.map { cb =>
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }

  /**
   * Perform a join where the build side fits in a single GPU batch.
   *
   * @param info join information from the probing phase
   * @param joinOptions options for the join operation including target size and strategy
   * @param metricsMap metrics to update
   * @return iterator to produce the results of the join
   */
  private def doSmallBuildJoin(
      info: JoinInfo,
      joinOptions: JoinOptions,
      metricsMap: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    val opTime = metricsMap(OP_TIME_LEGACY)
    val lazyStream = new Iterator[LazySpillableColumnarBatch]() {
      override def hasNext: Boolean = info.streamIter.hasNext

      override def next(): LazySpillableColumnarBatch = {
        withResource(info.streamIter.next()) { batch =>
          LazySpillableColumnarBatch(batch, "stream_batch")
        }
      }
    }
    val buildIter = new GpuCoalesceIterator(
      info.buildIter,
      info.exprs.buildTypes,
      RequireSingleBatch,
      numInputRows = NoopMetric,
      numInputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      numOutputBatches = NoopMetric,
      collectTime = NoopMetric,
      concatTime = metricsMap(CONCAT_TIME),
      opTime = opTime,
      opName = "build batch")
    val batch = if (buildIter.hasNext) {
      buildIter.next()
    } else {
      GpuColumnVector.emptyBatchFromTypes(info.exprs.buildTypes)
    }
    val spillableBuiltBatch = withResource(batch) { batch =>
      assertInTests(!buildIter.hasNext, "build side should have a single batch")
      LazySpillableColumnarBatch(batch, "built")
    }
    createJoinIterator(info, spillableBuiltBatch, lazyStream, joinOptions,
      condition, opTime, metricsMap(JOIN_TIME))
  }

  /**
   * Perform a join where the build side does not fit in a single GPU batch.
   *
   * @param info join information from the probing phase
   * @param joinOptions options for the join operation including target size and strategy
   * @param metricsMap metrics to update
   * @param partitionNumAmplification boost number of partitions for build size by this times
   * @return iterator to produce the results of the join
   */
  private def doBigBuildJoin(
      info: JoinInfo,
      joinOptions: JoinOptions,
      partitionNumAmplification: Double,
      metricsMap: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    new BigSizedJoinIterator(info, joinOptions, condition,
      partitionNumAmplification, metricsMap)
  }

  /**
   * Probe for join information when both inputs are coming from host memory (i.e.: both
   * inputs are coming from a shuffle when not using a GPU-centered shuffle manager).
   */
  private def getHostHostJoinInfo(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      readOption: CoalesceReadOption,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = createHostHostSizer(readOption)
    sizer.getJoinInfo(joinType, leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for join information when the left input is coming from host memory and the
   * right table is coming from GPU memory.
   */
  private def getHostGpuJoinInfo(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      rawLeftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      readOption: CoalesceReadOption,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = createSpillableColumnarBatchSizer(startWithLeftSide = true)
    val concatMetrics = getConcatMetrics(metrics)
    val leftIter = GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
      rawLeftIter,
      gpuBatchSizeBytes,
      leftOutput.map(_.dataType).toArray,
      readOption,
      concatMetrics)
    sizer.getJoinInfo(joinType, leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for the join information when the left input is coming from GPU memory and the
   * left table is coming from host memory.
   */
  private def getGpuHostJoinInfo(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rawRightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      readOption: CoalesceReadOption,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = createSpillableColumnarBatchSizer(startWithLeftSide = false)
    val concatMetrics = getConcatMetrics(metrics)
    val rightIter = GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
      rawRightIter,
      gpuBatchSizeBytes,
      rightOutput.map(_.dataType).toArray,
      readOption,
      concatMetrics)
    sizer.getJoinInfo(joinType, leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for the join information when both inputs are coming from GPU memory.
   */
  private def getGpuGpuJoinInfo(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = createSpillableColumnarBatchSizer(startWithLeftSide = true)
    sizer.getJoinInfo(joinType, leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Determines if a plan produces data in host memory.
   *
   * @param plan the plan to check
   * @return true if the plan produces batches in host memory, false otherwise
   */
  private def isHostBatchProducer(plan: SparkPlan): Boolean = {
    if (isGpuShuffle) {
      false
    } else {
      plan match {
        case _: GpuShuffleCoalesceExec =>
          throw new IllegalStateException("Should not have shuffle coalesce before this node")
        case _: GpuShuffleExchangeExecBase | _: GpuCustomShuffleReaderExec => true
        case _: ReusedExchangeExec => true
        case _: ShuffleQueryStageExec => true
        case _ => false
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}

object GpuShuffledSymmetricHashJoinExec {
  import GpuShuffledSizedHashJoinExec._

  /**
   * Trait to house common code for determining the ideal build/stream
   * assignments for symmetric joins.
   */
  trait SymmetricJoinSizer[T <: AutoCloseable] extends JoinSizer[T] {
    /**
     * Whether to start pulling from the left or right input iterator when probing for data sizes.
     * This helps avoid grabbing the GPU semaphore too early when probing.
     */
    val startWithLeftSide: Boolean

    override def getJoinInfo(
        joinType: JoinType,
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rawLeftIter: Iterator[ColumnarBatch],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        rawRightIter: Iterator[ColumnarBatch],
        condition: Option[Expression],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): JoinInfo = {
      val leftTime = new LocalGpuMetric
      val rightTime = new LocalGpuMetric
      val buildTime = metrics(BUILD_TIME)
      val streamTime = metrics(STREAM_TIME)
      val leftIter = new CollectTimeIterator(NvtxRegistry.PROBE_LEFT,
        setupForProbe(rawLeftIter), leftTime)
      val rightIter = new CollectTimeIterator(NvtxRegistry.PROBE_RIGHT,
        setupForProbe(rawRightIter), rightTime)
      closeOnExcept(mutable.Queue.empty[T]) { leftQueue =>
        closeOnExcept(mutable.Queue.empty[T]) { rightQueue =>
          var leftSize = 0L
          var rightSize = 0L
          var buildSide: GpuBuildSide = null
          while (buildSide == null) {
            if (leftSize < rightSize || (startWithLeftSide && leftSize == rightSize)) {
              if (leftIter.hasNext) {
                val leftBatch = leftIter.next()
                if (getProbeBatchRowCount(leftBatch) > 0) {
                  leftQueue += leftBatch
                  leftSize += getProbeBatchDataSize(leftBatch)
                }
              } else {
                buildSide = GpuBuildLeft
              }
            } else {
              if (rightIter.hasNext) {
                val rightBatch = rightIter.next()
                if (getProbeBatchRowCount(rightBatch) > 0) {
                  rightQueue += rightBatch
                  rightSize += getProbeBatchDataSize(rightBatch)
                }
              } else {
                buildSide = GpuBuildRight
              }
            }
          }
          val exprs = BoundJoinExprs.bind(joinType, leftKeys, leftOutput, rightKeys, rightOutput,
            condition, buildSide, metrics)
          val (buildQueue, buildSize, streamQueue, rawStreamIter) = buildSide match {
            case GpuBuildRight =>
              buildTime += rightTime.value
              streamTime += leftTime.value
              (rightQueue, rightSize, leftQueue, rawLeftIter)
            case GpuBuildLeft =>
              buildTime += leftTime.value
              streamTime += rightTime.value
              (leftQueue, leftSize, rightQueue, rawRightIter)
          }
          metrics(BUILD_DATA_SIZE).set(buildSize)
          val baseBuildIter = setupForJoin(buildQueue, Iterator.empty, exprs.buildTypes,
            gpuBatchSizeBytes, metrics)
          val buildIter = if (exprs.buildSideNeedsNullFilter) {
            new NullFilteredBatchIterator(
              baseBuildIter, exprs.boundBuildKeys, metrics(OP_TIME_LEGACY))
          } else {
            baseBuildIter
          }
          val streamIter = new CollectTimeIterator(NvtxRegistry.FETCH_JOIN_STREAM,
            setupForJoin(streamQueue, rawStreamIter, exprs.streamTypes, gpuBatchSizeBytes, metrics),
            streamTime)
          JoinInfo(joinType, buildSide, buildIter, buildSize, None, streamIter, exprs)
        }
      }
    }
  }

  class HostHostSymmetricJoinSizer(override val readOption: CoalesceReadOption)
    extends SymmetricJoinSizer[SpillableHostConcatResult] with HostHostJoinSizer {

    override val startWithLeftSide: Boolean = true
  }

  class SpillableColumnarBatchSymmetricJoinSizer(override val startWithLeftSide: Boolean)
    extends SymmetricJoinSizer[SpillableColumnarBatch] with SpillableColumnarBatchJoinSizer {
  }
}

/**
 * A GPU shuffled hash join optimized to handle symmetric joins like inner and full outer.
 * Probes the sizes of the input tables before performing the join to determine which to use
 * as the build side.
 *
 * @param leftKeys join keys for the left table
 * @param rightKeys join keys for the right table
 * @param condition inequality portions of the join condition
 * @param left plan for the left table
 * @param right plan for the right table
 * @param isGpuShuffle whether the shuffle is GPU-centric (e.g.: UCX-based)
 * @param gpuBatchSizeBytes target GPU batch size
 * @param partitionNumAmplification amplify the number of partitions for the build side
 * @param readOption options encloses KUDO related configs
 * @param isSkewJoin whether is skew join or not
 * @param cpuLeftKeys original CPU expressions for the left join keys
 * @param cpuRightKeys original CPU expressions for the right join keys
 */
case class GpuShuffledSymmetricHashJoinExec(
                                             override val joinType: JoinType,
                                             override val leftKeys: Seq[Expression],
                                             override val rightKeys: Seq[Expression],
                                             override val condition: Option[Expression],
                                             override val left: SparkPlan,
                                             override val right: SparkPlan,
                                             override val isGpuShuffle: Boolean,
                                             override val gpuBatchSizeBytes: Long,
                                             override val partitionNumAmplification: Double,
                                             override val readOption: CoalesceReadOption,
                                             override val isSkewJoin: Boolean)(
    override val cpuLeftKeys: Seq[Expression],
    override val cpuRightKeys: Seq[Expression])
    extends GpuShuffledSizedHashJoinExec[SpillableHostConcatResult] {
  import GpuShuffledSizedHashJoinExec.JoinSizer
  import GpuShuffledSymmetricHashJoinExec._

  override def otherCopyArgs: Seq[AnyRef] = Seq(cpuLeftKeys, cpuRightKeys)

  override protected def createHostHostSizer(
      readOption: CoalesceReadOption): JoinSizer[SpillableHostConcatResult] = {
    new HostHostSymmetricJoinSizer(readOption)
  }

  override protected def createSpillableColumnarBatchSizer(
      startWithLeftSide: Boolean): JoinSizer[SpillableColumnarBatch] = {
    new SpillableColumnarBatchSymmetricJoinSizer(startWithLeftSide)
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case FullOuter =>
      UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(
        s"GpuShuffledSymmetricHashJoinExec should not take $x as the JoinType")
  }
}

object GpuShuffledAsymmetricHashJoinExec {
  import GpuShuffledSizedHashJoinExec._

  /**
   * Trait to house common code for determining the ideal build/stream
   * assignments for asymmetric outer joins (i.e.: LeftOuter, RightOuter).
   */
  trait AsymmetricJoinSizer[T <: AutoCloseable] extends JoinSizer[T] {
    val magnificationThreshold: Int

    override def getJoinInfo(
        joinType: JoinType,
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rawLeftIter: Iterator[ColumnarBatch],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        rawRightIter: Iterator[ColumnarBatch],
        condition: Option[Expression],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): JoinInfo = {
      val (probeBuildIter, rawBuildIter, probeStreamIter, rawStreamIter, buildSide) =
        joinType match {
          case LeftOuter =>
            (setupForProbe(rawRightIter), rawRightIter, setupForProbe(rawLeftIter), rawLeftIter,
              GpuBuildRight)
          case RightOuter =>
            (setupForProbe(rawLeftIter), rawLeftIter, setupForProbe(rawRightIter), rawRightIter,
              GpuBuildLeft)
          case _ => throw new IllegalStateException(s"unexpected join type $joinType")
        }
      val exprs = BoundJoinExprs.bind(joinType, leftKeys, leftOutput, rightKeys,
        rightOutput, condition, buildSide, metrics)
      val buildQueue = mutable.Queue.empty[T]
      val (buildRows, buildSize) = closeOnExcept(buildQueue) { _ =>
        fetchProbeTargetSize(probeBuildIter, buildQueue, gpuBatchSizeBytes)
      }
      val baseBuildIter = setupForJoin(buildQueue, rawBuildIter, exprs.buildTypes,
        gpuBatchSizeBytes, metrics)
      // setupForJoin may start async threads to fetch from rawBuildIter. So after
      // calling setupForJoin, current thread should not call use rawBuildIter or
      // probeBuildIter again, to avoid thread safety issues.

      if (buildRows <= Int.MaxValue && buildSize <= gpuBatchSizeBytes) {
        getJoinInfoSmallBuildSide(joinType, buildSide, condition, exprs,
          baseBuildIter, buildRows, buildSize,
          rawStreamIter, leftOutput, rightOutput, gpuBatchSizeBytes, metrics)
      } else {
        // The natural build side does not fit in a single batch, so use the stream side
        // as the hash table if we can fit it in a single batch.
        val streamQueue = mutable.Queue.empty[T]
        val (streamRows, streamSize) = closeOnExcept(streamQueue) { _ =>
          fetchProbeTargetSize(probeStreamIter, streamQueue, gpuBatchSizeBytes)
        }
        val streamIter = setupForJoin(streamQueue, rawStreamIter, exprs.streamTypes,
          gpuBatchSizeBytes, metrics)
        // setupForJoin may start async threads to fetch from rawStreamIter. So after
        // calling setupForJoin, current thread should not call use rawStreamIter or
        // probeStreamIter again, to avoid thread safety issues.

        if (streamRows <= Int.MaxValue && streamSize <= gpuBatchSizeBytes) {
          metrics(BUILD_DATA_SIZE).set(streamSize)
          val flippedSide = flipped(buildSide)
          JoinInfo(joinType, flippedSide, streamIter, streamSize, None, baseBuildIter,
            exprs.flipped(joinType, flippedSide, condition, leftOutput, rightOutput, metrics))
        } else {
          val buildIter = addNullFilterIfNecessary(baseBuildIter, exprs.boundBuildKeys,
            exprs.buildSideNeedsNullFilter, metrics)
          JoinInfo(joinType, buildSide, buildIter, buildSize, None, streamIter, exprs)
        }
      }
    }

    // Get the join info when the natural build side of the join can be a single batch.
    private def getJoinInfoSmallBuildSide(
        joinType: JoinType,
        buildSide: GpuBuildSide,
        condition: Option[Expression],
        exprs: BoundJoinExprs,
        baseBuildIter: Iterator[ColumnarBatch],
        buildRows: Long,
        buildSize: Long,
        rawStreamIter: Iterator[ColumnarBatch],
        leftOutput: Seq[Attribute],
        rightOutput: Seq[Attribute],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]) = {
      val streamIter = setupForJoin(mutable.Queue.empty, rawStreamIter, exprs.streamTypes,
        gpuBatchSizeBytes, metrics)
      // The natural build side fits in the target batch size, but we might have performance
      // problems if there are many duplicate keys in the build-side batch leading to a high
      // magnification factor, see https://github.com/NVIDIA/spark-rapids/issues/7529
      if (buildRows < magnificationThreshold) {
        // impossible for build-side to exceed magnification threshold with so few rows
        metrics(BUILD_DATA_SIZE).set(buildSize)
        val buildIter = addNullFilterIfNecessary(baseBuildIter, exprs.boundBuildKeys,
          exprs.buildSideNeedsNullFilter, metrics)
        JoinInfo(joinType, buildSide, buildIter, buildSize, None, streamIter, exprs)
      } else {
        val buildBatch = getAsSingleBuildBatch(baseBuildIter, exprs, metrics)
        val buildIter = new SingleGpuColumnarBatchIterator(buildBatch)
        val buildStats = JoinBuildSideStats.fromBatch(buildBatch, exprs.boundBuildKeys)
        if (buildStats.streamMagnificationFactor < magnificationThreshold) {
          metrics(BUILD_DATA_SIZE).set(buildSize)
          JoinInfo(joinType, buildSide, buildIter, buildSize, Some(buildStats), streamIter,
            exprs)
        } else {
          // The natural build side is explosive, so check the natural stream side to see
          // if it should be used for the hash table instead.
          closeOnExcept(mutable.Queue.empty[SpillableColumnarBatch]) { streamQueue =>
            val (streamRows, streamSize) =
              fetchTargetSize(streamIter, streamQueue, gpuBatchSizeBytes)
            if (streamRows <= Int.MaxValue && streamSize <= gpuBatchSizeBytes) {
              assertInTests(!streamIter.hasNext, "stream side not exhausted")
              // cannot filter out the nulls on the stream-side since they need to be
              // preserved in the outer join
              val streamBatchIter = new GpuCoalesceIterator(
                new SpillableColumnarBatchQueueIterator(streamQueue, Iterator.empty),
                exprs.streamTypes,
                RequireSingleBatch,
                numInputRows = NoopMetric,
                numInputBatches = NoopMetric,
                numOutputRows = NoopMetric,
                numOutputBatches = NoopMetric,
                collectTime = NoopMetric,
                concatTime = metrics(CONCAT_TIME),
                opTime = metrics(OP_TIME_LEGACY),
                opName = "stream as build")
              if (streamBatchIter.hasNext) {
                val streamBatch = streamBatchIter.next()
                val singleStreamIter = new SingleGpuColumnarBatchIterator(streamBatch)
                assertInTests(!streamBatchIter.hasNext, "stream side not exhausted")
                val streamStats = JoinBuildSideStats.fromBatch(streamBatch, exprs.boundStreamKeys)
                if (buildStats.streamMagnificationFactor <
                    streamStats.streamMagnificationFactor) {
                  metrics(BUILD_DATA_SIZE).set(buildSize)
                  JoinInfo(joinType, buildSide, buildIter, buildSize, Some(buildStats),
                    singleStreamIter, exprs)
                } else {
                  metrics(BUILD_DATA_SIZE).set(streamSize)
                  val flippedSide = flipped(buildSide)
                  JoinInfo(joinType, flippedSide, singleStreamIter, streamSize, Some(streamStats),
                    buildIter, exprs.flipped(joinType, flippedSide, condition,
                      leftOutput, rightOutput, metrics))
                }
              } else {
                metrics(BUILD_DATA_SIZE).set(streamSize)
                val flippedSide = flipped(buildSide)
                JoinInfo(joinType, flippedSide, streamBatchIter, streamSize, None,
                  buildIter, exprs.flipped(joinType, flippedSide, condition,
                    leftOutput, rightOutput, metrics))
              }
            } else {
              metrics(BUILD_DATA_SIZE).set(buildSize)
              JoinInfo(joinType, buildSide, buildIter, buildSize, Some(buildStats),
                new SpillableColumnarBatchQueueIterator(streamQueue, streamIter), exprs)
            }
          }
        }
      }
    }

    private def flipped(buildSide: GpuBuildSide): GpuBuildSide = buildSide match {
      case GpuBuildRight => GpuBuildLeft
      case GpuBuildLeft => GpuBuildRight
      case x => throw new IllegalStateException(s"unexpected build side: $x")
    }

    /**
     * Try to exhaust a probe iterator by fetching batches up to a specified target size.
     * @param iter probe iterator to fetch from
     * @param queue queue to place fetched batches into
     * @param targetSize target size to limit fetching
     * @return the total rows and total bytes fetched into the queue
     */
    private def fetchProbeTargetSize(
        iter: Iterator[T],
        queue: mutable.Queue[T],
        targetSize: Long): (Long, Long) = {
      NvtxRegistry.JOIN_ASYMMETRIC_PROBE_FETCH {
        var totalRows: Long = 0
        var totalSize: Long = 0L
        while (totalRows <= Integer.MAX_VALUE && totalSize <= targetSize && iter.hasNext) {
          val batch = iter.next()
          val rowCount = getProbeBatchRowCount(batch)
          if (rowCount > 0) {
            queue += batch
            totalRows += rowCount
            totalSize += getProbeBatchDataSize(batch)
          } else {
            batch.close()
          }
        }
        (totalRows, totalSize)
      }
    }

    /**
     * Try to exhaust a join input iterator by fetching batches up to a specified target size.
     * @param iter iterator to fetch from
     * @param queue queue to place fetched batches into
     * @param targetSize target size to limit fetching
     * @return the total rows and total bytes fetched into the queue
     */
    private def fetchTargetSize(
        iter: Iterator[ColumnarBatch],
        queue: mutable.Queue[SpillableColumnarBatch],
        targetSize: Long): (Long, Long) = {
      NvtxRegistry.JOIN_ASYMMETRIC_FETCH {
        var totalRows: Long = 0
        var totalSize: Long = 0L
        while (totalRows <= Integer.MAX_VALUE && totalSize <= targetSize && iter.hasNext) {
          val batch = iter.next()
          val rowCount = batch.numRows()
          if (rowCount > 0) {
            totalRows += rowCount
            totalSize += GpuColumnVector.getTotalDeviceMemoryUsed(batch)
            queue += SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          } else {
            batch.close()
          }
        }
        (totalRows, totalSize)
      }
    }

    private def addNullFilterIfNecessary(
        buildIter: Iterator[ColumnarBatch],
        boundKeys: Seq[GpuExpression],
        needsNullFilter: Boolean,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      if (needsNullFilter) {
        new NullFilteredBatchIterator(buildIter, boundKeys, metrics(OP_TIME_LEGACY))
      } else {
        buildIter
      }
    }

    private def getAsSingleBuildBatch(
        baseIter: Iterator[ColumnarBatch],
        exprs: BoundJoinExprs,
        metrics: Map[String, GpuMetric]): ColumnarBatch = {
      val iter = addNullFilterIfNecessary(baseIter, exprs.boundBuildKeys,
        exprs.buildSideNeedsNullFilter, metrics)
      // Multiple small batches may exist when split-retry happens in the previous op.
      // So need to concat them into a single one
      val spBatches = mutable.Queue.empty[SpillableColumnarBatch]
      closeOnExcept(spBatches) { _ =>
        while(iter.hasNext) {
          spBatches.enqueue(
            SpillableColumnarBatch(iter.next(), SpillPriorities.ACTIVE_BATCHING_PRIORITY))
        }
      }
      assert(spBatches.nonEmpty, "At least one batch is expected")
      val cbTypes = spBatches.head.dataTypes
      withRetryNoSplit(spBatches.toSeq) { _ =>
        ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(
          spBatches.toArray.safeMap(_.getColumnarBatch()), cbTypes)
      }
    }
  }

  class HostHostAsymmetricJoinSizer(
      override val magnificationThreshold: Int,
      override val readOption: CoalesceReadOption)
    extends AsymmetricJoinSizer[ColumnarBatch] with HostHostUnspillableJoinSizer {
  }

  class SpillableColumnarBatchAsymmetricJoinSizer(override val magnificationThreshold: Int)
    extends AsymmetricJoinSizer[SpillableColumnarBatch] with SpillableColumnarBatchJoinSizer {
  }
}

/**
 * A GPU shuffled hash join optimized to handle asymmetric joins like left outer and right outer.
 * Probes the sizes of the input tables before performing the join to determine which to use
 * as the build side.
 *
 * @param leftKeys join keys for the left table
 * @param rightKeys join keys for the right table
 * @param condition inequality portions of the join condition
 * @param left plan for the left table
 * @param right plan for the right table
 * @param isGpuShuffle whether the shuffle is GPU-centric (e.g.: UCX-based)
 * @param gpuBatchSizeBytes target GPU batch size
 * @param partitionNumAmplification amplify the number of partitions for the build side
 * @param readOption options encloses KUDO related configs
 * @param isSkewJoin whether is skew join or not
 * @param cpuLeftKeys original CPU expressions for the left join keys
 * @param cpuRightKeys original CPU expressions for the right join keys
 */
case class GpuShuffledAsymmetricHashJoinExec(
                                              override val joinType: JoinType,
                                              override val leftKeys: Seq[Expression],
                                              override val rightKeys: Seq[Expression],
                                              override val condition: Option[Expression],
                                              override val left: SparkPlan,
                                              override val right: SparkPlan,
                                              override val isGpuShuffle: Boolean,
                                              override val gpuBatchSizeBytes: Long,
                                              override val partitionNumAmplification: Double,
                                              override val readOption: CoalesceReadOption,
                                              override val isSkewJoin: Boolean)(
    override val cpuLeftKeys: Seq[Expression],
    override val cpuRightKeys: Seq[Expression],
    magnificationThreshold: Integer) extends GpuShuffledSizedHashJoinExec[ColumnarBatch] {
  import GpuShuffledAsymmetricHashJoinExec._
  import GpuShuffledSizedHashJoinExec.JoinSizer

  override def otherCopyArgs: Seq[AnyRef] = Seq(cpuLeftKeys, cpuRightKeys, magnificationThreshold)

  override protected def createHostHostSizer(
      readOption: CoalesceReadOption): JoinSizer[ColumnarBatch] = {
    new HostHostAsymmetricJoinSizer(magnificationThreshold, readOption)
  }

  override protected def createSpillableColumnarBatchSizer(
      startWithLeftSide: Boolean): JoinSizer[SpillableColumnarBatch] = {
    new SpillableColumnarBatchAsymmetricJoinSizer(magnificationThreshold)
  }

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"GpuShuffledAsymmetricHashJoinExec should not take $x as the JoinType")
  }
}

/**
 * A spillable form of a HostConcatResult. Takes ownership of the specified host buffer.
 */
sealed trait SpillableHostConcatResult extends AutoCloseable {
  def hmb: HostMemoryBuffer
  def toBatch: ColumnarBatch
  def getNumRows: Long
  def getDataLen: Long

  protected var spillableBuffer = {
    SpillableHostBuffer(hmb, hmb.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  override def close(): Unit = if (spillableBuffer != null) {
    spillableBuffer.close()
    spillableBuffer = null
  }
}

class CudfSpillableHostConcatResult(
    header: SerializedTableHeader,
    val hmb: HostMemoryBuffer) extends SpillableHostConcatResult {

  override def toBatch: ColumnarBatch = {
    closeOnExcept(spillableBuffer.getHostBuffer()) { hostBuf =>
      SerializedTableColumn.from(header, hostBuf)
    }
  }

  override def getNumRows: Long = header.getNumRows

  override def getDataLen: Long = header.getDataLen
}

class KudoSpillableHostConcatResult(kudoTableHeader: KudoTableHeader,
    val hmb: HostMemoryBuffer
) extends SpillableHostConcatResult  {
  require(kudoTableHeader != null, "KudoTableHeader cannot be null")
  require(hmb != null, "HostMemoryBuffer cannot be null")
  val bufferLength: Long = hmb.getLength

  override def toBatch: ColumnarBatch = closeOnExcept(spillableBuffer.getHostBuffer()) { hostBuf =>
    KudoSerializedTableColumn.from(kudoTableHeader, hostBuf)
  }

  override def getNumRows: Long = kudoTableHeader.getNumRows

  // Should not reference hmb after its ownership is transferred to spillableBuffer,
  // so we just cache the length at the time of construction
  override def getDataLen: Long = bufferLength
}

object SpillableHostConcatResult {
  def from(batch: ColumnarBatch): SpillableHostConcatResult = {
    require(batch.numCols() == 1, "Batch must have exactly 1 column")
    batch.column(0) match {
      case col: KudoSerializedTableColumn => {
        // This will be closed
        val oldKudoTable = col.spillableKudoTable
        val buffer = col.spillableKudoTable.makeKudoTable.getBuffer
        new KudoSpillableHostConcatResult(oldKudoTable.header, buffer)
      }
      case col: SerializedTableColumn =>
        val buffer = col.hostBuffer
        buffer.incRefCount()
        new CudfSpillableHostConcatResult(col.header, buffer)
      case c =>
        throw new IllegalStateException(s"Expected SerializedTableColumn, got ${c.getClass}")
    }
  }
}

/**
 * Converts an iterator of shuffle batches in host memory into an iterator of spillable
 * host memory batches.
 */
class SpillableHostConcatResultFromColumnarBatchIterator(
    iter: Iterator[ColumnarBatch]) extends Iterator[SpillableHostConcatResult] {
  override def hasNext: Boolean = iter.hasNext

  override def next(): SpillableHostConcatResult = {
    withResource(iter.next())(SpillableHostConcatResult.from)
  }
}

/**
 * Iterator that produces SerializedTableColumn batches from a queue of spillable host memory
 * batches that were fetched first during probing and the (possibly empty) remaining iterator of
 * un-probed host memory batches. The iterator returns the queue elements first, followed by
 * the elements of the remaining iterator.
 *
 * @param spillableQueue queue of spillable host memory batches
 * @param batchIter iterator of remaining host memory batches
 */
class HostQueueBatchIterator(
    spillableQueue: mutable.Queue[SpillableHostConcatResult],
    batchIter: Iterator[ColumnarBatch]) extends GpuColumnarBatchIterator(true) {
  override def hasNext: Boolean = spillableQueue.nonEmpty || batchIter.hasNext

  override def next(): ColumnarBatch = {
    if (spillableQueue.nonEmpty) {
      withResource(spillableQueue.dequeue())(_.toBatch)
    } else {
      batchIter.next()
    }
  }

  override def doClose(): Unit = {
    spillableQueue.safeClose()
  }
}

/**
 * Iterator that produces columnar batches from a queue of spillable batches that were fetched
 * first during probing and the (possibly empty) remaining iterator fo un-probed batches. The
 * iterator returns the queue elements first, followed by the elements of the remaining iterator.
 */
class SpillableColumnarBatchQueueIterator(
    queue: mutable.Queue[SpillableColumnarBatch],
    batchIter: Iterator[ColumnarBatch]) extends GpuColumnarBatchIterator(true) {

  override def hasNext: Boolean = queue.nonEmpty || batchIter.hasNext

  override def next(): ColumnarBatch = {
    if (queue.nonEmpty) {
      withResource(queue.dequeue()) { spillable =>
        spillable.getColumnarBatch()
      }
    } else {
      batchIter.next()
    }
  }

  override def doClose(): Unit = {
    queue.safeClose()
  }
}

/**
 * Iterator that filters out rows with null keys.
 *
 * @param iter iterator of batches to filter
 * @param boundKeys expressions to produce the keys
 * @param opTime metric to update for time taken during the filter operation
 */
class NullFilteredBatchIterator(
    iter: Iterator[ColumnarBatch],
    boundKeys: Seq[Expression],
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with AutoCloseable {
  private var onDeck: Option[ColumnarBatch] = None

  onTaskCompletion(close())

  override def hasNext: Boolean = {
    while (onDeck.isEmpty && iter.hasNext) {
      val rawBatch = iter.next()
      val batch = opTime.ns {
        val spillable = SpillableColumnarBatch(rawBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        GpuHashJoin.filterNullsWithRetryAndClose(spillable, boundKeys)
      }
      if (batch.numRows > 0) {
        onDeck = Some(batch)
      } else {
        batch.close()
      }
    }
    onDeck.nonEmpty
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("no more batches")
    }
    val batch = onDeck.get
    onDeck = None
    batch
  }

  override def close(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
}

/** Tracks a collection of batches associated with a partition in a large join */
class JoinPartition extends AutoCloseable {
  private var totalSize = 0L
  private val batches = new mutable.ArrayBuffer[SpillableColumnarBatch]()

  def addPartitionBatch(part: SpillableColumnarBatch, dataSize: Long): Unit = {
    batches.append(part)
    totalSize += dataSize
  }

  def getTotalSize: Long = totalSize

  def releaseBatches(): Array[SpillableColumnarBatch] = {
    val result = batches.toArray
    batches.clear()
    totalSize = 0
    result
  }

  override def close(): Unit = {
    batches.safeClose()
    batches.clear()
    totalSize = 0L
  }
}

/**
 * Base class for a partitioner in a large join.
 *
 * @param numPartitions number of partitions being used in the join
 * @param batchTypes schema of the batches
 * @param boundJoinKeys bound keys used in the join
 * @param metrics metrics to update
 */
abstract class JoinPartitioner(
    numPartitions: Int,
    batchTypes: Array[DataType],
    boundJoinKeys: Seq[Expression],
    metrics: Map[String, GpuMetric]) extends GpuHashPartitioner with AutoCloseable {
  protected val partitions: Array[JoinPartition] =
    (0 until numPartitions).map(_ => new JoinPartition).toArray
  protected val opTime = metrics(OP_TIME_LEGACY)

  override protected val hashFunc: GpuHashExpression =
    GpuMurmur3Hash(boundJoinKeys, JoinPartitioner.HASH_SEED)

  /**
   * Hash partitions a batch in preparation for performing a sub-join. The input batch will
   * be closed by this method.
   */
  protected def partitionBatch(inputBatch: ColumnarBatch): Unit = {
    val spillableBatch = SpillableColumnarBatch(inputBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    withRetryNoSplit(spillableBatch) { _ =>
      opTime.ns {
        val partsTable = hashPartitionAndClose(spillableBatch.getColumnarBatch(), numPartitions,
          NvtxRegistry.PARTITION_FOR_JOIN)
        val contigTables = withResource(partsTable) { _ =>
          partsTable.getTable.contiguousSplit(partsTable.getPartitions.tail: _*)
        }
        withResource(contigTables) { _ =>
          contigTables.zipWithIndex.foreach { case (ct, i) =>
            if (ct.getRowCount > 0) {
              contigTables(i) = null
              addPartition(i, ct)
            }
          }
        }
      }
    }
  }

  /**
   * Adds a batch associated with a partition.
   *
   * @param partIndex index of the partition
   * @param ct contiguous table to add to the specified partition
   */
  protected def addPartition(partIndex: Int, ct: ContiguousTable): Unit = {
    val dataSize = ct.getBuffer.getLength
    partitions(partIndex).addPartitionBatch(SpillableColumnarBatch(ct, batchTypes,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY), dataSize)
  }

  override def close(): Unit = {
    partitions.safeClose()
  }
}

object JoinPartitioner {
  /**
   * A seed to use for the hash partitioner when sub-partitioning. Needs to be different than
   * the seed used by Spark for partitioning a hash join (i.e.: 42)
   */
  val HASH_SEED: Int = 100
}

/**
 * Join partitioner for the build side of a large join where the build side of the join does not
 * fit in a single GPU batch.
 *
 * @param joinType type of join being performed
 * @param numPartitions number of partitions being used in the join
 * @param buildSideIter iterator of build side batches
 * @param buildSideTypes schema of the build side batches
 * @param boundBuildKeys bound join key expressions for the build side
 * @param gpuBatchSizeBytes target GPU batch size
 * @param metrics metrics to update
 */
class BuildSidePartitioner(
    val joinType: JoinType,
    val numPartitions: Int,
    buildSideIter: Iterator[ColumnarBatch],
    buildSideTypes: Array[DataType],
    boundBuildKeys: Seq[Expression],
    gpuBatchSizeBytes: Long,
    metrics: Map[String, GpuMetric])
  extends JoinPartitioner(numPartitions, buildSideTypes, boundBuildKeys, metrics) {

  // partition all of the build-side batches
  closeOnExcept(partitions) { _ =>
    while (buildSideIter.hasNext) {
      partitionBatch(buildSideIter.next())
    }
  }

  private val (emptyPartitions, joinGroups) = findEmptyAndJoinGroups()
  private val partitionBatches = new Array[LazySpillableColumnarBatch](joinGroups.length)
  private val concatTime = metrics(CONCAT_TIME)

  /** Returns a BitSet where a set bit corresponds to an empty partition at that index. */
  def getEmptyPartitions: BitSet = emptyPartitions

  /**
   * Returns a BitSet array where each BitSet corresponds to a "join group," a group of partitions
   * that can together be used as a single build batch for a sub-join. A set bit in a join group's
   * BitSet corresponds to a partition index that is part of the join group.
   */
  def getJoinGroups: Array[BitSet] = joinGroups

  /**
   * Returns the batch of data for the specified join group index. All of the data across the
   * partitions comprising the join group are concatenated together to produce the batch.
   * The concatenated batches are lazily cached, so the cost of concatenation is only incurred
   * by the first caller for a particular join group.
   *
   * @param partitionGroupIndex the index of the join group for which to produce the batch
   * @return the batch of data for the join group
   */
  def getBuildBatch(partitionGroupIndex: Int): LazySpillableColumnarBatch = {
    var batch = partitionBatches(partitionGroupIndex)
    if (batch == null) {
      val spillBatchesBuffer = new mutable.ArrayBuffer[SpillableColumnarBatch]()
      closeOnExcept(spillBatchesBuffer) { _ =>
        joinGroups(partitionGroupIndex).foreach { i =>
          val batches = partitions(i).releaseBatches()
          spillBatchesBuffer ++= batches
        }
      }
      val concatBatch = withRetryNoSplit(spillBatchesBuffer.toSeq) { spillBatches =>
        val batchesToConcat = spillBatches.safeMap(_.getColumnarBatch()).toArray
        opTime.ns {
          concatTime.ns {
            ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(batchesToConcat, buildSideTypes)
          }
        }
      }
      withResource(concatBatch) { _ =>
        batch = LazySpillableColumnarBatch(concatBatch, "build subtable")
        partitionBatches(partitionGroupIndex) = batch
      }
    }
    LazySpillableColumnarBatch.spillOnly(batch)
  }

  override def close(): Unit = {
    partitions.safeClose()
    partitionBatches.safeClose()
  }

  /**
   * After partitioning the build-side table, find the set of partition indices
   * that are empty partitions along with an iterator of partition index sets that,
   * for each set of indices, produces a build-side sub-table that is ideally
   * within the GPU batch size limits. If there is a partition that is larger than
   * the target size, it will be in its own partition index set.
   *
   * @return the set of empty partition indices and an iterator of partition index
   *         sets where each set identifies partitions that can be processed together
   *         in a join pass.
   */
  private def findEmptyAndJoinGroups(): (BitSet, Array[BitSet]) = {
    val emptyPartitions = new mutable.BitSet(numPartitions)
    val joinGroups = new mutable.ArrayBuffer[BitSet]()
    val sortedIndices = (0 until numPartitions).sortBy(i => partitions(i).getTotalSize)
    val (emptyIndices, nonEmptyIndices) = joinType match {
      case FullOuter | LeftOuter | RightOuter =>
        // empty build partitions still need to produce outputs for corresponding stream partitions
        (Seq.empty, sortedIndices)
      case _: InnerLike =>
        sortedIndices.partition { i =>
          partitions(i).getTotalSize == 0
        }
      case x => throw new IllegalStateException(s"unsupported join type: $x")
    }
    emptyPartitions ++= emptyIndices
    var group = new mutable.BitSet(numPartitions)
    var groupSize = 0L
    nonEmptyIndices.foreach { i =>
      val newSize = groupSize + partitions(i).getTotalSize
      if (groupSize > 0L && newSize > gpuBatchSizeBytes) {
        if (group.nonEmpty) {
          joinGroups.append(group)
        }
        group = new mutable.BitSet(numPartitions)
        groupSize = partitions(i).getTotalSize
      } else {
        groupSize = newSize
      }
      group.add(i)
    }
    if (group.nonEmpty) {
      joinGroups.append(group)
    }
    (emptyPartitions, joinGroups.toArray)
  }
}

/**
 * Join partitioner for the stream side of a large join.
 *
 * @param numPartitions number of partitions being used in the join
 * @param emptyBuildPartitions BitSet indicating which build side partitions are empty
 * @param iter iterator of stream side batches
 * @param streamTypes schema of the stream side batches
 * @param boundStreamKeys bound join key expressions for the stream side
 * @param metrics metrics to update
 */
class StreamSidePartitioner(
    numPartitions: Int,
    emptyBuildPartitions: BitSet,
    iter: Iterator[ColumnarBatch],
    streamTypes: Array[DataType],
    boundStreamKeys: Seq[Expression],
    metrics: Map[String, GpuMetric])
  extends JoinPartitioner(numPartitions, streamTypes, boundStreamKeys, metrics) {

  override protected def addPartition(partIndex: Int, ct: ContiguousTable): Unit = {
    // Ignore partitions that correspond to empty build-side partitions, since
    // no stream-side keys in this partition will match anything on the build-side.
    if (emptyBuildPartitions.contains(partIndex)) {
      ct.close()
    } else {
      super.addPartition(partIndex, ct)
    }
  }

  def hasInputBatches: Boolean = iter.hasNext

  def partitionNextBatch(): Unit = {
    assertInTests(partitions.forall(_.getTotalSize == 0), "leftover partitions from previous batch")
    partitionBatch(iter.next)
  }

  def releasePartitions(partIndices: BitSet): Array[SpillableColumnarBatch] = {
    partIndices.iterator.flatMap(i => partitions(i).releaseBatches().toSeq).toArray
  }

  override def close(): Unit = {
    partitions.safeClose()
  }
}

/**
 * Iterator that produces the result of a large symmetric join where the build side of the join is
 * too large for a single GPU batch. The prior join input probing phase has sized the build side
 * of the join, so this partitions both the build side and stream side into N+1 partitions, where
 * N is the size of the build side divided by the target GPU batch size.
 *
 * Once the build side is partitioned completely, the partitions are placed into "join groups"
 * where all the build side data of a join group fits in the GPU target batch size. If the input
 * data is skewed, a single build partition could be larger than the target GPU batch size.
 * Currently such oversized partitions are placed in separate join groups consisting just of one
 * partition each in the hopes that there will be enough GPU memory to proceed with the join
 * despite the skew. We will need to revisit this for very large, skewed build side data arriving
 * at a single task.
 *
 * Once the build side join groups are identified, each stream batch is partitioned into the same
 * number of partitions as the build side with the same hash key used for the build side. The
 * partitions from the batch are grouped into join groups matching the partition grouping from
 * the build side, and each join group is processed as a sub-join. Once all the join groups for
 * a stream batch have been processed, the next stream batch is fetched, partitioned, and sub-joins
 * are processed against the build side join groups. Repeat until the stream side is exhausted.
 *
 * @param info join information from input probing phase
 * @param joinOptions options for the join operation including target size and strategy
 * @param partitionNumAmplification boost number of partitions for build size by this times
 * @param metrics metrics to update
 */
class BigSizedJoinIterator(
    info: JoinInfo,
    joinOptions: JoinOptions,
    conditionForLogging: Option[Expression],
    partitionNumAmplification: Double,
    metrics: Map[String, GpuMetric])
  extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  private val buildPartitioner = {
    val numPartitions =
      (((info.buildSize / joinOptions.targetSize) + 1) * partitionNumAmplification).toLong
    require(numPartitions <= Int.MaxValue, "too many build partitions")
    new BuildSidePartitioner(info.joinType, numPartitions.toInt, info.buildIter,
      info.exprs.buildTypes, info.exprs.boundBuildKeys, joinOptions.targetSize, metrics)
  }
  use(buildPartitioner)

  private val joinGroups = buildPartitioner.getJoinGroups
  private var currentJoinGroupIndex = joinGroups.length - 1
  private val needTracker = info.joinType match {
    case FullOuter => true
    case LeftOuter if info.buildSide == GpuBuildLeft => true
    case RightOuter if info.buildSide == GpuBuildRight => false
    case _ => false
  }

  private val streamPartitioner = use(new StreamSidePartitioner(buildPartitioner.numPartitions,
    buildPartitioner.getEmptyPartitions, info.streamIter, info.exprs.streamTypes,
    info.exprs.boundStreamKeys, metrics))

  private var subIter: Option[Iterator[ColumnarBatch]] = None

  // Buffer per join group to track build-side rows that have been referenced for outer joins
  private val buildSideRowTrackers: Array[Option[SpillableColumnarBatch]] = {
    if (needTracker) {
      val arr = new Array[Option[SpillableColumnarBatch]](joinGroups.length)
      arr.indices.foreach { i => arr(i) = None }
      arr
    } else {
      Array.empty
    }
  }

  private var isExhausted = joinGroups.isEmpty
  private val opTime = metrics(OP_TIME_LEGACY)
  private val joinTime = metrics(JOIN_TIME)

  private lazy val lazyCompiledCondition = info.exprs.createLazyCompiledCondition().map { cond =>
    use(cond)
    cond
  }

  override def hasNext: Boolean = {
    if (isExhausted) {
      false
    } else if (subIter.exists(_.hasNext)) {
      true
    } else {
      setupNextJoinIterator()
      val result = subIter.exists(_.hasNext)
      if (!result) {
        isExhausted = true
        close()
      }
      result
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("join batches exhausted")
    }
    subIter.get.next()
  }

  override def close(): Unit = {
    super.close()
    buildSideRowTrackers.flatten.safeClose()
    isExhausted = true
  }

  private def setupNextJoinIterator(): Unit = {
    while (!isExhausted && !subIter.exists(_.hasNext)) {
      if (needTracker) {
        // save off the build side tracker buffer for the join group just processed
        subIter match {
          case Some(streamIter: HashJoinStreamSideIterator) =>
            assert(buildSideRowTrackers(currentJoinGroupIndex).isEmpty, "unexpected row tracker")
            buildSideRowTrackers(currentJoinGroupIndex) = streamIter.releaseBuiltSideTracker()
            streamIter.close()
          case _ =>
        }
      }
      if (currentJoinGroupIndex >= joinGroups.length - 1) {
        // try to pull in the next stream batch
        if (streamPartitioner.hasInputBatches) {
          streamPartitioner.partitionNextBatch()
          subIter = Some(moveToNextBuildGroup())
        } else if (needTracker) {
          currentJoinGroupIndex = buildSideRowTrackers.indexWhere(_.isDefined)
          if (currentJoinGroupIndex == -1) {
            isExhausted = true
            subIter = None
          } else {
            // TODO: Can free the build-side batch in the build partitioner early here since
            //       this will be the last iterator to use it.
            // https://github.com/NVIDIA/spark-rapids/issues/10282
            val tracker = buildSideRowTrackers(currentJoinGroupIndex)
            buildSideRowTrackers(currentJoinGroupIndex) = None
            // Setup an iterator to produce the final full outer join batches for the join group.
            // All stream batches have been consumed, so an empty iterator is used for the stream
            // side. The condition also doesn't need to be passed since there are no join row pairs
            // left to evaluate conditionally. The only rows that will be emitted by this are the
            // build-side rows that never matched rows on the stream side.
            subIter = Some(new HashOuterJoinIterator(info.joinType,
              buildPartitioner.getBuildBatch(currentJoinGroupIndex), info.exprs.boundBuildKeys,
              info.buildStats, tracker, Iterator.empty, info.exprs.boundStreamKeys,
              info.exprs.streamOutput, None, joinOptions,
              info.buildSide, info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime))
          }
        } else {
          isExhausted = true
          subIter = None
        }
      } else {
        subIter = Some(moveToNextBuildGroup())
      }
    }
  }

  private def moveToNextBuildGroup(): Iterator[ColumnarBatch] = {
    // If we were at the last build group, loop back to the first since we're processing the next
    // stream batch.
    currentJoinGroupIndex = (currentJoinGroupIndex + 1) % joinGroups.length
    val builtBatch = buildPartitioner.getBuildBatch(currentJoinGroupIndex)
    val group = joinGroups(currentJoinGroupIndex)
    val streamBatches = streamPartitioner.releasePartitions(group)
    val lazyStream = new Iterator[LazySpillableColumnarBatch] {
      onTaskCompletion(streamBatches.safeClose())

      private var i = 0

      override def hasNext: Boolean = i < streamBatches.length

      override def next(): LazySpillableColumnarBatch = {
        withResource(streamBatches(i)) { spillBatch =>
          streamBatches(i) = null
          i += 1
          withResource(spillBatch.getColumnarBatch()) { batch =>
            LazySpillableColumnarBatch(batch, "stream_batch")
          }
        }
      }
    }
    if (needTracker) {
      // Build an iterator to perform the stream-side of the outer join for the join group,
      // tracking which rows are referenced so far. The iterator will own the tracker of build side
      // rows referenced until we release it after the iterator has produced all of the batches.
      val buildRowTracker = buildSideRowTrackers(currentJoinGroupIndex)
      buildSideRowTrackers(currentJoinGroupIndex) = None
      new HashJoinStreamSideIterator(info.joinType,
        builtBatch, info.exprs.boundBuildKeys, info.buildStats, buildRowTracker,
        lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput, lazyCompiledCondition,
        joinOptions, info.buildSide,
        info.exprs.compareNullsEqual, conditionForLogging, opTime, joinTime)
    } else {
      GpuShuffledSizedHashJoinExec.createJoinIterator(info, builtBatch, lazyStream,
        joinOptions, conditionForLogging, opTime, joinTime)
    }
  }
}
