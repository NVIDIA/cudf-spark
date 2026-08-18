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

package com.nvidia.spark.rapids

import scala.collection.mutable

// No longer need NvtxColor, NvtxRange imports
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, ShimBinaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, PartitioningCollection, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.GpuOr
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, GpuSubPartitionHashJoin, JoinTypeChecks}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuShuffledHashJoinMeta(
    join: ShuffledHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[ShuffledHashJoinExec](join, conf, parent, rule) {
  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override protected def runChildExprBridgeOptimization(): Unit = {
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(leftKeys ++ rightKeys)
    conditionMeta.foreach { cond =>
      val leftExprIds = join.left.output.map(_.exprId)
      val rightExprIds = join.right.output.map(_.exprId)
      if (AstUtil.canExtractNonAstConditionIfNeed(cond, leftExprIds, rightExprIds)) {
        GpuCpuBridgeOptimizer.checkAndOptimizeNonAstSubtrees(cond)
      } else {
        // Inner joins can consume a bridged post-filter. Joins that require an AST condition
        // will call requireAstForGpuOn later and reject GPU execution if the bridge remains.
        GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(cond))
      }
    }
  }

  // This is used by shuffled hash join
  def tagBuildSide(meta: SparkPlanMeta[_], joinType: JoinType, buildSide: GpuBuildSide): Unit = {
    buildSide match {
      case GpuBuildLeft if !canBuildLeft(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support left-side build")
      case GpuBuildRight if !canBuildRight(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support right-side build")
      case _ =>
    }
  }

  /** Determine if this type of join supports using the right side of the join as the build side. */
  // supports right outer join when build right
  def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | LeftSemi |
         LeftAnti | FullOuter | _: ExistenceJoin => true
    case _ => false
  }

  /** Determine if this type of join supports using the left side of the join as the build side. */
  // supports left outer join when build left
  def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter => true
    case _ => false
  }

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
    tagBuildSide(this, join.joinType, buildSide)
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val extractedCondition = GpuHashJoin.extractJoinConditionIfNeeded(
      conditionMeta, join.joinType, left, right)
    val useSizedJoin = GpuShuffledSizedHashJoinExec.useSizedJoin(conf, join.joinType,
      join.leftKeys, join.rightKeys)
    val readOpt = CoalesceReadOption(conf)
    val joinExec = join.joinType match {
      case LeftOuter | RightOuter if useSizedJoin =>
        GpuShuffledAsymmetricHashJoinExec(
          join.joinType,
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          conf.isGPUShuffle,
          conf.gpuTargetBatchSizeBytes,
          conf.sizedJoinPartitionAmplification,
          readOpt,
          isSkewJoin = false)(
          join.leftKeys,
          join.rightKeys,
          conf.joinOuterMagnificationThreshold)
      case Inner | FullOuter if useSizedJoin =>
        GpuShuffledSymmetricHashJoinExec(
          join.joinType,
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          conf.isGPUShuffle,
          conf.gpuTargetBatchSizeBytes,
          conf.sizedJoinPartitionAmplification,
          readOpt,
          isSkewJoin = false)(
          join.leftKeys,
          join.rightKeys)
      case _ =>
        GpuShuffledHashJoinExec(
          leftKeys.map(_.convertToGpu()),
          rightKeys.map(_.convertToGpu()),
          join.joinType,
          buildSide,
          extractedCondition.joinCondition,
          extractedCondition.left,
          extractedCondition.right,
          readOpt,
          isSkewJoin = false)(
          join.leftKeys,
          join.rightKeys)
    }
    // For inner joins we can apply a post-join condition for any conditions that cannot be
    // evaluated directly in a mixed join that leverages a cudf AST expression
    val filteredJoinExec = extractedCondition.filterCondition.map(c => GpuFilterExec(c,
      joinExec)()).getOrElse(joinExec)
    extractedCondition.projectIfNeeded(filteredJoinExec)
  }
}

case class GpuShuffledHashJoinExec(
    override val leftKeys: Seq[Expression],
    override val rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    readOption: CoalesceReadOption,
    override val isSkewJoin: Boolean)(
    cpuLeftKeys: Seq[Expression],
    cpuRightKeys: Seq[Expression]) extends ShimBinaryExecNode with GpuHashJoin
  with GpuSubPartitionHashJoin {

  override def otherCopyArgs: Seq[AnyRef] = cpuLeftKeys :: cpuRightKeys :: Nil

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"GpuShuffledHashJoinExec should not take $x as the JoinType")
  }

  import GpuMetric._

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(GpuHashPartitioning.getDistribution(cpuLeftKeys),
      GpuHashPartitioning.getDistribution(cpuRightKeys))


  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledHashJoin does not support the execute() code path.")
  }

  // Goal to be used for the coalescing the build side. Note that this is internal to
  // the join and not used for planning purposes. The two valid choices are `RequireSingleBatch` or
  // `RequireSingleBatchWithFilter`
  private lazy val buildGoal: CoalesceSizeGoal = joinType match {
    case _: InnerLike | LeftSemi | LeftAnti =>
      val nullFilteringMask = boundBuildKeys.map(GpuIsNotNull).reduce(GpuOr)
      RequireSingleBatchWithFilter(nullFilteringMask)
    case _ => RequireSingleBatch
  }

  private def realTargetBatchSize(): Long = {
    val configValue = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    Math.max(configValue, 10 * 1024)
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = {
    val batchedBuildGoal = TargetSize(realTargetBatchSize())
    (joinType, buildSide) match {
      case (_, GpuBuildLeft) => Seq(batchedBuildGoal, null)
      case (_, GpuBuildRight) => Seq(null, batchedBuildGoal)
    }
  }

  override def internalDoExecuteColumnar() : RDD[ColumnarBatch] = {
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val numPartitions = RapidsConf.NUM_SUB_PARTITIONS.get(conf)
    val subPartConf = RapidsConf.HASH_SUB_PARTITION_TEST_ENABLED.get(conf)
       .map(_ && RapidsConf.TEST_CONF.get(conf))
    val localBuildOutput = buildPlan.output

    // Create a map of metrics that can be handed down to shuffle and coalesce
    // iterators, setting as noop certain metrics that the coalesce iterators
    // normally update, but that in the case of the join they would produce
    // the wrong statistics (since there are conflicts)
    val coalesceMetrics = allMetrics ++
      Map(GpuMetric.NUM_INPUT_ROWS -> NoopMetric,
          GpuMetric.NUM_INPUT_BATCHES -> NoopMetric,
          GpuMetric.NUM_OUTPUT_BATCHES -> NoopMetric,
          GpuMetric.NUM_OUTPUT_ROWS -> NoopMetric)

    val realTarget = realTargetBatchSize()
    val joinOptions = RapidsConf.getJoinOptions(conf, realTarget)

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        val (buildData, maybeBufferedStreamIter) =
          GpuShuffledHashJoinExec.prepareBuildBatchesForJoin(buildIter,
            new CollectTimeIterator(NvtxRegistry.SHUFFLED_JOIN_STREAM, streamIter, streamTime),
            realTarget, localBuildOutput, buildGoal, subPartConf, coalesceMetrics, readOption)

        buildData match {
          case Left(singleBatch) =>
            closeOnExcept(singleBatch) { _ =>
              buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(singleBatch)
            }
            // doJoin will close singleBatch
            doJoin(singleBatch, maybeBufferedStreamIter, joinOptions,
              numOutputRows, numOutputBatches, opTime, joinTime)
          case Right(builtBatchIter) =>
            // For big joins, when the build data can not fit into a single batch.
            val sizeBuildIter = builtBatchIter.map { cb =>
              closeOnExcept(cb) { _ =>
                buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(cb)
              }
              cb
            }
            doJoinBySubPartition(sizeBuildIter, maybeBufferedStreamIter, joinOptions,
              numPartitions, numOutputRows, numOutputBatches, opTime, joinTime)
        }
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}

object GpuShuffledHashJoinExec extends Logging {
  /**
   * Return the build data as a single ColumnarBatch when sub-partitioning is not enabled,
   * while as an iterator of ColumnarBatch when sub-partitioning is enabled.
   *
   * sub-partitioning can be activated by specifying its relevant config but this is intended
   * for tests only. In production, whether sub-partitioning will be enabled depends on
   * if all the data in build side can fit into a single batch. If yes, sub-partitioning
   * will not be enabled. Otherwise, it will.
   *
   * This function also takes care of acquiring the GPU semaphore optimally in the scenario
   * where the build side is relatively small (less than `targetSize`).
   *
   * In the optimal case, this function will load the build side on the host up to the
   * goal configuration and if it fits entirely, allow the stream iterator
   * to also pull to host its first batch. After the first stream batch is on the host, the
   * stream iterator acquires the semaphore and then the build side is copied to the GPU.
   *
   * Prior to this we would get a build batch on the GPU, acquiring
   * the semaphore in the process, and then begin pulling from the stream iterator,
   * which could include IO (while holding onto the semaphore).
   *
   * @param buildIter build side iterator
   * @param streamIter stream side iterator
   * @param targetSize target batch size goal
   * @param buildOutput output attributes of the build plan
   * @param buildGoal the build goal to use when coalescing batches
   * @param subPartConf the config whether to enable sub-partitioning algorithm
   * @param coalesceMetrics metrics map with metrics to be used in downstream
   *                        iterators
   * @return a pair of an Either for build and streamed iterator that can be used
   *         for the join.
   */
  private[rapids] def prepareBuildBatchesForJoin(
      buildIter: Iterator[ColumnarBatch],
      streamIter: Iterator[ColumnarBatch],
      targetSize: Long,
      buildOutput: Seq[Attribute],
      buildGoal: CoalesceSizeGoal,
      subPartConf: Option[Boolean],
      coalesceMetrics: Map[String, GpuMetric],
      readOption: CoalesceReadOption):
  (Either[ColumnarBatch, Iterator[ColumnarBatch]], Iterator[ColumnarBatch]) = {
    val buildTime = coalesceMetrics(GpuMetric.BUILD_TIME)
    val buildDataType = buildOutput.map(_.dataType).toArray
    closeOnExcept(new CloseableBufferedIterator(buildIter)) { bufBuildIter =>
      val startTime = System.nanoTime()
      var isHostSerialized = false
      // Batches type detection
      val coalesceBuiltIter = getShuffleCoalesceIterator(
        bufBuildIter, buildDataType, targetSize, readOption, coalesceMetrics).map {
          case (iter, isHostResult) =>
        isHostSerialized = isHostResult
        iter
      }.getOrElse(bufBuildIter)
      if (coalesceBuiltIter.hasNext) {
        val firstBuildBatch = coalesceBuiltIter.next()
        // Batches have coalesced to the target size, so size will overflow if there are
        // more than one batch, or the first batch size already exceeds the target.
        val sizeOverflow = {
          closeOnExcept(firstBuildBatch) { _ =>
            coalesceBuiltIter.hasNext || getBatchSize(firstBuildBatch) > targetSize
          }
        }
        val needSingleBuildBatch = !subPartConf.getOrElse(sizeOverflow)
        if (needSingleBuildBatch && isHostSerialized && !sizeOverflow) {
          // add the time it took to fetch that first host-side build batch
          buildTime += System.nanoTime() - startTime
          // It can be optimized for grabbing the GPU semaphore when there is only a single
          // serialized host batch and the sub-partitioning is not activated.
          val (singleBuildCb, bufferedStreamIter) = getBuildBatchOptimizedAndClose(
            firstBuildBatch.asInstanceOf[CoalescedHostResult], streamIter, buildDataType,
            buildGoal, buildTime)
          logDebug("In the optimized case for grabbing the GPU semaphore, return " +
            s"a single batch (size: ${getBatchSize(singleBuildCb)}) for the build side " +
            s"with $buildGoal goal.")
          (Left(singleBuildCb), bufferedStreamIter)

        } else { // Other cases without optimization
          val safeIter = {
            val autoCloseableSeq = Seq(firstBuildBatch)
            GpuSubPartitionHashJoin.safeIteratorFromSeq(autoCloseableSeq) ++
              coalesceBuiltIter
          }
          val gpuBuildIter = if (isHostSerialized) {
            // batches on host, move them to GPU
            new GpuShuffleCoalesceIterator(safeIter.asInstanceOf[Iterator[CoalescedHostResult]],
              buildDataType)
          } else { // batches already on GPU
            safeIter.asInstanceOf[Iterator[ColumnarBatch]]
          }

          val buildRet = getFilterFunc(buildGoal).map { filterAndClose =>
            // Filtering is required
            getFilteredBuildBatches(gpuBuildIter, filterAndClose, targetSize, subPartConf,
              buildTime)
          }.getOrElse {
            if (needSingleBuildBatch) {
              val oneCB = getAsSingleBatch(gpuBuildIter, buildOutput, buildGoal, coalesceMetrics)
              logDebug(s"Return a single batch (size: ${getBatchSize(oneCB)}) for the " +
                s"build side with $buildGoal goal.")
              Left(oneCB)
            } else {
              logDebug("Return multiple batches as the build side data for the following " +
                "sub-partitioning join")
              Right(new CollectTimeIterator(NvtxRegistry.HASH_JOIN_BUILD, gpuBuildIter, buildTime))
            }
          }
          buildTime += System.nanoTime() - startTime
          (buildRet, streamIter)
        }
      } else {
        // build is empty
        (Left(GpuColumnVector.emptyBatchFromTypes(buildDataType)), streamIter)
      }
    }
  }

  private def getFilterFunc(goal: CoalesceSizeGoal): Option[ColumnarBatch => ColumnarBatch] = {
    goal match {
      case RequireSingleBatchWithFilter(filterExpr) =>
        Some(cb => withResource(cb)(GpuFilter(_, filterExpr)))
      case _ =>
        None
    }
  }

  private def getFilteredBuildBatches(
      buildIter: Iterator[ColumnarBatch],
      filterFunc: ColumnarBatch => ColumnarBatch,
      targetSize: Long,
      subPartConf: Option[Boolean],
      buildTime: GpuMetric): Either[ColumnarBatch, Iterator[ColumnarBatch]] = {
    val filteredIter = buildIter.map(filterFunc)
    // Redo the size estimation for filtered batches
    var accBatchSize = 0L
    closeOnExcept(new mutable.ArrayBuffer[SpillableColumnarBatch]) { spillBuf =>
      while (accBatchSize < targetSize && filteredIter.hasNext) {
        closeOnExcept(filteredIter.next()) { cb =>
          accBatchSize += GpuColumnVector.getTotalDeviceMemoryUsed(cb)
          spillBuf.append(
            SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
        }
      }
      val multiBuildBatches = subPartConf.getOrElse {
        // Total size goes beyond the target size.
        accBatchSize > targetSize || (accBatchSize == targetSize && filteredIter.hasNext)
      }
      if (multiBuildBatches) {
        // The size still overflows after filtering or sub-partitioning is enabled for test.
        logDebug("Return multiple batches as the build side data for the following " +
          "sub-partitioning join in null-filtering mode.")
        val safeIter = GpuSubPartitionHashJoin.safeIteratorFromSeq(spillBuf.toSeq).map { sp =>
          withRetryNoSplit(sp)(_.getColumnarBatch())
        } ++ filteredIter
        Right(new CollectTimeIterator(NvtxRegistry.HASH_JOIN_BUILD, safeIter, buildTime))
      } else {
        // The size after filtering is within the target size or sub-partitioning is disabled.
        while(filteredIter.hasNext) {
          // Pull out all the remaining batches, this is for the case when sub-partitioning
          // is disabled by setting the relevant conf to false no matter how big the data is.
          spillBuf.append(
            SpillableColumnarBatch(filteredIter.next(), SpillPriorities.ACTIVE_BATCHING_PRIORITY))
        }
        val spill = GpuSubPartitionHashJoin.concatSpillBatchesAndClose(spillBuf.toSeq)
        // There is a prior empty check so this `spill` can not be a None.
        assert(spill.isDefined, "The build data iterator should not be empty.")
        withResource(spill) { _ =>
          logDebug(s"Return a single batch (size: ${spill.get.sizeInBytes}) for the " +
            s"build side in null-filtering mode.")
          Left(spill.get.getColumnarBatch())
        }
      }
    }
  }

  /** Only accepts a CoalescedHostResult or a ColumnarBatch as input */
  private def getBatchSize(maybeBatch: AnyRef): Long = maybeBatch match {
    case batch: ColumnarBatch => GpuColumnVector.getTotalDeviceMemoryUsed(batch)
    case hostBatch: CoalescedHostResult => hostBatch.getDataSize
    case _ => throw new IllegalStateException(s"Expect a CoalescedHostResult or a " +
      s"ColumnarBatch, but got a ${maybeBatch.getClass.getSimpleName}")
  }

  private def getBuildBatchOptimizedAndClose(
      hostConcatResult: CoalescedHostResult,
      streamIter: Iterator[ColumnarBatch],
      buildDataTypes: Array[DataType],
      buildGoal: CoalesceSizeGoal,
      buildTime: GpuMetric): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    // For the optimal case, the build iterator is already drained and didn't have a
    // prior so it was a single batch, and is entirely on the host.
    // We peek at the stream iterator with `hasNext` on the buffered iterator, which
    // will grab the semaphore when putting the first stream batch on the GPU, and
    // then we bring the build batch to the GPU and return.
    withResource(hostConcatResult) { _ =>
      closeOnExcept(new CloseableBufferedIterator(streamIter)) { bufStreamIter =>
        NvtxRegistry.JOIN_FIRST_STREAM_BATCH {
          if (bufStreamIter.hasNext) {
            bufStreamIter.head
          } else {
            GpuSemaphore.acquireIfNecessary(TaskContext.get())
          }
        }
        // Bring the build batch to the GPU now
        val buildBatch = buildTime.ns {
          val cb = hostConcatResult.toGpuBatch(buildDataTypes)
          getFilterFunc(buildGoal).map(filterAndClose => filterAndClose(cb)).getOrElse(cb)
        }
        (buildBatch, bufStreamIter)
      }
    }
  }

  private def getAsSingleBatch(
      inputIter: Iterator[ColumnarBatch],
      inputAttrs: Seq[Attribute],
      goal: CoalesceSizeGoal,
      coalesceMetrics: Map[String, GpuMetric]): ColumnarBatch = {
    val singleBatchIter = new GpuCoalesceIterator(inputIter,
      inputAttrs.map(_.dataType).toArray, goal,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
      coalesceMetrics(GpuMetric.CONCAT_TIME), coalesceMetrics(GpuMetric.OP_TIME_LEGACY),
      "single build batch")
    ConcatAndConsumeAll.getSingleBatchWithVerification(singleBatchIter, inputAttrs)
  }

  private def getShuffleCoalesceIterator(
      iter: BufferedIterator[ColumnarBatch],
      dataTypes: Array[DataType],
      targetSize: Long,
      readOption: CoalesceReadOption,
      coalesceMetrics: Map[String, GpuMetric]): Option[(Iterator[AutoCloseable], Boolean)] = {
    var retIter: Option[(Iterator[AutoCloseable], Boolean)] = None
    if (iter.hasNext && iter.head.numCols() == 1) {
      val concatTime = coalesceMetrics(GpuMetric.CONCAT_TIME)
      iter.head.column(0) match {
        case _: KudoSerializedTableColumn =>
          if (readOption.kudoMode == RapidsConf.ShuffleKudoMode.GPU) {
            val gpuIter = new KudoGpuShuffleCoalesceIterator(iter, targetSize, dataTypes,
              concatTimeMetric = concatTime, readOption = readOption)
            retIter = Some((gpuIter, false)) // GPU batches
          } else {
            val hostIter = new KudoHostShuffleCoalesceIterator(iter, targetSize, dataTypes,
              concatTimeMetric = concatTime, readOption = readOption)
            retIter = Some((hostIter, true)) // host results
          }
        case _: SerializedTableColumn =>
          val hostIter = new HostShuffleCoalesceIterator(iter, targetSize,
            concatTimeMetric = concatTime)
          retIter = Some((hostIter, true)) // host results
        case _ => // should be gpu batches
      }
    }
    retIter
  }
}
