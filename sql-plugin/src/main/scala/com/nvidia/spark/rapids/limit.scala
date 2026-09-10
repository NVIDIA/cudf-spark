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

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuBaseLimitIterator(
    input: Iterator[ColumnarBatch],
    limit: Int,
    offset: Int,
    dataTypes : Array[DataType],
    opTime: GpuMetric,
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric) extends Iterator[ColumnarBatch] {
  private var remainingLimit = limit - offset
  private var remainingOffset = offset

  override def hasNext: Boolean = (limit == -1 || remainingLimit > 0) && input.hasNext

  override def next(): ColumnarBatch = {
    if (!this.hasNext) {
      throw new NoSuchElementException("Next on empty iterator")
    }

    var batch = input.next()

    // In each partition, we need to skip `offset` rows
    while (batch != null && remainingOffset >= batch.numRows()) {
      remainingOffset -= batch.numRows()
      batch.safeClose()
      batch = if (this.hasNext) {
        input.next()
      } else {
        null
      }
    }

    // If the last batch is null, then we have offset >= numRows in this partition.
    // In such case, we should return an empty batch
    if (batch == null || batch.numRows() == 0) {
      val fields = dataTypes.zipWithIndex.map {
        case (dt, idx) => StructField(s"_col$idx", dt, nullable = true)
      }
      return GpuColumnVector.emptyBatch(StructType(fields))
    }

    // Here 0 <= remainingOffset < batch.numRow(), we need to get batch[remainingOffset:]
    NvtxIdWithMetrics(NvtxRegistry.LIMIT_AND_OFFSET, opTime) {
      var result: ColumnarBatch = null
      // limit < 0 (limit == -1) denotes there is no limitation, so when
      // (remainingOffset == 0 && (remainingLimit >= batch.numRows() || limit < 0)) is true,
      // we can take this batch completely
      if (remainingOffset == 0 && (remainingLimit >= batch.numRows() || limit < 0)) {
        result = batch
      } else {
        // otherwise, we need to slice batch with (remainingOffset, remainingLimit).
        // And remainingOffset > 0 will be used only once, for the latter batches in this
        // partition, set remainingOffset = 0
        val length = if (remainingLimit >= batch.numRows() || limit < 0) {
          batch.numRows()
        } else {
          remainingLimit
        }
        val scb = closeOnExcept(batch) { _ =>
          SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        result = sliceBatchAndCloseWithRetry(scb, remainingOffset, length)
        remainingOffset = 0
      }
      remainingLimit -= result.numRows()
      numOutputBatches += 1
      numOutputRows += result.numRows()
      result
    }
  }

  private def sliceBatchAndCloseWithRetry(
      spillBatch: SpillableColumnarBatch,
      start: Int,
      length: Int): ColumnarBatch = {
    val end = Math.min(start + length, spillBatch.numRows())
    RmmRapidsRetryIterator.withRetryNoSplit(spillBatch) { _ =>
      withResource(spillBatch.getColumnarBatch()) { batch =>
        val subCols = (0 until batch.numCols()).safeMap { i =>
          val col = batch.column(i).asInstanceOf[GpuColumnVector]
          val subVector = col.getBase.subVector(start, end)
          assert(subVector != null)
          GpuColumnVector.from(subVector, col.dataType)
        }
        new ColumnarBatch(subCols.toArray, end - start)
      }
    }
  }
}

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec with ShimUnaryExecNode {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY)
  )

  override def output: Seq[Attribute] = child.output

  // The same as what feeds us, even though we might make it smaller
  // the reality is that nothing is coming out after this, so it does fit
  // the requirements
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    sliceRDD(child.executeColumnar(), limit, 0)
  }

  protected def sliceRDD(rdd: RDD[ColumnarBatch], limit: Int, offset: Int): RDD[ColumnarBatch] = {
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    rdd.mapPartitions { iter =>
      new GpuBaseLimitIterator(iter, limit, offset, output.map(_.dataType).toArray,
        opTime, numOutputBatches, numOutputRows)
    }
  }

}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class GpuLocalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GpuGlobalLimitExec(limit: Int = -1, child: SparkPlan,
                              offset: Int = 0) extends GpuBaseLimitExec {
  // In CPU code of spark, there is an assertion 'limit >= 0 || (limit == -1 && offset > 0)'.

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch]  = {
    super.sliceRDD(child.executeColumnar(), limit, offset)
  }
}

class GpuCollectLimitMeta(
                      collectLimit: CollectLimitExec,
                      conf: RapidsConf,
                      parent: Option[RapidsMeta[_, _, _]],
                      rule: DataFromReplacementRule)
  extends SparkPlanMeta[CollectLimitExec](collectLimit, conf, parent, rule) {
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(collectLimit.outputPartitioning, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // Use full class name to avoid compile errors on Spark versions
    // where CommandResultExec (added in 3.2) does not exist.
    val childClass = collectLimit.child.getClass.getName
    if (childClass.endsWith(
          ".execution.CommandResultExec") ||
        childClass.endsWith(
          ".execution.command.ExecutedCommandExec")) {
      willNotWorkOnGpu(
        s"child ${collectLimit.child.getClass.getSimpleName}" +
        " already provides pre-computed results; replacing" +
        " CollectLimit would trigger an unnecessary Spark job")
    }
    // When the child cannot run on GPU, fall back the entire
    // CollectLimit to CPU. CPU CollectLimitExec.doExecute() applies
    // per-partition .take(limit) that stops upstream iterators early,
    // which is strictly better than row-to-columnar conversion
    // followed by GPU limiting.
    if (!childPlans.head.canThisBeReplaced) {
      willNotWorkOnGpu(
        "child cannot run on GPU; falling back entire" +
        " CollectLimit to CPU to preserve per-partition" +
        " row-level .take(limit) optimization")
    }
  }

  protected def buildCollectLimitGpu(offset: Int): GpuExec = {
    GpuGlobalLimitExec(collectLimit.limit,
      GpuShuffleExchangeExec(
        GpuSinglePartitioning,
        GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
        ENSURE_REQUIREMENTS
      )(SinglePartition), offset)
  }

  override def convertToGpu(): GpuExec = buildCollectLimitGpu(0)
}

object GpuTopN {
  private[this] def concatAndClose(a: ColumnarBatch,
      b: ColumnarBatch,
      concatTime: GpuMetric): ColumnarBatch = {
    NvtxIdWithMetrics(NvtxRegistry.READ_N_CONCAT, concatTime) {
      val dataTypes = GpuColumnVector.extractTypes(b)
      val aTable = withResource(a) { a =>
        GpuColumnVector.from(a)
      }
      val ret = withResource(aTable) { aTable =>
        withResource(b) { b =>
          withResource(GpuColumnVector.from(b)) { bTable =>
            Table.concatenate(aTable, bTable)
          }
        }
      }
      withResource(ret) { ret =>
        GpuColumnVector.from(ret, dataTypes)
      }
    }
  }

  private[this] def sliceBatch(batch: ColumnarBatch, begin: Int, limit: Int): ColumnarBatch = {
    val end = Math.min(limit, batch.numRows())
    val start = Math.max(0, Math.min(begin, end))
    val numColumns = batch.numCols()
    closeOnExcept(new Array[ColumnVector](numColumns)) { columns =>
      val bases = GpuColumnVector.extractBases(batch)
      (0 until numColumns).foreach { i =>
        columns(i) =
          GpuColumnVector.from(bases(i).subVector(start, end), batch.column(i).dataType())
      }
      new ColumnarBatch(columns, end - start)
    }
  }

  private[this] def takeN(batch: ColumnarBatch, count: Int): ColumnarBatch = {
    sliceBatch(batch, 0, count)
  }


  private[this] def applyOffset(batch: ColumnarBatch, offset: Int): ColumnarBatch = {
    sliceBatch(batch, offset, batch.numRows())
  }

  def sortAndTakeNClose(limit: Int,
      sorter: GpuSorter,
      batch: ColumnarBatch,
      sortTime: GpuMetric): ColumnarBatch = {
    withResource(batch) { _ =>
      withResource(sorter.fullySortBatch(batch, sortTime)) { sorted =>
        takeN(sorted, limit)
      }
    }
  }

  def apply(limit: Int,
      sorter: GpuSorter,
      iter: Iterator[ColumnarBatch],
      opTime: GpuMetric,
      sortTime: GpuMetric,
      concatTime: GpuMetric,
      inputBatches: GpuMetric,
      inputRows: GpuMetric,
      outputBatches: GpuMetric,
      outputRows: GpuMetric,
      offset: Int): Iterator[SpillableColumnarBatch] =
    new Iterator[SpillableColumnarBatch]() {
      override def hasNext: Boolean = iter.hasNext

      private[this] var pending: Option[SpillableColumnarBatch] = None

      // Don't install the callback if in a unit test
      Option(TaskContext.get()).foreach { tc =>
        ScalableTaskCompletion.onTaskCompletion(tc) {
          pending.foreach(_.safeClose())
        }
      }

      override def next(): SpillableColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        while (iter.hasNext) {
          val inputScb = closeOnExcept(iter.next()) { cb =>
            inputBatches += 1
            inputRows += cb.numRows()
            SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          }
          withRetry(inputScb, splitSpillableInHalfByRows) { attempt =>
            NvtxIdWithMetrics(NvtxRegistry.TOP_N, opTime) {
              val inputCb = attempt.getColumnarBatch()
              if (pending.isEmpty) {
                sortAndTakeNClose(limit, sorter, inputCb, sortTime)
              } else { // pending is not empty
                val totalSize = attempt.sizeInBytes + pending.get.sizeInBytes
                val tmpCb = if (totalSize > Int.MaxValue) {
                  // The intermediate size is likely big enough we don't want to risk an overflow,
                  // so sort/slice before we concat and sort/slice again.
                  sortAndTakeNClose(limit, sorter, inputCb, sortTime)
                } else {
                  // The intermediate size looks like we could never overflow the indexes so
                  // do it the more efficient way and concat first followed by the sort/slice
                  inputCb
                }
                val pendingCb = closeOnExcept(tmpCb) { _ =>
                  pending.get.getColumnarBatch()
                }
                sortAndTakeNClose(limit, sorter, concatAndClose(pendingCb, tmpCb, concatTime),
                  sortTime)
              }
            }
          }.foreach { runningResult =>
            pending.foreach(_.close())
            pending = None
            pending = closeOnExcept(runningResult) { _ =>
              Some(SpillableColumnarBatch(runningResult, ACTIVE_ON_DECK_PRIORITY))
            }
          }
        } // end of while

        val tempScb = pending.get
        pending = None
        val ret = if (offset > 0) {
          val retCb = RmmRapidsRetryIterator.withRetryNoSplit(tempScb) { _ =>
            NvtxIdWithMetrics(NvtxRegistry.TOP_N_OFFSET, opTime) {
              withResource(tempScb.getColumnarBatch()) { tempCb =>
                applyOffset(tempCb, offset)
              }
            }
          }
          closeOnExcept(retCb)(SpillableColumnarBatch(_, ACTIVE_ON_DECK_PRIORITY))
        } else {
          tempScb
        }
        outputBatches += 1
        outputRows += ret.numRows()
        ret
      }
    }
}

/**
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a Limit operator after a `SortExec` operator,
 * or having a `ProjectExec` operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class GpuTopN(
    limit: Int,
    gpuSortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0)(
    cpuSortOrder: Seq[SortOrder]) extends GpuBaseLimitExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuSortOrder :: Nil

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    SORT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SORT_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME)
  )

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val sorter = new GpuSorter(gpuSortOrder, child.output, allMetrics)
    val boundProjectExprs = GpuBindReferences.bindGpuReferences(projectList, child.output,
      allMetrics)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val inputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val inputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val outputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val sortTime = gpuLongMetric(SORT_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val localLimit = limit
    val localProjectList = projectList
    val childOutput = child.output

    child.executeColumnar().mapPartitions { iter =>
      val topN = GpuTopN(localLimit, sorter, iter, opTime, sortTime, concatTime,
        inputBatches, inputRows, outputBatches, outputRows, offset)
      if (localProjectList != childOutput) {
        topN.map { scb =>
          opTime.ns {
            GpuProjectExec.projectAndCloseWithRetrySingleBatch(scb, boundProjectExprs)
          }
        }
      } else {
        topN.map { scb =>
          opTime.ns {
            RmmRapidsRetryIterator.withRetryNoSplit(scb)(_.getColumnarBatch())
          }
        }
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def outputOrdering: Seq[SortOrder] = cpuSortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(gpuSortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"GpuTopN(limit=$limit, orderBy=$orderByString, output=$outputString, offset=$offset)"
  }
}

case class GpuTakeOrderedAndProjectExecMeta(
   takeExec: TakeOrderedAndProjectExec,
   rapidsConf: RapidsConf,
   parentOpt: Option[RapidsMeta[_, _, _]],
   rule: DataFromReplacementRule
) extends SparkPlanMeta[TakeOrderedAndProjectExec](takeExec, rapidsConf, parentOpt, rule) {
  val sortOrder: Seq[BaseExprMeta[SortOrder]] =
    takeExec.sortOrder.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
  private val projectList: Seq[BaseExprMeta[NamedExpression]] =
    takeExec.projectList.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] = sortOrder ++ projectList

  override def convertToGpu(): GpuExec = {
    // To avoid metrics confusion we split a single stage up into multiple parts but only
    // if there are multiple partitions to make it worth doing.
    val so = sortOrder.map(_.convertToGpu().asInstanceOf[SortOrder])
    if (takeExec.child.outputPartitioning.numPartitions == 1) {
      GpuTopN(takeExec.limit, so,
        projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        childPlans.head.convertIfNeeded())(takeExec.sortOrder)
    } else {
      GpuTopN(
        takeExec.limit,
        so,
        projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        GpuShuffleExchangeExec(
          GpuSinglePartitioning,
          GpuTopN(
            takeExec.limit,
            so,
            takeExec.child.output,
            childPlans.head.convertIfNeeded())(takeExec.sortOrder),
          ENSURE_REQUIREMENTS
        )(SinglePartition)
      )(takeExec.sortOrder)
    }
  }
}
