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

package com.nvidia.spark.rapids

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuAggregateIterator.{computeAggregateAndClose, computeAggregateWithoutPreprocessAndClose, concatenateBatchesWithRetry}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.GpuOverrides.pluginSupportedOrderableSig
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.AggregationTagging

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, ExprId, If, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ExplainUtils, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.aggregate.{CudfAggregate, GpuAggregateExpression}
import org.apache.spark.sql.rapids.execution.{GpuBatchSubPartitioner, GpuShuffleMeta, TrampolineUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object AggregateUtils extends Logging {

  private val aggs = List("min", "max", "avg", "sum", "count", "first", "last")

  /**
   * Return true if the Attribute passed is one of aggregates in the aggs list.
   * Use it with caution. We are comparing the name of a column looking for anything that matches
   * with the values in aggs.
   */
  def validateAggregate(attributes: AttributeSet): Boolean = {
    attributes.toSeq.exists(attr => aggs.exists(agg => attr.name.contains(agg)))
  }

  /**
   * Return true if there are multiple distinct functions along with non-distinct functions.
   */
  def shouldFallbackMultiDistinct(aggExprs: Seq[AggregateExpression]): Boolean = {
    // Check if there is an `If` within `First`. This is included in the plan for non-distinct
    // functions only when multiple distincts along with non-distinct functions are present in the
    // query. We fall back to CPU in this case when references of `If` are an aggregate. We cannot
    // call `isDistinct` here on aggregateExpressions to get the total number of distinct functions.
    // If there are multiple distincts, the plan is rewritten by `RewriteDistinctAggregates` where
    // regular aggregations and every distinct aggregation is calculated in a separate group.
    aggExprs.map(e => e.aggregateFunction).exists {
      func => {
        func match {
          case First(If(_, _, _), _) if validateAggregate(func.references) => true
          case _ => false
        }
      }
    }
  }

  /**
   * Computes a target input batch size based on the assumption that computation can consume up to
   * 4X the configured batch size.
   * @param confTargetSize user-configured maximum desired batch size
   * @param inputTypes input batch schema
   * @param outputTypes output batch schema
   * @param isReductionOnly true if this is a reduction-only aggregation without grouping
   * @return maximum target batch size to keep computation under the 4X configured batch limit
   */
  def computeTargetBatchSize(
      confTargetSize: Long,
      inputTypes: Seq[DataType],
      outputTypes: Seq[DataType],
      isReductionOnly: Boolean): Long = {

    def typesToSize(types: Seq[DataType]): Long =
      types.map(GpuBatchUtils.estimateGpuMemory(_, nullable = false, rowCount = 1)).sum

    val inputRowSize = typesToSize(inputTypes)
    val outputRowSize = typesToSize(outputTypes)
    // The cudf hash table implementation allocates four 32-bit integers per input row.
    val hashTableRowSize = 4 * 4

    // Using the memory management for joins as a reference, target 4X batch size as a budget.
    var totalBudget = 4 * confTargetSize

    // Compute the amount of memory being consumed per-row in the computation
    var computationBytesPerRow = inputRowSize + hashTableRowSize
    if (isReductionOnly) {
      // Remove the lone output row size from the budget rather than track per-row in computation
      totalBudget -= outputRowSize
    } else {
      // The worst-case memory consumption during a grouping aggregation is the case where the
      // grouping does not combine any input rows, so just as many rows appear in the output.
      computationBytesPerRow += outputRowSize
    }

    // Calculate the max rows that can be processed during computation within the budget
    // Make sure it's not less than 1, otherwise some corner test cases may fail
    val maxRows = Math.max(totalBudget / computationBytesPerRow, 1)

    // Finally compute the input target batching size taking into account the cudf row limits
    Math.min(inputRowSize * maxRows, Int.MaxValue)
  }

  /**
   * Concatenate batches together and perform a merge aggregation on the result. The input batches
   * will be closed as part of this operation.
   *
   * @param batches batches to concatenate and merge aggregate
   * @return lazy spillable batch which has NOT been marked spillable
   */
  def concatenateAndMergeWithRetry(
      batches: mutable.ArrayBuffer[SpillableColumnarBatch],
      metrics: GpuHashAggregateMetrics,
      concatAndMergeHelper: AggHelper): SpillableColumnarBatch = {
    // TODO: concatenateAndMerge (and calling code) could output a sequence
    //   of batches for the partial aggregate case. This would be done in case
    //   a retry failed a certain number of times.
    val concatBatch = withResource(batches) { _ =>
      val concatSpillable = concatenateBatchesWithRetry(metrics, batches.toSeq)
      withRetryNoSplit(concatSpillable)(_.getColumnarBatch())
    }
    computeAggregateAndClose(metrics, concatBatch, concatAndMergeHelper)
  }

  /**
   * Try to concat and merge neighbour input batches to reduce the number of output batches.
   * For some cases where input is highly aggregate-able, we can merge multiple input batches
   * into a single output batch. In such cases we can skip repartition at all.
   */
  def streamAggregateNeighours(
      aggregatedBatches: CloseableBufferedIterator[SpillableColumnarBatch],
      metrics: GpuHashAggregateMetrics,
      targetMergeBatchSize: Long,
      configuredTargetBatchSize: Long,
      helper: AggHelper
  ): Iterator[SpillableColumnarBatch] = {
    new Iterator[SpillableColumnarBatch] {

      override def hasNext: Boolean = aggregatedBatches.hasNext

      override def next(): SpillableColumnarBatch = {
        closeOnExcept(new ArrayBuffer[SpillableColumnarBatch]) { stagingBatches => {
          var currentSize = 0L
          while (aggregatedBatches.hasNext) {
            val nextBatch = aggregatedBatches.head
            if (currentSize + nextBatch.sizeInBytes > targetMergeBatchSize) {
              if (stagingBatches.size == 1) {
                return stagingBatches.head
              } else if (stagingBatches.isEmpty) {
                aggregatedBatches.next
                return nextBatch
              }
              val merged = concatenateAndMergeWithRetry(stagingBatches, metrics, helper)
              stagingBatches.clear
              currentSize = 0L
              if (merged.sizeInBytes < configuredTargetBatchSize * 0.5) {
                stagingBatches += merged
                currentSize += merged.sizeInBytes
              } else {
                return merged
              }
            } else {
              stagingBatches.append(nextBatch)
              currentSize += nextBatch.sizeInBytes
              aggregatedBatches.next
            }
          }

          if (stagingBatches.size == 1) {
            return stagingBatches.head
          }
          concatenateAndMergeWithRetry(stagingBatches, metrics, helper)
        }
        }
      }
    }
  }

  /**
   * Read the input batches and repartition them into buckets.
   */
  def iterateAndRepartition(
      aggregatedBatches: Iterator[SpillableColumnarBatch],
      metrics: GpuHashAggregateMetrics,
      targetMergeBatchSize: Long,
      helper: AggHelper,
      hashKeys: Seq[GpuExpression],
      hashBucketNum: Int,
      baseHashSeed: Int,
      recursiveDepth: Int,
      maxRecursiveDepth: Int,
      batchesByBucket: ArrayBuffer[AutoClosableArrayBuffer[SpillableColumnarBatch]]
  ): Boolean = {
    val hashSeed = baseHashSeed + recursiveDepth * 7
    var repartitionHappened = false

    def repartitionAndClose(batch: SpillableColumnarBatch): Unit = {

      // OPTIMIZATION
      if (!aggregatedBatches.hasNext && batchesByBucket.forall(_.size() == 0)) {
        // If this is the only batch (after merging neighbours) to be repartitioned,
        // we can just add it to the first bucket and skip repartitioning.
        // This is a common case when total input size can fit into a single batch.
        batchesByBucket.head.append(batch)
        return
      }

      NvtxIdWithMetrics(NvtxRegistry.AGG_REPARTITION, metrics.repartitionTime) {

        withResource(new GpuBatchSubPartitioner(
          Iterator(withRetryNoSplit(batch)(_.getColumnarBatch())),
          hashKeys, hashBucketNum, hashSeed, "aggRepartition")) {
          partitioner => {
            (0 until partitioner.partitionsCount).foreach { id =>
              closeOnExcept(batchesByBucket) { _ => {
                val newBatches = partitioner.releaseBatchesByPartition(id)
                newBatches.foreach { newBatch =>
                  if (newBatch.numRows() > 0) {
                    batchesByBucket(id).append(newBatch)
                  } else {
                    newBatch.safeClose()
                  }
                }
              }
              }
            }
          }
        }
      }
      repartitionHappened = true
    }

    while (aggregatedBatches.hasNext) {
      repartitionAndClose(aggregatedBatches.next)
    }

    // Deal with the over sized buckets
    def needRepartitionAgain(bucket: AutoClosableArrayBuffer[SpillableColumnarBatch]) = {
      bucket.map(_.sizeInBytes).sum > targetMergeBatchSize &&
        bucket.size() != 1 &&
        !bucket.forall(_.numRows() == 1) // this is for test
    }

    if (repartitionHappened && batchesByBucket.exists(needRepartitionAgain)) {
      logDebug("Some of the repartition buckets are over sized, trying to split them")

      val newBuckets = batchesByBucket.flatMap(bucket => {
        if (needRepartitionAgain(bucket)) {
          if (recursiveDepth >= maxRecursiveDepth) {
            // Normally this should not happen, because we are repartitioning data that has
            // already gone through first round of aggregation, so there shouldn't be too many
            // duplicated rows (the duplication only happens in different batches) to prevent
            // repartitioning out (considering we're changing seed each time we repartition).
            // However, for some test cases with really small batch size, this can happen. So
            // we're just logging some warnings here.
            log.warn(s"The bucket is still too large after $recursiveDepth repartitions. " +
              s"See https://github.com/NVIDIA/spark-rapids/issues/11834. " +
              s"Sizes for each batch in current bucket: ${bucket.map(_.sizeInBytes).mkString(", ")}"
              + s" rows: ${bucket.map(_.numRows()).mkString(", ")}" +
              s" targetMergeBatchSize: $targetMergeBatchSize")
            ArrayBuffer(bucket)
          } else {
            val nextLayerBuckets =
              ArrayBuffer.fill(hashBucketNum)(new AutoClosableArrayBuffer[SpillableColumnarBatch]())
            // Recursively merge and repartition the over sized bucket
            repartitionHappened =
              iterateAndRepartition(
                new CloseableBufferedIterator(bucket.iterator), metrics, targetMergeBatchSize,
                helper, hashKeys, hashBucketNum, baseHashSeed, recursiveDepth + 1,
                maxRecursiveDepth, nextLayerBuckets) || repartitionHappened
            nextLayerBuckets
          }
        } else {
          ArrayBuffer(bucket)
        }
      })
      batchesByBucket.clear()
      batchesByBucket.appendAll(newBuckets)
    }

    repartitionHappened
  }
}

/** Utility class to hold all of the metrics related to hash aggregation */
case class GpuHashAggregateMetrics(
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    numTasksRepartitioned: GpuMetric,
    numTasksSkippedAgg: GpuMetric,
    opTime: GpuMetric,
    computeAggTime: GpuMetric,
    concatTime: GpuMetric,
    sortTime: GpuMetric,
    repartitionTime: GpuMetric,
    numAggOps: GpuMetric,
    numPreSplits: GpuMetric,
    singlePassTasks: GpuMetric,
    heuristicTime: GpuMetric) {
}

/** Utility class to convey information on the aggregation modes being used */
case class AggregateModeInfo(
    uniqueModes: Seq[AggregateMode],
    hasPartialMode: Boolean,
    hasPartialMergeMode: Boolean,
    hasFinalMode: Boolean,
    hasCompleteMode: Boolean)

object AggregateModeInfo {
  def apply(uniqueModes: Seq[AggregateMode]): AggregateModeInfo = {
    AggregateModeInfo(
      uniqueModes = uniqueModes,
      hasPartialMode = uniqueModes.contains(Partial),
      hasPartialMergeMode = uniqueModes.contains(PartialMerge),
      hasFinalMode = uniqueModes.contains(Final),
      hasCompleteMode = uniqueModes.contains(Complete)
    )
  }
}

/**
 * Internal class used in `computeAggregates` for the pre, agg, and post steps
 *
 * @param inputAttributes      input attributes to identify the input columns from the input batches
 * @param groupingExpressions  expressions used for producing the grouping keys
 * @param aggregateExpressions GPU aggregate expressions used to produce the aggregations
 * @param forceMerge           if true, we are merging two pre-aggregated batches, so we should use
 *                             the merge steps for each aggregate function
 * @param isSorted             if the batch is sorted this is set to true and is passed to cuDF
 *                             as an optimization hint
 * @param conf                 A configuration used to control TieredProject operations in an
 *                             aggregation.
 */
class AggHelper(
    inputAttributes: Seq[Attribute],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    forceMerge: Boolean,
    conf: SQLConf,
    isSorted: Boolean = false,
    metrics: Map[String, GpuMetric]) extends Serializable {

  private var doSortAgg = isSorted

  def setSort(isSorted: Boolean): Unit = {
    doSortAgg = isSorted
  }

  // `CudfAggregate` instances to apply, either update or merge aggregates
  // package private for testing
  private[rapids] val cudfAggregates = new mutable.ArrayBuffer[CudfAggregate]()

  // integers for each column the aggregate is operating on
  // package private for testing
  private[rapids] val aggOrdinals = new mutable.ArrayBuffer[Int]

  // grouping ordinals are the indices of the tables to aggregate that need to be
  // the grouping key
  // package private for testing
  private[rapids] val groupingOrdinals: Array[Int] = groupingExpressions.indices.toArray

  // the resulting data type from the cuDF aggregate (from
  // the update or merge aggregate, be it reduction or group by)
  private[rapids] val postStepDataTypes = new mutable.ArrayBuffer[DataType]()

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val aggBufferAttributes = groupingAttributes ++
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

  // `GpuAggregateFunction` can add a pre and post step for update
  // and merge aggregates.
  private val preStep = new mutable.ArrayBuffer[Expression]()
  private val postStep = new mutable.ArrayBuffer[Expression]()
  private val postStepAttr = new mutable.ArrayBuffer[Attribute]()

  // we add the grouping expression first, which should bind as pass-through
  if (forceMerge) {
    // a grouping expression can do actual computation, but we cannot do that computation again
    // on a merge, nor would we want to if we could. So use the attributes instead of the
    // original expression when we are forcing a merge.
    preStep ++= groupingAttributes
  } else {
    preStep ++= groupingExpressions
  }
  postStep ++= groupingAttributes
  postStepAttr ++= groupingAttributes
  postStepDataTypes ++=
    groupingExpressions.map(_.dataType)

  private var ix = groupingAttributes.length
  for (aggExp <- aggregateExpressions) {
    val aggFn = aggExp.aggregateFunction
    if ((aggExp.mode == Partial || aggExp.mode == Complete) && !forceMerge) {
      val ordinals = (ix until ix + aggFn.updateAggregates.length)
      aggOrdinals ++= ordinals
      ix += ordinals.length
      val updateAggs = aggFn.updateAggregates
      postStepDataTypes ++= updateAggs.map(_.dataType)
      cudfAggregates ++= updateAggs
      preStep ++= aggFn.inputProjection
      postStep ++= aggFn.postUpdate
      postStepAttr ++= aggFn.postUpdateAttr
    } else {
      val ordinals = (ix until ix + aggFn.mergeAggregates.length)
      aggOrdinals ++= ordinals
      ix += ordinals.length
      val mergeAggs = aggFn.mergeAggregates
      postStepDataTypes ++= mergeAggs.map(_.dataType)
      cudfAggregates ++= mergeAggs
      preStep ++= aggFn.preMerge
      postStep ++= aggFn.postMerge
      postStepAttr ++= aggFn.postMergeAttr
    }
  }

  // a bound expression that is applied before the cuDF aggregate
  private val preStepAttributes = if (forceMerge) {
    aggBufferAttributes
  } else {
    inputAttributes
  }
  val preStepBound =
    GpuBindReferences.bindGpuReferencesTiered(preStep.toList, preStepAttributes.toList, 
      conf, metrics)

  // a bound expression that is applied after the cuDF aggregate
  private val postStepBound =
    GpuBindReferences.bindGpuReferencesTiered(postStep.toList, postStepAttr.toList, conf, metrics)

  /**
   * Apply the "pre" step: preMerge for merge, or pass-through in the update case
   *
   * @param toAggregateBatch - input (to the agg) batch from the child directly in the
   *                         merge case, or from the `inputProjection` in the update case.
   * @return a pre-processed batch that can be later cuDF aggregated
   */
  def preProcess(
      toAggregateBatch: ColumnarBatch,
      metrics: GpuHashAggregateMetrics): SpillableColumnarBatch = {
    val inputBatch = SpillableColumnarBatch(toAggregateBatch,
      SpillPriorities.ACTIVE_ON_DECK_PRIORITY)

    val projectedCb = NvtxRegistry.AGG_PRE_PROCESS {
      preStepBound.projectAndCloseWithRetrySingleBatch(inputBatch)
    }
    SpillableColumnarBatch(
      projectedCb,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  def aggregate(preProcessed: ColumnarBatch, numAggs: GpuMetric): ColumnarBatch = {
    val ret = if (groupingOrdinals.nonEmpty) {
      performGroupByAggregation(preProcessed)
    } else {
      performReduction(preProcessed)
    }
    numAggs += 1
    ret
  }

  def aggregateWithoutCombine(metrics: GpuHashAggregateMetrics,
      preProcessed: Iterator[SpillableColumnarBatch]): Iterator[SpillableColumnarBatch] = {
    val computeAggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    val numAggs = metrics.numAggOps
    preProcessed.flatMap { sb =>
      withRetry(sb, splitSpillableInHalfByRows) { preProcessedAttempt =>
        NvtxIdWithMetrics(NvtxRegistry.COMPUTE_AGGREGATE, computeAggTime, opTime) {
          withResource(preProcessedAttempt.getColumnarBatch()) { cb =>
            SpillableColumnarBatch(
              aggregate(cb, numAggs),
              SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          }
        }
      }
    }
  }

  def aggregate(
      metrics: GpuHashAggregateMetrics,
      preProcessed: SpillableColumnarBatch): SpillableColumnarBatch = {
    val numAggs = metrics.numAggOps
    val aggregatedSeq =
      withRetry(preProcessed, splitSpillableInHalfByRows) { preProcessedAttempt =>
        withResource(preProcessedAttempt.getColumnarBatch()) { cb =>
          SpillableColumnarBatch(
            aggregate(cb, numAggs),
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }
      }.toSeq

    // We need to merge the aggregated batches into 1 before calling post process,
    // if the aggregate code had to split on a retry
    if (aggregatedSeq.size > 1) {
      val concatted = concatenateBatchesWithRetry(metrics, aggregatedSeq)
      withRetryNoSplit(concatted) { attempt =>
        withResource(attempt.getColumnarBatch()) { cb =>
          SpillableColumnarBatch(
            aggregate(cb, numAggs),
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }
      }
    } else {
      aggregatedSeq.head
    }
  }

  /**
   * Invoke reduction functions as defined in each `CudfAggreagte`
   *
   * @param preProcessed - a batch after the "pre" step
   * @return
   */
  def performReduction(preProcessed: ColumnarBatch): ColumnarBatch = {
    NvtxRegistry.AGG_REDUCE {
      val cvs = mutable.ArrayBuffer[GpuColumnVector]()
      cudfAggregates.zipWithIndex.foreach { case (cudfAgg, ix) =>
        val aggFn = cudfAgg.reductionAggregate
        val cols = GpuColumnVector.extractColumns(preProcessed)
        val reductionCol = cols(aggOrdinals(ix))
        withResource(aggFn(reductionCol.getBase)) { res =>
          cvs += GpuColumnVector.from(
            cudf.ColumnVector.fromScalar(res, 1), cudfAgg.dataType)
        }
      }
      new ColumnarBatch(cvs.toArray, 1)
    }
  }

  /**
   * Used to produce a group-by aggregate
   *
   * @param preProcessed the batch after the "pre" step
   * @return a Table that has been cuDF aggregated
   */
  def performGroupByAggregation(preProcessed: ColumnarBatch): ColumnarBatch = {
    NvtxRegistry.AGG_GROUPBY {
      withResource(GpuColumnVector.from(preProcessed)) { preProcessedTbl =>
        val groupOptions = cudf.GroupByOptions.builder()
          .withIgnoreNullKeys(false)
          .withKeysSorted(doSortAgg)
          .build()

        val cudfAggsOnColumn = cudfAggregates.zip(aggOrdinals).map {
          case (cudfAgg, ord) => cudfAgg.groupByAggregate.onColumn(ord)
        }

        // perform the aggregate
        val aggTbl = preProcessedTbl
          .groupBy(groupOptions, groupingOrdinals: _*)
          .aggregate(cudfAggsOnColumn.toSeq: _*)

        withResource(aggTbl) { _ =>
          GpuColumnVector.from(aggTbl, postStepDataTypes.toArray)
        }
      }
    }
  }

  /**
   * Used to produce the outbound batch from the aggregate that could be
   * shuffled or could be passed through the evaluateExpression if we are in the final
   * stage.
   * It takes a cuDF aggregated batch and applies the "post" step:
   * postUpdate for update, or postMerge for merge
   *
   * @param resultBatch - cuDF aggregated batch
   * @return output batch from the aggregate
   */
  def postProcess(
      aggregatedSpillable: SpillableColumnarBatch,
      metrics: GpuHashAggregateMetrics): SpillableColumnarBatch = {
    val postProcessed = NvtxRegistry.AGG_POST_PROCESS {
      postStepBound.projectAndCloseWithRetrySingleBatch(aggregatedSpillable)
    }
    SpillableColumnarBatch(
      postProcessed,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  def postProcess(input: Iterator[SpillableColumnarBatch],
      metrics: GpuHashAggregateMetrics): Iterator[SpillableColumnarBatch] = {
    val computeAggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    input.map { aggregated =>
      NvtxIdWithMetrics(NvtxRegistry.POST_PROCESS_AGG, computeAggTime, opTime) {
        val postProcessed = postStepBound.projectAndCloseWithRetrySingleBatch(aggregated)
        SpillableColumnarBatch(
          postProcessed,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
  }
}

object GpuAggregateIterator extends Logging {
  /**
   * @note abstracted away for a unit test..
   * @param helper
   * @param preProcessed
   * @return
   */
  def aggregate(
      helper: AggHelper,
      preProcessed: SpillableColumnarBatch,
      metrics: GpuHashAggregateMetrics): SpillableColumnarBatch = {
    helper.aggregate(metrics, preProcessed)
  }

  /**
   * Compute the aggregations on the projected input columns, and close input batch.
   *
   * @note public for testing
   * @param metrics metrics that will be updated during aggregation
   * @param inputBatch input batch to aggregate
   * @param helper an internal object that carries state required to execute the aggregate from
   *               different parts of the codebase.
   * @return aggregated batch
   */
  def computeAggregateAndClose(
      metrics: GpuHashAggregateMetrics,
      inputBatch: ColumnarBatch,
      helper: AggHelper): SpillableColumnarBatch = {
    val computeAggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    NvtxIdWithMetrics(NvtxRegistry.COMPUTE_AGGREGATE, computeAggTime, opTime) {
      // 1) a pre-processing step required before we go into the cuDF aggregate,
      // in some cases casting and in others creating a struct (MERGE_M2 for instance,
      // requires a struct)
      // OOM retry happens within the projection in preProcess
      val preProcessed = helper.preProcess(inputBatch, metrics)

      // 2) perform the aggregation
      // OOM retry means we could get a list of batches
      val aggregatedSpillable = aggregate(helper, preProcessed, metrics)

      // 3) a post-processing step required in some scenarios, casting or picking
      // apart a struct
      helper.postProcess(aggregatedSpillable, metrics)
    }
  }

  def computeAggregateWithoutPreprocessAndClose(
      metrics: GpuHashAggregateMetrics,
      inputBatches: Iterator[ColumnarBatch],
      helper: AggHelper): Iterator[SpillableColumnarBatch] = {
    val computeAggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    // 1) a pre-processing step required before we go into the cuDF aggregate, This has already
    // been done and is skipped

    val spillableInput = inputBatches.map { cb =>
      withResource(new MetricRange(computeAggTime, opTime)) { _ =>
        SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }

    // 2) perform the aggregation
    // OOM retry means we could get a list of batches
    val aggregatedSpillable = helper.aggregateWithoutCombine(metrics, spillableInput)

    // 3) a post-processing step required in some scenarios, casting or picking
    // apart a struct
    helper.postProcess(aggregatedSpillable, metrics)
  }

  /**
   * Concatenates batches after extracting them from `SpllableColumnarBatch`
   * @note the input batches are not closed as part of this operation
   * @param metrics metrics that will be updated during aggregation
   * @param toConcat spillable batches to concatenate
   * @return concatenated batch result
   */
  def concatenateBatchesWithRetry(
      metrics: GpuHashAggregateMetrics,
      toConcat: Seq[SpillableColumnarBatch]): SpillableColumnarBatch = {
    if (toConcat.size == 1) {
      toConcat.head
    } else {
      withRetryNoSplit(toConcat) { attempt =>
        val concatTime = metrics.concatTime
        val opTime = metrics.opTime
        NvtxIdWithMetrics(NvtxRegistry.CONCATENATE_BATCHES, concatTime, opTime) {
          val batchesToConcat = attempt.safeMap(_.getColumnarBatch())
          withResource(batchesToConcat) { _ =>
            val numCols = batchesToConcat.head.numCols()
            val dataTypes = (0 until numCols).map {
              c => batchesToConcat.head.column(c).dataType
            }.toArray
            withResource(batchesToConcat.safeMap(GpuColumnVector.from)) { tbl =>
              withResource(cudf.Table.concatenate(tbl: _*)) { concatenated =>
                val cb = GpuColumnVector.from(concatenated, dataTypes)
                SpillableColumnarBatch(cb,
                  SpillPriorities.ACTIVE_BATCHING_PRIORITY)
              }
            }
          }
        }
      }
    }
  }
}

object GpuAggFirstPassIterator {
  def apply(cbIter: Iterator[ColumnarBatch],
      aggHelper: AggHelper,
      metrics: GpuHashAggregateMetrics
  ): Iterator[SpillableColumnarBatch] = {
    val preprocessProjectIter = cbIter.map { cb =>
      val sb = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      aggHelper.preStepBound.projectAndCloseWithRetrySingleBatch(sb)
    }
    computeAggregateWithoutPreprocessAndClose(metrics, preprocessProjectIter, aggHelper)
  }
}

// Partial mode:
//  * boundFinalProjections: is a pass-through of the agg buffer
//  * boundResultReferences: is a pass-through of the merged aggregate
//
// Final mode:
//  * boundFinalProjections: on merged batches, finalize aggregates
//     (GpuAverage => CudfSum/CudfCount)
//  * boundResultReferences: project the result expressions Spark expects in the output.
//
// Complete mode:
//  * boundFinalProjections: on merged batches, finalize aggregates
//     (GpuAverage => CudfSum/CudfCount)
//  * boundResultReferences: project the result expressions Spark expects in the output.
case class BoundExpressionsModeAggregates(
    boundFinalProjections: Option[Seq[GpuExpression]],
    boundResultReferences: Seq[Expression])

object GpuAggFinalPassIterator {

  /**
   * `setupReferences` binds input, final and result references for the aggregate with
   * metrics injection.
   * - input: used to obtain columns coming into the aggregate from the child
   * - final: some aggregates like average use this to specify an expression to produce
   * the final output of the aggregate. Average keeps sum and count throughout,
   * and at the end it has to divide the two, to produce the single sum/count result.
   * - result: used at the end to output to our parent
   */
  def setupReferences(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[GpuAggregateExpression],
      aggregateAttributes: Seq[Attribute],
      resultExpressions: Seq[NamedExpression],
      modeInfo: AggregateModeInfo,
      metrics: Map[String, GpuMetric]): BoundExpressionsModeAggregates = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val aggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    val boundFinalProjections = if (modeInfo.hasFinalMode || modeInfo.hasCompleteMode) {
      val finalProjections = groupingAttributes ++
        aggregateExpressions.map(_.aggregateFunction.evaluateExpression)
      Some(GpuBindReferences.bindGpuReferences(finalProjections, aggBufferAttributes, metrics))
    } else {
      None
    }

    // allAttributes can be different things, depending on aggregation mode:
    // - Partial mode: grouping key + cudf aggregates (e.g. no avg, instead sum::count
    // - Final mode: grouping key + spark aggregates (e.g. avg)
    val finalAttributes = groupingAttributes ++ aggregateAttributes

    // boundResultReferences is used to project the aggregated input batch(es) for the result.
    // - Partial mode: it's a pass through. We take whatever was aggregated and let it come
    //   out of the node as is.
    // - Final or Complete mode: we use resultExpressions to pick out the correct columns that
    //   finalReferences has pre-processed for us
    val boundResultReferences = if (modeInfo.hasPartialMode || modeInfo.hasPartialMergeMode) {
      GpuBindReferences.bindGpuReferences(resultExpressions,
        resultExpressions.map(_.toAttribute), metrics)
    } else if (modeInfo.hasFinalMode || modeInfo.hasCompleteMode) {
      GpuBindReferences.bindGpuReferences(resultExpressions, finalAttributes, metrics)
    } else {
      GpuBindReferences.bindGpuReferences(resultExpressions, groupingAttributes, metrics)
    }
    BoundExpressionsModeAggregates(
      boundFinalProjections,
      boundResultReferences)
  }

  private[this] def reorderFinalBatch(finalBatch: SpillableColumnarBatch,
      boundExpressions: BoundExpressionsModeAggregates,
      metrics: GpuHashAggregateMetrics): ColumnarBatch = {
    // Perform the last project to get the correct shape that Spark expects. Note this may
    // add things like literals that were not part of the aggregate into the batch.
    closeOnExcept(GpuProjectExec.projectAndCloseWithRetrySingleBatch(finalBatch,
      boundExpressions.boundResultReferences)) { ret =>
      metrics.numOutputRows += ret.numRows()
      metrics.numOutputBatches += 1
      ret
    }
  }

  def makeIter(cbIter: Iterator[ColumnarBatch],
      boundExpressions: BoundExpressionsModeAggregates,
      metrics: GpuHashAggregateMetrics): Iterator[ColumnarBatch] = {
    val aggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    cbIter.map { batch =>
      NvtxIdWithMetrics(NvtxRegistry.FINALIZE_AGG, aggTime, opTime) {
        val finalBatch = boundExpressions.boundFinalProjections.map { exprs =>
          GpuProjectExec.projectAndCloseWithRetrySingleBatch(
            SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY), exprs)
        }.getOrElse(batch)
        val finalSCB =
          SpillableColumnarBatch(finalBatch, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        reorderFinalBatch(finalSCB, boundExpressions, metrics)
      }
    }
  }

  def makeIterFromSpillable(sbIter: Iterator[SpillableColumnarBatch],
      boundExpressions: BoundExpressionsModeAggregates,
      metrics: GpuHashAggregateMetrics): Iterator[ColumnarBatch] = {
    val aggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    sbIter.map { sb =>
      NvtxIdWithMetrics(NvtxRegistry.FINALIZE_AGG, aggTime, opTime) {
        val finalBatch = boundExpressions.boundFinalProjections.map { exprs =>
          SpillableColumnarBatch(
            GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, exprs),
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        }.getOrElse(sb)
        reorderFinalBatch(finalBatch, boundExpressions, metrics)
      }
    }
  }
}


/**
 * Iterator that takes another columnar batch iterator as input and emits new columnar batches that
 * are aggregated based on the specified grouping and aggregation expressions. This iterator tries
 * to perform a hash-based aggregation but is capable of falling back to a repartition-based
 * aggregation which can operate on data that is either larger than can be represented by a cudf
 * column or larger than can fit in GPU memory.
 *
 * In general, GpuMergeAggregateIterator works in this flow:
 *
 * (1) The iterator starts by pulling all batches from the input iterator, performing an initial
 * projection and aggregation on each individual batch via `GpuAggFirstPassIterator`, we call it
 * "First Pass Aggregate".
 * (2) Then the batches after first pass agg is sent to "streamAggregateNeighours", where it tries
 * to concat & merge the neighbour batches into fewer batches, then "iterateAndRepartition"
 * repartition the batch into fixed size buckets. Recursive repartition will be applied on
 * over-sized buckets until each bucket * is within the target size.
 * We call this phase "Second Pass Aggregate".
 * (3) At "Third Pass Aggregate", we take each bucket and perform a final aggregation on all batches
 * in the bucket, check "RepartitionAggregateIterator" for details.
 *
 * @param firstPassIter iterator that has done a first aggregation pass over the input data.
 * @param inputAttributes input attributes to identify the input columns from the input batches
 * @param groupingExpressions expressions used for producing the grouping keys
 * @param aggregateExpressions GPU aggregate expressions used to produce the aggregations
 * @param aggregateAttributes attribute references to each aggregate expression
 * @param resultExpressions output expression for the aggregation
 * @param modeInfo identifies which aggregation modes are being used
 * @param metrics metrics that will be updated during aggregation
 * @param configuredTargetBatchSize user-specified value for the targeted input batch size
 * @param allowNonFullyAggregatedOutput if allowed to skip third pass Agg
 * @param skipAggPassReductionRatio skip if the ratio of rows after a pass is bigger than this value
 * @param localInputRowsCount metric to track the number of input rows processed locally
 */
class GpuMergeAggregateIterator(
    firstPassIter: CloseableBufferedIterator[SpillableColumnarBatch],
    inputAttributes: Seq[Attribute],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    modeInfo: AggregateModeInfo,
    metrics: GpuHashAggregateMetrics,
    configuredTargetBatchSize: Long,
    conf: SQLConf,
    allowNonFullyAggregatedOutput: Boolean,
    skipAggPassReductionRatio: Double,
    localInputRowsCount: LocalGpuMetric,
    allMetrics: Map[String, GpuMetric]
)
  extends Iterator[ColumnarBatch] with AutoCloseable with Logging {
  private[this] val isReductionOnly = groupingExpressions.isEmpty
  private[this] val targetMergeBatchSize = computeTargetMergeBatchSize(configuredTargetBatchSize)

  private[this] val defaultHashBucketNum = 16
  private[this] val maxLevelsOfRepartition = 10 // this is the max level for recursive repartition
  private[this] val baseHashSeed = 107 // this is the seed used for first level for repartition
  private[this] var batchesByBucket =
    ArrayBuffer.fill(defaultHashBucketNum)(new AutoClosableArrayBuffer[SpillableColumnarBatch]())

  private[this] var firstBatchChecked = false

  private[this] var bucketIter: Option[RepartitionAggregateIterator] = None

  private[this] var realIter: Option[Iterator[ColumnarBatch]] = None

  /** Whether a batch is pending for a reduction-only aggregation */
  private[this] var hasReductionOnlyBatch: Boolean = isReductionOnly

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  override def hasNext: Boolean = {
    realIter.map(_.hasNext).getOrElse {
      // reductions produce a result even if the input is empty
      hasReductionOnlyBatch || firstPassIter.hasNext
    }
  }

  override def next(): ColumnarBatch = {
    realIter.map(_.next()).getOrElse {

      // Handle reduction-only aggregation
      if (isReductionOnly) {
        val batches = ArrayBuffer[SpillableColumnarBatch]()
        while (firstPassIter.hasNext) {
          batches += firstPassIter.next()
        }

        if (batches.isEmpty || batches.forall(_.numRows() == 0)) {
          hasReductionOnlyBatch = false
          return generateEmptyReductionBatch()
        } else {
          hasReductionOnlyBatch = false
          val concat =
            AggregateUtils.concatenateAndMergeWithRetry(batches, metrics, concatAndMergeHelper)
          return withRetryNoSplit(concat)(_.getColumnarBatch())
        }
      }

      // Handle the case of skipping second and third pass of aggregation
      // This only work when spark.rapids.sql.agg.skipAggPassReductionRatio < 1
      if (!firstBatchChecked && firstPassIter.hasNext
        && allowNonFullyAggregatedOutput) {
        firstBatchChecked = true

        val peek = firstPassIter.head
        // It's only based on first batch of first pass agg, so it's an estimate
        val firstPassReductionRatioEstimate = 1.0 * peek.numRows() / localInputRowsCount.value
        if (firstPassReductionRatioEstimate > skipAggPassReductionRatio) {
          logInfo("Skipping second and third pass aggregation due to " +
            "too high reduction ratio in first pass: " +
            s"$firstPassReductionRatioEstimate")
          // if so, skip any aggregation, return the origin batch directly

          realIter = Some(ConcatIterator(firstPassIter, configuredTargetBatchSize))
          metrics.numTasksSkippedAgg += 1
          return realIter.get.next()
        } else {
          logInfo(s"The reduction ratio in first pass is not high enough to skip " +
            s"second and third pass aggregation: peek.numRows: ${peek.numRows()}, " +
            s"localInputRowsCount.value: ${localInputRowsCount.value}")
        }
      }
      firstBatchChecked = true

      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
      // No metrics needed here - we're just binding AttributeReferences to GpuBoundReferences
      // for hash key computation, not executing complex expressions
      val hashKeys: Seq[GpuExpression] =
        GpuBindReferences.bindGpuReferencesNoMetrics(groupingAttributes,
          aggBufferAttributes.toSeq)

      val repartitionHappened = AggregateUtils.iterateAndRepartition(
        AggregateUtils.streamAggregateNeighours(
          firstPassIter,
          metrics,
          targetMergeBatchSize,
          configuredTargetBatchSize,
          concatAndMergeHelper)
        , metrics, targetMergeBatchSize, concatAndMergeHelper,
        hashKeys, defaultHashBucketNum, baseHashSeed, 0, maxLevelsOfRepartition, batchesByBucket)
      if (repartitionHappened) {
        metrics.numTasksRepartitioned += 1
      }

      realIter = Some(ConcatIterator(
        new CloseableBufferedIterator(buildBucketIterator()), configuredTargetBatchSize))
      realIter.get.next()
    }
  }

  override def close(): Unit = {
    batchesByBucket.foreach(_.close())
    batchesByBucket.clear()
    hasReductionOnlyBatch = false
  }

  private def computeTargetMergeBatchSize(confTargetSize: Long): Long = {
    val mergedTypes = groupingExpressions.map(_.dataType) ++ aggregateExpressions.map(_.dataType)
    AggregateUtils.computeTargetBatchSize(confTargetSize, mergedTypes, mergedTypes, isReductionOnly)
  }

  private lazy val concatAndMergeHelper =
    new AggHelper(inputAttributes, groupingExpressions, aggregateExpressions,
      forceMerge = true, conf, isSorted = false, allMetrics)

  private case class ConcatIterator(
      input: CloseableBufferedIterator[SpillableColumnarBatch],
      targetSize: Long)
    extends Iterator[ColumnarBatch] {

    override def hasNext: Boolean = input.hasNext

    override def next(): ColumnarBatch = {
      // combine all the data into a single batch
      val spillCbs = ArrayBuffer[SpillableColumnarBatch]()
      var totalBytes = 0L
      closeOnExcept(spillCbs) { _ =>
        while (input.hasNext && (
          // for some test cases targetSize is too small to fit any SpillableColumnarBatch,
          // in this case we put the first SpillableColumnarBatch into spillCbs anyway
          // refer to https://github.com/NVIDIA/spark-rapids/issues/11790 for examples
          spillCbs.isEmpty ||
            (totalBytes + input.head.sizeInBytes) <= targetSize)) {
          val tmp = input.next
          totalBytes += tmp.sizeInBytes
          spillCbs += tmp
        }

        val concat = concatenateBatchesWithRetry(metrics, spillCbs.toSeq)
        withRetryNoSplit(concat)(_.getColumnarBatch())
      }
    }
  }

  private case class RepartitionAggregateIterator(opTime: GpuMetric)
    extends Iterator[SpillableColumnarBatch] {

    batchesByBucket = batchesByBucket.filter(_.size() > 0)

    override def hasNext: Boolean = batchesByBucket.nonEmpty

    override def next(): SpillableColumnarBatch = {
      NvtxIdWithMetrics(NvtxRegistry.REPARTITION_AGG_ITERATOR_NEXT, opTime) {

        if (batchesByBucket.last.size() == 1) {
          batchesByBucket.remove(batchesByBucket.size - 1).removeLast()
        } else {
          // put as many buckets as possible together to aggregate, to reduce agg times
          closeOnExcept(new ArrayBuffer[AutoClosableArrayBuffer[SpillableColumnarBatch]]) {
            toAggregateBuckets =>
              var currentSize = 0L
              var keepGoing = true
              while (batchesByBucket.nonEmpty && keepGoing) {
                currentSize += batchesByBucket.last.map(_.sizeInBytes).sum
                keepGoing = currentSize <= targetMergeBatchSize || toAggregateBuckets.isEmpty
                if (keepGoing) {
                  toAggregateBuckets += batchesByBucket.remove(batchesByBucket.size - 1)
                }
              }

              AggregateUtils.concatenateAndMergeWithRetry(
                toAggregateBuckets.flatMap(_.data), metrics, concatAndMergeHelper)
          }
        }
      }
    }
  }


  /** Build an iterator merging aggregated batches in each bucket. */
  private def buildBucketIterator(): Iterator[SpillableColumnarBatch] = {
    bucketIter = Some(RepartitionAggregateIterator(opTime = metrics.opTime))
    bucketIter.get
  }


  /**
   * Generates the result of a reduction-only aggregation on empty input by emitting the
   * initial value of each aggregator.
   */
  private def generateEmptyReductionBatch(): ColumnarBatch = {
    val aggregateFunctions = aggregateExpressions.map(_.aggregateFunction)
    val defaultValues =
      aggregateFunctions.flatMap(_.initialValues)
    // We have to grab the semaphore in this scenario, since this is a reduction that produces
    // rows on the GPU out of empty input, meaning that if a batch has 0 rows, a new single
    // row is getting created with 0 as the count (if count is the operation), and other default
    // values.
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    val vecs = defaultValues.safeMap { ref =>
      withResource(GpuScalar.from(ref.asInstanceOf[GpuLiteral].value, ref.dataType)) {
        scalar => GpuColumnVector.from(scalar, 1, ref.dataType)
      }
    }
    new ColumnarBatch(vecs.toArray, 1)
  }
}

object GpuBaseAggregateMeta {
  private val aggPairReplaceChecked = TreeNodeTag[Boolean](
    "rapids.gpu.aggPairReplaceChecked")

  def getAggregateOfAllStages(
      currentMeta: SparkPlanMeta[_], logical: LogicalPlan): List[GpuBaseAggregateMeta[_]] = {
    currentMeta match {
      case aggMeta: GpuBaseAggregateMeta[_] if aggMeta.agg.logicalLink.contains(logical) =>
        List[GpuBaseAggregateMeta[_]](aggMeta) ++
          getAggregateOfAllStages(aggMeta.childPlans.head, logical)
      case shuffleMeta: GpuShuffleMeta =>
        getAggregateOfAllStages(shuffleMeta.childPlans.head, logical)
      case sortMeta: GpuSortMeta =>
        getAggregateOfAllStages(sortMeta.childPlans.head, logical)
      case _ =>
        List[GpuBaseAggregateMeta[_]]()
    }
  }
}

abstract class GpuBaseAggregateMeta[INPUT <: SparkPlan](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  val agg: BaseAggregateExec

  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def tagPlanForGpu(): Unit = {
    // We don't support Maps as GroupBy keys yet, even if they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val mapGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType, dt => dt.isInstanceOf[MapType]))
    if (mapGroupings) {
      willNotWorkOnGpu("MapType in grouping expressions are not supported")
    }

    // We support Arrays as grouping expression but not if the child is a struct. So we need to
    // run recursive type check on the lists of structs
    val arrayWithStructsGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt match {
          case ArrayType(_: StructType, _) => true
          case _ => false
        })
    )
    if (arrayWithStructsGroupings) {
      willNotWorkOnGpu("ArrayTypes with Struct children in grouping expressions are not " +
        "supported")
    }

    tagForReplaceMode()

    if (agg.aggregateExpressions.exists(expr => expr.isDistinct)
      && agg.aggregateExpressions.exists(expr => expr.filter.isDefined)) {
      // Distinct with Filter is not supported on the GPU currently,
      // This makes sure that if we end up here, the plan falls back to the CPU
      // which will do the right thing.
      willNotWorkOnGpu(
        "DISTINCT and FILTER cannot be used in aggregate functions at the same time")
    }

    if (AggregationTagging.mustReplaceBoth) {
      tagForMixedReplacement()
    }
  }

  /**
   * Tagging checks tied to configs that control the aggregation modes that are replaced.
   *
   * The rule of replacement is determined by `spark.rapids.sql.hashAgg.replaceMode`, which
   * is a string configuration consisting of AggregateMode names in lower cases connected by
   * &(AND) and |(OR). The default value of this config is `all`, which indicates replacing all
   * aggregates if possible.
   *
   * The `|` serves as the outer connector, which represents patterns of both sides are able to be
   * replaced. For instance, `final|partialMerge` indicates that aggregate plans purely in either
   * Final mode or PartialMerge mode can be replaced. But aggregate plans also contain
   * AggExpressions of other mode will NOT be replaced, such as: stage 3 of single distinct
   * aggregate who contains both Partial and PartialMerge.
   *
   * On the contrary, the `&` serves as the inner connector, which intersects modes of both sides
   * to form a mode pattern. The replacement only takes place for aggregate plans who have the
   * exact same mode pattern as what defined the rule. For instance, `partial&partialMerge` means
   * that aggregate plans can be only replaced if they contain AggExpressions of Partial and
   * contain AggExpressions of PartialMerge and don't contain AggExpressions of other modes.
   *
   * In practice, we need to combine `|` and `&` to form some sophisticated patterns. For instance,
   * `final&complete|final|partialMerge` represents aggregate plans in three different patterns are
   * GPU-replaceable: plans contain both Final and Complete modes; plans only contain Final mode;
   * plans only contain PartialMerge mode.
   */
  private def tagForReplaceMode(): Unit = {
    val aggPattern = agg.aggregateExpressions.map(_.mode).toSet
    val strPatternToReplace = conf.hashAggReplaceMode.toLowerCase

    if (aggPattern.nonEmpty && strPatternToReplace != "all") {

      /*
      * Type inferencing by the Scala compiler will choose the most specific return type
      * something like Array[Set[Product with Serializable with AggregateMode]] or with
      * slight differences depending on Scala version. Here we ensure this is
      * Array[Set[AggregateMode]] to perform the subsequent Set and Array operations properly.
      */
      val aggPatternsCanReplace = strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }.asInstanceOf[Array[Set[AggregateMode]]]

      if (!aggPatternsCanReplace.contains(aggPattern)) {
        val message = aggPattern.map(_.toString).mkString(",")
        willNotWorkOnGpu(s"Replacing mode pattern `$message` hash aggregates disabled")
      } else if (aggPattern == Set(Partial)) {
        // In partial mode, if there are non-distinct functions and multiple distinct functions,
        // non-distinct functions are computed using the First operator. The final result would be
        // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
        // the first batch computed and sent to CPU doesn't contain all the rows required to
        // compute non-distinct function(s), then Spark would consider that value as final result
        // (due to First). Fall back to CPU in this case.
        if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
          willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
            "functions are non-deterministic for non-distinct functions as it is " +
            "computed using First.")
        }
      }
    }

    if (!conf.partialMergeDistinctEnabled && aggPattern.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
        s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }

  /** Prevent mixing of CPU and GPU aggregations */
  private def tagForMixedReplacement(): Unit = {
    // only run the check for final stages that have not already been checked
    val haveChecked =
      agg.getTagValue[Boolean](GpuBaseAggregateMeta.aggPairReplaceChecked).contains(true)
    val needCheck = !haveChecked && agg.aggregateExpressions.exists {
      case e: AggregateExpression if e.mode == Final => true
      case _ => false
    }

    if (needCheck) {
      agg.setTagValue(GpuBaseAggregateMeta.aggPairReplaceChecked, true)
      val stages = GpuBaseAggregateMeta.getAggregateOfAllStages(this, agg.logicalLink.get)
      // check if aggregations will mix CPU and GPU across stages
      val hasMixedAggs = stages.indices.exists {
        case i if i == stages.length - 1 => false
        case i => stages(i).canThisBeReplaced ^ stages(i + 1).canThisBeReplaced
      }
      if (hasMixedAggs) {
        stages.foreach {
          case aggMeta if aggMeta.canThisBeReplaced =>
            aggMeta.willNotWorkOnGpu("mixing CPU and GPU aggregations is not supported")
          case _ =>
        }
      }
    }
  }

  private val orderable = pluginSupportedOrderableSig

  override def convertToGpu(): GpuExec = {
    lazy val aggModes = agg.aggregateExpressions.map(_.mode).toSet
    lazy val canUsePartialSortAgg = aggModes.forall { mode =>
      mode == Partial || mode == PartialMerge
    } && agg.groupingExpressions.nonEmpty // Don't do this for a reduce...

    // for a aggregateExpressions.isEmpty case, we cannot distinguish between final and non-final,
    // so don't allow it.
    lazy val allowNonFullyAggregatedOutput = aggModes.forall { mode =>
      mode == Partial || mode == PartialMerge
    } && agg.aggregateExpressions.nonEmpty

    lazy val groupingCanBeSorted = agg.groupingExpressions.forall { expr =>
      orderable.isSupportedByPlugin(expr.dataType)
    }

    // This is a short term heuristic until we can better understand the cost
    // of sort vs the cost of doing aggregations so we can better decide.
    lazy val hasSingleBasicGroupingKey = agg.groupingExpressions.length == 1 &&
      agg.groupingExpressions.headOption.map(_.dataType).exists {
        case StringType | BooleanType | ByteType | ShortType | IntegerType |
             LongType | _: DecimalType | DateType | TimestampType => true
        case _ => false
      }

    val gpuChild = childPlans.head.convertIfNeeded()
    val gpuAggregateExpressions =
      aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression])
    val gpuGroupingExpressions =
      groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression])

    // Sorting before we do an aggregation helps if the size of the input data is
    // smaller than the size of the output data. But it is an aggregation how can
    // the output be larger than the input? This happens if there are a lot of
    // aggregations relative to the number of input columns. So if we will have
    // to cache/spill data we would rather cache and potentially spill the smaller
    // data. By sorting the data on a partial agg we don't have to worry about
    // combining the output of multiple batch because the final agg will take care
    // of it, and in the worst case we will end up with one extra row per-batch that
    // would need to be shuffled to the final aggregate.  An okay tradeoff.
    //
    // The formula for the estimated output size after aggregation is
    // input * pre-project-growth * agg-reduction.
    //
    // We are going to estimate the pre-project-growth here, and if it looks like
    // it is going to be larger than the input, then we will dynamically estimate
    // the agg-reduction in each task and make a choice there what to do.

    lazy val estimatedPreProcessGrowth = {
      val inputAggBufferAttributes =
        GpuHashAggregateExecBase.calcInputAggBufferAttributes(gpuAggregateExpressions)
      val inputAttrs = GpuHashAggregateExecBase.calcInputAttributes(gpuAggregateExpressions,
        gpuChild.output, inputAggBufferAttributes)
      val preProcessAggHelper = new AggHelper(
        inputAttrs, gpuGroupingExpressions, gpuAggregateExpressions,
        forceMerge = false, conf = agg.conf, metrics = Map.empty[String, GpuMetric])

      // We are going to estimate the growth by looking at the estimated size the output could
      // be compared to the estimated size of the input (both based off of the schemas).
      // It if far from perfect, but it should work okay-ish.
      val numRowsForEstimate = 1000000 // 1 million rows...
      val estimatedInputSize = gpuChild.output.map { attr =>
        GpuBatchUtils.estimateGpuMemory(attr.dataType, attr.nullable, numRowsForEstimate)
      }.sum
      val estimatedOutputSize = preProcessAggHelper.preStepBound.outputExprs.map { e =>
        GpuBatchUtils.estimateGpuMemory(e.dataType, true, numRowsForEstimate)
      }.sum
      if (estimatedInputSize == 0 && estimatedOutputSize == 0) {
        1.0
      } else if (estimatedInputSize == 0) {
        100.0
      } else {
        estimatedOutputSize.toDouble / estimatedInputSize
      }
    }

    val allowSinglePassAgg = (conf.forceSinglePassPartialSortAgg ||
      (conf.allowSinglePassPartialSortAgg &&
        hasSingleBasicGroupingKey &&
        estimatedPreProcessGrowth > 1.1)) &&
      canUsePartialSortAgg &&
      groupingCanBeSorted

    GpuHashAggregateExec(
      aggRequiredChildDistributionExpressions,
      gpuGroupingExpressions,
      gpuAggregateExpressions,
      aggregateAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
      resultExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
      gpuChild,
      conf.gpuTargetBatchSizeBytes,
      estimatedPreProcessGrowth,
      conf.forceSinglePassPartialSortAgg,
      allowSinglePassAgg,
      allowNonFullyAggregatedOutput,
      conf.skipAggPassReductionRatio
    )
  }
}

/**
 * Base class for metadata around `SortAggregateExec` and `ObjectHashAggregateExec`, which may
 * contain TypedImperativeAggregate functions in aggregate expressions.
 */
abstract class GpuTypedImperativeSupportedAggregateExecMeta[INPUT <: BaseAggregateExec](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBaseAggregateMeta[INPUT](plan,
  aggRequiredChildDistributionExpressions, conf, parent, rule) {

  private val mayNeedAggBufferConversion: Boolean =
    agg.aggregateExpressions.exists { expr =>
      expr.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]] &&
        (expr.mode == Partial || expr.mode == PartialMerge)
    }

  // overriding data types of Aggregation Buffers if necessary
  if (mayNeedAggBufferConversion) overrideAggBufTypes()

  override protected lazy val outputTypeMetas: Option[Seq[DataTypeMeta]] =
    if (mayNeedAggBufferConversion) {
      Some(resultExpressions.map(_.typeMeta))
    } else {
      None
    }

  override val availableRuntimeDataTransition: Boolean =
    aggregateExpressions.map(_.childExprs.head).forall {
      case aggMeta: TypedImperativeAggExprMeta[_] => aggMeta.supportBufferConversion
      case _ => true
    }

  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // If a typedImperativeAggregate function run across CPU and GPU (ex: Partial mode on CPU,
    // Merge mode on GPU), it will lead to a runtime crash. Because aggregation buffers produced
    // by the previous stage of function are NOT in the same format as the later stage consuming.
    // If buffer converters are available for all incompatible buffers, we will build these buffer
    // converters and bind them to certain plans, in order to integrate these converters into
    // R2C/C2R Transitions during GpuTransitionOverrides to fix the gap between GPU and CPU.
    // Otherwise, we have to fall back all Aggregate stages to CPU once any of them did fallback.
    //
    // The binding also works when AQE is on, since it leverages the TreeNodeTag to cache buffer
    // converters.
    GpuTypedImperativeSupportedAggregateExecMeta.handleAggregationBuffer(this)
  }

  override def convertToGpu(): GpuExec = {
    if (mayNeedAggBufferConversion) {
      // transforms the data types of aggregate attributes with typeMeta
      val aggAttributes = aggregateAttributes.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      // transforms the data types of result expressions with typeMeta
      val retExpressions = resultExpressions.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      GpuHashAggregateExec(
        aggRequiredChildDistributionExpressions,
        groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression]),
        aggAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
        retExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        childPlans.head.convertIfNeeded(),
        conf.gpuTargetBatchSizeBytes,
        // For now we are just going to go with the original hash aggregation
        1.0,
        forceSinglePassAgg = false,
        allowSinglePassAgg = false,
        allowNonFullyAggregatedOutput = false,
        1)

    } else {
      super.convertToGpu()
    }
  }

  /**
   * The method replaces data types of aggregation buffers created by TypedImperativeAggregate
   * functions with the actual data types used in the GPU runtime.
   *
   * Firstly, this method traverses aggregateFunctions, to search attributes referring to
   * aggregation buffers of TypedImperativeAggregate functions.
   * Then, we extract the desired (actual) data types on GPU runtime for these attributes,
   * and map them to expression IDs of attributes.
   * At last, we traverse aggregateAttributes and resultExpressions, overriding data type in
   * RapidsMeta if necessary, in order to ensure TypeChecks tagging exact data types in runtime.
   */
  private def overrideAggBufTypes(): Unit = {
    val desiredAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    val desiredInputAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    // Collects exprId from TypedImperativeAggBufferAttributes, and maps them to the data type
    // of `TypedImperativeAggExprMeta.aggBufferAttribute`.
    aggregateExpressions.map(_.childExprs.head).foreach {
      case aggMeta: TypedImperativeAggExprMeta[_] =>
        val aggFn = aggMeta.wrapped.asInstanceOf[TypedImperativeAggregate[_]]
        val desiredType = aggMeta.aggBufferAttribute.dataType

        desiredAggBufTypes(aggFn.aggBufferAttributes.head.exprId) = desiredType
        desiredInputAggBufTypes(aggFn.inputAggBufferAttributes.head.exprId) = desiredType

      case _ =>
    }

    // Overrides the data types of typed imperative aggregation buffers for type checking
    aggregateAttributes.foreach { attrMeta =>
      attrMeta.wrapped match {
        case ar: AttributeReference if desiredAggBufTypes.contains(ar.exprId) =>
          attrMeta.overrideDataType(desiredAggBufTypes(ar.exprId))
        case _ =>
      }
    }
    resultExpressions.foreach { retMeta =>
      retMeta.wrapped match {
        case ar: AttributeReference if desiredInputAggBufTypes.contains(ar.exprId) =>
          retMeta.overrideDataType(desiredInputAggBufTypes(ar.exprId))
        case _ =>
      }
    }
  }
}

object GpuTypedImperativeSupportedAggregateExecMeta {

  // (conversion expressions, stage mask specialized for Aggregate)
  type TypedAggBufConverter = (Seq[NamedExpression], Int)

  val preRowToColProjection: TreeNodeTag[TypedAggBufConverter] = {
    TreeNodeTag[TypedAggBufConverter]("rapids.gpu.preRowToColProcessing")
  }

  val postColToRowProjection: TreeNodeTag[TypedAggBufConverter] = {
    TreeNodeTag[TypedAggBufConverter]("rapids.gpu.postColToRowProcessing")
  }

  private val bufferConverterInjected = TreeNodeTag[Boolean]("rapids.gpu.bufferConverterInjected")

  /**
   * The method will bind buffer converters (CPU Expressions) to certain CPU Plans if necessary,
   * so the method returns nothing. The binding work will help us to insert these converters as
   * pre/post processing of GpuRowToColumnarExec/GpuColumnarToRowExec during the materialization
   * of these transition Plans.
   *
   * In the beginning, it collects all physical Aggregate Plans (stages) of current Aggregate.
   * Then, it goes through all stages in pair, to check whether buffer converters need to be
   * inserted between its child and itself.
   * If it is necessary, it goes on to check whether all these buffers are available to convert.
   * If not, it falls back all stages to CPU to keep consistency of intermediate data; Otherwise,
   * it creates buffer converters based on the createBufferConverter methods of
   * TypedImperativeAggExprMeta, and binds these newly-created converters to certain Plans.
   *
   * In terms of binding, it is critical and sophisticated to find out where to bind these
   * converters. Generally, we need to find out CPU plans right before/after the potential R2C/C2R
   * transitions, so as to integrate these converters during post columnar overriding. These plans
   * may be Aggregate stages themselves. They may also be plans inserted between Aggregate stages,
   * such as: ShuffleExchangeExec, SortExec.
   *
   * The binding carries out by storing buffer converters as tag values of certain CPU Plans. These
   * plans are either the child of GpuRowToColumnarExec or the parent of GpuColumnarToRowExec. The
   * converters are cached inside CPU Plans rather than GPU ones (like: the child of
   * GpuColumnarToRowExec), because we can't access the GPU Plans during binding since they are
   * yet created. And GPU Plans created by RapidsMeta don't keep the tags of their CPU
   * counterparts.
   */
  private def handleAggregationBuffer(
      meta: GpuTypedImperativeSupportedAggregateExecMeta[_]): Unit = {
    // We only run the check for final stages which contain TypedImperativeAggregate.
    val needToCheck = containTypedImperativeAggregate(meta, Some(Final))
    if (!needToCheck) return
    // Avoid duplicated check and fallback.
    val checked = meta.agg.getTagValue[Boolean](bufferConverterInjected).contains(true)
    if (checked) return
    meta.agg.setTagValue(bufferConverterInjected, true)

    // Fetch AggregateMetas of all stages which belong to current Aggregate
    val stages = GpuBaseAggregateMeta.getAggregateOfAllStages(meta, meta.agg.logicalLink.get)

    // Find out stages in which the buffer converters are essential.
    val needBufferConversion = stages.indices.map {
      case i if i == stages.length - 1 => false
      case i =>
        // Buffer converters are only needed to inject between two stages if [A] and [B].
        // [A]. there will be a R2C or C2R transition between them
        // [B]. there exists TypedImperativeAggregate functions in each of them
        (stages(i).canThisBeReplaced ^ stages(i + 1).canThisBeReplaced) &&
          containTypedImperativeAggregate(stages(i)) &&
          containTypedImperativeAggregate(stages(i + 1))
    }

    // Return if all internal aggregation buffers are compatible with GPU Overrides.
    if (needBufferConversion.forall(!_)) return

    // Fall back all GPU supported stages to CPU, if there exists TypedImperativeAggregate buffer
    // who doesn't support data format transition in runtime. Otherwise, build buffer converters
    // and bind them to certain SparkPlans.
    val entireFallback = !stages.zip(needBufferConversion).forall { case (stage, needConvert) =>
      !needConvert || stage.availableRuntimeDataTransition
    }
    if (entireFallback) {
      stages.foreach {
        case aggMeta if aggMeta.canThisBeReplaced =>
          aggMeta.willNotWorkOnGpu("Associated fallback for TypedImperativeAggregate")
        case _ =>
      }
    } else {
      bindBufferConverters(stages, needBufferConversion)
    }
  }

  /**
   * Bind converters as TreeNodeTags into the CPU plans who are right before/after the potential
   * R2C/C2R transitions (the transitions are yet inserted).
   *
   * These converters are CPU expressions, and they will be integrated into GpuRowToColumnarExec/
   * GpuColumnarToRowExec as pre/post transitions. What we do here, is to create these converters
   * according to the stage pairs.
   */
  private def bindBufferConverters(stages: Seq[GpuBaseAggregateMeta[_]],
      needBufferConversion: Seq[Boolean]): Unit = {

    needBufferConversion.zipWithIndex.foreach {
      case (needConvert, i) if needConvert =>
        // Find the next edge from the given stage which is a splitting point between CPU plans
        // and GPU plans. The method returns a pair of plans around the edge. For instance,
        //
        // stage(i): GpuObjectHashAggregateExec ->
        //    pair_head: GpuShuffleExec ->
        //        pair_tail and stage(i + 1): ObjectHashAggregateExec
        //
        // In example above, stage(i) is a GpuPlan while stage(i + 1) is not. And we assume
        // both of them including TypedImperativeAgg. So, in terms of stage(i) the buffer
        // conversion is necessary. Then, we call the method nextEdgeForConversion to find
        // the edge next to stage(i), which is between GpuShuffleExec and stage(i + 1).
        nextEdgeForConversion(stages(i)) match {
          // create preRowToColumnarTransition, and bind it to the child node (CPU plan) of
          // GpuRowToColumnarExec
          case List(parent, child) if parent.canThisBeReplaced =>
            val childPlan = child.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1),
              fromCpuToGpu = true)
            // Insert the converter into childPlan as a tag value
            insertBufferConverter(childPlan, expressions, fromCpuToGpu = true)
          // create postColumnarToRowTransition, and bind it to the parent node (CPU plan) of
          // GpuColumnarToRowExec
          case List(parent, _) =>
            val parentPlan = parent.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1),
              fromCpuToGpu = false)
            // Insert the converter into parentPlan as a tag value
            insertBufferConverter(parentPlan, expressions, fromCpuToGpu = false)
        }
      case _ =>
    }
  }

  /**
   * Insert buffer converters into the given plan as TreeNodeTags, so that these converters
   * can be retrieved later during GpuTransitionOverrides to fill the gap between CPU and GPU
   * aggregation buffers.
   * For non-AQE mode, we can directly bind the converters to the given plan.
   * For AQE mode, we have to bind the converters to the logical plan linked by the given plan,
   * along with a stage mask to distinguish different aggregate stages. The reason not binding
   * directly to the physical plan is that AQE may re-create physical plans during execution,
   * and the newly-created plans won't carry the tags stored in the original physical plans.
   */
  private def insertBufferConverter(plan: SparkPlan,
      expr: Seq[NamedExpression], fromCpuToGpu: Boolean): Unit = {
    val tag: TreeNodeTag[TypedAggBufConverter] = bufConverterTag(fromCpuToGpu)
    if (!plan.conf.adaptiveExecutionEnabled) {
      plan.setTagValue(tag, (expr, 0))
    } else {
      plan.logicalLink match {
        case Some(logicalPlan) =>
          logicalPlan.setTagValue(tag, (expr, aggregateStageMask(plan)))
        case None => // logicalLink is guaranteed to be present normally
          throw new IllegalStateException(
            s"Failed to store ${tag.name} due to missing LogicalPlanLink: $plan")
      }
    }
  }

  private def bufConverterTag(isR2C: Boolean): TreeNodeTag[TypedAggBufConverter] = {
    if (isR2C) {
      preRowToColProjection
    } else {
      postColToRowProjection
    }
  }

  /**
   * Calculate a bitmask representing the aggregate modes used in the given aggregate.
   * Returns 0 if the plan is not an aggregate. Otherwise, the bitmask uses the following
   * mapping to represent the different Aggregate modes in distinct bits:
   *   Partial      -> 1
   *   PartialMerge -> 2
   *   Final        -> 4
   *   Complete     -> 8
   */
  private def aggregateStageMask(plan: SparkPlan): Int = {
    plan match {
      case aggExec: BaseAggregateExec =>
        aggExec.aggregateExpressions.foldLeft(0) { (mask, expr) =>
          val modeBit = expr.mode match {
            case Partial => 1
            case PartialMerge => 2
            case Final => 4
            case Complete => 8
          }
          mask | modeBit
        }
      case _ => 0
    }
  }

  /**
   * Retrieve buffer converters for TypedImperativeAggregate which are previously bound to the
   * given query plan, if any. Return format conversion expressions which shall be plugged into
   * the plan tree as ProjectExec.
   *
   * NOTE: This method is a counterpart of insertBufferConverter.
   */
  def readBufferConverter(plan: SparkPlan, isR2C: Boolean): Option[Seq[NamedExpression]] = {
    val tag: TreeNodeTag[TypedAggBufConverter] = bufConverterTag(isR2C)
    // First try to read from the physical plan directly
    plan.getTagValue[TypedAggBufConverter](tag).map(_._1).orElse {
      // If missing, try to read from the logical plan link using stage mask to distinguish
      // different aggregate stages
      val cvt = plan.logicalLink.flatMap { logicalPlan =>
        logicalPlan.getTagValue[TypedAggBufConverter](tag)
      }
      cvt match {
        case Some((exp, stageMask)) if aggregateStageMask(plan) == stageMask => Some(exp)
        case _ => None
      }
    }
  }

  private def containTypedImperativeAggregate(meta: GpuBaseAggregateMeta[_],
      desiredMode: Option[AggregateMode] = None): Boolean =
    meta.agg.aggregateExpressions.exists {
      case e: AggregateExpression if desiredMode.forall(_ == e.mode) =>
        e.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]]
      case _ => false
    }

  private def createBufferConverter(mergeAggMeta: GpuBaseAggregateMeta[_],
      partialAggMeta: GpuBaseAggregateMeta[_],
      fromCpuToGpu: Boolean): Seq[NamedExpression] = {

    val converters = mergeAggMeta.childExprs.collect {
      case e if e.childExprs.length == 1 &&
          e.childExprs.head.isInstanceOf[TypedImperativeAggExprMeta[_]] =>
        val aggExpr = e.wrapped.asInstanceOf[AggregateExpression]
        aggExpr.mode match {
          case Final | PartialMerge =>
            val typImpAggMeta = e.childExprs.head.asInstanceOf[TypedImperativeAggExprMeta[_]]
            val converter = if (fromCpuToGpu) {
              Left(typImpAggMeta.createCpuToGpuBufferConverter())
            } else {
              Right(typImpAggMeta.createGpuToCpuBufferConverter())
            }
            val aggFn = aggExpr.aggregateFunction.asInstanceOf[TypedImperativeAggregate[_]]
            Some(aggFn.inputAggBufferAttributes.head.exprId -> converter)
          case _ => None
        }
      case _ => None
    }.flatten.toMap

    val expressions = partialAggMeta.resultExpressions.map {
      case retExpr =>
        retExpr.wrapped match {
          // Some typed imperative aggregates, like BloomFilterAggregate, keep BinaryType on both
          // CPU and GPU but still need a bridge conversion because the buffer semantics differ.
          case resultExpr: AttributeReference if converters.contains(resultExpr.exprId) =>
            val ref = if (fromCpuToGpu || !retExpr.typeMeta.typeConverted) {
              resultExpr
            } else {
              resultExpr.copy(dataType = retExpr.typeMeta.dataType.get)(
                resultExpr.exprId, resultExpr.qualifier)
            }
            val convertedExpr = converters(resultExpr.exprId).fold(
              _.createExpression(ref),
              _.createExpression(ref))
            Alias(convertedExpr, ref.name + "_converted")(NamedExpression.newExprId)
          case _ =>
            retExpr.wrapped.asInstanceOf[NamedExpression]
        }
    }

    expressions
  }

  @tailrec
  private def nextEdgeForConversion(meta: SparkPlanMeta[_]): Seq[SparkPlanMeta[_]] = {
    val child = meta.childPlans.head
    if (meta.canThisBeReplaced ^ child.canThisBeReplaced) {
      List(meta, child)
    } else {
      nextEdgeForConversion(child)
    }
  }
}

class GpuHashAggregateMeta(
    override val agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuBaseAggregateMeta(agg, agg.requiredChildDistributionExpressions,
    conf, parent, rule)

class GpuSortAggregateExecMeta(
    override val agg: SortAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
    agg.requiredChildDistributionExpressions, conf, parent, rule) {
  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // Make sure this is the last check - if this is SortAggregate, the children can be sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // HashAggregate.
    if (canThisBeReplaced) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu("one of the preceding SortExec's cannot be replaced")
          } else {
            // But if this includes a first or last aggregate and the sort includes more than what
            // the group by requires we cannot drop the sort. For example
            // if the group by is on a single key "a", but the ordering is on "a" and "b", then
            // we have to keep the sort, so that the rows are ordered to take "b" into account
            // before first/last work on it.
            val hasFirstOrLast = agg.aggregateExpressions.exists { agg =>
              agg.aggregateFunction match {
                case _: First | _: Last => true
                case _ => false
              }
            }
            val shouldRemoveSort = if (hasFirstOrLast) {
              val sortedOrder = plan.wrapped.asInstanceOf[SortExec].sortOrder
              val groupByRequiredOrdering = agg.requiredChildOrdering.head
              sortedOrder == groupByRequiredOrdering
            } else {
              true
            }

            if (shouldRemoveSort) {
              plan.shouldBeRemoved("replacing sort aggregate with hash aggregate")
            }
          }
        }
      }
    }
  }
}

class GpuObjectHashAggregateExecMeta(
    override val agg: ObjectHashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
    agg.requiredChildDistributionExpressions, conf, parent, rule)

object GpuHashAggregateExecBase {

  def calcInputAttributes(aggregateExpressions: Seq[GpuAggregateExpression],
      childOutput: Seq[Attribute],
      inputAggBufferAttributes: Seq[Attribute]): Seq[Attribute] = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(PartialMerge)) {
      // SPARK-31620: when planning aggregates, the partial aggregate uses aggregate function's
      // `inputAggBufferAttributes` as its output. And Final and PartialMerge aggregate rely on the
      // output to bind references used by `mergeAggregates`. But if we copy the
      // aggregate function somehow after aggregate planning, the `DeclarativeAggregate` will
      // be replaced by a new instance with new `inputAggBufferAttributes`. Then Final and
      // PartialMerge aggregate can't bind the references used by `mergeAggregates` with the output
      // of the partial aggregate, as they use the `inputAggBufferAttributes` of the
      // original `DeclarativeAggregate` before copy. Instead, we shall use
      // `inputAggBufferAttributes` after copy to match the new `mergeExpressions`.
      val aggAttrs = inputAggBufferAttributes
      childOutput.dropRight(aggAttrs.length) ++ aggAttrs
    } else {
      childOutput
    }
  }

  def calcInputAggBufferAttributes(aggregateExpressions: Seq[GpuAggregateExpression]):
  Seq[Attribute] = {
    aggregateExpressions
      // there are exactly four cases needs `inputAggBufferAttributes` from child according to the
      // agg planning in `AggUtils`: Partial -> Final, PartialMerge -> Final,
      // Partial -> PartialMerge, PartialMerge -> PartialMerge.
      .filter(a => a.mode == Final || a.mode == PartialMerge)
      .flatMap(_.aggregateFunction.aggBufferAttributes)
  }
}

/**
 * The GPU version of AggregateExec that is intended for partial aggregations that are not
 * reductions and so it sorts the input data ahead of time to do it in a single pass.
 *
 * @param requiredChildDistributionExpressions this is unchanged by the GPU. It is used in
 *                                             EnsureRequirements to be able to add shuffle nodes
 * @param groupingExpressions The expressions that, when applied to the input batch, return the
 *                            grouping key
 * @param aggregateExpressions The GpuAggregateExpression instances for this node
 * @param aggregateAttributes References to each GpuAggregateExpression (attribute references)
 * @param resultExpressions the expected output expression of this hash aggregate (which this
 *                          node should project)
 * @param child incoming plan (where we get input columns from)
 * @param configuredTargetBatchSize user-configured maximum device memory size of a batch
 * @param allowNonFullyAggregatedOutput whether we can skip the third pass of aggregation
 *                                      (can omit non fully aggregated data for non-final
 *                                      stage of aggregation)
 * @param skipAggPassReductionRatio skip if the ratio of rows after a pass is bigger than this value
 */
case class GpuHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan,
    configuredTargetBatchSize: Long,
    estimatedPreProcessGrowth: Double,
    forceSinglePassAgg: Boolean,
    allowSinglePassAgg: Boolean,
    allowNonFullyAggregatedOutput: Boolean,
    skipAggPassReductionRatio: Double
) extends GpuPartitioningPreservingUnaryExecNode with GpuExec with Logging {

  // lifted directly from `BaseAggregateExec.inputAttributes`, edited comment.
  def inputAttributes: Seq[Attribute] =
    GpuHashAggregateExecBase.calcInputAttributes(aggregateExpressions,
      child.output,
      inputAggBufferAttributes)

  private val inputAggBufferAttributes: Seq[Attribute] =
    GpuHashAggregateExecBase.calcInputAggBufferAttributes(aggregateExpressions)

  protected lazy val uniqueModes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_TASKS_REPARTITIONED -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_TASKS_REPARTITIONED),
    NUM_TASKS_SKIPPED_AGG -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_TASKS_SKIPPED_AGG),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    AGG_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_AGG_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    SORT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SORT_TIME),
    REPARTITION_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_REPARTITION_TIME),
    "NUM_AGGS" -> createMetric(DEBUG_LEVEL, "num agg operations"),
    "NUM_PRE_SPLITS" -> createMetric(DEBUG_LEVEL, "num pre splits"),
    "NUM_TASKS_SINGLE_PASS" -> createMetric(MODERATE_LEVEL, "number of single pass tasks"),
    "HEURISTIC_TIME" -> createNanoTimingMetric(DEBUG_LEVEL, "time in heuristic"),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME)
  )

  // requiredChildDistributions are CPU expressions, so remove it from the GPU expressions list
  override def gpuExpressions: Seq[Expression] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
       |Lore: ${loreArgs.mkString(", ")}
       |""".stripMargin
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val aggMetrics = GpuHashAggregateMetrics(
      numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS),
      numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES),
      numTasksRepartitioned = gpuLongMetric(NUM_TASKS_REPARTITIONED),
      numTasksSkippedAgg = gpuLongMetric(NUM_TASKS_SKIPPED_AGG),
      opTime = gpuLongMetric(OP_TIME_LEGACY),
      computeAggTime = gpuLongMetric(AGG_TIME),
      concatTime = gpuLongMetric(CONCAT_TIME),
      sortTime = gpuLongMetric(SORT_TIME),
      repartitionTime = gpuLongMetric(REPARTITION_TIME),
      numAggOps = gpuLongMetric("NUM_AGGS"),
      numPreSplits = gpuLongMetric("NUM_PRE_SPLITS"),
      singlePassTasks = gpuLongMetric("NUM_TASKS_SINGLE_PASS"),
      heuristicTime = gpuLongMetric("HEURISTIC_TIME"))

    // cache in a local variable to avoid serializing the full child plan
    val inputAttrs = inputAttributes
    val groupingExprs = groupingExpressions
    val aggregateExprs = aggregateExpressions
    val aggregateAttrs = aggregateAttributes
    val resultExprs = resultExpressions
    val modeInfo = AggregateModeInfo(uniqueModes)
    val targetBatchSize = configuredTargetBatchSize

    val rdd = child.executeColumnar()

    val localForcePre = forceSinglePassAgg
    val localAllowPre = allowSinglePassAgg
    val expectedOrdering = expectedChildOrderingIfNeeded
    val alreadySorted = SortOrder.orderingSatisfies(child.outputOrdering,
      expectedOrdering) && expectedOrdering.nonEmpty
    val localEstimatedPreProcessGrowth = estimatedPreProcessGrowth

    val boundGroupExprs = GpuBindReferences.bindGpuReferencesTiered(groupingExprs,
      inputAttrs, conf, allMetrics)

    rdd.mapPartitions { cbIter =>
      val postBoundReferences = GpuAggFinalPassIterator.setupReferences(groupingExprs,
        aggregateExprs, aggregateAttrs, resultExprs, modeInfo, allMetrics)

      new DynamicGpuPartialAggregateIterator(cbIter, inputAttrs, groupingExprs,
        boundGroupExprs, aggregateExprs, aggregateAttrs, resultExprs, modeInfo,
        localEstimatedPreProcessGrowth, alreadySorted, expectedOrdering,
        postBoundReferences, targetBatchSize, aggMetrics, conf,
        localForcePre, localAllowPre, allowNonFullyAggregatedOutput, skipAggPassReductionRatio,
        allMetrics
      )
    }
  }

  protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  //
  // This section is derived (copied in most cases) from HashAggregateExec
  //
  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  /**
   * Override buildAttributeMap to handle aggregate output semantics.
   */
  override protected def buildAttributeMap(): Map[Attribute, Attribute] = {
    resultExpressions.collect {
      case a @ GpuAlias(child: Attribute, _) => child -> a.toAttribute
      case a @ Alias(child: Attribute, _) => child -> a.toAttribute
    }.toMap
  }

  protected def hasAlias: Boolean = outputExpressions.collectFirst { case _: Alias => }.isDefined

  protected def replaceAliases(exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map {
      case a: AttributeReference => replaceAlias(a).getOrElse(a)
      case other => other
    }
  }

  protected def replaceAlias(attr: AttributeReference): Option[Attribute] = {
    outputExpressions.collectFirst {
      case a@Alias(child: AttributeReference, _) if child.semanticEquals(attr) =>
        a.toAttribute
    }
  }

  // Used in de-duping and optimizer rules
  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  // AllTuples = distribution with a single partition and all tuples of the dataset are co-located.
  // Clustered = dataset with tuples co-located in the same partition if they share a specific value
  // Unspecified = distribution with no promises about co-location
  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def doExecute(): RDD[InternalRow] = throw new IllegalStateException(
    "Row-based execution should not occur for this class")

  /**
   * All the attributes that are used for this plan. NOT used for aggregation
   */
  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString =
      truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString =
      truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"$nodeName (keys=$keyString, functions=$functionString, output=$outputString) " +
        s"""${loreArgs.mkString(", ")}"""
    } else {
      s"$nodeName (keys=$keyString, functions=$functionString)," +
        s" filters=${aggregateExpressions.map(_.filter)})" +
        s""" ${loreArgs.mkString(", ")}"""
    }
  }
  //
  // End copies from HashAggregateExec
  //

  // We are not going to override requiredChildOrdering because we don't want to
  // always sort the data. So we will insert in the sort ourselves if we need to
  def expectedChildOrderingIfNeeded: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }
}

class DynamicGpuPartialAggregateIterator(
    cbIter: Iterator[ColumnarBatch],
    inputAttrs: Seq[Attribute],
    groupingExprs: Seq[NamedExpression],
    boundGroupExprs: GpuTieredProject,
    aggregateExprs: Seq[GpuAggregateExpression],
    aggregateAttrs: Seq[Attribute],
    resultExprs: Seq[NamedExpression],
    modeInfo: AggregateModeInfo,
    estimatedPreGrowth: Double,
    alreadySorted: Boolean,
    ordering: Seq[SortOrder],
    postBoundReferences: BoundExpressionsModeAggregates,
    configuredTargetBatchSize: Long,
    metrics: GpuHashAggregateMetrics,
    conf: SQLConf,
    forceSinglePassAgg: Boolean,
    allowSinglePassAgg: Boolean,
    allowNonFullyAggregatedOutput: Boolean,
    skipAggPassReductionRatio: Double,
    allMetrics: Map[String, GpuMetric]
) extends Iterator[ColumnarBatch] {
  private var aggIter: Option[Iterator[ColumnarBatch]] = None
  private[this] val isReductionOnly = boundGroupExprs.outputExprs.isEmpty

  // When doing a reduction we don't have the aggIter setup for the very first time
  // so we have to match what happens for the normal reduction operations.
  override def hasNext: Boolean = aggIter.map(_.hasNext)
    .getOrElse(isReductionOnly || cbIter.hasNext)

  private[this] def estimateCardinality(cb: ColumnarBatch): Int = {
    withResource(boundGroupExprs.project(cb)) { groupingKeys =>
      withResource(GpuColumnVector.from(groupingKeys)) { table =>
        table.distinctCount()
      }
    }
  }

  private[this] def firstBatchHeuristic(
      cbIter: Iterator[ColumnarBatch],
      helper: AggHelper): (Iterator[ColumnarBatch], Boolean) = {
    // we need to decide if we are going to sort the data or not, so the very
    // first thing we need to do is get a batch and make a choice.
    withRetryNoSplit(SpillableColumnarBatch(cbIter.next(),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY)) { sb =>
      NvtxIdWithMetrics(NvtxRegistry.DYNAMIC_SORT_HEURISTIC,
          metrics.opTime, metrics.heuristicTime) {
        withResource(sb.getColumnarBatch()) { cb =>
          val numRows = cb.numRows()
          val cardinality = estimateCardinality(cb)
          val minPreGrowth = PreProjectSplitIterator.calcMinOutputSize(cb,
            helper.preStepBound).toDouble / GpuColumnVector.getTotalDeviceMemoryUsed(cb)
          val estimatedGrowthAfterAgg =
            (math.max(minPreGrowth, estimatedPreGrowth) * cardinality) / numRows
          val wrappedIter = Seq(GpuColumnVector.incRefCounts(cb)).toIterator ++ cbIter
          (wrappedIter, estimatedGrowthAfterAgg > 1.0)
        }
      }
    }
  }

  private[this] def singlePassSortedAgg(
      inputIter: Iterator[ColumnarBatch],
      preProcessAggHelper: AggHelper): Iterator[ColumnarBatch] = {
    // The data is already sorted so just do the sorted agg either way...
    val sortedIter = if (alreadySorted) {
      inputIter
    } else {
      val sorter = new GpuSorter(ordering, inputAttrs, allMetrics)
      GpuOutOfCoreSortIterator(inputIter,
        sorter,
        configuredTargetBatchSize,
        opTime = metrics.opTime,
        sortTime = metrics.sortTime,
        outputBatches = NoopMetric,
        outputRows = NoopMetric)
    }

    // After sorting we want to split the input for the project so that
    // we don't get ourselves in trouble.
    val sortedSplitIter = new PreProjectSplitIterator(sortedIter,
      preProcessAggHelper.preStepBound, metrics.opTime, metrics.numPreSplits)

    val firstPassIter = GpuAggFirstPassIterator(sortedSplitIter, preProcessAggHelper,
      metrics)

    // Technically on a partial-agg, which this only works for, this last iterator should
    // be a noop except for some metrics. But for consistency between all of the
    // agg paths and to be more resilient in the future with code changes we include a final pass
    // iterator here.
    GpuAggFinalPassIterator.makeIterFromSpillable(firstPassIter, postBoundReferences, metrics)
  }

  private[this] def fullHashAggWithMerge(
      inputIter: Iterator[ColumnarBatch],
      preProcessAggHelper: AggHelper,
      allMetrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    // We still want to split the input, because the heuristic may not be perfect and
    //  this is relatively light weight
    val splitInputIter = new PreProjectSplitIterator(inputIter,
      preProcessAggHelper.preStepBound, metrics.opTime, metrics.numPreSplits)

    val localInputRowsMetrics = new LocalGpuMetric

    val firstPassIter = GpuAggFirstPassIterator(
      splitInputIter.map(cb => {
        localInputRowsMetrics += cb.numRows()
        cb
      }),
      preProcessAggHelper,
      metrics)

    val mergeIter = new GpuMergeAggregateIterator(
      new CloseableBufferedIterator(firstPassIter),
      inputAttrs,
      groupingExprs,
      aggregateExprs,
      aggregateAttrs,
      resultExprs,
      modeInfo,
      metrics,
      configuredTargetBatchSize,
      conf,
      allowNonFullyAggregatedOutput,
      skipAggPassReductionRatio,
      localInputRowsMetrics,
      allMetrics
    )

    GpuAggFinalPassIterator.makeIter(mergeIter, postBoundReferences, metrics)
  }

  override def next(): ColumnarBatch = {
    if (aggIter.isEmpty) {
      val preProcessAggHelper = new AggHelper(
        inputAttrs, groupingExprs, aggregateExprs,
        forceMerge = false, isSorted = true, conf = conf, metrics = allMetrics)
      val (inputIter, doSinglePassAgg) = if (allowSinglePassAgg) {
        if (forceSinglePassAgg || alreadySorted) {
          (cbIter, true)
        } else {
          firstBatchHeuristic(cbIter, preProcessAggHelper)
        }
      } else {
        (cbIter, false)
      }
      val newIter = if (doSinglePassAgg) {
        metrics.singlePassTasks += 1
        singlePassSortedAgg(inputIter, preProcessAggHelper)
      } else {
        // Not sorting so go back to that
        preProcessAggHelper.setSort(false)
        fullHashAggWithMerge(inputIter, preProcessAggHelper, allMetrics)
      }
      aggIter = Some(newIter)
    }
    aggIter.map(_.next()).getOrElse {
      throw new NoSuchElementException()
    }
  }
}
