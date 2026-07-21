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
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf._
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.DataTypeUtils.hasOffset
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.PreProjectSplitIterator.{KEY_NUM_PRE_SPLIT, PreSplitOutSizeEstimator}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRestoreOnRetry, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM
import com.nvidia.spark.rapids.shims._

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SampleExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuCreateArray, GpuCreateMap, GpuCreateNamedStruct, GpuPartitionwiseSampledRDD, GpuPoissonSampler}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.random.BernoulliCellSampler

class GpuProjectExecMeta(
    proj: ProjectExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[ProjectExec](proj, conf, p, r)
    with Logging {
  override def convertToGpu(): GpuExec = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    val gpuExprs = childExprs.map(_.convertToGpu().asInstanceOf[NamedExpression]).toList
    val gpuChild = childPlans.head.convertIfNeeded()
    if (conf.isProjectAstEnabled) {
      // cuDF requires return column is fixed width
      val allReturnTypesFixedWidth = gpuExprs.forall(e => GpuBatchUtils.isFixedWidth(e.dataType))
      if (allReturnTypesFixedWidth && childExprs.forall(_.canThisBeAst)) {
        return GpuProjectAstExec(gpuExprs, gpuChild)
      }
      // explain AST because this is optional and it is sometimes hard to debug
      if (conf.shouldExplain) {
        val explain = childExprs.map(_.explainAst(conf.shouldExplainAll))
            .filter(_.nonEmpty)
        if (explain.nonEmpty) {
          logWarning(s"AST PROJECT\n$explain")
        }
        if (!allReturnTypesFixedWidth) {
          logWarning(s"AST PROJECT\n  have non fixed return column, " +
            s"return types: ${gpuExprs.map(_.dataType)}")
        }
      }
    }
    GpuProjectExec(gpuExprs, gpuChild)
  }
}

object GpuProjectExec {
  def projectAndClose[A <: Expression](cb: ColumnarBatch, boundExprs: Seq[A],
      opTime: GpuMetric): ColumnarBatch = {
    NvtxIdWithMetrics(NvtxRegistry.PROJECT_EXEC, opTime) {
      try {
        project(cb, boundExprs)
      } finally {
        cb.close()
      }
    }
  }

  @tailrec
  private def extractSingleBoundIndex(expr: Expression): Option[Int] = expr match {
    case ga: GpuAlias => extractSingleBoundIndex(ga.child)
    case br: GpuBoundReference => Some(br.ordinal)
    case _ => None
  }

  def extractSingleBoundIndex(boundExprs: Seq[Expression]): Seq[Option[Int]] =
    boundExprs.map(extractSingleBoundIndex)

  def isNoopProject(cb: ColumnarBatch, boundExprs: Seq[Expression]): Boolean = {
    if (boundExprs.length == cb.numCols()) {
      extractSingleBoundIndex(boundExprs).zip(0 until cb.numCols()).forall {
        case (Some(foundIndex), expectedIndex) => foundIndex == expectedIndex
        case _ => false
      }
    } else {
      false
    }
  }

  def project(cb: ColumnarBatch, boundExprs: Seq[Expression]): ColumnarBatch = {
    if (isNoopProject(cb, boundExprs)) {
      // This can help avoid contiguous splits in some cases when the input data is also contiguous
      GpuColumnVector.incRefCounts(cb)
    } else {
      try {
        // In some cases like Expand, we have a lot Expressions generating null vectors.
        // We can cache the null vectors to avoid creating them every time.
        // Since we're attempting to reuse the whole null vector, it is important to aware that
        // datatype and vector length should be the same.
        // Within project(cb: ColumnarBatch, boundExprs: Seq[Expression]), all output vectors share
        // the same vector length, which facilitates the reuse of null vectors.
        // When leaving the scope of project(cb: ColumnarBatch, boundExprs: Seq[Expression]),
        // the cached null vectors will be cleared because the next ColumnBatch may have
        // different vector length, thus not able to reuse cached vectors.
        GpuExpressionsUtils.cachedNullVectors.get.clear()

        GpuArrayHofFusion.project(cb, boundExprs).getOrElse {
          val newColumns = boundExprs.safeMap(_.columnarEval(cb)).toArray[ColumnVector]
          new ColumnarBatch(newColumns, cb.numRows())
        }
      } finally {
        GpuExpressionsUtils.cachedNullVectors.get.clear()
      }
    }
  }

  /**
   * Similar to project, but it will try and retry the operations if it can. It also will close
   * the input SpillableColumnarBatch.
   * @param sb the input batch
   * @param boundExprs the expressions to run
   * @return the resulting batch
   */
  def projectAndCloseWithRetrySingleBatch(sb: SpillableColumnarBatch,
      boundExprs: Seq[Expression]): ColumnarBatch = {
    withResource(sb) { _ =>
      projectWithRetrySingleBatch(sb, boundExprs)
    }
  }

  /**
   * Similar to project, but it will try and retry the operations if it can.  The caller is
   * responsible for closing the input batch.
   * @param sb the input batch
   * @param boundExprs the expressions to run
   * @return the resulting batch
   */
  def projectWithRetrySingleBatch(sb: SpillableColumnarBatch,
      boundExprs: Seq[Expression]): ColumnarBatch = {
    // For retryable expressions, use split-retry: on GPU OOM, halve the input
    // batch by rows and re-run on each half. This recovers from cuDF-internal
    // scratch allocations that the pre-split estimator cannot see (e.g. regex
    // / string-replace working memory).
    //
    // Mixed retryable + non-retryable projections fall through to the
    // withRetryNoSplit path: non-retryable expressions are computed once on the
    // full input batch and stitched row-by-row to the retryable results.
    if (RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.get(SQLConf.get) &&
        boundExprs.forall(_.asInstanceOf[GpuExpression].retryable)) {
      val retryables = GpuExpressionsUtils.collectRetryables(boundExprs)
      // runWithSplitRetry takes ownership of the SpillableColumnarBatch; bump
      // the ref count so the caller (which is responsible for closing `sb`,
      // per this method's contract) doesn't double-close it.
      return runWithSplitRetry(sb.incRefCount(), retryables, project(_, boundExprs))
    }

    // First off we want to find/run all of the expressions that are not retryable,
    // These cannot be retried.
    val (retryableExprs, notRetryableExprs) = boundExprs.partition(
      _.asInstanceOf[GpuExpression].retryable)
    val retryables = GpuExpressionsUtils.collectRetryables(retryableExprs)

    val snd = if (notRetryableExprs.nonEmpty) {
      withResource(sb.getColumnarBatch()) { cb =>
        Some(SpillableColumnarBatch(project(cb, notRetryableExprs),
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      }
    } else {
      None
    }

    withResource(snd) { snd =>
      RmmRapidsRetryIterator.withRetryNoSplit {
        retryables.foreach(_.checkpoint())
        val retryableResults = withResource(sb.getColumnarBatch()) { cb =>
          withRestoreOnRetry(retryables) {
            // For now we are just going to run all of these and deal with losing work...
            project(cb, retryableExprs)
          }
        }
        if (snd.isEmpty) {
          // We are done and the order should be the same so we don't need to do anything...
          retryableResults
        } else {
          // There was a mix of retryable and non-retryable expressions.
          withResource(retryableResults) { _ =>
            withResource(snd.get.getColumnarBatch()) { nd =>
              var ndAt = 0
              var retryableAt = 0
              val outputColumns = ArrayBuffer[ColumnVector]()
              boundExprs.foreach { expr =>
                if (expr.asInstanceOf[GpuExpression].retryable) {
                  outputColumns += retryableResults.column(retryableAt)
                  retryableAt += 1
                } else {
                  outputColumns += nd.column(ndAt)
                  ndAt += 1
                }
              }
              GpuColumnVector.incRefCounts(new ColumnarBatch(outputColumns.toArray, sb.numRows()))
            }
          }
        }
      }
    }
  }

  /**
   * Run a retryable projection with row-split retry and return the projected
   * pieces directly. A no-split run yields one batch.
   *
   * Caller must ensure `runProject` is retryable; non-deterministic expressions
   * must restore their state for each split attempt. `runProject` must not
   * close its input.
   *
   * Takes ownership of `sb`. If the iterator is abandoned before the first
   * next(), task completion closes `sb` via the retry framework.
   */
  private[rapids] def runStreamingWithSplitRetry(
      sb: SpillableColumnarBatch,
      retryables: Seq[Retryable],
      runProject: ColumnarBatch => ColumnarBatch): Iterator[ColumnarBatch] = {
    withRetry(sb, splitSpillableInHalfByRows) { spillable =>
      retryables.foreach(_.checkpoint())
      withResource(spillable.getColumnarBatch()) { cb =>
        withRestoreOnRetry(retryables) {
          runProject(cb)
        }
      }
    }
  }

  /** Drains the streaming iterator and concatenates pieces for single-batch callers. */
  private[rapids] def runWithSplitRetry(
      sb: SpillableColumnarBatch,
      retryables: Seq[Retryable],
      runProject: ColumnarBatch => ColumnarBatch): ColumnarBatch = {
    val resultIter = runStreamingWithSplitRetry(sb, retryables, runProject)
    val pieces = ArrayBuffer[ColumnarBatch]()
    closeOnExcept(pieces) { _ =>
      while (resultIter.hasNext) {
        pieces += resultIter.next()
      }
    }
    val spillablePieces = ArrayBuffer[SpillableColumnarBatch]()
    closeOnExcept(pieces) { _ =>
      closeOnExcept(spillablePieces) { _ =>
        while (pieces.nonEmpty) {
          val piece = pieces.remove(0)
          val spillablePiece = closeOnExcept(piece) { _ =>
            SpillableColumnarBatch(piece, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          }
          closeOnExcept(spillablePiece) { _ =>
            spillablePieces += spillablePiece
          }
        }
      }
    }
    withRetryNoSplit(spillablePieces.toSeq) { attempt =>
      val batches = attempt.safeMap(_.getColumnarBatch()).toArray
      val outputTypes = closeOnExcept(batches) { _ =>
        GpuColumnVector.extractTypes(batches.head)
      }
      // Hand batches over to buildNonEmptyBatchFromTypes; it closes them in
      // its own finally block, so do not wrap the concat call in closeOnExcept.
      ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(batches, outputTypes)
    }
  }
}

/**
 * Mirrors Spark's PartitioningPreservingUnaryExecNode trait behavior, which is used by
 * operators like Project, Filter, and HashAggregate that don't change partitioning.
 *
 * Subclasses can override buildAttributeMap() to customize how input attributes
 * are mapped to output attributes.
 */
trait GpuPartitioningPreservingUnaryExecNode extends ShimUnaryExecNode {

  /**
   * Builds a mapping from child output attributes to this operator's output attributes.
   */
  protected def buildAttributeMap(): Map[Attribute, Attribute] = {
    child.output.zip(output).toMap
  }

  /**
   * Compute output partitioning, handling PartitioningCollection from joins.
   *
   * This matches Spark's PartitioningPreservingUnaryExecNode behavior:
   * 1. Flatten any PartitioningCollection into individual partitionings
   * 2. Filter to only keep partitionings whose attributes are in the output
   * 3. Remap attributes through aliases
   *
   * This is critical for Spark 4.1+ where UnionExec uses outputPartitioning
   * to decide between partitioner-aware union vs concatenation.
   */
  final override def outputPartitioning: Partitioning = {
    val attributeMap = buildAttributeMap()
    val outputSet = AttributeSet(output)

    // Flatten a PartitioningCollection into individual partitionings
    def flattenPartitioning(p: Partitioning): Seq[Partitioning] = p match {
      case PartitioningCollection(childPartitionings) =>
        childPartitionings.flatMap(flattenPartitioning)
      case other => Seq(other)
    }

    def remapPartitioning(p: Partitioning): Partitioning = p match {
      case e: Expression =>
        e.transform {
          case a: Attribute if attributeMap.contains(a) => attributeMap(a)
        }.asInstanceOf[Partitioning]
      case other => other
    }

    val partitionings = flattenPartitioning(child.outputPartitioning).flatMap {
      case e: Expression =>
        // Only keep partitionings whose attributes are all in the output
        val remapped = remapPartitioning(e.asInstanceOf[Partitioning])
        remapped match {
          case re: Expression if re.references.subsetOf(outputSet) => Some(remapped)
          case _ => None
        }
      case other => Some(other)
    }

    partitionings match {
      case Seq() => UnknownPartitioning(child.outputPartitioning.numPartitions)
      case Seq(single) => single
      case multiple => PartitioningCollection(multiple)
    }
  }
}

object GpuProjectExecLike {
  def unapply(plan: SparkPlan): Option[(Seq[Expression], SparkPlan)] = plan match {
    case gpuProjectLike: GpuProjectExecLike =>
      Some((gpuProjectLike.projectList, gpuProjectLike.child))
    case _ => None
  }
}

trait GpuProjectExecLike extends GpuPartitioningPreservingUnaryExecNode with GpuExec {

  def projectList: Seq[Expression]

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY))

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)
}

/**
 * An iterator that is intended to split the input to or output of a project on rows.
 * In practice this is only used for splitting the input prior to a project in some
 * very special cases. If the projected size of the output is so large that it would
 * risk us not being able to split it later on if we ran into trouble.
 * @param iter the input iterator of columnar batches.
 * @param opTime metric for how long this took
 * @param numSplitsMetric metric for the number of splits that happened.
 */
abstract class AbstractProjectSplitIterator(iter: Iterator[ColumnarBatch],
    opTime: GpuMetric,
    numSplitsMetric: GpuMetric) extends Iterator[ColumnarBatch] {
  private[this] val pending = new scala.collection.mutable.Queue[SpillableColumnarBatch]()

  override def hasNext: Boolean = pending.nonEmpty || iter.hasNext

  protected def calcNumSplits(cb: ColumnarBatch): Int

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    } else if (pending.nonEmpty) {
      opTime.ns {
        withRetryNoSplit(pending.dequeue()) { sb =>
          sb.getColumnarBatch()
        }
      }
    } else {
      val cb = iter.next()
      opTime.ns {
        val numSplits = closeOnExcept(cb) { cb =>
          calcNumSplits(cb)
        }
        if (numSplits <= 1) {
          cb
        } else {
          // this should never happen but it is here just in case
          require(cb.numCols() > 0,
            "About to perform cuDF table operations with a rows-only batch.")
          numSplitsMetric += numSplits - 1
          val sb = SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          val schema = sb.dataTypes
          val tables = withRetryNoSplit(sb) { sb =>
            withResource(sb.getColumnarBatch()) { cb =>
              withResource(GpuColumnVector.from(cb)) { table =>
                val rows = table.getRowCount.toInt
                val rowsPerSplit = math.ceil(rows.toDouble / numSplits).toInt
                val splitIndexes = rowsPerSplit until rows by rowsPerSplit
                table.contiguousSplit(splitIndexes: _*)
              }
            }
          }
          withResource(tables) { tables =>
            (1 until tables.length).foreach { ix =>
              val tbl = tables(ix)
              tables(ix) = null // everything but the head table will be nulled, queued
                                // as spillable batches in `pending`
              pending.enqueue(
                SpillableColumnarBatch(tbl, schema, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
            }
            GpuColumnVector.from(tables.head.getTable, schema)
          }
        }
      }
    }
  }
}

object PreProjectSplitIterator {

  val KEY_NUM_PRE_SPLIT = "NUM_PRE_SPLITS"

  def getSplitUntilSize: Long = GpuDeviceManager.getSplitUntilSize

  def calcMinOutputSize(cb: ColumnarBatch, boundExprs: GpuTieredProject): Long = {
    new PreSplitOutSizeEstimator(cb, boundExprs).calcMinOutputSize()
  }

  class PreSplitOutSizeEstimator(cb: ColumnarBatch, tieredProject: GpuTieredProject) {
    assert(cb != null, "The input batch should not be null.")

    private var maxOffsetColumnSize = 0L
    private var outputSize: Option[Long] = None

    def calcMinOutputSize(): Long = {
      if (outputSize.isEmpty) {
        val rows = cb.numRows()
        outputSize = Some(tieredProject.outputExprs.map(oe =>
          estimateExprMinSize(oe, rows, oe.nullable, Some(1))
        ).sum)
      }
      outputSize.get
    }

    // Return the max size of columns that have an offset buffer.
    // e.g. array or string columns.
    def getMaxOffsetColumnSize: Long = {
      calcMinOutputSize() // make sure the calculation is done
      maxOffsetColumnSize
    }

    private def updateOffsetColumnSize(newSize: Long): Unit = {
      if (newSize > maxOffsetColumnSize) {
        maxOffsetColumnSize = newSize
      }
    }

    /**
     * Estimate the size for a given expression and number of rows after a project.
     *
     * For some expressions of complex type, each is a union of the child columns, and its
     * size will be the sum of its children sizes. So need to dig into its children. Now
     * it covers only GpuCreateNamedStruct, GpuCreateNamedStruct and GpuCreateMap.
     */
    private def estimateExprMinSize(expr: Expression, rowsNum: Int, nullable: Boolean,
        exprAmount: Option[Int]): Long = {
      expr match {
        case GpuAlias(child, _) => // Alias should be ignored
          estimateExprMinSize(child, rowsNum, child.nullable, exprAmount)
        case gcs: GpuCreateNamedStruct =>
          // nullable is always false here, so no meta size is needed.
          gcs.valExprs.map(e => estimateExprMinSize(e, rowsNum, e.nullable, exprAmount)).sum
        case gcm: GpuCreateMap =>
          // Map is list of struct(key, value) in cuDF, so first a offset for the top list.
          // While CreateMap is not nullable, so no validity size.
          //    total_size = meta_size + sum_of_children_sizes
          val metaSize = computeMetaSize(false, true, rowsNum, exprAmount)
          metaSize + Seq(gcm.keys, gcm.values).map { keysOrValues =>
            val entryNullable = keysOrValues.exists(_.nullable)
            var includeMeta = true
            keysOrValues.map { kOrV =>
              // Need to go through the children to accumulate the data size, but only
              // let the first child to include the offset and/or validity size. Because
              // all the key (or values) columns will be concatenated into one column.
              val childAmount = if (includeMeta) Some(keysOrValues.length) else None
              includeMeta = false
              estimateExprMinSize(kOrV, rowsNum, entryNullable, childAmount)
            }.sum
          }.sum
        case gca: GpuCreateArray =>
          // Similar as GpuCreateMap, but with only one children set, while Map has two,
          // keys and values.
          val metaSize = computeMetaSize(false, true, rowsNum, exprAmount)
          val elemNullable = gca.children.exists(_.nullable)
          var includeMeta = true
          metaSize + gca.children.map { elem =>
            val childAmount = if (includeMeta) Some(gca.children.length) else None
            includeMeta = false
            estimateExprMinSize(elem, rowsNum, elemNullable, childAmount)
          }.sum
        case glit: GpuLiteral =>
          val ret = calcSizeForLiteral(glit.value, glit.dataType, rowsNum, nullable, exprAmount)
          glit.dataType match {
            case _: ArrayType | StringType | _: BinaryType => updateOffsetColumnSize(ret)
            case _ => // noop
          }
          ret
        case otherExpr => // other cases
          val exprType = otherExpr.dataType
          // Get the actual size if it is just a pass-through column
          tieredProject.getPassThroughIndex(otherExpr).map { colId =>
            getColumnSize(cb.column(colId).asInstanceOf[GpuColumnVector].getBase,
              exprType, nullable, exprAmount)
          }.getOrElse {
            // minGpuMemory is not suitable for the meta size calculation here, so do it
            // separately.
            computeMetaSize(nullable, hasOffset(exprType), rowsNum, exprAmount) +
              GpuBatchUtils.minGpuMemory(exprType, false, rowsNum, false)
          }
      }
    }
  }

  private def computeMetaSize(hasNull: Boolean, hasOffset: Boolean, rowsNum: Int,
      amountOpt: Option[Int]): Long = {
    // Compute the meta size only when amountOpt is defined. This is designed for
    // the case when calculating the size of a child of a complex type expression.
    // e.g. GpuCreateArray. There may be multiple children in it, but meta size can
    // be added only once for all the children. The parent expression will control
    // which child will take this job.
    amountOpt.map { amount =>
      computeMetaSize(hasNull, hasOffset, amount * rowsNum)
    }.getOrElse(0L)
  }

  private def computeMetaSize(hasNull: Boolean, hasOffset: Boolean, rowsNum: Int): Long = {
    var metaSize = 0L
    if (hasNull) {
      metaSize += GpuBatchUtils.calculateValidityBufferSize(rowsNum)
    }
    if (hasOffset) {
      metaSize += GpuBatchUtils.calculateOffsetBufferSize(rowsNum)
    }
    metaSize
  }

  private def getColumnSize(col: ColumnView, colType: DataType, nullable: Boolean,
      colAmount: Option[Int]): Long = {
    var colSize = 0L
    // data size
    if (col.getData != null) {
      colSize += col.getData.getLength
    } else if (colType.isInstanceOf[BinaryType]) {
      // Binary is list of byte in cudf, so take care of it specially for data size.
      withResource(col.getChildColumnView(0)) { dataView =>
        if (dataView.getData != null) {
          colSize += dataView.getData.getLength
        }
      }
    }
    // meta size
    colSize += computeMetaSize(nullable, hasOffset(colType), col.getRowCount.toInt, colAmount)

    // 3) sum of children sizes
    // Leverage the Spark type to get the nullability info for children. Because
    // it may be in one of the child of a complex type expression.
    colSize += (colType match {
      case ArrayType(elemType, hasNulElem) =>
        withResource(col.getChildColumnView(0))( elemCol =>
          getColumnSize(elemCol, elemType, hasNulElem, colAmount)
        )
      case StructType(fields) =>
        assert(fields.length == col.getNumChildren,
          "Fields number and children number don't match")
        fields.zipWithIndex.map { case (f, idx) =>
          withResource(col.getChildColumnView(idx)) { fCol =>
            getColumnSize(fCol, f.dataType, f.nullable, colAmount)
          }
        }.sum
      case MapType(keyType, valueType, hasNullValue) =>
        withResource(col.getChildColumnView(0)) { structView =>
          withResource(structView.getChildColumnView(0)) { keyView =>
            getColumnSize(keyView, keyType, false, colAmount)
          } + withResource(structView.getChildColumnView(1)) { valueView =>
            getColumnSize(valueView, valueType, hasNullValue, colAmount)
          }
        }
      case _ => 0L
    })
    colSize
  }

  private[rapids] def calcSizeForLiteral(litVal: Any, litType: DataType, valueRows: Int,
      nullable: Boolean, litAmount: Option[Int]): Long = {
    // First calculate the meta buffers size
    val metaSize = litAmount.map { amount =>
      val metaRows = valueRows * amount
      new LitMetaCollector(litVal, litType, nullable).collect.map { litMeta =>
        computeMetaSize(litMeta.hasNull, litMeta.hasOffset, litMeta.getRowsNum * metaRows)
      }.sum
    }.getOrElse(0L)
    // finalSize = oneLitValueSize * numRows + metadata size
    calcLitValueSize(litVal, litType) * valueRows + metaSize
  }

  /**
   * Represent the metadata information of a literal or one of its children,
   * which will be used to calculate the final metadata size after expanding
   * this literal to a column.
   */
  private class LitMeta(val hasNull: Boolean, val hasOffset: Boolean, name: String = "") {
    private var rowsNum: Int = 0
    def incRowsNum(rows: Int = 1): Unit = rowsNum += rows
    def getRowsNum: Int = rowsNum

    override def toString: String =
      s"LitMeta-$name{rowsNum: $rowsNum, hasNull: $hasNull, hasOffset: $hasOffset}"
  }

  /**
   * Collect the metadata information of a literal, the result also includes
   * its children for a nested type literal.
   */
  private class LitMetaCollector(litValue: Any, litType: DataType, nullable: Boolean) {
    private var collected = false
    private val metaInfos: ArrayBuffer[LitMeta] = ArrayBuffer.empty

    def collect: Seq[LitMeta] = {
      if (!collected) {
        executeCollect(litValue, litType, nullable, 0)
        collected = true
      }
      metaInfos.filter(_ != null).toSeq
    }

    /**
     * Go through the literal and all its children to collect the meta information and
     * save to the cache, call "collect" to get the result.
     * Each LitMeta indicates whether the literal or a child will has offset/validity
     * buffers after being expanded to a column, along with the number of original rows.
     * For nested types, it follows the type definition from
     *   https://github.com/rapidsai/cudf/blob/a0487be669326175982c8bfcdab4d61184c88e27/
     *   cpp/doxygen/developer_guide/DEVELOPER_GUIDE.md#list-columns
     */
    private def executeCollect(lit: Any, litTp: DataType, nullable: Boolean,
        depth: Int): Unit = {
      litTp match {
        case ArrayType(elemType, hasNullElem) =>
          // It may be at a middle element of a nested array, so use the nullable
          // from the parent.
          getOrInitAt(depth, new LitMeta(nullable, true)).incRowsNum()
          // Go into the child
          val arrayData = lit.asInstanceOf[ArrayData]
          if (arrayData != null) { // Only need to go into child when nonempty
            (0 until arrayData.numElements()).foreach(i =>
              executeCollect(arrayData.get(i, elemType), elemType, hasNullElem, depth + 1)
            )
          }
        case StructType(fields) =>
          if (nullable) {
            // Add a meta for only a nullable struct, and a struct doesn't have offsets.
            getOrInitAt(depth, new LitMeta(nullable, false)).incRowsNum()
          }
          // Always go into children, which is different from array.
          val stData = lit.asInstanceOf[InternalRow]
          fields.zipWithIndex.foreach { case (f, i) =>
            val fLit = if (stData != null) stData.get(i, f.dataType) else null
            executeCollect(fLit, f.dataType, f.nullable, depth + 1 + i)
          }
        case MapType(keyType, valType, hasNullValue) =>
          // Map is list of struct in cudf. But the nested struct has no offset or
          // validity, so only need a meta for the top list.
          getOrInitAt(depth, new LitMeta(nullable, true)).incRowsNum()
          val mapData = lit.asInstanceOf[MapData]
          if (mapData != null) {
            mapData.foreach(keyType, valType, { case (key, value) =>
              executeCollect(key, keyType, false, depth + 1)
              executeCollect(value, valType, hasNullValue, depth + 2)
            })
          }
        case otherType => // primitive types
          val hasOffsetBuf = hasOffset(otherType)
          if (nullable || hasOffsetBuf) {
            getOrInitAt(depth, new LitMeta(nullable, hasOffsetBuf)).incRowsNum()
          }
      }
    }

    private def getOrInitAt(pos: Int, initMeta: LitMeta): LitMeta = {
      if (pos >= metaInfos.length) {
        (metaInfos.length until pos).foreach { _ =>
          metaInfos.append(null)
        }
        metaInfos.append(initMeta)
      } else if (metaInfos(pos) == null) {
        metaInfos(pos) = initMeta
      }
      metaInfos(pos)
    }
  }

  private def calcLitValueSize(lit: Any, litTp: DataType): Long = {
    litTp match {
      case StringType =>
        if (lit == null) 0L else lit.asInstanceOf[UTF8String].numBytes()
      case BinaryType =>
        if (lit == null) 0L else lit.asInstanceOf[Array[Byte]].length
      case ArrayType(elemType, _) =>
        val arrayData = lit.asInstanceOf[ArrayData]
        if (arrayData == null) {
          0L
        } else {
          (0 until arrayData.numElements()).map { idx =>
            calcLitValueSize(arrayData.get(idx, elemType), elemType)
          }.sum
        }
      case MapType(keyType, valType, _) =>
        val mapData = lit.asInstanceOf[MapData]
        if (mapData == null) {
          0L
        } else {
          val keyData = mapData.keyArray()
          val valData = mapData.valueArray()
          (0 until mapData.numElements()).map { i =>
            calcLitValueSize(keyData.get(i, keyType), keyType) +
              calcLitValueSize(valData.get(i, valType), valType)
          }.sum
        }
      case StructType(fields) =>
        // A special case that it should always go into children even lit is null.
        // Because the children of fixed width will always take some memory.
        val stData = lit.asInstanceOf[InternalRow]
        fields.zipWithIndex.map { case (f, i) =>
          val fLit = if (stData == null) null else stData.get(i, f.dataType)
          calcLitValueSize(fLit, f.dataType)
        }.sum
      case _ => litTp.defaultSize
    }
  }
}

/**
 * An iterator that can be used to split the input of a project before it happens to prevent
 * situations where the output could not be split later on. In testing we tried to see what
 * would happen if we split it to the target batch size, but there was a very significant
 * performance degradation when that happened. For now this is only used in a few specific
 * places and not everywhere.  In the future this could be extended, but if we do that there
 * are some places where we don't want a split, like a project before a window operation.
 * @param iter the input iterator of columnar batches.
 * @param boundExprs the bound project so we can get a good idea of the output size.
 * @param opTime metric for how long this took
 * @param numSplits the number of splits that happened.
 */
class PreProjectSplitIterator(
    iter: Iterator[ColumnarBatch],
    boundExprs: GpuTieredProject,
    opTime: GpuMetric,
    numSplits: GpuMetric) extends AbstractProjectSplitIterator(iter, opTime, numSplits) {

  // We memoize this parameter here as the value doesn't change during the execution
  // of a SQL query. This is the highest level we can cache at without getting it
  // passed in from the Exec that instantiates this split iterator.
  // NOTE: this is overwritten by tests to trigger various corner cases
  private lazy val splitUntilSize = PreProjectSplitIterator.getSplitUntilSize

  // The Java "ceilDiv" is introduced in JDK-18, so we create one ourselves for earlier
  // versions, e.g. JDK-8 and JDK-11.
  private def ceilDiv(a: Long, b: Long): Long = {
    if (a % b == 0) {
      a / b
    } else {
      a / b + 1
    }
  }

  /**
   * calcNumSplit will return the number of splits that we need for the input, in the case
   * that we can detect that a projection using `boundExprs` would expand the output above
   * `GpuDeviceManager.getSplitUntilSize`.
   *
   * @note In the corner case that `cb` is rows-only (no columns), this function returns 0 and the
   * caller must be prepared to handle that case.
   */
  override def calcNumSplits(cb: ColumnarBatch): Int = {
    if (cb.numCols() == 0) {
      0 // rows-only batches should not be split
    } else {
      val sizeEstimator = new PreSplitOutSizeEstimator(cb, boundExprs)
      // If the minimum size is too large we will split before doing the project, to help avoid
      // extreme cases where the output size is so large that we cannot split it afterwards.
      val splitsForOut = math.max(1, ceilDiv(sizeEstimator.calcMinOutputSize(), splitUntilSize))
      // Need to consider individual column size limit. If a cudf column has an
      // offset buffer, its size should be <= Int.MaxValue (~2G). See
      // https://github.com/NVIDIA/spark-rapids/issues/14099
      math.max(splitsForOut, ceilDiv(sizeEstimator.getMaxOffsetColumnSize, Int.MaxValue)).toInt
    }
  }
}

case class GpuProjectExec(
   // NOTE for Scala 2.12.x and below we enforce usage of (eager) List to prevent running
   // into a deep recursion during serde of lazy lists. See
   // https://github.com/NVIDIA/spark-rapids/issues/2036
   //
   // Whereas a similar issue https://issues.apache.org/jira/browse/SPARK-27100 is resolved
   // using an Array, we opt in for List because it implements Seq while having non-recursive
   // serde: https://github.com/scala/scala/blob/2.12.x/src/library/scala/collection/
   //   immutable/List.scala#L516
   projectList: List[NamedExpression],
   child: SparkPlan,
   enablePreSplit: Boolean = true) extends GpuProjectExecLike {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def outputBatching: CoalesceGoal = if (enablePreSplit) {
    // Pre-split will make sure the size of each output batch will not be larger
    // than the splitUntilSize.
    TargetSize(PreProjectSplitIterator.getSplitUntilSize)
  } else {
    super.outputBatching
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    KEY_NUM_PRE_SPLIT -> createMetric(DEBUG_LEVEL, "num pre-splits"),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  override def internalDoExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val numPreSplit = gpuLongMetric(KEY_NUM_PRE_SPLIT)
    
    val boundProjectList = GpuBindReferences.bindGpuReferencesTiered(projectList, child.output,
      conf, allMetrics)
    val localEnablePreSplit = enablePreSplit

    val rdd = child.executeColumnar()
    // Use mapPartitionsWithIndex (not mapPartitions) so that when a downstream
    // coalesce/union wraps this RDD, our lambda fires once per parent partition
    // with that parent's index. GpuNondeterministic expressions
    // (spark_partition_id / monotonically_increasing_id / rand) are reseeded
    // per parent partition so their values stay stable across coalesce/union
    // (SPARK-14393, #14156).
    rdd.mapPartitionsWithIndex { (partIndex, iter) =>
      GpuNondeterministic.initializeAll(boundProjectList.exprTiers.flatten, partIndex)
      val maybeSplitIter = if (localEnablePreSplit) {
        new PreProjectSplitIterator(iter, boundProjectList, opTime, numPreSplit)
      } else {
        iter
      }
      // The streaming entry owns `sb` and may emit multiple pieces per input
      // only when this project is allowed to change batch boundaries.
      // Wrap next() so lazy projection work is counted in the project metric.
      maybeSplitIter.flatMap { split =>
        val pieces = NvtxIdWithMetrics(NvtxRegistry.PROJECT_EXEC, opTime) {
          val sb = SpillableColumnarBatch(split, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          boundProjectList.projectAndCloseStreamingWithSplitRetry(
            sb, allowMultipleOutputBatches = localEnablePreSplit)
        }
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = pieces.hasNext
          override def next(): ColumnarBatch = {
            val ret = NvtxIdWithMetrics(NvtxRegistry.PROJECT_EXEC, opTime) {
              pieces.next()
            }
            numOutputBatches += 1
            numOutputRows += ret.numRows()
            ret
          }
        }
      }
    }
  }
}

/** Use cudf AST expressions to project columnar batches */
case class GpuProjectAstExec(
    // NOTE for Scala 2.12.x and below we enforce usage of (eager) List to prevent running
    // into a deep recursion during serde of lazy lists. See
    // https://github.com/NVIDIA/spark-rapids/issues/2036
    //
    // Whereas a similar issue https://issues.apache.org/jira/browse/SPARK-27100 is resolved
    // using an Array, we opt in for List because it implements Seq while having non-recursive
    // serde: https://github.com/scala/scala/blob/2.12.x/src/library/scala/collection/
    //   immutable/List.scala#L516
    projectList: List[Expression],
    child: SparkPlan
) extends GpuProjectExecLike {

  override def output: Seq[Attribute] = {
    projectList.collect { case ne: NamedExpression => ne.toAttribute }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions(buildRetryableAstIterator)
  }

  def buildRetryableAstIterator(
      input: Iterator[ColumnarBatch]): GpuColumnarBatchIterator = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val boundProjectList = GpuBindReferences.bindGpuReferences(projectList, child.output,
      allMetrics)
    val outputTypes = output.map(_.dataType).toArray
    new GpuColumnarBatchIterator(true) {
      private[this] var maybeSplittedItr: Iterator[ColumnarBatch] = Iterator.empty
      private[this] var compiledAstExprs =
        NvtxIdWithMetrics(NvtxRegistry.COMPILE_ASTS, opTime) {
          boundProjectList.safeMap { expr =>
            // Use intmax for the left table column count since there's only one input table here.
            expr.convertToAst(Int.MaxValue).compile()
          }
        }

      override def hasNext: Boolean = maybeSplittedItr.hasNext || {
        if (input.hasNext) {
          true
        } else {
          close()
          false
        }
      }

      override def next(): ColumnarBatch = {
        if (!maybeSplittedItr.hasNext) {
          val spillable = SpillableColumnarBatch(
            input.next(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          // AST currently doesn't support non-deterministic expressions so it's not needed
          // to check whether compiled expressions are retryable.
          maybeSplittedItr = withRetry(spillable, splitSpillableInHalfByRows) { spillable =>
            NvtxIdWithMetrics(NvtxRegistry.PROJECT_AST, opTime) {
              withResource(spillable.getColumnarBatch()) { cb =>
                val projectedTable = withResource(tableFromBatch(cb)) { table =>
                  withResource(
                    compiledAstExprs.safeMap(_.computeColumn(table))) { projectedColumns =>
                    new Table(projectedColumns: _*)
                  }
                }
                withResource(projectedTable) { _ =>
                  GpuColumnVector.from(projectedTable, outputTypes)
                }
              }
            }
          }
        }

        val ret = maybeSplittedItr.next()
        numOutputBatches += 1
        numOutputRows += ret.numRows()
        ret
      }

      override def doClose(): Unit = {
        compiledAstExprs.safeClose()
        compiledAstExprs = Nil
      }

      private def tableFromBatch(cb: ColumnarBatch): Table = {
        if (cb.numCols != 0) {
          GpuColumnVector.from(cb)
        } else {
          // Count-only batch but cudf Table cannot be created with no columns.
          // Create the cheapest table we can to evaluate the AST expression.
          withResource(Scalar.fromBool(false)) { falseScalar =>
            withResource(cudf.ColumnVector.fromScalar(falseScalar, cb.numRows())) { falseColumn =>
              new Table(falseColumn)
            }
          }
        }
      }
    }
  }
}

/**
 * Do projections in a tiered fashion, where earlier tiers contain sub-expressions that are
 * referenced in later tiers.  Each tier adds columns to the original batch corresponding
 * to the output of the sub-expressions.  It also removes columns that are no longer needed,
 * based on inputAttrTiers for the current tier and the next tier.
 * Example of how this is processed:
 *   Original projection expressions:
 *   (((a + b) + c) * e), (((a + b) + d) * f), (a + e), (c + f)
 *   Input columns for tier 1: a, b, c, d, e, f  (original projection inputs)
 *   Tier 1: (a + b) as ref1
 *   Input columns for tier 2: a, c, d, e, f, ref1
 *   Tier 2: (ref1 + c) as ref2, (ref1 + d) as ref3
 *   Input columns for tier 3: a, c, e, f, ref2, ref3
 *   Tier 3: (ref2 * e), (ref3 * f), (a + e), (c + f)
 */
 case class GpuTieredProject(exprTiers: Seq[Seq[GpuExpression]]) {

  /**
   * Inject metrics into all expressions across all tiers.
   * @param metrics Map of metric names to GpuMetric instances
   */
  def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    exprTiers.foreach { tier =>
      GpuMetric.injectMetrics(tier, metrics)
    }
  }

  /**
   * Is everything retryable. This can help with reliability in the common case.
   */
  lazy val areAllRetryable = !exprTiers.exists { tier =>
    tier.exists { expr =>
      !expr.retryable
    }
  }

  lazy val retryables: Seq[Retryable] = exprTiers.flatMap(GpuExpressionsUtils.collectRetryables)

  lazy val outputExprs = exprTiers.last.toArray

  private[this] def getPassThroughIndex(tierIndex: Int,
      expr: Expression): Option[Int] = expr match {
    case GpuAlias(child, _) =>
      getPassThroughIndex(tierIndex, child)
    case GpuBoundReference(index, _, _) =>
      if (tierIndex <= 0) {
        // We are at the input tier so the bound attribute is good!!!
        Some(index)
      } else {
        // Not at the input yet
        val newTier = tierIndex - 1
        val newExpr = exprTiers(newTier)(index)
        getPassThroughIndex(newTier, newExpr)
      }
    case _ =>
      None
  }

  /**
   * Given an output index check to see if this is just going to be a pass through to a
   * specific input column index.
   * @param index the output column index to check
   * @return the index of the input column that it passes through to or else None
   */
  def getPassThroughIndex(index: Int): Option[Int] = {
    getPassThroughIndex(exprTiers.last(index))
  }

  /**
   * Similar as "getPassThroughIndex(index: Int)", but with a given expression.
   * The input expression is always a child of an output expression of complex type,
   * or the output itself. e.g. GpuCreateArray, GpuCreateMap, GpuCreateStruct.
   *
   * Designed for the output size estimation by the pre-split iterator.
   */
  private[rapids] def getPassThroughIndex(expr: Expression): Option[Int] = {
    val startTier = exprTiers.length - 1
    getPassThroughIndex(startTier, expr)
  }

  private [this] def projectWithRetrySingleBatchInternal(sb: SpillableColumnarBatch,
      closeInputBatch: Boolean): ColumnarBatch = {
    if (areAllRetryable) {
      // If all of the expressions are retryable we can just run everything and retry it
      // at the top level. If some things are not retryable we need to split them up and
      // do the processing in a way that makes it so retries are more likely to succeed.
      if (RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.get(SQLConf.get)) {
        // Split-retry path: on GPU OOM, halve the input batch by rows and
        // re-run the projection on each half. runWithSplitRetry takes
        // ownership of the SpillableColumnarBatch and closes it; if the
        // caller asked us not to close `sb`, increment the ref count to
        // compensate.
        val sbForRetry = if (closeInputBatch) sb else sb.incRefCount()
        GpuProjectExec.runWithSplitRetry(sbForRetry, retryables, project(_))
      } else {
        val sbToClose = if (closeInputBatch) {
          Some(sb)
        } else {
          None
        }
        withResource(sbToClose) { _ =>
          retryables.foreach(_.checkpoint())
          RmmRapidsRetryIterator.withRetryNoSplit {
            withResource(sb.getColumnarBatch()) { cb =>
              withRestoreOnRetry(retryables) {
                project(cb)
              }
            }
          }
        }
      }
    } else {
      @tailrec
      def recurse(
          boundExprs: Seq[Seq[GpuExpression]],
          sb: SpillableColumnarBatch,
          recurseCloseInputBatch: Boolean): SpillableColumnarBatch = boundExprs match {
        case Nil => sb
        case exprSet :: tail =>
          val projectSb = NvtxRegistry.PROJECT_TIER {
            val projectResult = if (recurseCloseInputBatch) {
              GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, exprSet)
            } else {
              GpuProjectExec.projectWithRetrySingleBatch(sb, exprSet)
            }
            SpillableColumnarBatch(projectResult, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          }
          // We always want to close the temp batches that we make...
          recurse(tail, projectSb, recurseCloseInputBatch = true)
      }
      // Process tiers sequentially. The input batch is closed by recurse if requested.
      withResource(recurse(exprTiers, sb, closeInputBatch)) { ret =>
        ret.getColumnarBatch()
      }
    }
  }

  /**
   * Do a project with retry and close the input batch when done.
   */
  def projectAndCloseWithRetrySingleBatch(sb: SpillableColumnarBatch): ColumnarBatch =
    projectWithRetrySingleBatchInternal(sb, closeInputBatch = true)

  /**
   * Do a project with retry, but don't close the input batch.
   */
  def projectWithRetrySingleBatch(sb: SpillableColumnarBatch): ColumnarBatch =
    projectWithRetrySingleBatchInternal(sb, closeInputBatch = false)

  /**
   * Project `sb` and close it, returning an iterator so split-retry pieces can
   * flow downstream without concatenation. Non-streaming paths are wrapped in a
   * lazy one-shot iterator so callers can measure projection work around next().
   *
   * Set `allowMultipleOutputBatches` only for callers that can consume multiple
   * output batches per input. Operators with one-output-per-input state must
   * preserve the single-batch path.
   */
  def projectAndCloseStreamingWithSplitRetry(sb: SpillableColumnarBatch): Iterator[ColumnarBatch] =
    projectAndCloseStreamingWithSplitRetry(sb, allowMultipleOutputBatches = true)

  def projectAndCloseStreamingWithSplitRetry(
      sb: SpillableColumnarBatch,
      allowMultipleOutputBatches: Boolean): Iterator[ColumnarBatch] = closeOnExcept(sb) { _ =>
    if (allowMultipleOutputBatches &&
        areAllRetryable &&
        RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.get(SQLConf.get)) {
      GpuProjectExec.runStreamingWithSplitRetry(sb, retryables, project(_))
    } else {
      new Iterator[ColumnarBatch] {
        @volatile private var consumed = false
        // Until first next(), `sb` is not inside projectAndCloseWithRetrySingleBatch.
        private val onClose = Option(TaskContext.get()).map { tc =>
          onTaskCompletion(tc) {
            if (!consumed) {
              sb.close()
            }
          }
        }
        override def hasNext: Boolean = !consumed
        override def next(): ColumnarBatch = {
          if (consumed) {
            throw new NoSuchElementException
          }
          consumed = true
          onClose.foreach(_.removeCallback())
          projectAndCloseWithRetrySingleBatch(sb)
        }
      }
    }
  }

  def project(batch: ColumnarBatch): ColumnarBatch = {
    @tailrec
    def recurse(
        boundExprs: Seq[Seq[GpuExpression]],
        cb: ColumnarBatch,
        isFirst: Boolean): ColumnarBatch = {
      boundExprs match {
        case Nil => cb
        case exprSet :: tail =>
          val projectCb = try {
            NvtxRegistry.PROJECT_TIER {
              GpuProjectExec.project(cb, exprSet)
            }
          } finally {
            // Close intermediate batches
            if (!isFirst) {
              cb.close()
            }
          }
          recurse(tail, projectCb, false)
      }
    }
    // Process tiers sequentially
    recurse(exprTiers, batch, true)
  }
}

/**
 * Run a filter on a batch.  The batch will be consumed.
 */
object GpuFilter {

  def apply(
      batch: ColumnarBatch,
      boundCondition: Expression,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): ColumnarBatch = {
    NvtxIdWithMetrics(NvtxRegistry.FILTER_BATCH, filterTime) {
      val filteredBatch = GpuFilter(batch, boundCondition)
      numOutputBatches += 1
      numOutputRows += filteredBatch.numRows()
      filteredBatch
    }
  }

  def filterAndClose(batch: ColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): Iterator[ColumnarBatch] = {
    if (boundCondition.areAllRetryable) {
      val sb = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      filterAndCloseWithRetry(sb, boundCondition, numOutputRows, numOutputBatches, filterTime)
    } else {
      filterAndCloseNoRetry(batch, boundCondition, numOutputRows, numOutputBatches,
        filterTime)
    }
  }

  private def filterAndCloseNoRetry(batch: ColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric): Iterator[ColumnarBatch] = {
    NvtxIdWithMetrics(NvtxRegistry.FILTER_BATCH, filterTime) {
      val filteredBatch = withResource(batch) { batch =>
        GpuFilter(batch, boundCondition)
      }
      numOutputBatches += 1
      numOutputRows += filteredBatch.numRows()
      Seq(filteredBatch).toIterator
    }
  }

  private def filterAndCloseWithRetry(input: SpillableColumnarBatch,
      boundCondition: GpuTieredProject,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    boundCondition.retryables.foreach(_.checkpoint())
    val ret = withRetry(input, splitSpillableInHalfByRows) { sb =>
      withResource(sb.getColumnarBatch()) { cb =>
        withRestoreOnRetry(boundCondition.retryables) {
          NvtxIdWithMetrics(NvtxRegistry.FILTER_BATCH, opTime) {
            GpuFilter(cb, boundCondition)
          }
        }
      }
    }
    ret.map { cb =>
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }

  private def allEntriesAreTrue(mask: GpuColumnVector): Boolean = {
    if (mask.hasNull) {
      false
    } else {
      withResource(mask.getBase.all()) { all =>
        all.getBoolean
      }
    }
  }

  private def doFilter(checkedFilterMask: Option[cudf.ColumnVector],
      cb: ColumnarBatch): ColumnarBatch = {
    checkedFilterMask.map { checkedFilterMask =>
      withResource(checkedFilterMask) { checkedFilterMask =>
        if (cb.numCols() <= 0) {
          val rowCount = withResource(checkedFilterMask.sum(DType.INT32)) { sum =>
            if (sum.isValid) {
              sum.getInt
            } else {
              0
            }
          }
          new ColumnarBatch(Array(), rowCount)
        } else {
          val colTypes = GpuColumnVector.extractTypes(cb)
          withResource(GpuColumnVector.from(cb)) { tbl =>
            withResource(tbl.filter(checkedFilterMask)) { filteredData =>
              GpuColumnVector.from(filteredData, colTypes)
            }
          }
        }
      }
    }.getOrElse {
      // Nothing to filter so it is a NOOP
      GpuColumnVector.incRefCounts(cb)
    }
  }

  private def computeCheckedFilterMask(boundCondition: Expression,
      cb: ColumnarBatch): Option[cudf.ColumnVector] = {
    withResource(boundCondition.columnarEval(cb)) { filterMask =>
      // If  filter is a noop then return a None for the mask
      if (allEntriesAreTrue(filterMask)) {
        None
      } else {
        Some(filterMask.getBase.incRefCount())
      }
    }
  }

  private def computeCheckedFilterMask(boundCondition: GpuTieredProject,
      cb: ColumnarBatch): Option[cudf.ColumnVector] = {
    withResource(boundCondition.project(cb)) { filterBatch =>
      val filterMask = filterBatch.column(0).asInstanceOf[GpuColumnVector]
      // If  filter is a noop then return a None for the mask
      if (allEntriesAreTrue(filterMask)) {
        None
      } else {
        Some(filterMask.getBase.incRefCount())
      }
    }
  }

  private[rapids] def apply(batch: ColumnarBatch,
      boundCondition: Expression) : ColumnarBatch = {
    val checkedFilterMask = computeCheckedFilterMask(boundCondition, batch)
    doFilter(checkedFilterMask, batch)
  }


  def apply(
      batch: ColumnarBatch,
      boundCondition: GpuTieredProject): ColumnarBatch = {
    val checkedFilterMask = computeCheckedFilterMask(boundCondition, batch)
    doFilter(checkedFilterMask, batch)
  }
}

case class GpuFilterExecMeta(
  filter: FilterExec,
  override val conf: RapidsConf,
  parentMetaOpt: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule
) extends SparkPlanMeta[FilterExec](filter, conf, parentMetaOpt, rule) {
  override def convertToGpu(): GpuExec = {
    GpuFilterExec(childExprs.head.convertToGpu(),
      childPlans.head.convertIfNeeded())()
  }
}

case class GpuFilterExec(
    condition: Expression,
    child: SparkPlan)(
    override val coalesceAfter: Boolean = true)
    extends ShimUnaryExecNode with ShimPredicateHelper with GpuExec {

  override def otherCopyArgs: Seq[AnyRef] =
    Seq[AnyRef](coalesceAfter.asInstanceOf[java.lang.Boolean])

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    CPU_BRIDGE_PROCESSING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_PROCESSING_TIME),
    CPU_BRIDGE_WAIT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, 
      DESCRIPTION_CPU_BRIDGE_WAIT_TIME))

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case GpuIsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val rdd = child.executeColumnar()
    val boundCondition = GpuBindReferences.bindGpuReferencesTiered(Seq(condition), child.output,
      conf, allMetrics)
    // Mirrors GpuProjectExec: mapPartitionsWithIndex (not flatMap) so that
    // when a downstream coalesce/union wraps this RDD, our lambda fires once
    // per parent partition with that parent's index. GpuNondeterministic
    // expressions inside the filter condition (e.g. rand(seed)) are reseeded
    // per parent partition so their values stay stable across coalesce/union
    // (SPARK-14393, #14156).
    rdd.mapPartitionsWithIndex { (partIndex, iter) =>
      GpuNondeterministic.initializeAll(boundCondition.exprTiers.flatten, partIndex)
      iter.flatMap { batch =>
        GpuFilter.filterAndClose(batch, boundCondition, numOutputRows,
          numOutputBatches, opTime)
      }
    }
  }
}

class GpuSampleExecMeta(
    sample: SampleExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[SampleExec](sample, conf, p, r)
    with Logging {
  override def convertToGpu(): GpuExec = {
    val gpuChild = childPlans.head.convertIfNeeded()
    val seed = SampleExecShims.resolvedSeed(sample)
    if (conf.isFastSampleEnabled) {
      // Use GPU sample JNI, this is faster, but the output is not the same as CPU produces
      GpuFastSampleExec(sample.lowerBound, sample.upperBound, sample.withReplacement,
        seed, gpuChild)
    } else {
      // The output is the same as CPU produces
      // First generates row indexes by CPU sampler, then use GPU to gathers
      GpuSampleExec(sample.lowerBound, sample.upperBound, sample.withReplacement,
        seed, gpuChild)
    }
  }
}

case class GpuSampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long, child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY))

  override def output: Seq[Attribute] = {
    child.output
  }

  // add one coalesce exec to avoid empty batch and small batch,
  // because sample will shrink the batch
  override val coalesceAfter: Boolean = true

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)

    val rdd = child.executeColumnar()
    // CPU consistent, first generates sample row indexes by CPU, then gathers by GPU
    if (withReplacement) {
      new GpuPartitionwiseSampledRDD(
        rdd,
        new GpuPoissonSampler(upperBound - lowerBound, useGapSamplingIfPossible = false,
          numOutputRows, numOutputBatches, opTime),
        preservesPartitioning = true,
        seed)
    } else {
      rdd.mapPartitionsWithIndex(
        (index, iterator) => {
          // use CPU sampler generate row indexes
          val sampler = new BernoulliCellSampler(lowerBound, upperBound)
          sampler.setSeed(seed + index)
          iterator.map[ColumnarBatch] { columnarBatch =>
            // collect sampled row idx
            // samples idx in batch one by one, so it's same as CPU execution
            NvtxIdWithMetrics(NvtxRegistry.SAMPLE_EXEC, opTime) {
              withResource(columnarBatch) { cb =>
                // generate sampled row indexes by CPU
                val sampledRows = new ArrayBuffer[Int]
                var rowIndex = 0
                while (rowIndex < cb.numRows()) {
                  if (sampler.sample() > 0) {
                    sampledRows += rowIndex
                  }
                  rowIndex += 1
                }
                numOutputBatches += 1
                numOutputRows += sampledRows.length
                // gather by row indexes
                GatherUtils.gather(cb, sampledRows)
              }
            }
          }
        }
        , preservesPartitioning = true
      )
    }
  }
}

case class GpuFastSampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY))

  override def output: Seq[Attribute] = {
    child.output
  }

  // add one coalesce exec to avoid empty batch and small batch,
  // because sample will shrink the batch
  override val coalesceAfter: Boolean = true

  // Note GPU sample does not preserve the ordering
  override def outputOrdering: Seq[SortOrder] = Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val rdd = child.executeColumnar()

    // CPU inconsistent, uses GPU sample JNI
    rdd.mapPartitionsWithIndex(
      (index, iterator) => {
        iterator.map[ColumnarBatch] { columnarBatch =>
          NvtxIdWithMetrics(NvtxRegistry.FAST_SAMPLE_EXEC, opTime) {
            withResource(columnarBatch) { cb =>
              numOutputBatches += 1
              val numSampleRows = (cb.numRows() * (upperBound - lowerBound)).toLong

              val colTypes = GpuColumnVector.extractTypes(cb)
              if (numSampleRows == 0L) {
                GpuColumnVector.emptyBatchFromTypes(colTypes)
              } else if (cb.numCols() == 0) {
                // for count agg, num of cols is 0
                val c = GpuColumnVector.emptyBatchFromTypes(colTypes)
                c.setNumRows(numSampleRows.toInt)
                c
              } else {
                withResource(GpuColumnVector.from(cb)) { table =>
                  // GPU sample
                  withResource(table.sample(numSampleRows, withReplacement, seed + index)) {
                    sampled =>
                      val cb = GpuColumnVector.from(sampled, colTypes)
                      numOutputRows += cb.numRows()
                      cb
                  }
                }
              }
            }
          }
        }
      }
      , preservesPartitioning = true
    )
  }
}

private[rapids] class GpuRangeIterator(
    partitionStart: BigInt,
    partitionEnd: BigInt,
    step: Long,
    maxRowCountPerBatch: Long,
    taskContext: TaskContext,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with Logging {

  // This iterator is designed for GpuRangeExec, so it has the requirement for the inputs.
  assert((partitionEnd - partitionStart) % step == 0)

  private def getSafeMargin(bi: BigInt): Long = {
    if (bi.isValidLong) {
      bi.toLong
    } else if (bi > 0) {
      Long.MaxValue
    } else {
      Long.MinValue
    }
  }

  private val safePartitionStart = getSafeMargin(partitionStart) // inclusive
  private val safePartitionEnd = getSafeMargin(partitionEnd) // exclusive, unless start == this
  private[this] var currentPosition: Long = safePartitionStart
  private[this] var done: Boolean = false

  override def hasNext: Boolean = {
    if (!done) {
      if (step > 0) {
        currentPosition < safePartitionEnd
      } else {
        currentPosition > safePartitionEnd
      }
    } else false
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    GpuSemaphore.acquireIfNecessary(taskContext)
    NvtxIdWithMetrics(NvtxRegistry.GPU_RANGE, opTime) {
      val start = currentPosition
      val remainingRows = (safePartitionEnd - start) / step
      // Start is inclusive so we need to produce at least one row
      val rowsExpected = Math.max(1, Math.min(remainingRows, maxRowCountPerBatch))
      val iter = withRetry(AutoCloseableLong(rowsExpected), reduceRowsNumberByHalf) { rows =>
        withResource(Scalar.fromLong(start)) { startScalar =>
          withResource(Scalar.fromLong(step)) { stepScalar =>
            withResource(
                cudf.ColumnVector.sequence(startScalar, stepScalar, rows.value.toInt)) { vec =>
              withResource(new Table(vec)) { tab =>
                GpuColumnVector.from(tab, Array[DataType](LongType))
              }
            }
          }
        }
      }
      assertInTests(iter.hasNext)
      closeOnExcept(iter.next()) { batch =>
        // This "iter" returned from the "withRetry" block above has only one batch,
        // because the split function "reduceRowsNumberByHalf" returns a Seq with a single
        // element inside.
        // By doing this, we can pull out this single batch directly without maintaining
        // this extra `iter` for the next loop.
        assertInTests(iter.isEmpty)
        val endInclusive = start + ((batch.numRows() - 1) * step)
        currentPosition = endInclusive + step
        if (currentPosition < endInclusive ^ step < 0) {
          // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
          // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a
          // step back, we are pretty sure that we have an overflow.
          done = true
        }
        if (batch.numRows() < rowsExpected) {
          logDebug(s"Retried with ${batch.numRows()} rows when expected $rowsExpected rows")
        }
        batch
      }
    }
  }

  /**
   * Reduce the input rows number by half, and it returns a Seq with only one element,
   * that is the half value.
   * This will be used with the split_retry block to generate a single batch a with smaller
   * size when getting relevant OOMs.
   */
  private def reduceRowsNumberByHalf: AutoCloseableLong => Seq[AutoCloseableLong] =
    (rowsNumber) => {
      withResource(rowsNumber) { _ =>
        if (rowsNumber.value < 10) {
          throw new GpuSplitAndRetryOOM(s"GPU OutOfMemory: the number of rows generated is" +
            s" too small to be split ${rowsNumber.value}!")
        }
        Seq(AutoCloseableLong(rowsNumber.value / 2))
      }
    }

  /** A bridge class between Long and AutoCloseable for retry */
  case class AutoCloseableLong(value: Long) extends AutoCloseable {
    override def close(): Unit = { /* Nothing to be closed */ }
  }
}

/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class GpuRangeExec(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Int,
    output: Seq[Attribute],
    targetSizeBytes: Long)
  extends ShimLeafExecNode with GpuExec {

  val numElements: BigInt = {
    val safeStart = BigInt(start)
    val safeEnd = BigInt(end)
    if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
      (safeEnd - safeStart) / step
    } else {
      // the remainder has the same sign with range, could add 1 more
      (safeEnd - safeStart) / step + 1
    }
  }

  val isEmptyRange: Boolean = start == end || (start < end ^ 0 < step)

  override protected val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override protected val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY)
  )

  override def outputOrdering: Seq[SortOrder] = {
    val order = if (step > 0) {
      Ascending
    } else {
      Descending
    }
    output.map(a => SortOrder(a, order))
  }

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override def outputBatching: CoalesceGoal = TargetSize(targetSizeBytes)

  protected override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val maxRowCountPerBatch = Math.min(targetSizeBytes/8, Int.MaxValue)

    if (isEmptyRange) {
      sparkContext.emptyRDD[ColumnarBatch]
    } else {
      sparkSession
        .sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex { (i, _) =>
          val partitionStart = (i * numElements) / numSlices * step + start
          val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
          val taskContext = TaskContext.get()
          val inputMetrics = taskContext.taskMetrics().inputMetrics

          val rangeIter = new GpuRangeIterator(partitionStart, partitionEnd, step,
              maxRowCountPerBatch, taskContext, opTime).map { batch =>
            numOutputRows += batch.numRows()
            TrampolineUtil.incInputRecordsRows(inputMetrics, batch.numRows())
            numOutputBatches += 1
            batch
          }
          new InterruptibleIterator(taskContext, rangeIter)
        }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"GpuRange ($start, $end, step=$step, splits=$numSlices)"
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
}


case class GpuUnionExec(children: Seq[SparkPlan]) extends ShimSparkPlan with GpuExec {

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(TrampolineUtil.unionLikeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  override def outputPartitioning: Partitioning =
    GpuUnionExecShim.getOutputPartitioning(children, output, conf)

  // The smallest of our children
  override def outputBatching: CoalesceGoal =
    children.map(GpuExec.outputBatching).reduce(CoalesceGoal.minProvided)

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)

    GpuUnionExecShim.unionColumnarRdds(
      sparkContext,
      children.map(_.executeColumnar()),
      outputPartitioning,
      numOutputRows,
      numOutputBatches)
  }
}

case class GpuCoalesceExec(numPartitions: Int, child: SparkPlan)
    extends ShimUnaryExecNode with GpuExec {

  // This operator does not record any metrics
  override lazy val allMetrics: Map[String, GpuMetric] = Map.empty

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar()
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions, shuffle = false)
    }
  }

  override val coalesceAfter: Boolean = true
}

object GpuCoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[ColumnarBatch](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}
