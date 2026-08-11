/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.OutOfBoundsPolicy
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitTargetSizeInHalfGpu, withRestoreOnRetry, withRetry}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.rapids.execution.GatherMapsResult
import org.apache.spark.sql.vectorized.ColumnarBatch

trait TaskAutoCloseableResource extends AutoCloseable {
  protected var closed = false
  // iteration-independent resources
  private val resources = scala.collection.mutable.ArrayBuffer[AutoCloseable]()
  def use[T <: AutoCloseable](ac: T): T = {
    resources += ac
    ac
  }

  override def close() = if (!closed) {
    closed = true
    resources.reverse.safeClose()
    resources.clear()
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }
}

/**
 * Base class for iterators producing the results of a join.
 * @param gatherNvtxId NvtxId to use for the NVTX range when producing the join gather maps
 * @param targetSize configured target batch size in bytes
 * @param sizeEstimateThreshold threshold for deciding when to skip expensive size estimation
 * @param opTime metric to record op time in the iterator
 * @param joinTime metric to record GPU time spent in join
 */
abstract class AbstractGpuJoinIterator(
    gatherNvtxId: NvtxId,
    targetSize: Long,
    sizeEstimateThreshold: Double,
    val opTime: GpuMetric,
    joinTime: GpuMetric)
    extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {
  private[this] var nextCb: Option[ColumnarBatch] = None
  private[this] var gathererStore: Option[JoinGatherer] = None

  /** Returns whether there are any more batches on the stream side of the join */
  protected def hasNextStreamBatch: Boolean

  /**
   * Called to setup the next join gatherer instance when the previous instance is done or
   * there is no previous instance. Because this is likely to call next or has next on the
   * stream side all implementations must track their own opTime metrics.
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def setupNextGatherer(): Option[JoinGatherer]

  /** Whether to automatically call close() on this iterator when it is exhausted. */
  protected val shouldAutoCloseOnExhaust: Boolean = true

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    var mayContinue = true
    while (nextCb.isEmpty && mayContinue) {
      if (gathererStore.exists(!_.isDone)) {
        opTime.ns {
          nextCb = nextCbFromGatherer()
        }
      } else {
        if (hasNextStreamBatch) {
          // Need to refill the gatherer
          opTime.ns {
            gathererStore.foreach(_.close())
            gathererStore = None
          }
          gathererStore = setupNextGatherer()
          opTime.ns {
            nextCb = nextCbFromGatherer()
          }
        } else {
          mayContinue = false
        }
      }
    }
    if (nextCb.isEmpty && shouldAutoCloseOnExhaust) {
      // Nothing is left to return so close ASAP.
      opTime.ns(close())
    }
    nextCb.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val ret = nextCb.get
    nextCb = None
    ret
  }

  override def close(): Unit = {
    if (!closed) {
      nextCb.foreach(_.close())
      nextCb = None
      gathererStore.foreach(_.close())
      gathererStore = None
      closed = true
    }
  }

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    NvtxIdWithMetrics(gatherNvtxId, joinTime) {
      val minTargetSize = Math.min(targetSize, 64L * 1024 * 1024)
      val ret = gathererStore.map { gather =>
        // This withRetry block will always return an iterator with one ColumnarBatch.
        // The gatherer tracks how many rows we have used already.  The withRestoreOnRetry
        // ensures that we restart at the same place in the gatherer.  In the case of a
        // GpuSplitAndRetryOOM, we retry with a smaller (halved) targetSize, so we are taking
        // less from the gatherer, but because the gatherer tracks how much is used, the
        // next call to this function will start in the right place.
        val estimatedDataSize = (gather.numRowsLeft * gather.realCheapPerRowSizeEstimate).toLong
        val targetSizeWrapper = AutoCloseableTargetSize(targetSize, minTargetSize,
          estimatedDataSize)
        gather.checkpoint()
        withRetry(targetSizeWrapper, splitTargetSizeInHalfGpu) { attempt =>
          withRestoreOnRetry(gather) {
            val nextRows = JoinGatherer.getRowsInNextBatch(gather, attempt.targetSize,
              sizeEstimateThreshold)
            gather.gatherNext(nextRows)
          }
        }.next()
      }
      if (gathererStore.exists(_.isDone)) {
        gathererStore.foreach(_.close())
        gathererStore = None
      }

      if (ret.isDefined) {
        // We are about to return something. We got everything we need from it so now let it spill
        // if there is more to be gathered later on.
        gathererStore.foreach(_.allowSpilling())
      }
      ret
    }
  }
}

/**
 * Base class for join iterators that split and spill batches to avoid GPU OOM errors.
 * @param gatherNvtxId NvtxId to use for the NVTX range when producing the join gather maps
 * @param stream iterator to produce the batches for the streaming side input of the join
 * @param streamAttributes attributes corresponding to the streaming side input
 * @param builtBatch batch for the built side input of the join
 * @param targetSize configured target batch size in bytes
 * @param sizeEstimateThreshold threshold for deciding when to skip expensive size estimation
 * @param opTime metric to record time spent for this operation
 * @param joinTime metric to record GPU time spent in join
 */
abstract class SplittableJoinIterator(
    gatherNvtxId: NvtxId,
    stream: Iterator[LazySpillableColumnarBatch],
    streamAttributes: Seq[Attribute],
    builtBatch: LazySpillableColumnarBatch,
    targetSize: Long,
    sizeEstimateThreshold: Double,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends AbstractGpuJoinIterator(
      gatherNvtxId,
      targetSize,
      sizeEstimateThreshold,
      opTime = opTime,
      joinTime = joinTime) with Logging {
  // For some join types even if there is no stream data we might output something
  private var isInitialJoin = true
  // If the join explodes this holds batches from the stream side split into smaller pieces.
  private val pendingSplits = scala.collection.mutable.Queue[LazySpillableColumnarBatch]()

  protected def computeNumJoinRows(cb: LazySpillableColumnarBatch): Long

  /**
   * Create a join gatherer.
   * @param cb next column batch from the streaming side of the join
   * @param numJoinRows if present, the number of join output rows computed for this batch
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def createGatherer(cb: LazySpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer]

  override def hasNextStreamBatch: Boolean = {
    isInitialJoin || pendingSplits.nonEmpty || stream.hasNext
  }

  override def setupNextGatherer(): Option[JoinGatherer] = {
    val wasInitialJoin = isInitialJoin
    isInitialJoin = false
    if (pendingSplits.nonEmpty || stream.hasNext) {
      val scb = if (pendingSplits.nonEmpty) {
        pendingSplits.dequeue()
      } else {
        stream.next()
      }
      opTime.ns {
        withResource(scb) { scb =>
          val numJoinRows = computeNumJoinRows(scb)

          // We want the gather maps size to be around the target size. There are two gather maps
          // that are made up of ints, so compute how many rows on the stream side will produce the
          // desired gather maps size.
          val maxJoinRows = Math.max(1, targetSize / (2 * Integer.BYTES))
          if (numJoinRows > maxJoinRows && scb.numRows > 1) {
            // Need to split the batch to reduce the gather maps size. This takes a simplistic
            // approach of assuming the data is uniformly distributed in the stream table.
            val numSplits = Math.min(scb.numRows,
              Math.ceil(numJoinRows.toDouble / maxJoinRows).toInt)
            splitAndSave(scb.getBatch, numSplits)

            // Return no gatherer so the outer loop will try again
            return None
          }

          createGatherer(scb, Some(numJoinRows))
        }
      }
    } else {
      opTime.ns {
        assert(wasInitialJoin)
        import scala.collection.JavaConverters._
        withResource(GpuColumnVector.emptyBatch(streamAttributes.asJava)) { cb =>
          withResource(LazySpillableColumnarBatch(cb, "empty_stream")) { scb =>
            createGatherer(scb, None)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtBatch.close()
      pendingSplits.foreach(_.close())
      pendingSplits.clear()
    }
  }

  private def splitStreamBatch(
      cb: ColumnarBatch,
      numBatches: Int): Seq[LazySpillableColumnarBatch] = {
    val batchSize = cb.numRows() / numBatches
    val splits = withResource(GpuColumnVector.from(cb)) { tab =>
      val splitIndexes = (1 until numBatches).map(num => num * batchSize)
      tab.contiguousSplit(splitIndexes: _*)
    }
    withResource(splits) { splits =>
      val schema = GpuColumnVector.extractTypes(cb)
      withResource(splits.safeMap(_.getTable)) { tables =>
        withResource(tables.safeMap(GpuColumnVector.from(_, schema))) { batches =>
          batches.safeMap { splitBatch =>
            val lazyCb = LazySpillableColumnarBatch(splitBatch, "stream_data")
            lazyCb.allowSpilling()
            lazyCb
          }
        }
      }
    }
  }

  /**
   * Split a stream-side input batch, making all splits spillable, and replacing this batch with
   * the splits in the stream-side input
   * @param cb stream-side input batch to split
   * @param numBatches number of splits to produce with approximately the same number of rows each
   * @param oom a prior OOM exception that this will try to recover from by splitting
   */
  protected def splitAndSave(
      cb: ColumnarBatch,
      numBatches: Int,
      oom: Option[Throwable] = None): Unit = {
    val batchSize = cb.numRows() / numBatches
    if (oom.isDefined && batchSize < 100) {
      // We just need some kind of cutoff to not get stuck in a loop if the batches get to be too
      // small but we want to at least give it a chance to work (mostly for tests where the
      // targetSize can be set really small)
      throw oom.get
    }
    val msg = s"Split stream batch into $numBatches batches of about $batchSize rows"
    if (oom.isDefined) {
      logWarning(s"OOM Encountered: $msg")
    } else {
      logInfo(msg)
    }
    pendingSplits ++= splitStreamBatch(cb, numBatches)
  }

  /**
   * Create a join gatherer from gather maps.
   * @param maps gather maps produced from a cudf join
   * @param leftData batch corresponding to the left table in the join
   * @param rightData batch corresponding to the right table in the join
   * @return some gatherer or None if there are no rows to gather in this join batch
   */
  protected def makeGatherer(
      maps: GatherMapsResult,
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch,
      joinType: JoinType): Option[JoinGatherer] = {
    withResource(maps) { _ =>
      val leftGatherer = joinType match {
        case LeftOuter if !maps.hasLeft =>
          // Distinct left outer joins only produce a single gather map since left table rows
          // are not rearranged by the join.
          new JoinGathererSameTable(leftData)
        case _ =>
          val lazyLeftMap = LazySpillableGatherMap(maps.left, "left_map")
          // Inner joins -- manifest the intersection of both left and right sides. The gather maps
          //   contain the number of rows that must be manifested, and every index
          //   must be within bounds, so we can skip the bounds checking.
          //
          // Left outer  -- Left outer manifests all rows for the left table. The left gather map
          //   must contain valid indices, so we skip the check for the left side.
          val leftOutOfBoundsPolicy = joinType match {
            case _: InnerLike | LeftOuter => OutOfBoundsPolicy.DONT_CHECK
            case _ => OutOfBoundsPolicy.NULLIFY
          }
          JoinGatherer(lazyLeftMap, leftData, leftOutOfBoundsPolicy)
      }
      val rightMap = if (rightData.numCols == 0 || !maps.hasRight) {
        None
      } else {
        Some(maps.right)
      }
      val gatherer = rightMap match {
        case None if joinType == RightOuter && rightData.numCols > 0 =>
          // Distinct right outer joins only produce a single gather map since right table rows
          // are not rearranged by the join.
          MultiJoinGather(leftGatherer, new JoinGathererSameTable(rightData))
        case None =>
          // When there isn't a `rightMap` we are in either LeftSemi or LeftAnti joins.
          // In these cases, the map and the table are both the left side, and everything in the map
          // is a match on the left table, so we don't want to check for bounds.
          rightData.close()
          leftGatherer
        case Some(right) =>
          // Inner joins -- manifest the intersection of both left and right sides. The gather maps
          //   contain the number of rows that must be manifested, and every index
          //   must be within bounds, so we can skip the bounds checking.
          //
          // Right outer -- Is the opposite from left outer (skip right bounds check, keep left)
          val rightOutOfBoundsPolicy = joinType match {
            case _: InnerLike | RightOuter => OutOfBoundsPolicy.DONT_CHECK
            case _ => OutOfBoundsPolicy.NULLIFY
          }
          val lazyRightMap = LazySpillableGatherMap(right, "right_map")
          val rightGatherer = JoinGatherer(lazyRightMap, rightData, rightOutOfBoundsPolicy)
          MultiJoinGather(leftGatherer, rightGatherer)
      }
      if (gatherer.isDone) {
        // Nothing matched...
        gatherer.close()
        None
      } else {
        Some(gatherer)
      }
    }
  }
}
