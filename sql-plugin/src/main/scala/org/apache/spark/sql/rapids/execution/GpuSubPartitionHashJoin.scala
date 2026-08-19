/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{GpuBatchUtils, GpuColumnVector, GpuExpression, GpuHashPartitioner, GpuMetric, NvtxRegistry, RmmRapidsRetryIterator, SpillableColumnarBatch, SpillPriorities, TaskAutoCloseableResource}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.rapids.{GpuHashExpression, GpuMurmur3Hash}
import org.apache.spark.sql.rapids.shims.DataTypeUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuSubPartitionHashJoin {
  /**
   * Concatenate the input batches into a single one.
   * The caller is responsible for closing the returned batch.
   *
   * @param spillBatches the batches to be concatenated, will be closed after the call
   *                     returns.
   * @return the concatenated SpillableColumnarBatch or None if the input is empty.
   */
  def concatSpillBatchesAndClose(
      spillBatches: Seq[SpillableColumnarBatch]): Option[SpillableColumnarBatch] = {
    GpuBatchUtils.concatSpillBatchesAndClose(spillBatches)
  }

  /**
   * Create an iterator that takes over the input resources, and make sure closing
   * all the resources when needed.
   */
  def safeIteratorFromSeq[R <: AutoCloseable](closeables: Seq[R]): Iterator[R] = {
    new Iterator[R] with TaskAutoCloseableResource {

      private[this] val remainingCloseables: ArrayBuffer[R] =
        ArrayBuffer(closeables: _*)

      override def hasNext: Boolean = remainingCloseables.nonEmpty && !closed

      override def next(): R = {
        if (!hasNext) throw new NoSuchElementException()
        remainingCloseables.remove(0)
      }

      override def close(): Unit = {
        remainingCloseables.safeClose()
        remainingCloseables.clear()
        super.close()
      }
    }
  }
}

/**
 * Drain the batches in the input iterator and partition each batch into smaller parts.
 * It assumes all the batches are on GPU.
 * e.g. There are two batches as below
 *     (2,4,6), (6,8,8),
 * and split them into 6 partitions. The result will be
 *   0 -> (2)
 *   1 -> (6), (6) (two separate not merged sub batches)
 *   2 -> empty
 *   3 -> (4)
 *   4 -> empty
 *   5 -> (8, 8) (A single batch)
 */
class GpuBatchSubPartitioner(
    inputIter: Iterator[ColumnarBatch],
    inputBoundKeys: Seq[GpuExpression],
    numPartitions: Int,
    hashSeed: Int,
    name: String = "GpuBatchSubPartitioner") extends GpuHashPartitioner
  with AutoCloseable with Logging {

  private var isNotInited = true
  private var numCurBatches = 0
  // At least two partitions
  private val realNumPartitions = Math.max(2, numPartitions)
  private val pendingParts =
    Array.fill(realNumPartitions)(ArrayBuffer.empty[SpillableColumnarBatch])

  override protected val hashFunc: GpuHashExpression = GpuMurmur3Hash(inputBoundKeys, hashSeed)

  /** The actual count of partitions */
  def partitionsCount: Int = realNumPartitions

  /**
   * Get the count of remaining batches (nonempty) in all the partitions currently.
   */
  def batchesCount: Int = {
    initPartitions()
    numCurBatches
  }

  /**
   * Get a partition data as a Seq of SpillableColumnarBatch. The caller should NOT close
   * the returned batches.
   * If the caller wants to own the returned batches, call `releaseBatchByPartition` instead.
   * @param partId the partition id. An exception will be raised up if it is out of range.
   * @return a Seq of SpillableColumnarBatch if the given "partId" is in range, or null if
   *         the partition has been released.
   */
  def getBatchesByPartition(partId: Int): Seq[SpillableColumnarBatch] = {
    initPartitions()
    val ret = pendingParts(partId)
    if (ret != null) {
      ret.toSeq
    } else {
      null
    }
  }

  /**
   * Release a partition data as a Seq of SpillableColumnarBatch, and the caller is
   * responsible for closing the returned batch.
   *
   * @param partId the partition id. An exception will be raised up if it is out of range.
   * @return a Seq of SpillableColumnarBatch if the given "partId" is in range, or null if
   *         the partition has been released.
   */
  def releaseBatchesByPartition(partId: Int): Seq[SpillableColumnarBatch] = {
    initPartitions()
    val ret = pendingParts(partId)
    if (ret != null) {
      numCurBatches -= ret.length
      pendingParts(partId) = null
      ret.toSeq
    } else {
      null
    }
  }

  override def close(): Unit = {
    pendingParts.filterNot(_ == null).flatten.safeClose()
    // batches are closed, safe to set all to null
    (0 until realNumPartitions).foreach(releaseBatchesByPartition)
  }

  private def initPartitions(): Unit = {
    if (isNotInited) {
      partitionBatches()
      isNotInited = false
      logDebug(subPartitionsInfoAsString())
    }
  }

  private def subPartitionsInfoAsString(): String = {
    val stringBuilder = new mutable.StringBuilder()
    stringBuilder.append(s"$name subpartitions: \n")
    stringBuilder.append(s"    Part Id        Part Size        Batch number\n")
    pendingParts.zipWithIndex.foreach { case (part, id) =>
      val batchNum = if (part == null) 0 else part.length
      val partSize = if (part == null) 0L else part.map(_.sizeInBytes).sum
      stringBuilder.append(s"    $id        $partSize        $batchNum\n")
    }
    stringBuilder.toString()
  }

  private[this] def partitionBatches(): Unit = {
    while (inputIter.hasNext) {
      val gpuBatch = inputIter.next()
      if (gpuBatch.numRows() > 0 && gpuBatch.numCols() > 0) {
        val types = GpuColumnVector.extractTypes(gpuBatch)
        // 1) Hash partition on the batch
        val partedTable = hashPartitionAndClose(gpuBatch, realNumPartitions,
            NvtxRegistry.SUB_JOIN_PART)
        val (spillBatch, partitions) = withResource(partedTable) { _ =>
          // Convert to SpillableColumnarBatch for the following retry.
          (SpillableColumnarBatch(GpuColumnVector.from(partedTable.getTable, types),
            SpillPriorities.ACTIVE_BATCHING_PRIORITY),
            partedTable.getPartitions)
        }
        // 2) Split into smaller tables according to partitions
        val subTables = RmmRapidsRetryIterator.withRetryNoSplit(spillBatch) { _ =>
          withResource(spillBatch.getColumnarBatch()) { cb =>
            withResource(GpuColumnVector.from(cb)) { tbl =>
              tbl.contiguousSplit(partitions.tail: _*)
            }
          }
        }
        // 3) Make each smaller table spillable and cache them in the queue
        withResource(subTables) { _ =>
          subTables.zipWithIndex.foreach { case (table, id) =>
            // skip empty tables
            if (table.getRowCount > 0) {
              subTables(id) = null
              pendingParts(id) += SpillableColumnarBatch(table, types,
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
              numCurBatches += 1
            }
          }
        }
      } else if (gpuBatch.numRows() > 0 && gpuBatch.numCols() == 0) {
        // Rows only batch. This should never happen for a hash join in Spark.
        gpuBatch.close()
      } else {
        // Skip empty batches
        gpuBatch.close()
      }
    } // end of while
  }
}

/**
 * Iterate all the partitions in the input "batchSubPartitioner," and each call to
 * "next()" will return one or multiple parts as a Seq of "SpillableColumnarBatch",
 * or None for an empty partition, along with its partition id(s).
 */
class GpuBatchSubPartitionIterator(
    batchSubPartitioner: GpuBatchSubPartitioner,
    targetBatchSize: Long)
  extends Iterator[(Seq[Int], Seq[SpillableColumnarBatch])] with Logging {

  // The partitions to be read. Initially it is all the partitions.
  private val remainingPartIds: ArrayBuffer[Int] =
    ArrayBuffer.range(0, batchSubPartitioner.partitionsCount)

  override def hasNext: Boolean = remainingPartIds.nonEmpty

  override def next(): (Seq[Int], Seq[SpillableColumnarBatch]) = {
    if (!hasNext) throw new NoSuchElementException()
    // Get the next partition ids for this output.
    val partIds = nextPartitions()
    // Take over the batches of one or multiple partitions according to the ids.
    closeOnExcept(ArrayBuffer.empty[SpillableColumnarBatch]) { buf =>
      partIds.foreach { pid =>
        buf ++= batchSubPartitioner.releaseBatchesByPartition(pid)
      }
      // Update the remaining partitions
      remainingPartIds --= partIds
      (partIds, buf.toSeq)
    }
  }

  private[this] def nextPartitions(): Seq[Int] = {
    val ret = ArrayBuffer.empty[Int]
    var accPartitionSize = 0L
    // always append the first one.
    val firstPartId = remainingPartIds.head
    val firstPartSize = computePartitionSize(firstPartId)
    ret += firstPartId
    accPartitionSize += firstPartSize
    // For each output, try to collect small nonempty partitions to reach
    // "targetBatchSize" as much as possible.
    // e.g. We have 5 partitions as below. And the "targetBatchSize" is 16 bytes.
    //    p1: (3,4)            -> 8 bytes
    //    p2: (1,2), (11, 22)  -> 16 bytes
    //    p3: (5,6)            -> 8 bytes
    //    p4: <empty>          -> 0 bytes
    //    p5: <empty>          -> 0 bytes
    // Then the p1 and p3 will be put together (8 bytes + 8 bytes) in one output.
    //    next: (3,4), (5,6)
    //    next: (1,2), (11, 22)
    //    next: <empty>
    //    next: <empty>
    if (firstPartSize > 0) {
      remainingPartIds.tail.foreach { partId =>
        val partSize = computePartitionSize(partId)
        // Do not coalesce empty partitions, and output them one by one.
        if (partSize > 0 && (accPartitionSize + partSize) <= targetBatchSize) {
          ret += partId
          accPartitionSize += partSize
        }
      }
    }
    ret.toSeq
  }

  private[this] def computePartitionSize(partId: Int): Long = {
    val batches = batchSubPartitioner.getBatchesByPartition(partId)
    if (batches != null) batches.map(_.sizeInBytes).sum else 0L
  }
}

/**
 * An utils class that will take over the two input resources representing data from
 * build side and stream side separately.
 * build data is passed in as a Option of a single batch, while stream data is as
 * a Seq of batches.
 */
class PartitionPair(
    private var build: Option[SpillableColumnarBatch],
    private var stream: Seq[SpillableColumnarBatch]) extends AutoCloseable {

  /**
   * Whether the PartitionPair pair is empty.
   * A pair is empty when both build and stream are empty.
   */
  def isEmpty: Boolean = build.isEmpty && stream.isEmpty

  /**
   * Get the batches from two sides as a Tuple(build, stream).
   */
  def get: (Option[SpillableColumnarBatch], Seq[SpillableColumnarBatch]) = {
    (build, stream)
  }

  /**
   * Release the batches from two sides as a Tuple(build, stream).
   * Callers should close the returned batches.
   */
  def release: (Option[SpillableColumnarBatch], Seq[SpillableColumnarBatch]) = {
    val ret = (build, stream)
    build = None
    stream = Seq.empty
    ret
  }

  override def close(): Unit = {
    stream.safeClose()
    stream = Seq.empty
    build.foreach(_.safeClose())
    build = None
  }
}

/**
 * Iterator that returns a pair of batches (build side, stream side) with the same key set
 * generated by sub-partitioning algorithm when each call to "next".
 * Each pair may have data from one or multiple partitions. And for build side, batches are
 * concatenated into a single one.
 *
 * It will skip the empty pairs by default. Set "skipEmptyPairs" to false to also get
 * the empty pairs.
 */
class GpuSubPartitionPairIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    numPartitions: Int,
    targetBatchSize: Long,
    skipEmptyPairs: Boolean = true)
  extends Iterator[PartitionPair] with AutoCloseable {

  private[this] var hashSeed = 100

  private[this] var buildSubPartitioner =
    new GpuBatchSubPartitioner(buildIter, boundBuildKeys, numPartitions, hashSeed, "initBuild")
  private[this] var streamSubPartitioner =
    new GpuBatchSubPartitioner(streamIter, boundStreamKeys, numPartitions, hashSeed, "initStream")
  private[this] var buildSubIterator =
    new GpuBatchSubPartitionIterator(buildSubPartitioner, targetBatchSize)

  private val bigBuildBatches = ArrayBuffer.empty[SpillableColumnarBatch]
  private val bigStreamBatches = ArrayBuffer.empty[SpillableColumnarBatch]
  private var repartitioned = false

  private[this] var closed = false

  private[this] var partitionPair: Option[PartitionPair] = None
  private[this] var pairConsumed = true

  override def hasNext: Boolean = {
    if (closed) return false
    if (pairConsumed) {
      do {
        partitionPair.foreach(_.close())
        partitionPair = None
        partitionPair = tryPullNextPair()
      } while (partitionPair.exists(_.isEmpty) && skipEmptyPairs)
      pairConsumed = false
    }
    partitionPair.isDefined
  }

  override def next(): PartitionPair = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val pair = partitionPair.get
    partitionPair = None
    pairConsumed = true
    pair
  }

  override def close(): Unit = if (!closed) {
    closed = true
    // for real safe close
    val e = new Exception()
    buildSubPartitioner.safeClose(e)
    streamSubPartitioner.safeClose(e)
    bigBuildBatches.safeClose(e)
    bigStreamBatches.safeClose(e)
    partitionPair.foreach(_.close())
    partitionPair = None
  }

  private[this] val hasNextBatch: () => Boolean = if (skipEmptyPairs) {
    // Check the batch numbers directly can stop early when the remaining partitions
    // are all empty on both build side and stream side.
    () => buildSubPartitioner.batchesCount > 0 || streamSubPartitioner.batchesCount > 0
  } else {
    () => buildSubIterator.hasNext
  }

  private[this] def tryPullNextPair(): Option[PartitionPair] = {
    var pair: Option[PartitionPair] = None
    var continue = true
    while(continue) {
      if (hasNextBatch()) {
        val (partIds, buildBatches) = buildSubIterator.next()
        val (buildPartsSize, streamBatches) = closeOnExcept(buildBatches) { _ =>
          val batchesSize = buildBatches.map(_.sizeInBytes).sum
          closeOnExcept(ArrayBuffer.empty[SpillableColumnarBatch]) { streamBuf =>
            partIds.foreach { id =>
              streamBuf ++= streamSubPartitioner.releaseBatchesByPartition(id)
            }
            (batchesSize, streamBuf)
          }
        }
        closeOnExcept(streamBatches) { _ =>
          if (!repartitioned && buildPartsSize > targetBatchSize) {
            // Got a partition the size is larger than the target size. Cache it and
            // its corresponding stream batches.
            closeOnExcept(buildBatches)(bigBuildBatches ++= _)
            bigStreamBatches ++= streamBatches
          } else {
            // Got a normal pair, return it
            continue = false
            val buildBatch = GpuSubPartitionHashJoin.concatSpillBatchesAndClose(buildBatches)
            pair = Some(new PartitionPair(buildBatch, streamBatches.toSeq))
          }
        }
      } else if (bigBuildBatches.nonEmpty) {
        // repartition big batches only once by resetting partitioners
        repartitioned = true
        repartition()
      } else {
        // no more data
        continue = false
      }
    }
    pair
  }

  private[this] def repartition(): Unit = {
    hashSeed += 10
    val realNumPartitions = computeNumPartitions(bigBuildBatches)
    // build partitioner
    val buildIt = GpuSubPartitionHashJoin.safeIteratorFromSeq(bigBuildBatches.toSeq)
      .map(scb => withResource(scb){_.getColumnarBatch()})
    bigBuildBatches.clear()
    buildSubPartitioner.safeClose(new Exception())
    buildSubPartitioner = new GpuBatchSubPartitioner(buildIt, boundBuildKeys,
      realNumPartitions, hashSeed, "repartedBuild")
    buildSubIterator = new GpuBatchSubPartitionIterator(buildSubPartitioner, targetBatchSize)

    // stream partitioner
    val streamIt = GpuSubPartitionHashJoin.safeIteratorFromSeq(bigStreamBatches.toSeq)
      .map(scb => withResource(scb){_.getColumnarBatch()})
    bigStreamBatches.clear()
    streamSubPartitioner.safeClose(new Exception())
    streamSubPartitioner = new GpuBatchSubPartitioner(streamIt, boundStreamKeys,
      realNumPartitions, hashSeed, "repartedStream")
  }

  private[this] def computeNumPartitions(parts: ArrayBuffer[SpillableColumnarBatch]): Int = {
    val totalSize = parts.map(_.sizeInBytes).sum
    val realTargetSize = math.max(targetBatchSize, 1)
    val requiredNum = Math.floorDiv(totalSize, realTargetSize).toInt + 1
    math.max(requiredNum, numPartitions)
  }

  /** For test only */
  def isRepartitioned: Boolean = repartitioned
}

/** Base class for joins by sub-partitioning algorithm */
abstract class BaseSubHashJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    numPartitions: Int,
    targetSize: Long,
    opTime: GpuMetric)
  extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  // skip empty partition pairs
  private[this] val subPartitionPairIter = new GpuSubPartitionPairIterator(buildIter,
    boundBuildKeys, streamIter, boundStreamKeys, numPartitions, targetSize)

  private[this] var joinIter: Option[Iterator[ColumnarBatch]] = None
  private[this] var nextCb: Option[ColumnarBatch] = None

  override def close(): Unit = {
    nextCb.foreach(_.safeClose(new Exception))
    nextCb = None
    subPartitionPairIter.close()
    super.close()
  }

  override def hasNext: Boolean = {
    if (closed) return false
    var mayContinue = true
    // Loop to support optimizing out some pairs by returning a None instead
    // of a join iterator.
    while (nextCb.isEmpty && mayContinue) {
      if (joinIter.exists(_.hasNext)) {
        nextCb = joinIter.map(_.next())
      } else {
        val hasNextPair = opTime.ns {
          subPartitionPairIter.hasNext
        }
        if (hasNextPair) {
          // Need to refill the join iterator
          joinIter.foreach {
            case closeable: AutoCloseable => closeable.close()
            case _ => // noop
          }
          joinIter = None
          opTime.ns {
            withResource(subPartitionPairIter.next()) { pair =>
              joinIter = setupJoinIterator(pair)
            }
          }
          // try to pull next batch right away to avoid a loop again
          if (joinIter.exists(_.hasNext)) {
            nextCb = joinIter.map(_.next())
          }
        } else {
          mayContinue = false
        }
      }
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

  protected def setupJoinIterator(pair: PartitionPair): Option[Iterator[ColumnarBatch]]
}

trait GpuSubPartitionHashJoin extends Logging { self: GpuHashJoin =>

  protected lazy val buildSchema: StructType = DataTypeUtilsShim.fromAttributes(buildPlan.output)

  def doJoinBySubPartition(
      builtIter: Iterator[ColumnarBatch],
      streamIter: Iterator[ColumnarBatch],
      joinOptions: JoinOptions,
      numPartitions: Int,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {

    // A log for test to verify that sub-partitioning is used.
    logInfo(s"$joinType hash join is executed by sub-partitioning " +
      s"in task ${TaskContext.get().taskAttemptId()}")

    new BaseSubHashJoinIterator(builtIter, boundBuildKeys, streamIter,
        boundStreamKeys, numPartitions, joinOptions.targetSize, opTime) {

      private[this] def canOptimizeOut(pair: PartitionPair): Boolean = {
        val (build, stream) = pair.get
        joinType match {
          case _: InnerLike =>
            // For inner join, no need to run if either side is empty
            build.isEmpty || stream.isEmpty
          case _ => false
        }
      }

      override def setupJoinIterator(pair: PartitionPair): Option[Iterator[ColumnarBatch]] = {
        if (canOptimizeOut(pair)) {
          // Skip it due to optimization
          None
        } else {
          val (build, stream) = pair.release
          val buildCb = closeOnExcept(stream) { _ =>
            withResource(build) { _ =>
              build.map(_.getColumnarBatch()).getOrElse(GpuColumnVector.emptyBatch(buildSchema))
            }
          }
          val streamIter = closeOnExcept(buildCb) { _ =>
            GpuSubPartitionHashJoin.safeIteratorFromSeq(stream).map { spill =>
              withResource(spill)(_.getColumnarBatch())
            }
          }
          // Leverage the original join iterators
          val joinIter = doJoin(buildCb, streamIter, joinOptions,
            numOutputRows, numOutputBatches, opTime, joinTime)
          Some(joinIter)
        }
      }

    } // end of "new BaseSubHashJoinIterator"
  }
}
