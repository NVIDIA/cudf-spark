/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Consumer, IntUnaryOperator}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.{SpillableDeviceBufferHandle, SpillableHandle}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockId

/** Identifier for a shuffle buffer that holds the data for a table */
case class ShuffleBufferId(
    blockId: ShuffleBlockId,
    tableId: Int) {
  val shuffleId: Int = blockId.shuffleId
  val mapId: Long = blockId.mapId
}

/** Catalog for lookup of shuffle buffers by block ID */
class ShuffleBufferCatalog extends Logging {
  /**
   * Information stored for each active shuffle.
   * A shuffle block can be comprised of multiple batches. Each batch
   * is given a `ShuffleBufferId`.
   */
  private type ShuffleInfo =
    ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[ShuffleBufferId]]

  private val bufferIdToHandle =
    new ConcurrentHashMap[
      ShuffleBufferId,
      (Option[SpillableDeviceBufferHandle], TableMeta)]()

  /** shuffle information for each active shuffle */
  private[this] val activeShuffles = new ConcurrentHashMap[Int, ShuffleInfo]

  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  private def trackCachedHandle(
      bufferId: ShuffleBufferId,
      handle: SpillableDeviceBufferHandle,
      meta: TableMeta): Unit = {
    bufferIdToHandle.put(bufferId, (Some(handle), meta))
  }

  private def trackDegenerate(bufferId: ShuffleBufferId,
                              meta: TableMeta): Unit = {
    bufferIdToHandle.put(bufferId, (None, meta))
  }

  def removeCachedHandles(): Unit = {
    val bufferIt = bufferIdToHandle.keySet().iterator()
    while (bufferIt.hasNext) {
      val buffer = bufferIt.next()
      val (maybeHandle, _) = bufferIdToHandle.remove(buffer)
      tableMap.remove(buffer.tableId)
      maybeHandle.foreach(_.close())
    }
  }

  /**
   * Adds a contiguous table shuffle table to the device storage. This does NOT take ownership of
   * the contiguous table, so it is the responsibility of the caller to close it.
   * The refcount of the underlying device buffer will be incremented so the contiguous table
   * can be closed before this buffer is destroyed.
   *
   * @param blockId              Spark's `ShuffleBlockId` that identifies this buffer
   * @param contigTable          contiguous table to track in storage
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return RapidsBufferHandle identifying this table
   */
  def addContiguousTable(blockId: ShuffleBlockId,
                         contigTable: ContiguousTable,
                         initialSpillPriority: Long): Unit = {
    withResource(contigTable) { _ =>
      val bufferId = nextShuffleBufferId(blockId)
      val tableMeta = MetaUtils.buildTableMeta(bufferId.tableId, contigTable)
      val buff = contigTable.getBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff, initialSpillPriority)
      trackCachedHandle(bufferId, handle, tableMeta)
    }
  }

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   *
   * @param blockId              Spark's `ShuffleBlockId` that identifies this buffer
   * @param compressedBatch      Compressed ColumnarBatch
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return RapidsBufferHandle associated with this buffer
   */
  def addCompressedBatch(
    blockId: ShuffleBlockId,
    compressedBatch: ColumnarBatch,
    initialSpillPriority: Long): Unit = {
    withResource(compressedBatch) { _ =>
      val bufferId = nextShuffleBufferId(blockId)
      val compressed = compressedBatch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val tableMeta = compressed.getTableMeta
      // update the table metadata for the buffer ID generated above
      tableMeta.bufferMeta().mutateId(bufferId.tableId)
      val buff = compressed.getTableBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff, initialSpillPriority)
      trackCachedHandle(bufferId, handle, tableMeta)
    }
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same block ID (extremely unlikely)
   */
  def addDegenerateRapidsBuffer(
      blockId: ShuffleBlockId,
      meta: TableMeta): Unit = {
    val bufferId = nextShuffleBufferId(blockId)
    trackDegenerate(bufferId, meta)
  }

  /**
   * Register a new shuffle.
   * This must be called before any buffer identifiers associated with this shuffle can be tracked.
   * @param shuffleId shuffle identifier
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffles.computeIfAbsent(shuffleId, _ => new ShuffleInfo)
  }

  /** Frees all buffers that correspond to the specified shuffle. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    // This might be called on a background thread that has not set the device yet.
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)

    val info = activeShuffles.remove(shuffleId)
    if (info != null) {
      val bufferRemover: Consumer[ArrayBuffer[ShuffleBufferId]] = { bufferIds =>
        // NOTE: Not synchronizing array buffer because this shuffle should be inactive.
        bufferIds.foreach { id =>
          tableMap.remove(id.tableId)
          val handleAndMeta = bufferIdToHandle.remove(id)
          handleAndMeta._1.foreach(_.close())
        }
      }
      info.forEachValue(Long.MaxValue, bufferRemover)
    } else {
      // currently shuffle unregister can get called on the driver which never saw a register
      if (!TrampolineUtil.isDriver(SparkEnv.get)) {
        logWarning(s"Ignoring unregister of unknown shuffle $shuffleId")
      }
    }
  }

  def hasActiveShuffle(shuffleId: Int): Boolean = activeShuffles.containsKey(shuffleId)

  /** Get all the buffer IDs that correspond to a shuffle block identifier. */
  private def blockIdToBuffersIds(blockId: ShuffleBlockId): Array[ShuffleBufferId] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle ${blockId.shuffleId}")
    }
    val entries = info.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized {
      entries.toArray
    }
  }

  def getColumnarBatchIterator(
    blockId: ShuffleBlockId,
    sparkTypes: Array[DataType]): Iterator[ColumnarBatch] = {
    val bufferIDs = blockIdToBuffersIds(blockId)
    bufferIDs.iterator.map { bId =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get)
      val (maybeHandle, meta) = bufferIdToHandle.get(bId)
      maybeHandle.map { handle =>
        withResource(handle.materialize()) { buff =>
          val bufferMeta = meta.bufferMeta()
          if (bufferMeta == null || bufferMeta.codecBufferDescrsLength == 0) {
            MetaUtils.getBatchFromMeta(buff, meta, sparkTypes)
          } else {
            GpuCompressedColumnVector.from(buff, meta)
          }
        }
      }.getOrElse {
        // degenerate table (handle is None)
        // make a batch out of denegerate meta
        val rowCount = meta.rowCount
        val packedMeta = meta.packedMetaAsByteBuffer()
        if (packedMeta != null) {
          withResource(DeviceMemoryBuffer.allocate(0)) { deviceBuffer =>
            withResource(Table.fromPackedTable(
              meta.packedMetaAsByteBuffer(), deviceBuffer)) { table =>
              GpuColumnVectorFromBuffer.from(table, deviceBuffer, meta, sparkTypes)
            }
          }
        } else {
          // no packed metadata, must be a table with zero columns
          new ColumnarBatch(Array.empty, rowCount.toInt)
        }
      }
    }
  }

  /** Get all the buffer metadata that correspond to a shuffle block identifier. */
  def blockIdToMetas(blockId: ShuffleBlockId): Seq[TableMeta] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle ${blockId.shuffleId}")
    }
    val entries = info.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized { 
      entries.map(bufferIdToHandle.get).map { case (_, meta) =>
        meta
      }
    }.toSeq
  }

  /** Allocate a new shuffle buffer identifier and update the shuffle block mapping. */
  private def nextShuffleBufferId(blockId: ShuffleBlockId): ShuffleBufferId = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new IllegalStateException(s"unknown shuffle ${blockId.shuffleId}")
    }

    val tableId = tableIdCounter.getAndUpdate(ShuffleBufferCatalog.TABLE_ID_UPDATER)
    val id = ShuffleBufferId(blockId, tableId)
    val prev = tableMap.put(tableId, id)
    if (prev != null) {
      throw new IllegalStateException(s"table ID $tableId is already in use")
    }

    // associate this new buffer with the shuffle block
    val blockBufferIds = info.computeIfAbsent(blockId, _ =>
      new ArrayBuffer[ShuffleBufferId])
    blockBufferIds.synchronized {
      blockBufferIds.append(id)
    }
    id
  }

  /** Lookup the shuffle buffer handle that corresponds to the specified table identifier. */
  def getShuffleBufferHandle(tableId: Int): RapidsShuffleHandle = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      throw new NoSuchElementException(s"unknown table ID $tableId")
    }
    val (maybeHandle, meta) = bufferIdToHandle.get(shuffleBufferId)
    maybeHandle match {
      case Some(spillable) =>
        RapidsShuffleHandle(spillable, meta)
      case None =>
        throw new IllegalStateException(
          "a buffer handle could not be obtained for a degenerate buffer")
    }
  }

  /**
   * Update the spill priority of a shuffle buffer that soon will be read locally.
   * @param handle shuffle buffer handle of buffer to update
   */
  // TODO: AB: priorities
  //def updateSpillPriorityForLocalRead(handle: RapidsBufferHandle): Unit = {
  //  handle.setSpillPriority(SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
  //}

  /**
   * Remove a buffer and table given a buffer handle
   * NOTE: This function is not thread safe! The caller should only invoke if
   * the handle being removed is not being utilized by another thread.
   * @param handle buffer handle
   */
  def removeBuffer(handle: SpillableHandle): Unit = {
    handle.close()
  }
}

object ShuffleBufferCatalog {
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }
}
