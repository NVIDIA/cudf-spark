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

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Consumer, IntUnaryOperator}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.rapids.ShuffleOutputGoneException
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

/**
 * Catalog for lookup of shuffle buffers by block ID
 *
 * @param mapIdsCanRepeat whether attempts of a map share its map id, as they do with
 *                        spark.shuffle.useOldFetchProtocol=true; see commitMapOutput
 */
class ShuffleBufferCatalog(val mapIdsCanRepeat: Boolean = false) extends Logging {
  import ShuffleBufferCatalog.ShuffleRegistration

  private val bufferIdToHandle =
    new ConcurrentHashMap[
      ShuffleBufferId,
      (Option[SpillableDeviceBufferHandle], TableMeta)]()

  /** shuffle information for each active shuffle */
  private[this] val activeShuffles = new ConcurrentHashMap[Int, ShuffleRegistration]

  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  /**
   * Shuffles a reclaim removed, for which a later unregister is expected. The set's own monitor
   * guards it, as a leaf lock held for one read or write and never across a free.
   */
  private[this] val reclaimedShuffleIds = new java.util.BitSet()

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

  /**
   * Removes the given buffers and closes their handles. The catalog is shared by every map
   * task and shuffle in the executor, so a failed writer passes only the buffers it added.
   * Buffers that are already gone are skipped, which makes a repeated removal harmless.
   */
  def removeCachedHandles(bufferIds: Iterable[ShuffleBufferId]): Unit = {
    bufferIds.groupBy(_.blockId).foreach { case (blockId, ids) =>
      val info = activeShuffles.get(blockId.shuffleId)
      if (info != null) {
        info.blocks.computeIfPresent(blockId, (_, blockBufferIds) => blockBufferIds.synchronized {
          blockBufferIds --= ids
          if (blockBufferIds.isEmpty) null else blockBufferIds
        })
      }
    }
    removeAndCloseBuffers(bufferIds)
  }

  /**
   * Removes the lookup entries of buffers already detached from their blocks, then closes
   * every removed handle, even if one close fails. Once `isStopped` reads true it closes no
   * further handle, and the spill framework's shutdown closes the rest.
   *
   * @return how many lookup entries this removed, and the size of the handles it removed
   */
  private def removeAndCloseBuffers(
      bufferIds: Iterable[ShuffleBufferId],
      isStopped: () => Boolean = () => false): (Int, Long) = {
    val handles = new ArrayBuffer[SpillableDeviceBufferHandle]()
    var removed = 0
    var bytes = 0L
    bufferIds.foreach { id =>
      // table ids wrap around, so only remove the mapping if it is still this buffer's
      tableMap.remove(id.tableId, id)
      val handleAndMeta = bufferIdToHandle.remove(id)
      if (handleAndMeta != null) {
        removed += 1
        handleAndMeta._1.foreach { handle =>
          handles += handle
          bytes += handle.sizeInBytes
        }
      }
    }
    // each close checks for a stop first, so a stop ends the frees at the next handle
    handles.map(handle => (() => if (!isStopped()) handle.close()): AutoCloseable).safeClose()
    (removed, bytes)
  }

  /** Entry counts of bufferIdToHandle, tableMap and the per-block lists, which agree. */
  private[rapids] def bookkeepingSizes: (Int, Int, Int) = {
    var listed = 0
    activeShuffles.values().forEach { (info: ShuffleRegistration) =>
      info.blocks.values().forEach { (blockBufferIds: ArrayBuffer[ShuffleBufferId]) =>
        listed += blockBufferIds.synchronized(blockBufferIds.size)
      }
    }
    (bufferIdToHandle.size, tableMap.size, listed)
  }

  /**
   * Allocates a buffer id for `blockId` and passes it to `register`. The caller learns the id
   * only if this returns, so any failure once the id exists removes what was registered under it.
   * A removal of the shuffle since the id was listed is such a failure, since that removal may
   * have missed what `register` tracked. The adds borrow their input: each takes a reference of
   * its own for the handle it registers, and the producer closes the input.
   */
  private def registerBuffer(blockId: ShuffleBlockId)(
      register: ShuffleBufferId => Unit): ShuffleBufferId = {
    val (bufferId, registration) = nextShuffleBufferId(blockId)
    val rollback: AutoCloseable = () => removeCachedHandles(Seq(bufferId))
    closeOnExcept(rollback) { _ =>
      register(bufferId)
      checkStillListed(bufferId, registration)
    }
    bufferId
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
   * @return the ShuffleBufferId of the new buffer
   */
  def addContiguousTable(blockId: ShuffleBlockId,
                         contigTable: ContiguousTable,
                         initialSpillPriority: Long): ShuffleBufferId = {
    registerBuffer(blockId) { bufferId =>
      val tableMeta = MetaUtils.buildTableMeta(bufferId.tableId, contigTable)
      val buff = contigTable.getBuffer
      buff.incRefCount()
      val handle = closeOnExcept(buff)(_ => SpillableDeviceBufferHandle(buff, initialSpillPriority))
      trackCachedHandle(bufferId, handle, tableMeta)
    }
  }

  /**
   * Adds a compressed batch to the device storage. This does NOT take ownership of the
   * compressed batch, so it is the responsibility of the caller to close it.
   * The refcount of the underlying device buffer will be incremented so the compressed batch
   * can be closed before this buffer is destroyed.
   *
   * @param blockId              Spark's `ShuffleBlockId` that identifies this buffer
   * @param compressedBatch      Compressed ColumnarBatch
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return the ShuffleBufferId of the new buffer
   */
  def addCompressedBatch(
    blockId: ShuffleBlockId,
    compressedBatch: ColumnarBatch,
    initialSpillPriority: Long): ShuffleBufferId = {
    registerBuffer(blockId) { bufferId =>
      val compressed = compressedBatch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val tableMeta = compressed.getTableMeta
      // update the table metadata for the buffer ID generated above
      tableMeta.bufferMeta().mutateId(bufferId.tableId)
      val buff = compressed.getTableBuffer
      buff.incRefCount()
      val handle = closeOnExcept(buff)(_ => SpillableDeviceBufferHandle(buff, initialSpillPriority))
      trackCachedHandle(bufferId, handle, tableMeta)
    }
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same block ID (extremely unlikely)
   * @return the ShuffleBufferId of the new buffer
   */
  def addDegenerateRapidsBuffer(
      blockId: ShuffleBlockId,
      meta: TableMeta): ShuffleBufferId = {
    val (bufferId, registration) = nextShuffleBufferId(blockId)
    trackDegenerate(bufferId, meta)
    val rollback: AutoCloseable = () => removeCachedHandles(Seq(bufferId))
    closeOnExcept(rollback)(_ => checkStillListed(bufferId, registration))
    bufferId
  }

  /**
   * Register a new shuffle.
   * This must be called before any buffer identifiers associated with this shuffle can be tracked.
   * @param shuffleId shuffle identifier
   * @param writerTaskId the task attempt id of the map task that writes through the registration
   */
  def registerShuffle(shuffleId: Int, writerTaskId: Long): Unit = {
    activeShuffles.compute(shuffleId, (_, existing) => {
      val registration =
        if (existing == null) new ShuffleRegistration(mapIdsCanRepeat) else existing
      registration.recordWriter(writerTaskId)
      registration
    })
  }

  /**
   * Registers a shuffle for a caller that passes no task id, as a writer at `Long.MaxValue`, so
   * only a reclaim at that watermark removes the registration. An overload, unlike a default
   * argument, lets `registerShuffle` still pass as a function of the shuffle id.
   */
  def registerShuffle(shuffleId: Int): Unit = registerShuffle(shuffleId, Long.MaxValue)

  /**
   * Frees what this executor holds of a shuffle when every writer that registered it is at or
   * below `watermark`, by removing the registration whole. While a writer above the watermark is
   * registered, it frees nothing, since that writer's output shares the registration. Only a UCX
   * shuffle cleanup endpoint reclaims, so a CACHE_ONLY catalog never does.
   *
   * @param shuffleId the shuffle to reclaim
   * @param watermark the largest task attempt id whose output may be freed
   * @param isStopped read before each buffer's free. Once it reads true, the reclaim frees no
   *                  further buffer, and the spill framework's shutdown closes the rest, which
   *                  the reclaim has already removed from the catalog and still counts.
   * @return what the reclaim freed
   */
  def reclaimShuffle(
      shuffleId: Int,
      watermark: Long,
      isStopped: () => Boolean = () => false): ShuffleReclaimResult = {
    // the frees need the device, which a caller's thread may not have set
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)
    val startNanos = System.nanoTime()
    var deferred = false
    val detached = removeRegistration(shuffleId) { registration =>
      val reclaimable = registration.mayReclaimAt(watermark)
      if (reclaimable) {
        // marked before the removal, so a removal that finds no registration finds the mark
        reclaimedShuffleIds.synchronized(reclaimedShuffleIds.set(shuffleId))
      } else {
        deferred = true
      }
      reclaimable
    }
    // frees are slow, so they run outside every map lock
    val (buffers, bytes) = detached
      .map(registration => removeAndCloseBuffers(registration.listedBufferIds, isStopped))
      .getOrElse((0, 0L))
    val outcome = if (deferred) {
      ShuffleReclaimOutcome.DEFERRED
    } else if (buffers > 0) {
      ShuffleReclaimOutcome.REMOVED
    } else {
      ShuffleReclaimOutcome.ABSENT
    }
    ShuffleReclaimResult(shuffleId, watermark, outcome, buffers, bytes,
      System.nanoTime() - startNanos, None)
  }

  /** Frees all buffers that correspond to the specified shuffle. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    // This might be called on a background thread that has not set the device yet.
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)

    removeRegistration(shuffleId)(_ => true) match {
      case Some(info) =>
        removeAndCloseBuffers(info.listedBufferIds)
      case None =>
        val reclaimed = reclaimedShuffleIds.synchronized(reclaimedShuffleIds.get(shuffleId))
        if (reclaimed) {
          logDebug(s"Ignoring unregister of reclaimed shuffle $shuffleId")
        } else if (!TrampolineUtil.isDriver(SparkEnv.get)) {
          // currently shuffle unregister can get called on the driver which never saw a register
          logWarning(s"Ignoring unregister of unknown shuffle $shuffleId")
        }
    }
  }

  /**
   * Takes a shuffle's registration out of `activeShuffles` when `shouldRemove` approves it, and
   * marks it removed first, inside the same map operation, so an old-protocol commit lands
   * wholly before the removal or fails. Every removal of a registration goes through here.
   *
   * @return the registration removed, if any
   */
  private def removeRegistration(shuffleId: Int)(
      shouldRemove: ShuffleRegistration => Boolean): Option[ShuffleRegistration] = {
    var removed: ShuffleRegistration = null
    activeShuffles.computeIfPresent(shuffleId, (_, registration) => {
      if (shouldRemove(registration)) {
        registration.markRemoved()
        removed = registration
        null
      } else {
        registration
      }
    })
    Option(removed)
  }

  def hasActiveShuffle(shuffleId: Int): Boolean = activeShuffles.containsKey(shuffleId)

  /** Get all the buffer IDs that correspond to a shuffle block identifier. */
  private def blockIdToBuffersIds(blockId: ShuffleBlockId): Array[ShuffleBufferId] = {
    if (mapIdsCanRepeat) {
      committedBufferIds(blockId)
    } else {
      val info = activeShuffles.get(blockId.shuffleId)
      if (info == null) {
        throw new ShuffleOutputGoneException(
          s"unknown shuffle ${blockId.shuffleId} for block $blockId")
      }
      val entries = info.blocks.get(blockId)
      if (entries == null) {
        throw new ShuffleOutputGoneException(s"unknown shuffle block $blockId")
      }
      entries.synchronized {
        entries.toArray
      }
    }
  }

  /**
   * Looks up a buffer's handle and metadata.
   *
   * @param bId a buffer id that `blockIdToBuffersIds` returned for the buffer's block
   * @return the handle, `None` for a degenerate table, and the metadata
   * @note throws `ShuffleOutputGoneException` if a removal dropped the buffer after that lookup
   */
  private def liveEntry(
      bId: ShuffleBufferId): (Option[SpillableDeviceBufferHandle], TableMeta) = {
    val entry = bufferIdToHandle.get(bId)
    if (entry == null) {
      throw new ShuffleOutputGoneException(
        s"unknown table ID ${bId.tableId} of shuffle block ${bId.blockId}")
    }
    entry
  }

  def getColumnarBatchIterator(
    blockId: ShuffleBlockId,
    sparkTypes: Array[DataType]): Iterator[ColumnarBatch] = {
    val bufferIDs = blockIdToBuffersIds(blockId)
    bufferIDs.iterator.map { bId =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get)
      val (maybeHandle, meta) = liveEntry(bId)
      maybeHandle.map { handle =>
        val materialized = try {
          handle.materialize()
        } catch {
          // A closed handle throws IllegalStateException, and a spill file deleted before it
          // opened IOException. Either means the output is gone only if the catalog let go of
          // the buffer, as in BufferSendState.
          case e @ (_: IllegalStateException | _: IOException)
              if isShuffleBufferRemoved(bId.tableId, handle) =>
            throw new ShuffleOutputGoneException(
              s"shuffle output for table ${bId.tableId} of shuffle block ${bId.blockId} is gone", e)
        }
        withResource(materialized) { buff =>
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
  def blockIdToMetas(blockId: ShuffleBlockId): Seq[TableMeta] =
    blockIdToBuffersIds(blockId).map(liveEntry(_)._2).toSeq

  /**
   * Commits the output of one map attempt, when map ids can repeat. Every attempt of a map then
   * writes the same blocks, so readers are served only the first output committed for the map on
   * this executor, as Spark's IndexShuffleBlockResolver keeps the first committed attempt. A
   * later attempt's buffers are removed, and its `sizes` are overwritten with the committed
   * output's, which its MapStatus then reports.
   *
   * @param bufferIds every buffer the attempt added, which the catalog owns once this returns
   * @param sizes the attempt's size of each partition, as its MapStatus reports them
   * @throws java.lang.IllegalStateException if the shuffle's registration is gone, is removed
   *                                         before the commit records its output, or does not
   *                                         list every buffer, leaving the buffers with the
   *                                         caller
   */
  def commitMapOutput(
      shuffleId: Int,
      mapId: Long,
      bufferIds: Iterable[ShuffleBufferId],
      sizes: Array[Long]): Unit = {
    val info = activeShuffles.get(shuffleId)
    if (info == null) {
      throw ShuffleBufferCatalog.cleanedUpBeforeCommit(shuffleId, mapId)
    }
    info.commitMapOutput(shuffleId, mapId, bufferIds, sizes).foreach { committed =>
      logInfo(s"Map $mapId of shuffle $shuffleId already has a committed output on this " +
        s"executor, discarding the ${bufferIds.size} buffers of a later attempt")
      committed.copySizesTo(sizes)
      removeCachedHandles(bufferIds)
    }
  }

  /** The buffers readers are served for a block when map ids can repeat: its committed ones. */
  private def committedBufferIds(blockId: ShuffleBlockId): Array[ShuffleBufferId] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new ShuffleOutputGoneException(
        s"unknown shuffle ${blockId.shuffleId} for block $blockId")
    }
    val ids = info.committedOutput(blockId.mapId).flatMap(_.bufferIds(blockId.reduceId))
    ids.getOrElse(throw new ShuffleOutputGoneException(s"unknown shuffle block $blockId"))
  }

  /**
   * Allocate a new shuffle buffer identifier and update the shuffle block mapping. Returns the
   * registration the id was listed in too, so the add can check that no removal took it since.
   */
  private def nextShuffleBufferId(
      blockId: ShuffleBlockId): (ShuffleBufferId, ShuffleRegistration) = {
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

    // associate this new buffer with the shuffle block, inside compute so that
    // removeCachedHandles cannot drop the block's list while the id is being added
    info.blocks.compute(blockId, (_, existing) => {
      val blockBufferIds = if (existing == null) new ArrayBuffer[ShuffleBufferId] else existing
      blockBufferIds.synchronized {
        blockBufferIds.append(id)
      }
      blockBufferIds
    })
    (id, info)
  }

  /**
   * Fails an add whose shuffle was removed after `nextShuffleBufferId` listed its buffer, since
   * the removal may have collected the buffer's id before the add tracked its handle.
   */
  private def checkStillListed(
      bufferId: ShuffleBufferId,
      registration: ShuffleRegistration): Unit = {
    if (activeShuffles.get(bufferId.shuffleId) ne registration) {
      throw new IllegalStateException(
        s"shuffle ${bufferId.shuffleId} was removed during an add to ${bufferId.blockId}")
    }
  }

  /** Lookup the shuffle buffer handle that corresponds to the specified table identifier. */
  def getShuffleBufferHandle(tableId: Int): RapidsShuffleHandle = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      throw new ShuffleOutputGoneException(s"unknown table ID $tableId")
    }
    // null if an unregister removed the buffer since the line above
    val handleAndMeta = bufferIdToHandle.get(shuffleBufferId)
    if (handleAndMeta == null) {
      throw new ShuffleOutputGoneException(s"unknown table ID $tableId")
    }
    handleAndMeta match {
      case (Some(spillable), meta) =>
        RapidsShuffleHandle(spillable, meta)
      case (None, _) =>
        throw new IllegalStateException(
          "a buffer handle could not be obtained for a degenerate buffer")
    }
  }

  /**
   * Whether the catalog no longer holds a buffer that a lookup of `tableId` returned, through
   * `getShuffleBufferHandle` or a local read. Every catalog close removes the entries before
   * closing the buffer, so a buffer found closed after its lookup is gone if, and only if, this
   * is true.
   */
  def isShuffleBufferRemoved(tableId: Int, spillable: SpillableDeviceBufferHandle): Boolean = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      true
    } else {
      val handleAndMeta = bufferIdToHandle.get(shuffleBufferId)
      // table ids wrap around, so the id may name another buffer by now
      handleAndMeta == null || !handleAndMeta._1.exists(_ eq spillable)
    }
  }

  /*
   * Update the spill priority of a shuffle buffer that soon will be read locally.
   * @param handle shuffle buffer handle of buffer to update
   */
  // TODO: AB: priorities
  //def updateSpillPriorityForLocalRead(handle: RapidsBufferHandle): Unit = {
  //  handle.setSpillPriority(SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
  //}
}

/** One attempt's whole output for a map id, immutable once built. */
private final class CommittedMapOutput(bufferIds: Iterable[ShuffleBufferId], sizes: Array[Long]) {
  // copies, so the writer may reuse what it passed in
  private val bufferIdsByReduceId: Map[Int, Array[ShuffleBufferId]] =
    bufferIds.groupBy(_.blockId.reduceId).map { case (reduceId, ids) => reduceId -> ids.toArray }
  // only the non-zero sizes, so a map does not keep one entry per partition
  private val nonEmptyReduceIds: Array[Int] = sizes.indices.filter(sizes(_) != 0).toArray
  private val nonEmptySizes: Array[Long] = nonEmptyReduceIds.map(sizes(_))

  /** This output's buffers for a reduce id, in the order they were added. */
  def bufferIds(reduceId: Int): Option[Array[ShuffleBufferId]] = bufferIdsByReduceId.get(reduceId)

  /** Overwrites every entry of `dest` with this output's size for that partition. */
  def copySizesTo(dest: Array[Long]): Unit = {
    java.util.Arrays.fill(dest, 0L)
    nonEmptyReduceIds.indices.foreach(i => dest(nonEmptyReduceIds(i)) = nonEmptySizes(i))
  }
}

object ShuffleBufferCatalog {
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }

  /**
   * Information stored for each active shuffle. A removal takes all of it at once, so a later
   * registration of the shuffle starts with no blocks and no committed outputs.
   *
   * The registration's monitor orders commits against removals. A removal takes it inside the
   * `activeShuffles` operation that removes the registration, and a commit holds it while it
   * takes block-list monitors, so the locks nest in that order only.
   */
  private[rapids] final class ShuffleRegistration(mapIdsCanRepeat: Boolean) {
    /**
     * A shuffle block can be comprised of multiple batches, each given a `ShuffleBufferId`. The
     * lists hold every buffer not yet removed, committed or not, and cleanup goes through them.
     */
    val blocks = new ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[ShuffleBufferId]]

    /** Only when map ids can repeat: the output readers are served for each map id. */
    private val committedOutputs: Option[ConcurrentHashMap[Long, CommittedMapOutput]] =
      if (mapIdsCanRepeat) Some(new ConcurrentHashMap[Long, CommittedMapOutput]) else None

    /** Whether a removal took this registration. Guarded by this registration's monitor. */
    private var removed = false

    /**
     * The largest task attempt id of a writer that registered the shuffle. `recordWriter` runs
     * only inside `activeShuffles.compute` and `mayReclaimAt` only inside
     * `activeShuffles.computeIfPresent`, for the same key, so the key's bin lock orders the two.
     */
    private var maxWriterTaskId: Long = Long.MinValue

    private[ShuffleBufferCatalog] def recordWriter(taskId: Long): Unit =
      maxWriterTaskId = math.max(maxWriterTaskId, taskId)

    /** Whether every writer that registered the shuffle is at or below `watermark`. */
    private[ShuffleBufferCatalog] def mayReclaimAt(watermark: Long): Boolean =
      maxWriterTaskId <= watermark

    /** Called by a removal before the registration leaves `activeShuffles`. */
    private[ShuffleBufferCatalog] def markRemoved(): Unit = synchronized {
      removed = true
    }

    /**
     * Records an attempt's output for `mapId`, unless an earlier attempt's is already recorded.
     * A commit then lands wholly before any removal of this registration, or fails.
     *
     * @return the output recorded earlier for the map, if there is one
     * @throws java.lang.IllegalStateException if a removal took this registration, or it does
     *                                         not list every buffer in `bufferIds`
     */
    private[ShuffleBufferCatalog] def commitMapOutput(
        shuffleId: Int,
        mapId: Long,
        bufferIds: Iterable[ShuffleBufferId],
        sizes: Array[Long]): Option[CommittedMapOutput] = {
      // built before the monitor, which a removal waits for while it holds its map bin
      val output = new CommittedMapOutput(bufferIds, sizes)
      val idsByBlock = bufferIds.groupBy(_.blockId)
      synchronized {
        if (removed || !listsAll(idsByBlock)) {
          throw cleanedUpBeforeCommit(shuffleId, mapId)
        }
        Option(committedOutputs.get.putIfAbsent(mapId, output))
      }
    }

    /** The output readers are served for `mapId`, if one is committed. */
    private[ShuffleBufferCatalog] def committedOutput(mapId: Long): Option[CommittedMapOutput] =
      Option(committedOutputs.get.get(mapId))

    /**
     * Whether this registration lists every buffer in `idsByBlock`. Each block's list is copied
     * into a set once, since a map can add many batches to one block.
     */
    private def listsAll(idsByBlock: Map[ShuffleBlockId, Iterable[ShuffleBufferId]]): Boolean =
      idsByBlock.forall { case (blockId, ids) =>
        val blockBufferIds = blocks.get(blockId)
        blockBufferIds != null && {
          val listed = blockBufferIds.synchronized(blockBufferIds.toSet)
          ids.forall(listed.contains)
        }
      }

    /** Every buffer this registration lists, once a removal has taken it out of the catalog. */
    private[ShuffleBufferCatalog] def listedBufferIds: Iterable[ShuffleBufferId] = {
      val bufferIds = new ArrayBuffer[ShuffleBufferId]()
      val idCollector: Consumer[ArrayBuffer[ShuffleBufferId]] = { blockBufferIds =>
        // a failed writer of this shuffle can still be removing its own buffers
        bufferIds ++= blockBufferIds.synchronized(blockBufferIds.toList)
      }
      blocks.forEachValue(Long.MaxValue, idCollector)
      bufferIds
    }
  }

  /** The failure of a commit that finds its shuffle's registration gone. */
  private def cleanedUpBeforeCommit(shuffleId: Int, mapId: Long): IllegalStateException =
    new IllegalStateException(s"Shuffle $shuffleId was cleaned up on this executor " +
      s"before map $mapId committed its output")
}
