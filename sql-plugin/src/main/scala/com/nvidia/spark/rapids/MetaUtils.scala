/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.google.flatbuffers.FlatBufferBuilder
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockBatchId

object MetaUtils {
  // Used in cases where the `tableId` is not known at meta creation. 
  // We pick a non-zero integer since FlatBuffers prevent mutating the 
  // field if originally set to the default value as specified in the 
  // FlatBuffer (0 by default).
  val TableIdDefaultValue: Int = -1

  /**
   * Build a TableMeta message from a Table in contiguous memory
   *
   * @param tableId the ID to use for this table
   * @param ct the contiguous table whose metadata will be encoded in the message
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(tableId: Int, ct: ContiguousTable): TableMeta =
    buildTableMeta(
      tableId,
      ct.getBuffer.getLength,
      ct.getMetadataDirectBuffer,
      ct.getRowCount)

  def buildTableMeta(tableId: Int, compressed: GpuCompressedColumnVector): TableMeta =
    buildTableMeta(
      tableId,
      compressed.getTableBuffer.getLength,
      compressed.getTableMeta.bufferMeta().getByteBuffer,
      compressed.getTableMeta.rowCount())

  def buildTableMeta(tableId: Int, bufferSize: Long,
                     packedMeta: ByteBuffer, rowCount: Long): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, tableId)
    BufferMeta.addSize(fbb, bufferSize)
    BufferMeta.addUncompressedSize(fbb, bufferSize)
    val bufferMetaOffset = Some(BufferMeta.endBufferMeta(fbb))
    buildTableMeta(fbb, bufferMetaOffset, packedMeta, rowCount)
  }

  /**
   * Build a TableMeta message from a Table that originated in contiguous memory that has
   * since been compressed.
   * @param tableId ID to use for this table
   * @param ct contiguous table representing the uncompressed data
   * @param codecId identifier of the codec being used, see CodecType
   * @param compressedSize compressed data from the uncompressed buffer
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(
      tableId: Option[Int],
      ct: ContiguousTable,
      codecId: Byte,
      compressedSize: Long): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    val uncompressedBuffer = ct.getBuffer
    val codecDescrOffset = CodecBufferDescriptor.createCodecBufferDescriptor(
      fbb,
      codecId,
      0,
      compressedSize,
      0,
      uncompressedBuffer.getLength)
    val codecDescrArrayOffset =
      BufferMeta.createCodecBufferDescrsVector(fbb, Array(codecDescrOffset))
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, tableId.getOrElse(MetaUtils.TableIdDefaultValue))
    BufferMeta.addSize(fbb, compressedSize)
    BufferMeta.addUncompressedSize(fbb, uncompressedBuffer.getLength)
    BufferMeta.addCodecBufferDescrs(fbb, codecDescrArrayOffset)
    val bufferMetaOffset = Some(BufferMeta.endBufferMeta(fbb))
    buildTableMeta(fbb, bufferMetaOffset, ct.getMetadataDirectBuffer, ct.getRowCount)
  }

  /**
   * Build a TableMeta message with a pre-built BufferMeta message
   * @param fbb flatbuffer builder that has an already built BufferMeta message
   * @param bufferMetaOffset offset where the BufferMeta message was built
   * @param packedMeta opaque metadata needed to unpack the table
   * @param numRows the number of rows in the table
   * @return flatbuffer message
   */
  def buildTableMeta(
      fbb: FlatBufferBuilder,
      bufferMetaOffset: Option[Int],
      packedMeta: ByteBuffer,
      numRows: Long): TableMeta = {
    val vectorBuffer = fbb.createUnintializedVector(1, packedMeta.remaining(), 1)
    packedMeta.mark()
    vectorBuffer.put(packedMeta)
    packedMeta.reset()
    val packedMetaOffset = fbb.endVector()

    TableMeta.startTableMeta(fbb)
    bufferMetaOffset.foreach { bmo => TableMeta.addBufferMeta(fbb, bmo) }
    TableMeta.addPackedMeta(fbb, packedMetaOffset)
    TableMeta.addRowCount(fbb, numRows)
    fbb.finish(TableMeta.endTableMeta(fbb))
    // copy the message to trim the backing array to only what is needed
    TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
  }

  /**
   * Build a TableMeta message for a degenerate table (zero columns or rows)
   * @param batch the degenerate columnar batch which must be compressed or packed
   * @return heap-based flatbuffer message
   */
  def buildDegenerateTableMeta(batch: ColumnarBatch): TableMeta = {
    require(batch.numRows == 0 || batch.numCols == 0, "batch not degenerate")
    if (batch.numCols() == 0) {
      val fbb = new FlatBufferBuilder(1024)
      TableMeta.startTableMeta(fbb)
      TableMeta.addRowCount(fbb, batch.numRows)
      fbb.finish(TableMeta.endTableMeta(fbb))
      // copy the message to trim the backing array to only what is needed
      TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
    } else {
      batch.column(0) match {
        case c: GpuCompressedColumnVector =>
          // Batched compression can result in degenerate batches appearing compressed. In that case
          // the table metadata has already been computed and can be returned directly.
          c.getTableMeta
        case c: GpuPackedTableColumn =>
          val contigTable = c.getContiguousTable
          val fbb = new FlatBufferBuilder(1024)
          buildTableMeta(fbb, None, contigTable.getMetadataDirectBuffer, contigTable.getRowCount)
        case _ =>
          throw new IllegalStateException("batch must be compressed or packed")
      }
    }
  }

  /**
   * Constructs a table metadata buffer from a buffer length without describing any schema
   * for the buffer.
   */
  def getTableMetaNoTable(bufferSize: Long): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, 0)
    BufferMeta.addSize(fbb, bufferSize)
    BufferMeta.addUncompressedSize(fbb, bufferSize)
    val bufferMetaOffset = BufferMeta.endBufferMeta(fbb)
    TableMeta.startTableMeta(fbb)
    TableMeta.addRowCount(fbb, 0)
    TableMeta.addBufferMeta(fbb, bufferMetaOffset)
    fbb.finish(TableMeta.endTableMeta(fbb))
    // copy the message to trim the backing array to only what is needed
    TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
  }

  /**
   * Construct a table from a contiguous device buffer and a
   * `TableMeta` message describing the schema of the buffer data.
   * @param deviceBuffer contiguous buffer
   * @param meta schema metadata
   * @return table that must be closed by the caller
   */
  def getTableFromMeta(deviceBuffer: DeviceMemoryBuffer, meta: TableMeta): Table = {
    val packedMeta = meta.packedMetaAsByteBuffer
    require(packedMeta != null, "Missing packed table metadata")
    Table.fromPackedTable(packedMeta, deviceBuffer)
  }

  /**
   * Construct a columnar batch from a contiguous device buffer and a
   * `TableMeta` message describing the schema of the buffer data.
   * @param deviceBuffer contiguous buffer
   * @param meta schema metadata
   * @param sparkTypes the spark types that the `ColumnarBatch` should have.
   * @return columnar batch that must be closed by the caller
   */
  def getBatchFromMeta(deviceBuffer: DeviceMemoryBuffer, meta: TableMeta,
      sparkTypes: Array[DataType]): ColumnarBatch = {
    withResource(getTableFromMeta(deviceBuffer, meta)) { table =>
      GpuColumnVectorFromBuffer.from(table, deviceBuffer, meta, sparkTypes)
    }
  }

  /**
   * Copies BufferMeta not setting codecs in the new BufferMeta, and setting both
   * the size and uncompressed size to be buffMeta.uncompressedSize (this is because
   * the incoming BufferMeta was for a compressed buffer)
   */
  private def copyBufferMetaNoCodec(fbb: FlatBufferBuilder, buffMeta: BufferMeta): Int = {
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, buffMeta.id)
    BufferMeta.addSize(fbb, buffMeta.uncompressedSize)
    BufferMeta.addUncompressedSize(fbb, buffMeta.uncompressedSize)
    BufferMeta.endBufferMeta(fbb)
  }

  /**
   * Build a copy of the input `TableMeta` but ignoring any codecs specified in `BufferMeta`
   * This is necessary after a decompression, such that batches that are reconstituted
   * have a metadata that indicate they are not compressed.
   * @param tableMeta - the incoming metadata with codec specifications
   * @return a TableMeta without codecs
   */
  def dropCodecs(tableMeta: TableMeta): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    val originalBufferMeta = tableMeta.bufferMeta()
    val buffMetaOffset = if (originalBufferMeta != null) {
      Some(copyBufferMetaNoCodec(fbb, originalBufferMeta))
    } else {
      None
    }

    val packedMetaBuffer = tableMeta.packedMetaAsByteBuffer()
    val packedMetaOffset = if (packedMetaBuffer != null) {
      val destBuffer = fbb.createUnintializedVector(1, packedMetaBuffer.remaining(), 1)
      destBuffer.put(packedMetaBuffer)
      Some(fbb.endVector())
    } else {
      None
    }

    TableMeta.startTableMeta(fbb)
    buffMetaOffset.foreach(bmo => TableMeta.addBufferMeta(fbb, bmo))
    packedMetaOffset.foreach(pmo => TableMeta.addPackedMeta(fbb, pmo))
    TableMeta.addRowCount(fbb, tableMeta.rowCount())
    fbb.finish(TableMeta.endTableMeta(fbb))
    // copy the message to trim the backing array to only what is needed
    TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
  }
}

class DirectByteBufferFactory extends FlatBufferBuilder.ByteBufferFactory {
  override def newByteBuffer(capacity : Int) : ByteBuffer = {
    ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN)
  }
}

object ShuffleMetadata extends Logging{

  val bbFactory = new DirectByteBufferFactory

  private def copyBufferMeta(fbb: FlatBufferBuilder, buffMeta: BufferMeta): Int = {
    val descrOffsets = (0 until buffMeta.codecBufferDescrsLength()).map { i =>
      val descr = buffMeta.codecBufferDescrs(i)
      CodecBufferDescriptor.createCodecBufferDescriptor(
        fbb,
        descr.codec,
        descr.compressedOffset,
        descr.compressedSize,
        descr.uncompressedOffset,
        descr.uncompressedSize)
    }
    val codecDescrArrayOffset = if (descrOffsets.nonEmpty) {
      Some(BufferMeta.createCodecBufferDescrsVector(fbb, descrOffsets.toArray))
    } else {
      None
    }
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, buffMeta.id)
    BufferMeta.addSize(fbb, buffMeta.size)
    BufferMeta.addUncompressedSize(fbb, buffMeta.uncompressedSize)
    codecDescrArrayOffset.foreach(off => BufferMeta.addCodecBufferDescrs(fbb, off))
    BufferMeta.endBufferMeta(fbb)
  }

  /**
   * Given a sequence of `TableMeta`, re-lay the metas using the flat buffer builder in `fbb`.
   * @param fbb builder to use
   * @param tables sequence of `TableMeta` to copy
   * @return an array of flat buffer offsets for the copied `TableMeta`s
   */
  def copyTables(fbb: FlatBufferBuilder, tables: Seq[TableMeta]): Array[Int] = {
    tables.map { tableMeta =>
      val buffMeta = tableMeta.bufferMeta()
      val buffMetaOffset = if (buffMeta != null) {
        Some(copyBufferMeta(fbb, buffMeta))
      } else {
        None
      }

      val packedMetaBuffer = tableMeta.packedMetaAsByteBuffer()
      val packedMetaOffset = if (packedMetaBuffer != null) {
        val destBuffer = fbb.createUnintializedVector(1, packedMetaBuffer.remaining(), 1)
        destBuffer.put(packedMetaBuffer)
        Some(fbb.endVector())
      } else {
        None
      }

      TableMeta.startTableMeta(fbb)
      buffMetaOffset.foreach(bmo => TableMeta.addBufferMeta(fbb, bmo))
      packedMetaOffset.foreach(pmo => TableMeta.addPackedMeta(fbb, pmo))
      TableMeta.addRowCount(fbb, tableMeta.rowCount())
      TableMeta.endTableMeta(fbb)
    }.toArray
  }

  def buildMetaResponse(tables: Seq[TableMeta]): ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val tableOffsets = copyTables(fbb, tables)
    val tableMetasOffset = MetadataResponse.createTableMetasVector(fbb, tableOffsets)
    val finIndex = MetadataResponse.createMetadataResponse(fbb, tableMetasOffset)
    fbb.finish(finIndex)
    fbb.dataBuffer()
  }

  def buildShuffleMetadataRequest(blockIds : Seq[ShuffleBlockBatchId]) : ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val blockIdOffsets = blockIds.map { blockId =>
      BlockIdMeta.createBlockIdMeta(fbb,
        blockId.shuffleId,
        blockId.mapId,
        blockId.startReduceId,
        blockId.endReduceId)
    }
    val blockIdVectorOffset = MetadataRequest.createBlockIdsVector(fbb, blockIdOffsets.toArray)
    val finIndex = MetadataRequest.createMetadataRequest(fbb, blockIdVectorOffset)
    fbb.finish(finIndex)
    fbb.dataBuffer()
  }

  def getBuilder = new FlatBufferBuilder(1024, bbFactory)

  def getHeapBuilder = new FlatBufferBuilder(1024)

  def getMetadataRequest(byteBuffer: ByteBuffer): MetadataRequest = {
    MetadataRequest.getRootAsMetadataRequest(byteBuffer)
  }

  def getMetadataResponse(byteBuffer: ByteBuffer): MetadataResponse = {
    MetadataResponse.getRootAsMetadataResponse(byteBuffer)
  }

  def getTransferResponse(resp: ByteBuffer): TransferResponse = {
    TransferResponse.getRootAsTransferResponse(resp)
  }

  def getTransferRequest(transferRequest: ByteBuffer): TransferRequest = {
    TransferRequest.getRootAsTransferRequest(transferRequest)
  }

  def buildBufferTransferRequest(fbb: FlatBufferBuilder, bufferId: Int): Int = {
    BufferTransferRequest.createBufferTransferRequest(fbb, bufferId)
  }


  def buildBufferTransferResponse(bufferMetas: Seq[BufferMeta]): ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val responses = bufferMetas.map { bm =>
      val buffMetaOffset = copyBufferMeta(fbb, bm)
      BufferTransferResponse.createBufferTransferResponse(fbb, bm.id(), TransferState.STARTED,
          buffMetaOffset)
    }.toArray
    val responsesVector = TransferResponse.createResponsesVector(fbb, responses)
    val root = TransferResponse.createTransferResponse(fbb, responsesVector)
    fbb.finish(root)
    fbb.dataBuffer()
  }

  def buildTransferRequest(id: Long, toIssue: Seq[Int]): ByteBuffer = {
    val fbb = ShuffleMetadata.getBuilder
    val requestIds = new ArrayBuffer[Int](toIssue.size)
    toIssue.foreach { case bufferId =>
      requestIds.append(
        ShuffleMetadata.buildBufferTransferRequest(
          fbb,
          bufferId))
    }
    val requestVec = TransferRequest.createRequestsVector(fbb, requestIds.toArray)
    val transferRequestOffset = TransferRequest.createTransferRequest(fbb, id, requestVec)
    fbb.finish(transferRequestOffset)
    fbb.dataBuffer()
  }

  def printResponse(state: String, res: MetadataResponse): String = {
    val out = new StringBuilder
    out.append(s"----------------------- METADATA RESPONSE $state --------------------------\n")
    out.append(s"${res.tableMetasLength()} tables\n")
    out.append("------------------------------------------------------------------------------\n")
    for (tableIndex <- 0 until res.tableMetasLength()) {
      val tableMeta = res.tableMetas(tableIndex)
      out.append(s"table: $tableIndex rows=${tableMeta.rowCount}")
    }
    out.append(s"----------------------- END METADATA RESPONSE $state ----------------------\n")
    out.toString()
  }


  def printRequest(req: MetadataRequest): String = {
    val out = new StringBuilder
    out.append("----------------------- METADATA REQUEST --------------------------\n")
    out.append(s"blockId length: ${req.blockIdsLength()}\n")
    for (i <- 0 until req.blockIdsLength()) {
      out.append(s"block_id = [shuffle_id=${req.blockIds(i).shuffleId()}, " +
          s"map_id=${req.blockIds(i).mapId()}, " +
          s"start_reduce_id=${req.blockIds(i).startReduceId()} , " +
          s"end_reduce_id=${req.blockIds(i).endReduceId()}]\n")
    }
    out.append("----------------------- END METADATA REQUEST ----------------------\n")
    out.toString()
  }


  /**
   * Utility function to transfer a `TableMeta` to the heap,
   * @todo we would like to look for an easier way, perhaps just a memcpy will do.
   */
  def copyTableMetaToHeap(meta: TableMeta): TableMeta = {
    val fbb = ShuffleMetadata.getHeapBuilder
    val tables = ShuffleMetadata.copyTables(fbb, Seq(meta))
    fbb.finish(tables.head)
    TableMeta.getRootAsTableMeta(fbb.dataBuffer())
  }
}
