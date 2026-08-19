/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import java.io.{EOFException, InputStream, IOException, OutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.shims.NullOutputStreamShim
import org.apache.avro.Schema
import org.apache.avro.file.DataFileConstants._
import org.apache.avro.file.SeekableInput
import org.apache.avro.io.{BinaryData, BinaryDecoder, DecoderFactory}
import org.apache.avro.mapred.FsInput
import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.rapids.shims.TrampolineConnectShims

private[rapids] class AvroSeekableInputStream(in: SeekableInput) extends InputStream
    with SeekableInput {
  var oneByte = new Array[Byte](1)

  override def read(): Int = {
    val n = read(oneByte, 0, 1)
    if (n == 1) oneByte(0) & 0xff else n
  }

  override def read(b: Array[Byte]): Int = in.read(b, 0, b.length)

  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)

  override def seek(p: Long): Unit = {
    if (p < 0) throw new IOException("Illegal seek: " + p)
    in.seek(p)
  }

  override def tell(): Long = in.tell()

  override def length(): Long = in.length()

  override def close(): Unit = {
    in.close()
    super.close()
  }

  override def available(): Int = {
    val remaining = in.length() - in.tell()
    if (remaining > Int.MaxValue) Int.MaxValue else remaining.toInt
  }
}

/**
 * The header information of an Avro file.
 */
case class Header(
    meta: Map[String, Array[Byte]],
    // Array in scala is mutable, so keep it private to avoid unexpected update.
    private val syncBuffer: Array[Byte]) {

  /** Get a copy of the sync marker. */
  def sync: Array[Byte] = syncBuffer.clone

  @transient
  lazy val schema: Schema = {
    getMetaString(SCHEMA)
      .map(s => TrampolineConnectShims.createSchemaParser().parse(s))
      .orNull
  }

  private def getMetaString(key: String): Option[String] = {
    meta.get(key).map(new String(_, StandardCharsets.UTF_8))
  }
}

object Header {
  /** Compute header size in bytes for serialization */
  def headerSizeInBytes(h: Header): Long = {
    val out = new CountingOutputStream(NullOutputStreamShim.INSTANCE)
    AvroFileWriter(out).writeHeader(h)
    out.getByteCount
  }

  /**
   * Merge the metadata of the given headers.
   * Note: It does not check the compatibility of the headers.
   * @param headers whose metadata to be merged.
   * @return a header with the new merged metadata and the first header's
   *         sync marker, or None if the input is empty.
   */
  def mergeMetadata(headers: Seq[Header]): Option[Header] = {
    if (headers.isEmpty) {
      None
    } else {
      val mergedMeta = headers.map(_.meta).reduce { (merged, meta) =>
        merged ++ meta
      }
      Some(Header(mergedMeta, headers.head.sync))
    }
  }

  /**
   * Test whether the two headers have conflicts in the metadata.
   * A conflict means a key exists in both of the two headers' metadata,
   * and maps to different values.
   */
  def hasConflictInMetadata(h1: Header, h2: Header): Boolean = h1.meta.exists {
    case (k, v) => h2.meta.get(k).exists(!_.sameElements(v))
  }
}

/**
 * The each Avro block information
 *
 * @param blockStart the start of block
 * @param blockSize  the whole block size = the size between two sync buffers + sync buffer
 * @param dataSize   the block data size
 * @param count      how many entries in this block
 */
case class BlockInfo(blockStart: Long, blockSize: Long, dataSize: Long, count: Long)

/**
 * The mutable version of the BlockInfo without block start.
 * This is for reusing an existing instance when accessing data in the iterator pattern.
 *
 * @param blockSize the whole block size (the size between two sync buffers + sync buffer size)
 * @param dataSize  the data size in this block
 * @param count   how many entries in this block
 */
case class MutableBlockInfo(var blockSize: Long, var dataSize: Long, var count: Long)

/** The parent of the Rapids Avro file readers */
abstract class AvroFileReader(si: SeekableInput) extends AutoCloseable {
  // Children should update this pointer accordingly.
  protected var curBlockStart = 0L
  protected val headerSync: Array[Byte] = new Array[Byte](SYNC_SIZE)

  protected val sin = new AvroSeekableInputStream(si)
  sin.seek(0) // seek to the file start to parse the header
  protected var vin: BinaryDecoder = DecoderFactory.get.binaryDecoder(sin, vin)

  val (header, headerSize): (Header, Long) = initialize()

  private lazy val partialMatchTable: Array[Int] = computePartialMatchTable(headerSync)

  protected def seek(position: Long): Unit = {
    sin.seek(position)
    vin = DecoderFactory.get.binaryDecoder(sin, vin)
  }

  // parse the Avro file header:
  //  ----------------------------------
  //  | magic | metadata | sync marker |
  //  ----------------------------------
  private def initialize(): (Header, Long) = {
    // read magic
    val magic = new Array[Byte](MAGIC.length)
    vin.readFixed(magic)
    magic match {
      case Array(79, 98, 106, 1) => // current avro format
      case Array(79, 98, 106, 0) => // old format
        throw new UnsupportedOperationException("avro 1.2 format is not support by GPU")
      case _ => throw new RuntimeException("Not an Avro data file.")
    }
    // read metadata map
    val meta = mutable.Map[String, Array[Byte]]()
    var l = vin.readMapStart().toInt
    if (l > 0) {
      do {
        for (_ <- 1 to l) {
          val key = vin.readString(null).toString
          val value = vin.readBytes(null)
          val bb = new Array[Byte](value.remaining())
          value.get(bb)
          meta += (key -> bb)
        }
        l = vin.mapNext().toInt
      } while (l != 0)
    }
    // read sync marker
    vin.readFixed(headerSync)
    // store some information
    curBlockStart = sin.tell - vin.inputStream.available
    (Header(meta.toMap, headerSync), curBlockStart)
  }

  /** Return true if current point is past the next sync point after a position. */
  def pastSync(position: Long): Boolean = {
    // If 'position' is in a block, this block will be read into this partition.
    // If 'position' is in a sync marker, the next block will be read into this partition.
    (curBlockStart >= position + SYNC_SIZE) || (curBlockStart >= sin.length())
  }

   /** Skip next length bytes */
  def skip(length: Int): Unit = {
    vin.skipFixed(length)
  }

  /**
   * Move to the next synchronization point after a position. To process a range
   * of file entries, call this with the starting position, then check
   * "pastSync(long)" with the end position before each call to "peekBlock()" or
   * "readNextRawBlock".
   * (Based off of the 'sync' in "DataFileReader" of apache/avro)
   */
  def sync(position: Long): Unit = {
    seek(position)
    val pm = partialMatchTable
    val in = vin.inputStream()
    // Search for the sequence of bytes in the stream using Knuth-Morris-Pratt
    var i = 0L
    var j = 0
    var b = in.read()
    while (b != -1) {
      val cb = b.toByte
      while (j > 0 && cb != headerSync(j)) {
        j = pm(j - 1)
      }
      if (cb == headerSync(j)) {
        j += 1
      }
      if (j == SYNC_SIZE) {
        curBlockStart = position + i + 1L
        return
      }
      b = in.read()
      i += 1
    }
    // if no match set to the end position
    curBlockStart = sin.tell()
  }

  /**
   * Compute that Knuth-Morris-Pratt partial match table.
   *
   * @param pattern The pattern being searched
   * @return the pre-computed partial match table
   *
   * @see <a href= "https://github.com/williamfiset/Algorithms">William Fiset
   *      Algorithms</a>
   * (Based off of the 'computePartialMatchTable' in "DataFileReader" of apache/avro)
   */
  private def computePartialMatchTable(pattern: Array[Byte]): Array[Int] = {
    val pm = new Array[Int](pattern.length)
    var i = 1
    var len = 0
    while (i < pattern.length) {
      if (pattern(i) == pattern(len)) {
        len += 1
        pm(i) = len
        i += 1
      } else {
        if (len > 0) {
          len = pm(len - 1)
        } else {
          i += 1
        }
      }
    }
    pm
  }

  override def close(): Unit = {
    vin.inputStream().close()
  }
}

/**
 * AvroMetaFileReader collects the blocks' information from the Avro file
 * without reading the block data.
 */
class AvroMetaFileReader(si: SeekableInput) extends AvroFileReader(si) {
  // store the blocks info
  private var blocks: Seq[BlockInfo] = null
  private var curStop: Long = -1L

  /**
   * Collect the metadata of the blocks until the given stop point.
   * The start block can also be specified by calling 'sync(start)' first.
   *
   * It is recommended setting start and stop positions to minimize what
   * is going to be read.
   */
  def getPartialBlocks(stop: Long): Seq[BlockInfo] = {
    if (curStop != stop || blocks == null) {
      blocks = parsePartialBlocks(stop)
      curStop = stop
    }
    blocks
  }

  private def parsePartialBlocks(stop: Long): Seq[BlockInfo] = {
    if (curBlockStart >= sin.length() || vin.isEnd()) {
      // no blocks
      return Seq.empty
    }
    val blocks = mutable.ArrayBuffer.empty[BlockInfo]
    // buf is used for writing a long, requiring at most 10 bytes.
    val buf = new Array[Byte](10)
    while (curBlockStart < sin.length() && !pastSync(stop)) {
      seek(curBlockStart)
      val blockCount = vin.readLong()
      val blockDataSize = vin.readLong()
      if (blockDataSize > Integer.MAX_VALUE || blockDataSize < 0) {
        throw new IOException("Block size invalid or too large: " + blockDataSize)
      }

      // Get how many bytes used to store the value of count and block data size.
      val countLongLen = BinaryData.encodeLong(blockCount, buf, 0)
      val dataSizeLongLen = BinaryData.encodeLong(blockDataSize, buf, 0)
      // (len of entries) + (len of block size) + (block size) + (sync size)
      val blockLength = countLongLen + dataSizeLongLen + blockDataSize + SYNC_SIZE
      blocks += BlockInfo(curBlockStart, blockLength, blockDataSize, blockCount)

      // Do we need to check the SYNC BUFFER, or just let cudf do it?
      curBlockStart += blockLength
    }
    blocks.toSeq
  }

}

/**
 * AvroDataFileReader reads the Avro file data in the iterator pattern.
 * You can use it as below.
 *    while(reader.hasNextBlock) {
 *      val b = reader.peekBlock
 *      estimateBufSize(b)
 *      // allocate the batch buffer
 *      reader.readNextRawBlock(buffer_as_out_stream)
 *    }
 */
class AvroDataFileReader(si: SeekableInput) extends AvroFileReader(si) {
  // Avro file format:
  //    ----------------------------------------
  //    | header | block | block | ... | block |
  //    ----------------------------------------
  // One block format:    /     \
  //    ----------------------------------------------------
  //    | Count | Data Size | Data in binary | sync marker |
  //    ----------------------------------------------------
  //  - longsBuffer for the encoded block count and data size, each is at most 10 bytes.
  //  - syncBuffer for the sync marker, with the fixed size: 16 bytes.
  //  - dataBuffer for the block binary data
  private val longsBuffer = new Array[Byte](20)
  private val syncBuffer = new Array[Byte](SYNC_SIZE)
  private var dataBuffer: Array[Byte] = null

  // count of objects in block
  private var curCount: Long = 0L
  // size in bytes of the serialized objects in block
  private var curDataSize: Long = 0L
  // block size = encoded count long size + encoded data-size long size + data size + 16
  private var curBlockSize: Long = 0L
  // a flag to indicate whether there is block available currently
  private var curBlockReady = false

  /** Test if there is a block. */
  def hasNextBlock(): Boolean = {
    try {
      if (curBlockReady) { return true }
      // if reaching the end of stream
      if (vin.isEnd()) { return false }
      curCount = vin.readLong() // read block count
      curDataSize = vin.readLong() // read block data size
      if (curDataSize > Int.MaxValue || curDataSize < 0) {
        throw new IOException(s"Invalid data size: $curDataSize, should be in (0, Int.MaxValue).")
      }
      // Get how many bytes used to store the values of count and block data size.
      val countLongLen = BinaryData.encodeLong(curCount, longsBuffer, 0)
      val dataSizeLongLen = BinaryData.encodeLong(curDataSize, longsBuffer, countLongLen)
      curBlockSize = countLongLen + dataSizeLongLen + curDataSize + SYNC_SIZE
      curBlockReady = true
      true
    } catch {
      case _: EOFException => false
    }
  }

  /**
   * Get the current block metadata. Call 'readNextRawBlock' to get the block raw data.
   * Better to check its existence by calling 'hasNextBlock' first.
   * This will not move the reader position forward.
   */
  def peekBlock(reuse: MutableBlockInfo): MutableBlockInfo = {
    if (!hasNextBlock) {
      throw new NoSuchElementException
    }
    if (reuse == null) {
      MutableBlockInfo(curBlockSize, curDataSize, curCount)
    } else {
      reuse.blockSize = curBlockSize
      reuse.dataSize = curDataSize
      reuse.count = curCount
      reuse
    }
  }

  /**
   * Read the current block raw data to the given output stream.
   */
  def readNextRawBlock(out: OutputStream): Unit = {
    // This is designed to reduce the data copy as much as possible.
    // Currently it leverages the BinarayDecoder, and data will be copied twice.
    // Once to the temporary buffer `dataBuffer`, again to the output stream (the
    // batch buffer in native).
    // Later we may want to implement a Decoder ourselves to copy the data from raw
    // buffer directly.
    if (!hasNextBlock) {
      throw new NoSuchElementException
    }
    val dataSize = curDataSize.toInt
    if (dataBuffer == null || dataBuffer.size < dataSize) {
      dataBuffer = new Array[Byte](dataSize)
    }
    // throws if it can't read the size requested
    vin.readFixed(dataBuffer, 0, dataSize)
    vin.readFixed(syncBuffer)
    curBlockStart = sin.tell - vin.inputStream.available
    if (!headerSync.sameElements(syncBuffer)) {
      curBlockReady = false
      throw new IOException("Invalid sync!")
    }
    out.write(longsBuffer, 0, (curBlockSize - curDataSize - SYNC_SIZE).toInt)
    out.write(dataBuffer, 0, dataSize)
    out.write(syncBuffer)
    curBlockReady = false
  }

  /**
   * Skip the current raw block
   */
  def skipCurrentBlock(): Unit = {
    if (!hasNextBlock) {
      throw new NoSuchElementException
    }
    val dataSize = curDataSize.toInt
    skip(dataSize + SYNC_SIZE)
    curBlockStart = sin.tell - vin.inputStream.available
    curBlockReady = false
  }

}

object AvroFileReader {

  def openMetaReader(filePath: String, conf: Configuration): AvroMetaFileReader = {
    closeOnExcept(openFile(filePath, conf)) { si =>
      new AvroMetaFileReader(si)
    }
  }

  def openDataReader(filePath: String, conf: Configuration): AvroDataFileReader = {
    closeOnExcept(openFile(filePath, conf)) { si =>
      new AvroDataFileReader(si)
    }
  }

  private def openFile(filePath: String, conf: Configuration): SeekableInput = {
    new FsInput(new Path(new URI(filePath)), conf)
  }
}
