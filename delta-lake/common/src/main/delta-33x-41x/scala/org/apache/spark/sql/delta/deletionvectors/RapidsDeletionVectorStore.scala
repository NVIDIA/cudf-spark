/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.deletionvectors

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.Hash
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import java.io.{DataInputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.CRC32
import org.apache.hadoop.fs.Path
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.delta.DeltaErrors

/**
 * RAPIDS version of [[DeletionVectorStore]]. It is simplified to only include the APIs needed
 * to load serialized deletion vectors into host memory.
 */
trait RapidsDeletionVectorStore {

  /**
   * Loads a serialized deletion vector from the given path and offset into a HostMemoryBuffer.
   */
  def load(path: Path, offset: Int, size: Int): HostMemoryBuffer
}

object RapidsDeletionVectorStore {
  def createInstance(fileIO: RapidsFileIO): RapidsDeletionVectorStore = {
    fileIO match {
      case hadoopFileIO: HadoopFileIO =>
        new RapidsHadoopDVStore(hadoopFileIO)
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported RapidsFileIO type for deletion vector store: ${other.getClass.getName}")
    }
  }
}

class RapidsHadoopDVStore(fileIO: HadoopFileIO) extends RapidsDeletionVectorStore {

  override def load(path: Path, offset: Int, size: Int): HostMemoryBuffer = {
    val file = fileIO.newInputFile(path)
    withResource(file.open()) { fin =>
      fin.seek(offset)
      withResource(new DataInputStream(fin)) { in =>
        DeltaSerializedBitmapLoader.load(in, size)
      }
    }
  }
}

/**
 * Trait for the "Delta" roaring bitmap serialization format loaders. Delta supports two
 * serialization formats for roaring bitmaps: "portable" and "native".
 * See [[RoaringBitmapArraySerializationFormat]] for details.
 */
sealed trait DeltaSerializedBitmapLoader {
  def loadAsStandardFormat(input: DataInputStream, size: Int, crc: CRC32): HostMemoryBuffer
}

object DeltaSerializedBitmapLoader {

  // scalastyle:off line.size.limit
  /**
   * The "Delta" roaring bitmap serialization formats begin with a 4-byte magic number.
   * When converting to the "standard" roaring bitmap serialization format, this magic number
   * should be stripped. See for details:
   * https://github.com/delta-io/delta/blob/ccd3092da05a68027bf9be9ec4273a810b4b9ef3/spark/src/main/scala/org/apache/spark/sql/delta/deletionvectors/RoaringBitmapArray.scala#L512-L515
   */
  // scalastyle:on line.size.limit
  val DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4

  /**
   * Reads the given input stream and loads the serialized bitmap into a HostMemoryBuffer.
   * The input stream is expected to be in one of the two "Delta" roaring bitmap serialization
   * formats: "portable" or "native". The format is determined by reading the magic number at the
   * current position of the input stream. The bitmap is then loaded and converted to the
   * "standard" roaring bitmap serialization format, and returned as a HostMemoryBuffer.
   */
  def load(input: DataInputStream, size: Int): HostMemoryBuffer = {
    // Sanity check for the bitmap size.
    // Migrated from DeletionVectorStore.readRangeFromStream.
    val sizeAccordingToFile = input.readInt()
    if (size != sizeAccordingToFile) {
      throw DeltaErrors.deletionVectorSizeMismatch()
    }

    val (magicNumber, crc) =
      withResource(HostMemoryBuffer.allocate(DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE, false)) {
        magicNumberBuf =>
          magicNumberBuf.copyFromStream(0, input, DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE)
          val magicNumber = magicNumberBuf.getInt(0)
          val crc = new CRC32()
          crc.update(magicNumberBuf.asByteBuffer())
          (magicNumber, crc)
      }

    val remainingSize = size - DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE

    magicNumber match {
      case PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        DeltaPortableFormatLoader.loadAsStandardFormat(input, remainingSize, crc)
      case NativeRoaringBitmapArraySerializationFormat.MAGIC_NUMBER =>
        DeltaNativeFormatLoader.loadAsStandardFormat(input, remainingSize, crc)
      case _ =>
        throw new IOException(s"Unexpected RoaringBitmapArray magic number $magicNumber")
    }
  }
}

object DeltaPortableFormatLoader extends DeltaSerializedBitmapLoader {

  override def loadAsStandardFormat(input: DataInputStream, size: Int, crc: CRC32)
  : HostMemoryBuffer = {
    // The Delta portable format is identical to the standard portable format except for the
    // magic number at the beginning, which is already stripped at this point. Therefore,
    // we can directly load the remaining bytes into a HostMemoryBuffer and return it.
    closeOnExcept(HostMemoryBuffer.allocate(size)) { buffer =>
      buffer.copyFromStream(0, input, size)

      val expectedChecksum = input.readInt()
      val prevCrc = crc.getValue
      // Should cast the computed checksum to an int since the expected checksum is an int.
      val actualChecksum = Hash.hostCrc32(prevCrc, buffer).toInt
      if (expectedChecksum != actualChecksum) {
        throw DeltaErrors.deletionVectorChecksumMismatch()
      }
      buffer
    }
  }
}

object DeltaNativeFormatLoader extends DeltaSerializedBitmapLoader {

  override def loadAsStandardFormat(input: DataInputStream, size: Int, crc: CRC32)
  : HostMemoryBuffer = {
    // The Delta native format is not compatible with the standard portable format, so we
    // load the bitmap into a RoaringBitmapArray first, then re-serialize it in the standard
    // portable format. This is sub-optimal since it requires deserializing and re-serializing
    // the bitmap, but this is the simplest that works for the legacy native format which
    // is expected to be rare. If this becomes a problem, we can consider implementing a more
    // efficient conversion without fully deserializing the bitmap.
    val originalBytes = readRangeFromStream(input, size, crc)
    val bb = ByteBuffer.wrap(originalBytes)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    val roaringBitmapArray = NativeRoaringBitmapArraySerializationFormat.deserialize(bb)
    val reserialized = serializeAsDeltaPortableFormat(roaringBitmapArray)
    val magicNumberSize = DeltaSerializedBitmapLoader.DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE
    val buffer = HostMemoryBuffer.allocate(reserialized.length - magicNumberSize)
    closeOnExcept(buffer) { buf =>
      buf.setBytes(0, reserialized, magicNumberSize, reserialized.length - magicNumberSize)
      buf
    }
  }

  /**
   * Migrated from DeletionVectorStore.readRangeFromStream and slightly modified.
   * This version does not read the bitmap size from the stream since that is already read
   * by the caller ([[DeltaSerializedBitmapLoader.load]]).
   */
  private def readRangeFromStream(reader: DataInputStream, size: Int, crc: CRC32): Array[Byte] = {
    val buffer = new Array[Byte](size)
    reader.readFully(buffer)

    val expectedChecksum = reader.readInt()
    crc.update(buffer)
    // Should cast the computed checksum to an int since the expected checksum is an int.
    val actualChecksum = crc.getValue.toInt

    if (expectedChecksum != actualChecksum) {
      throw DeltaErrors.deletionVectorChecksumMismatch()
    }

    buffer
  }

  private def serializeAsDeltaPortableFormat(bitmaps: Array[RoaringBitmap]): Array[Byte] = {
    val serializedSize =
      PortableRoaringBitmapArraySerializationFormat.serializedSizeInBytes(bitmaps) +
        DeltaSerializedBitmapLoader.DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE
    val buffer = ByteBuffer.allocate(serializedSize.toInt)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(PortableRoaringBitmapArraySerializationFormat.MAGIC_NUMBER)
    PortableRoaringBitmapArraySerializationFormat.serialize(bitmaps, buffer)
    buffer.array()
  }
}
