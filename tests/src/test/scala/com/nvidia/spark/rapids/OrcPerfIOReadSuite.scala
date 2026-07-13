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

package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.util.OptionalLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.filecache.{FileCache, FileCacheStub}
import com.nvidia.spark.rapids.fileio.hadoop.HadoopInputFile
import com.nvidia.spark.rapids.jni.fileio.{RapidsInputFile, SeekableInputStream}
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import com.nvidia.spark.rapids.shims.GpuOrcDataReader320Plus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{OrcFile, OrcProto, StripeInformation}
import org.apache.orc.impl.{BufferChunk, BufferChunkList, DataReaderProperties}
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class OrcPerfIOReadSuite extends AnyFunSuite with Matchers with MockitoSugar {
  private case class RecordedRange(inputOffset: Long, length: Long, outputOffset: Long)

  private class RecordingInputFile(
      bytes: Array[Byte],
      modificationTime: Long = 1234L,
      failReads: Boolean = false) extends RapidsInputFile {
    val vectoredReads = new ArrayBuffer[Seq[RecordedRange]]
    val tailReads = new ArrayBuffer[Long]
    var expectedVectoredOutput: Option[HostMemoryBuffer] = None

    override def path(): String = "s3a://bucket/test.orc"

    override def getLength(): Long = bytes.length

    override def getLastModificationTime(): OptionalLong = OptionalLong.of(modificationTime)

    override def readVectored(
        output: HostMemoryBuffer,
        copyRanges: java.util.List[RapidsInputFile.CopyRange]): Unit = {
      expectedVectoredOutput.foreach { expected =>
        assert(output eq expected, "readVectored did not receive the final output buffer")
      }
      if (failReads) {
        throw new IOException("injected vectored read failure")
      }
      val recorded = copyRanges.asScala.map { range =>
        output.setBytes(range.getOutputOffset, bytes, range.getInputOffset.toInt,
          range.getLength.toInt)
        RecordedRange(range.getInputOffset, range.getLength, range.getOutputOffset)
      }.toSeq
      vectoredReads += recorded
    }

    override def readTail(length: Long, output: HostMemoryBuffer): Unit = {
      if (failReads) {
        throw new IOException("injected tail read failure")
      }
      tailReads += length
      output.setBytes(0, bytes, bytes.length - length.toInt, length.toInt)
    }

    override def open(): SeekableInputStream =
      throw new AssertionError("ORC object bytes must not use an input stream")
  }

  private class ByteArrayChannel(bytes: Array[Byte]) extends SeekableByteChannel {
    private var pos = 0
    private var open = true

    override def read(dst: ByteBuffer): Int = {
      if (pos == bytes.length) {
        -1
      } else {
        val length = math.min(dst.remaining(), bytes.length - pos)
        dst.put(bytes, pos, length)
        pos += length
        length
      }
    }

    override def write(src: ByteBuffer): Int =
      throw new UnsupportedOperationException

    override def position(): Long = pos

    override def position(newPosition: Long): SeekableByteChannel = {
      pos = newPosition.toInt
      this
    }

    override def size(): Long = bytes.length

    override def truncate(size: Long): SeekableByteChannel =
      throw new UnsupportedOperationException

    override def isOpen: Boolean = open

    override def close(): Unit = {
      open = false
    }
  }

  private class RecordingFileCache extends FileCacheStub {
    val data = scala.collection.mutable.Map.empty[(Long, Long), Array[Byte]]

    override def getDataRangeChannel(
        inputFile: RapidsInputFile,
        offset: Long,
        length: Long): Option[SeekableByteChannel] = {
      data.get((offset, length)).map(new ByteArrayChannel(_))
    }

    override def startDataRangeCache(
        inputFile: RapidsInputFile,
        offset: Long,
        length: Long): Option[FileCacheStartedToken] = {
      Some(new FileCacheStartedToken {
        override def complete(buffer: HostMemoryBuffer): Unit = {
          withResource(buffer) { buffer =>
            val bytes = new Array[Byte](buffer.getLength.toInt)
            buffer.getBytes(bytes, 0, 0, bytes.length)
            data((offset, length)) = bytes
          }
        }

        override def cancel(): Unit = {}
      })
    }
  }

  private class TestDataReader(
      props: DataReaderProperties,
      testInputFile: RapidsInputFile,
      testFileCache: FileCache)
    extends GpuOrcDataReader320Plus(props, new Configuration(false), Map.empty) {

    override protected lazy val inputFile: RapidsInputFile = testInputFile

    val inputFileForTest: RecordingInputFile = testInputFile.asInstanceOf[RecordingInputFile]

    override protected def fileCache: FileCache = testFileCache

    // Required by Spark 4.x ORC's DataReader; harmless as an extra method on Spark 3.x.
    def releaseAllBuffers(): Unit = {}

    override protected def parseStripeFooter(
        buf: ByteBuffer,
        size: Int): OrcProto.StripeFooter = {
      OrcProto.StripeFooter.parseFrom(
        new ByteArrayInputStream(buf.array(), buf.arrayOffset(), size))
    }
  }

  private def newReader(
      inputFile: RapidsInputFile,
      fileCache: FileCache = FileCacheStub): TestDataReader = {
    val props = mock[DataReaderProperties]
    when(props.getPath).thenReturn(new Path(inputFile.path()))
    when(props.getCompression).thenReturn(null)
    new TestDataReader(props, inputFile, fileCache)
  }

  private def ranges(offsets: (Long, Long)*): DiskRangeList = {
    val first = new DiskRangeList(offsets.head._1, offsets.head._2)
    var last = first
    offsets.tail.foreach { case (offset, end) =>
      last = last.insertAfter(new DiskRangeList(offset, end))
    }
    first
  }

  private def readRanges(reader: TestDataReader, inputRanges: DiskRangeList): Array[Byte] = {
    val length = {
      var total = 0
      var current = inputRanges
      while (current != null) {
        total += current.getLength
        current = current.next
      }
      total
    }
    withResource(HostMemoryBuffer.allocate(length, false)) { output =>
      val stream = new HostMemoryOutputStream(output)
      reader.inputFileForTest.expectedVectoredOutput = Some(output)
      try {
        reader.copyFileDataToHostStream(stream, inputRanges)
      } finally {
        reader.inputFileForTest.expectedVectoredOutput = None
      }
      val bytes = new Array[Byte](length)
      output.getBytes(bytes, 0, 0, length)
      bytes
    }
  }

  private def makeOrcFile(
      metadataLength: Int,
      prefixLength: Int = 8): (Array[Byte], Array[Byte]) = {
    val footer = Array.emptyByteArray
    val postscript = OrcProto.PostScript.newBuilder()
      .setCompression(OrcProto.CompressionKind.NONE)
      .setFooterLength(footer.length)
      .setMetadataLength(metadataLength)
      .setMagic(OrcFile.MAGIC)
      .build()
      .toByteArray
    val tail = Array.fill[Byte](metadataLength)(0) ++ footer ++ postscript ++
      Array(postscript.length.toByte)
    (Array.tabulate[Byte](prefixLength)(_.toByte) ++ tail, tail)
  }

  test("ORC tail uses one suffix read when the complete tail fits in 16 KiB") {
    val (fileBytes, expectedTail) = makeOrcFile(metadataLength = 32)
    val inputFile = new RecordingInputFile(fileBytes)

    val buffer = GpuOrcTailReader.readOrcTailBuffer(
      new Path(inputFile.path()), inputFile)

    inputFile.tailReads should contain only fileBytes.length.toLong
    buffer.getLong shouldEqual fileBytes.length.toLong
    buffer.getLong shouldEqual 1234L
    val actualTail = new Array[Byte](buffer.remaining())
    buffer.get(actualTail)
    actualTail shouldEqual expectedTail
  }

  test("ORC tail refetches a large complete suffix through readTail") {
    val (fileBytes, expectedTail) = makeOrcFile(metadataLength = 17 * 1024)
    val inputFile = new RecordingInputFile(fileBytes)

    val buffer = GpuOrcTailReader.readOrcTailBuffer(
      new Path(inputFile.path()), inputFile)

    inputFile.tailReads shouldEqual Seq(16 * 1024L, expectedTail.length.toLong)
    buffer.position(2 * java.lang.Long.BYTES)
    val actualTail = new Array[Byte](buffer.remaining())
    buffer.get(actualTail)
    actualTail shouldEqual expectedTail
  }

  test("empty ORC files do not issue object reads") {
    val inputFile = new RecordingInputFile(Array.emptyByteArray)
    val buffer = GpuOrcTailReader.readOrcTailBuffer(
      new Path(inputFile.path()), inputFile)

    buffer.remaining() shouldEqual 0
    inputFile.tailReads shouldBe empty
    inputFile.vectoredReads shouldBe empty
  }

  test("tail read failures retain the path and requested suffix length") {
    val (fileBytes, _) = makeOrcFile(metadataLength = 32)
    val inputFile = new RecordingInputFile(fileBytes, failReads = true)

    val error = intercept[IOException] {
      GpuOrcTailReader.readOrcTailBuffer(new Path(inputFile.path()), inputFile)
    }

    error.getMessage should include(inputFile.path())
    error.getMessage should include(fileBytes.length.toString)
    error.getCause.getMessage should include("injected tail read failure")
  }

  test("non-S3 input retains the RapidsInputFile Hadoop fallback") {
    val file = Files.createTempFile("orc-perfio-fallback", ".bin")
    val fileBytes = Array.tabulate[Byte](16)(_.toByte)
    Files.write(file, fileBytes)
    try {
      val inputFile = HadoopInputFile.create(
        new Path(file.toUri), new Configuration(false))
      withResource(HostMemoryBuffer.allocate(5, false)) { output =>
        inputFile.readVectored(output, Seq(
          new CopyRange(2, 2, 0),
          new CopyRange(10, 3, 2)).asJava)
        val result = new Array[Byte](5)
        output.getBytes(result, 0, 0, result.length)
        result shouldEqual Array[Byte](2, 3, 10, 11, 12)
      }
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("stripe footer miss uses readVectored and populates the cache") {
    val footer = OrcProto.StripeFooter.newBuilder()
      .addStreams(OrcProto.Stream.newBuilder()
        .setColumn(1).setKind(OrcProto.Stream.Kind.DATA).setLength(2))
      .build()
    val footerBytes = footer.toByteArray
    val fileBytes = Array.fill[Byte](40)(0)
    Array.copy(footerBytes, 0, fileBytes, 20, footerBytes.length)
    val inputFile = new RecordingInputFile(fileBytes)
    val cache = new RecordingFileCache
    val reader = newReader(inputFile, cache)
    val stripe = mock[StripeInformation]
    when(stripe.getOffset).thenReturn(10L)
    when(stripe.getIndexLength).thenReturn(4L)
    when(stripe.getDataLength).thenReturn(6L)
    when(stripe.getFooterLength).thenReturn(footerBytes.length.toLong)

    reader.readStripeFooter(stripe) shouldEqual footer
    inputFile.vectoredReads.flatten shouldEqual
      Seq(RecordedRange(20, footerBytes.length, 0))
    cache.data((20L, footerBytes.length.toLong)) shouldEqual footerBytes
  }

  test("stripe footer cache hit does not issue an object read") {
    val footer = OrcProto.StripeFooter.newBuilder()
      .addStreams(OrcProto.Stream.newBuilder()
        .setColumn(1).setKind(OrcProto.Stream.Kind.DATA).setLength(2))
      .build()
    val footerBytes = footer.toByteArray
    val inputFile = new RecordingInputFile(Array.fill[Byte](40)(0))
    val cache = new RecordingFileCache
    cache.data((20L, footerBytes.length.toLong)) = footerBytes
    val reader = newReader(inputFile, cache)
    val stripe = mock[StripeInformation]
    when(stripe.getOffset).thenReturn(10L)
    when(stripe.getIndexLength).thenReturn(4L)
    when(stripe.getDataLength).thenReturn(6L)
    when(stripe.getFooterLength).thenReturn(footerBytes.length.toLong)

    reader.readStripeFooter(stripe) shouldEqual footer
    inputFile.vectoredReads shouldBe empty
  }

  test("adjacent and non-adjacent misses share one vectored call into the final HMB") {
    val fileBytes = Array.tabulate[Byte](32)(_.toByte)
    val inputFile = new RecordingInputFile(fileBytes)
    val reader = newReader(inputFile)

    val result = readRanges(reader, ranges((2, 5), (5, 7), (10, 13)))

    inputFile.vectoredReads shouldEqual Seq(Seq(
      RecordedRange(2, 5, 0),
      RecordedRange(10, 3, 5)))
    result shouldEqual Array[Byte](2, 3, 4, 5, 6, 10, 11, 12)
  }

  test("mixed cache hits and misses preserve output order and batch misses") {
    val fileBytes = Array.tabulate[Byte](32)(_.toByte)
    val inputFile = new RecordingInputFile(fileBytes)
    val cache = new RecordingFileCache
    cache.data((5L, 2L)) = Array[Byte](50, 51)
    val reader = newReader(inputFile, cache)

    val result = readRanges(reader, ranges((2, 4), (5, 7), (10, 13)))

    inputFile.vectoredReads shouldEqual Seq(Seq(
      RecordedRange(2, 2, 0),
      RecordedRange(10, 3, 4)))
    result shouldEqual Array[Byte](2, 3, 50, 51, 10, 11, 12)
  }

  test("ORC-owned heap and direct buffers remain valid after temporary HMBs close") {
    val fileBytes = Array.tabulate[Byte](32)(_.toByte)
    val inputFile = new RecordingInputFile(fileBytes)
    val cache = new RecordingFileCache
    cache.data((10L, 3L)) = Array[Byte](50, 51, 52)
    val reader = newReader(inputFile, cache)
    val chunks = new BufferChunkList
    val remoteChunk = new BufferChunk(2, 3)
    val cachedChunk = new BufferChunk(10, 3)
    chunks.add(remoteChunk)
    chunks.add(cachedChunk)

    reader.readFileData(chunks, true)

    remoteChunk.getData.isDirect shouldBe false
    cachedChunk.getData.isDirect shouldBe true
    val remoteBytes = new Array[Byte](3)
    val cachedBytes = new Array[Byte](3)
    remoteChunk.getData.get(remoteBytes)
    cachedChunk.getData.get(cachedBytes)
    remoteBytes shouldEqual Array[Byte](2, 3, 4)
    cachedBytes shouldEqual Array[Byte](50, 51, 52)
    cache.data((2L, 3L)) shouldEqual Array[Byte](2, 3, 4)
  }

  test("vectored failures include the file and requested ranges") {
    val inputFile = new RecordingInputFile(Array.fill[Byte](32)(0), failReads = true)
    val reader = newReader(inputFile)

    val error = intercept[IOException] {
      readRanges(reader, ranges((2, 4), (10, 13)))
    }

    error.getMessage should include(inputFile.path())
    error.getMessage should include("2:2")
    error.getMessage should include("10:3")
    error.getCause.getMessage should include("injected vectored read failure")
  }
}
