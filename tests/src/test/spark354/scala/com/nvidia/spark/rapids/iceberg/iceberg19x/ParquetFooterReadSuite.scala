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

/*** spark-rapids-shim-json-lines
{"spark":["354"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.iceberg19x

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{PerfIO, PerfIOConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.{InputFile, SeekableInputStream => IcebergSeekableInputStream}
import org.apache.iceberg.parquet.GpuParquetIO
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.column.page.DictionaryPageReadStore
import org.apache.iceberg.shaded.org.apache.parquet.filter2.compat.FilterCompat
import org.apache.iceberg.shaded.org.apache.parquet.filter2.predicate.FilterApi
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkConf

class ParquetFooterReadSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    PerfIO.initS3Client(PerfIOConf(new SparkConf(false)))
  }

  override protected def afterAll(): Unit = {
    try {
      PerfIO.shutdownS3Client()
    } finally {
      super.afterAll()
    }
  }

  private def parquetBytes: Array[Byte] =
    Files.readAllBytes(Paths.get(getClass.getResource("/file-splits.parquet").toURI))

  private def framedFooter(data: Array[Byte]): Array[Byte] = {
    val length = ByteBuffer.wrap(data, data.length - 8, 4)
      .order(ByteOrder.LITTLE_ENDIAN).getInt
    MAGIC ++ data.takeRight(length + 8)
  }

  private class MemoryFile(data: Array[Byte]) extends IcebergInputFile(mock(classOf[InputFile])) {
    var opens = 0
    var closes = 0
    var bodyReads = 0
    var tailReads = 0
    var failBodyRead = false
    private val name = s"s3://test-bucket/${UUID.randomUUID()}.parquet"

    override def path(): String = name
    override def getLength: Long = data.length

    // Expose the same bytes through native Iceberg streams and PerfIO tail reads.
    override def getDelegate: InputFile = new InputFile {
      override def getLength: Long = data.length
      override def location(): String = name
      override def exists(): Boolean = true
      override def newStream(): IcebergSeekableInputStream = {
        val input = MemoryFile.this.open()
        new IcebergSeekableInputStream {
          override def getPos: Long = input.getPos
          override def seek(position: Long): Unit = input.seek(position)
          override def read(): Int = input.read()
          override def read(bytes: Array[Byte], offset: Int, length: Int): Int =
            input.read(bytes, offset, length)
          override def close(): Unit = input.close()
        }
      }
    }

    override def readTail(length: Long, output: HostMemoryBuffer): Unit = {
      tailReads += 1
      output.setBytes(0, data, data.length - length, length)
    }

    override def open(): SeekableInputStream = {
      opens += 1
      new SeekableInputStream {
        private var position = 0
        override def getPos: Long = position
        override def seek(offset: Long): Unit = { position = offset.toInt }
        override def read(): Int = {
          val bytes = new Array[Byte](1)
          if (read(bytes, 0, 1) < 0) -1 else bytes(0) & 0xff
        }
        override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
          bodyReads += 1
          if (failBodyRead) throw new IOException("dictionary read failed")
          if (length == 0) return 0
          if (position >= data.length) return -1
          val count = math.min(length, data.length - position)
          System.arraycopy(data, position, bytes, offset, count)
          position += count
          count
        }
        override def close(): Unit = { closes += 1 }
      }
    }
  }

  test("prefetched footer retains original offsets across seeks and boundary reads") {
    val data = parquetBytes
    val footer = framedFooter(data)
    val start = data.length - footer.length + MAGIC.length
    val input = new MemoryFile(data)
    val prefetched = new FooterInputFile(input, footer)
    withResource(prefetched.newStream()) { stream =>
      stream.seek(data.length - 8)
      val trailer = new Array[Byte](8)
      stream.readFully(trailer)
      trailer shouldBe data.takeRight(8)
      input.opens shouldBe 0

      val crossing = new Array[Byte](12)
      stream.seek(start - 6)
      stream.readFully(crossing)
      crossing shouldBe data.slice(start - 6, start + 6)
      stream.seek(4)
      stream.read() shouldBe (data(4) & 0xff)
      stream.seek(data.length)
      stream.read() shouldBe -1
      stream.read(trailer, 0, 0) shouldBe 0
      intercept[IOException](stream.seek(-1))
      intercept[IndexOutOfBoundsException](stream.read(trailer, 1, 8))
    }
    input.opens shouldBe 1
    input.closes shouldBe 1
    withResource(prefetched.newStream()) { stream =>
      stream.seek(start)
      stream.read() shouldBe (data(start) & 0xff)
    }
    input.opens shouldBe 1
  }

  test("prefetched footer preserves split filtering and dictionary contents") {
    val data = parquetBytes
    for (options <- Seq(ParquetReadOptions.builder().build(),
        ParquetReadOptions.builder().withRange(0, data.length / 2).build())) {
      val input = new MemoryFile(data)
      val fixture = org.apache.iceberg.Files.localInput(
        Paths.get(getClass.getResource("/file-splits.parquet").toURI).toFile)
      withResource(ParquetFileReader.open(GpuParquetIO.file(fixture), options)) {
        expected =>
          withResource(GpuParquetIO.openReader(
              input, new Path(input.path()), options, Map.empty)) { actual =>
            actual.getRowGroups.asScala.map(_.getStartingPos) shouldBe
              expected.getRowGroups.asScala.map(_.getStartingPos)
            actual.getRecordCount shouldBe expected.getRecordCount
            input.bodyReads shouldBe 0
            input.tailReads should be > 0
            val columns = actual.getFileMetaData.getSchema.getColumns.asScala
            var dictionaryCount = 0
            actual.getRowGroups.asScala.zipWithIndex.foreach { case (block, index) =>
              val dictionaries: DictionaryPageReadStore = actual.getDictionaryReader(block)
              val expectedDictionaries: DictionaryPageReadStore =
                expected.getDictionaryReader(index)
              columns.foreach { column =>
                val page = dictionaries.readDictionaryPage(column)
                val expectedPage = expectedDictionaries.readDictionaryPage(column)
                if (expectedPage == null) {
                  page shouldBe null
                } else {
                  dictionaryCount += 1
                  val bytes = new ByteArrayOutputStream
                  val expectedBytes = new ByteArrayOutputStream
                  page.getBytes.writeAllTo(bytes)
                  expectedPage.getBytes.writeAllTo(expectedBytes)
                  bytes.toByteArray shouldBe expectedBytes.toByteArray
                }
              }
            }
            dictionaryCount should be > 0
            input.bodyReads should be > 0
          }
      }
      input.closes shouldBe input.opens
    }
  }

  test("constructor closes the native stream when dictionary filtering fails") {
    val input = new MemoryFile(parquetBytes)
    val column = withResource(GpuParquetIO.openReader(input, new Path(input.path()),
        ParquetReadOptions.builder().build(), Map.empty)) { reader =>
      reader.getRowGroups.asScala.flatMap(_.getColumns.asScala)
        .find(c => c.hasDictionaryPage &&
          c.getPrimitiveType.getPrimitiveTypeName == PrimitiveTypeName.INT32)
        .get.getPath.toDotString
    }
    input.opens shouldBe 0
    input.failBodyRead = true
    val options = ParquetReadOptions.builder()
      .useStatsFilter(false).useDictionaryFilter(true).useBloomFilter(false)
      .withRecordFilter(FilterCompat.get(FilterApi.eq(
        FilterApi.intColumn(column), Integer.valueOf(0))))
      .build()
    val failure = intercept[Exception] {
      GpuParquetIO.openReader(input, new Path(input.path()), options, Map.empty)
    }
    Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
      .exists(_.getMessage == "dictionary read failed") shouldBe true
    input.opens shouldBe 1
    input.closes shouldBe 1
  }

  test("tail read failure does not open a native stream") {
    val input = new MemoryFile(parquetBytes) {
      override def readTail(length: Long, output: HostMemoryBuffer): Unit =
        throw new IOException("tail read failed")
    }
    intercept[IOException] {
      GpuParquetIO.openReader(input, new Path(input.path()),
        ParquetReadOptions.builder().build(), Map.empty)
    }.getMessage shouldBe "tail read failed"
    input.opens shouldBe 0
  }
}
