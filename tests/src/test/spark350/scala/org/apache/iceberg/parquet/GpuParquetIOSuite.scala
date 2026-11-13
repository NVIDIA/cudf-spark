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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["411","412","413"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.parquet

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuMetric, LocalGpuMetric}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream
import com.nvidia.spark.rapids.shims.GpuBatchScanExec
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions
import org.apache.iceberg.shaded.org.apache.parquet.column.page.DictionaryPageReadStore
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.mockito.Mockito.mock
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.getBuilder

class GpuParquetIOSuite extends AnyFunSuite with Matchers with Eventually {
  private def withCacheSession(body: SparkSession => Unit): Unit = {
    // File cache is a startup-only setting; changing a shared session's SQLConf is insufficient.
    TrampolineUtil.cleanupAnyExistingSession()
    try {
      val spark = getBuilder().master("local[1]").appName("Iceberg footer cache")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.filecache.enabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
      body(spark)
    } finally {
      TrampolineUtil.cleanupAnyExistingSession()
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
    private val name = s"s3://test-bucket/${UUID.randomUUID()}.parquet"

    override def path(): String = name
    override def getLength: Long = data.length

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

  test("cached footer retains original offsets across seeks and boundary reads") {
    val data = parquetBytes
    val footer = framedFooter(data)
    val start = data.length - footer.length + MAGIC.length
    val input = new MemoryFile(data)
    val cached = new GpuParquetCachedFooterInputFile(input, footer)
    withResource(cached.newStream()) { stream =>
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
    withResource(cached.newStream()) { stream =>
      stream.seek(start)
      stream.read() shouldBe (data(start) & 0xff)
    }
    input.opens shouldBe 1
  }

  test("cached footer preserves split filtering and dictionary contents") {
    val data = parquetBytes
    val footer = framedFooter(data)
    for (options <- Seq(ParquetReadOptions.builder().build(),
        ParquetReadOptions.builder().withRange(0, data.length / 2).build())) {
      val input = new MemoryFile(data)
      withResource(ParquetFileReader.open(GpuParquetIO.file(new MemoryFile(data)), options)) {
        expected =>
          withResource(ParquetFileReader.open(
              new GpuParquetCachedFooterInputFile(input, footer), options)) { actual =>
            actual.getRowGroups.asScala.map(_.getStartingPos) shouldBe
              expected.getRowGroups.asScala.map(_.getStartingPos)
            actual.getRecordCount shouldBe expected.getRecordCount
            input.bodyReads shouldBe 0
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

  test("reader reuses cached footer without reading the source again") {
    withCacheSession { _ =>
      val input = new MemoryFile(parquetBytes)
      val hits = new LocalGpuMetric
      val misses = new LocalGpuMetric
      val metrics: Map[String, GpuMetric] = Map(
        GpuMetric.FILECACHE_FOOTER_HITS -> hits,
        GpuMetric.FILECACHE_FOOTER_MISSES -> misses)
      val options = ParquetReadOptions.builder().build()
      withResource(GpuParquetIO.openReader(input, new Path(input.path()), options, metrics)) {
        _.getRecordCount should be > 0L
      }
      misses.value shouldBe 1L
      val reads = input.tailReads + input.bodyReads
      reads should be > 0
      eventually(timeout(Span(30, Seconds))) {
        withResource(FileCache.get.getFooter(input)) { _.isDefined shouldBe true }
      }
      withResource(GpuParquetIO.openReader(input, new Path(input.path()), options, metrics)) {
        _.getRecordCount should be > 0L
      }
      hits.value shouldBe 1L
      misses.value shouldBe 1L
      input.tailReads + input.bodyReads shouldBe reads
      input.closes shouldBe input.opens
    }
  }

  test("Iceberg scans with dictionary predicates match CPU on cold and cached GPU reads") {
    val warehouse = Files.createTempDirectory("iceberg-cached-footer")
    try {
      withCacheSession { spark =>
        spark.conf.set("spark.sql.catalog.footer_test", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.footer_test.type", "hadoop")
        spark.conf.set("spark.sql.catalog.footer_test.warehouse", warehouse.toUri.toString)
        spark.conf.set("spark.rapids.sql.enabled", "false")
        spark.sql("CREATE TABLE footer_test.default.data USING ICEBERG " +
          "TBLPROPERTIES ('format-version'='2') AS " +
          "SELECT id, CAST(id % 4 AS STRING) AS payload FROM range(0, 2000, 1, 2)")
        // The absent value is within the column's min/max, so dictionary filtering can reject it.
        for (value <- Seq("2", "1.5")) {
          val query = s"SELECT id, payload FROM footer_test.default.data " +
            s"WHERE payload = '$value' AND id > 100"
          spark.conf.set("spark.rapids.sql.enabled", "false")
          val expected = spark.sql(query).collect().toSeq.sortBy(_.getLong(0))
          spark.conf.set("spark.rapids.sql.enabled", "true")
          for (reader <- Seq("COALESCING", "MULTITHREADED")) {
            spark.conf.set("spark.rapids.sql.format.parquet.reader.type", reader)
            (0 until 2).foreach { _ =>
              val df = spark.sql(query)
              df.collect().toSeq.sortBy(_.getLong(0)) shouldBe expected
              df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
                .isDefined shouldBe true
            }
          }
        }
      }
    } finally {
      FileUtil.fullyDelete(warehouse.toFile)
    }
  }
}
