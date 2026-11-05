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

package com.nvidia.spark.rapids.parquet

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ParquetDictFilterPrefetchSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val schema = MessageTypeParser.parseMessageType(
    "message test { required binary s (UTF8); required int32 i; required int64 u; }")
  private val dir = Files.createTempDirectory("dict-filter-prefetch").toFile
  private val file = new File(dir, "test.parquet")
  private val path = new Path(file.toURI)

  // withRowGroupSize(Int) is deprecated in newer parquet-mr but the only option in older ones
  @scala.annotation.nowarn("msg=deprecated")
  override def beforeAll(): Unit = {
    val conf = new Configuration(false)
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter.builder(path).withConf(conf)
      .withRowGroupSize(8 * 1024).withPageSize(1024).withDictionaryPageSize(64 * 1024)
      .withDictionaryEncoding(true).build()
    val groups = new SimpleGroupFactory(schema)
    try {
      // every group of rows has s in {"a", "c"} plus one group-specific value, and i in a
      // group-specific set of two values, so the statistics cover values the dictionaries miss
      (0 until 4000).foreach { r =>
        val g = r / 400
        val s = r % 3 match {
          case 0 => "a"
          case 1 => "c"
          case _ => s"b$g"
        }
        writer.write(groups.newGroup().append("s", s).append("i", 10 * g + 2 * (r % 2))
          .append("u", r.toLong))
      }
    } finally {
      writer.close()
    }
  }

  override def afterAll(): Unit = {
    file.delete()
    new File(dir, ".test.parquet.crc").delete()
    dir.delete()
  }

  private def footerFile(): Array[Byte] = {
    val raf = new RandomAccessFile(file, "r")
    try {
      val len = raf.length()
      val trailer = new Array[Byte](8)
      raf.seek(len - 8)
      raf.readFully(trailer)
      val footerLen = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN).getInt
      val tail = new Array[Byte](footerLen + 8)
      raf.seek(len - 8 - footerLen)
      raf.readFully(tail)
      MAGIC ++ tail
    } finally {
      raf.close()
    }
  }

  private class LocalReader extends ParquetDictFilterPrefetch.RangeReader {
    var calls = 0
    var bytes = 0L

    override def apply(ranges: Seq[(Long, Int)]): Seq[Array[Byte]] = {
      calls += 1
      bytes += ranges.map(_._2.toLong).sum
      val raf = new RandomAccessFile(file, "r")
      try {
        ranges.map { case (offset, length) =>
          val b = new Array[Byte](length)
          raf.seek(offset)
          raf.readFully(b)
          b
        }
      } finally {
        raf.close()
      }
    }
  }

  @scala.annotation.nowarn("msg=deprecated")
  private def footer(range: Option[(Long, Long)]): ParquetMetadata = {
    val filter = range.map { case (s, e) => ParquetMetadataConverter.range(s, e) }
      .getOrElse(ParquetMetadataConverter.NO_FILTER)
    ParquetFileReader.readFooter(new Configuration(false), path, filter)
  }

  @scala.annotation.nowarn("msg=deprecated")
  private def pathBased(conf: Configuration, f: ParquetMetadata): java.util.List[BlockMetaData] = {
    val reader = new ParquetFileReader(conf, f.getFileMetaData, path, f.getBlocks,
      Collections.emptyList[ColumnDescriptor])
    try reader.getRowGroups finally reader.close()
  }

  private def conf(p: FilterPredicate, dictionary: Boolean = true): Configuration = {
    val c = new Configuration(false)
    ParquetInputFormat.setFilterPredicate(c, p)
    c.setBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, dictionary)
    c
  }

  private val s = FilterApi.binaryColumn("s")
  private val i = FilterApi.intColumn("i")
  private def str(v: String) = Binary.fromString(v)

  private val predicates: Seq[(String, FilterPredicate, Set[String])] = Seq(
    ("s = b3", FilterApi.eq(s, str("b3")), Set("s")),
    ("s = b", FilterApi.eq(s, str("b")), Set("s")),
    ("s = zz", FilterApi.eq(s, str("zz")), Set("s")),
    ("i = 32", FilterApi.eq(i, Integer.valueOf(32)), Set("i")),
    ("i = 31", FilterApi.eq(i, Integer.valueOf(31)), Set("i")),
    ("s = b3 and i = 30", FilterApi.and(FilterApi.eq(s, str("b3")),
      FilterApi.eq(i, Integer.valueOf(30))), Set("s", "i")),
    ("s = b or i = 41", FilterApi.or(FilterApi.eq(s, str("b")),
      FilterApi.eq(i, Integer.valueOf(41))), Set("s", "i")),
    ("not s = a", FilterApi.not(FilterApi.eq(s, str("a"))), Set("s")))

  test("the test file has several row groups") {
    assert(footer(None).getBlocks.size >= 4)
  }

  for ((name, p, columns) <- predicates;
       (label, range) <- Seq(("whole file", None), ("split", Some((0L, file.length / 2))))) {
    test(s"same row groups as the path based reader: $name, $label") {
      val f = footer(range)
      val expected = pathBased(conf(p), f)
      val reader = new LocalReader
      val kept = ParquetDictFilterPrefetch.rowGroups(conf(p), path, footerFile(), range,
        f.getBlocks, p, columns, reader, () => fail("prefetch path fell back"))
      assertResult(expected.asScala.map(_.getStartingPos))(kept.asScala.map(_.getStartingPos))
      // the caller's block objects are returned
      kept.asScala.foreach(b => assert(f.getBlocks.asScala.exists(_ eq b)))
      assert(reader.calls <= 1, "dictionary pages are fetched with at most one vectored read")
    }
  }

  test("dictionary filtering is pruning row groups in this file") {
    val p = FilterApi.eq(s, str("b"))
    val f = footer(None)
    assert(pathBased(conf(p), f).size < f.getBlocks.size)
  }

  test("nothing is prefetched when dictionary filtering is disabled") {
    val p = FilterApi.eq(s, str("b"))
    val f = footer(None)
    val reader = new LocalReader
    val kept = ParquetDictFilterPrefetch.rowGroups(conf(p, dictionary = false), path,
      footerFile(), None, f.getBlocks, p, Set("s"), reader, () => fail("fell back"))
    assert(reader.calls == 0)
    assertResult(pathBased(conf(p, dictionary = false), f).size)(kept.size)
  }

  test("pages of columns the filters do not reference are read on demand") {
    val p = FilterApi.eq(s, str("b"))
    val f = footer(None)
    val reader = new LocalReader
    val kept = ParquetDictFilterPrefetch.rowGroups(conf(p), path, footerFile(), None,
      f.getBlocks, p, Set.empty, reader, () => fail("fell back"))
    assert(reader.calls >= 1, "dictionary pages are read on demand")
    assertResult(pathBased(conf(p), f).asScala.map(_.getStartingPos))(
      kept.asScala.map(_.getStartingPos))
  }

  test("a failure falls back to the path based reader") {
    val p = FilterApi.eq(s, str("b"))
    val f = footer(None)
    val expected = pathBased(conf(p), f)
    val kept = ParquetDictFilterPrefetch.rowGroups(conf(p), path, Array[Byte](1, 2, 3), None,
      f.getBlocks, p, Set("s"), new LocalReader, () => expected)
    assert(kept eq expected)
  }
}
