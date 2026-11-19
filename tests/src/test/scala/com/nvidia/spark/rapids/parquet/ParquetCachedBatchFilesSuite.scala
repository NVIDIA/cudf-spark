/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.parquet

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, CompressionCodecName, ParquetMetadata}
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

class ParquetCachedBatchFilesSuite extends AnyFunSuite {
  private class TemporaryDirectory extends AutoCloseable {
    val path: java.nio.file.Path = Files.createTempDirectory("cached-parquet")
    override def close(): Unit = FileUtils.deleteDirectory(path.toFile)
  }

  private val parquetSchema = MessageTypeParser.parseMessageType(
    "message test { required int64 id; optional binary text (UTF8); optional double value; }")

  private def writeFile(directory: java.nio.file.Path, codec: CompressionCodecName,
      dictionary: Boolean): Array[Byte] = {
    val original = directory.resolve("input.parquet")
    val factory = new SimpleGroupFactory(parquetSchema)
    val builder = ExampleParquetWriter.builder(new Path(original.toUri))
      .withConf(new Configuration()).withType(parquetSchema).withRowGroupSize(8192L)
      .withPageSize(1024).withDictionaryEncoding(dictionary).withCompressionCodec(codec)
      .withExtraMetaData(Map("cache-marker" -> "preserved").asJava)
    withResource(builder.build()) { writer =>
      (0 until 40000).foreach { row =>
        val group = factory.newGroup().append("id", row.toLong)
        if (row % 5 != 0) group.append("text", "dictionary-" + row % 11)
        if (row % 7 != 0) group.append("value", row * 0.5)
        writer.write(group)
      }
    }
    Files.readAllBytes(original)
  }

  test("row-group records copy lazily and decode independently") {
    val codec = CompressionCodecName.SNAPPY
    val dictionary = true
    withResource(new TemporaryDirectory) { directory =>
      val bytes = writeFile(directory.path, codec, dictionary)
      val wrapped = ParquetCachedBatchFiles.fromBytes(bytes)
      val footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
      val footerOffset = bytes.length - 8 - footerLength
      var payloadReads = 0
      var trailerReads = 0
      val source = new ParquetCachedBatchFiles.EncodedSource {
        override val length: Long = wrapped.length
        override def readFully(offset: Long, destination: Array[Byte], start: Int,
            count: Int): Unit = {
          if (offset >= 4 && offset < footerOffset) payloadReads += 1
          if (offset + count > bytes.length - 8) trailerReads += 1
          wrapped.readFully(offset, destination, start, count)
        }
      }
      val original = ParquetCachedBatchFiles.readFooter(source)
      assert(original.getBlocks.size > 1)
      val columns = original.getBlocks.asScala.flatMap(_.getColumns.asScala)
      assert(columns.exists(c =>
        c.hasDictionaryPage && c.getFirstDataPageOffset > c.getDictionaryPageOffset))
      var rowIndex = 0
      var recordIndex = 0
      val records = ParquetCachedBatchFiles.split(source)
      val initialTrailerReads = trailerReads
      assert(initialTrailerReads > 0)
      assert(payloadReads == 0)
      var more = records.hasNext
      assert(payloadReads == 0)
      while (more) {
        val reads = payloadReads
        val record = records.next()
        assert(payloadReads > reads)
        val footer = ParquetCachedBatchFiles.readFooter(
          ParquetCachedBatchFiles.fromBytes(record.buffer))
        assert(footer.getBlocks.size == 1)
        assert(footer.getBlocks.get(0).getRowCount == record.numRows)
        assert(footer.getFileMetaData.getSchema == parquetSchema)
        assert(footer.getFileMetaData.getKeyValueMetaData.get("cache-marker") == "preserved")
        val file = directory.path.resolve(s"record-$recordIndex.parquet")
        Files.write(file, record.buffer)
        withResource(ParquetReader.builder[Group](new GroupReadSupport(), new Path(file.toUri))
            .withConf(new Configuration()).build()) { reader =>
          var group = reader.read()
          var recordRows = 0
          while (group != null) {
            assert(group.getLong("id", 0) == rowIndex.toLong)
            assert(group.getFieldRepetitionCount("text") == (if (rowIndex % 5 == 0) 0 else 1))
            if (rowIndex % 5 != 0) {
              assert(group.getString("text", 0) == "dictionary-" + rowIndex % 11)
            }
            assert(group.getFieldRepetitionCount("value") == (if (rowIndex % 7 == 0) 0 else 1))
            if (rowIndex % 7 != 0) assert(group.getDouble("value", 0) == rowIndex * 0.5)
            rowIndex += 1
            recordRows += 1
            group = reader.read()
          }
          assert(recordRows == record.numRows)
        }
        recordIndex += 1
        val afterRecord = payloadReads
        more = records.hasNext
        assert(payloadReads == afterRecord)
      }
      assert(payloadReads > 0)
      assert(trailerReads == initialTrailerReads)
      assert(rowIndex == 40000)
      assert(recordIndex == original.getBlocks.size)
    }
  }

  test("invalid encoded column ranges are rejected before payload copy") {
    withResource(new TemporaryDirectory) { directory =>
      val bytes = writeFile(directory.path, CompressionCodecName.UNCOMPRESSED, dictionary = false)
      val original = ParquetCachedBatchFiles.readFooter(ParquetCachedBatchFiles.fromBytes(bytes))
      val block = original.getBlocks.get(0)
      val first = block.getColumns.get(0)
      val bad = ColumnChunkMetaData.get(first.getPath, first.getPrimitiveType, first.getCodec,
        first.getEncodingStats, first.getEncodings, first.getStatistics,
        bytes.length.toLong, 0L, first.getValueCount, first.getTotalSize,
        first.getTotalUncompressedSize)
      val columns = bad +: block.getColumns.asScala.drop(1).toSeq
      val rewritten = GpuParquetUtils.newBlockMeta(block, columns)
      val metadata = new ParquetMetadata(original.getFileMetaData, Seq(rewritten).asJava)
      val footer = new ByteArrayOutputStream()
      org.apache.parquet.format.Util.writeFileMetaData(
        new ParquetMetadataConverter().toParquetMetadata(1, metadata), footer)
      val footerLength = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
        .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
      val output = new ByteArrayOutputStream()
      output.write(bytes, 0, bytes.length - footerLength - 8)
      output.write(footer.toByteArray)
      BytesUtils.writeIntLittleEndian(output, footer.size())
      output.write("PAR1".getBytes(StandardCharsets.US_ASCII))
      intercept[IllegalArgumentException] {
        ParquetCachedBatchFiles.split(ParquetCachedBatchFiles.fromBytes(output.toByteArray)).next()
      }
    }
  }

  test("a row group larger than the Java array limit fails before copying its payload") {
    withResource(new TemporaryDirectory) { directory =>
      val bytes = writeFile(directory.path, CompressionCodecName.UNCOMPRESSED, dictionary = false)
      val original = ParquetCachedBatchFiles.readFooter(ParquetCachedBatchFiles.fromBytes(bytes))
      val block = original.getBlocks.get(0)
      val first = block.getColumns.get(0)
      val huge = ColumnChunkMetaData.get(first.getPath, first.getPrimitiveType, first.getCodec,
        first.getEncodingStats, first.getEncodings, first.getStatistics,
        4L, 0L, first.getValueCount, Int.MaxValue.toLong, Int.MaxValue.toLong)
      val metadata = new ParquetMetadata(original.getFileMetaData,
        Seq(GpuParquetUtils.newBlockMeta(block, Seq(huge))).asJava)
      val footer = new ByteArrayOutputStream()
      org.apache.parquet.format.Util.writeFileMetaData(
        new ParquetMetadataConverter().toParquetMetadata(1, metadata), footer)
      val tail = new ByteArrayOutputStream()
      tail.write(footer.toByteArray)
      BytesUtils.writeIntLittleEndian(tail, footer.size())
      val magic = "PAR1".getBytes(StandardCharsets.US_ASCII)
      tail.write(magic)
      val tailBytes = tail.toByteArray
      val footerOffset = 4L + Int.MaxValue
      val source = new ParquetCachedBatchFiles.EncodedSource {
        override val length: Long = footerOffset + tailBytes.length
        override def readFully(offset: Long, destination: Array[Byte], start: Int,
            count: Int): Unit = {
          if (offset == 0 && count == 4) {
            System.arraycopy(magic, 0, destination, start, count)
          } else {
            assert(offset >= footerOffset, "Oversized payload must not be copied")
            System.arraycopy(tailBytes, (offset - footerOffset).toInt, destination, start, count)
          }
        }
      }
      intercept[java.io.IOException] { ParquetCachedBatchFiles.split(source).next() }
    }
  }


}
