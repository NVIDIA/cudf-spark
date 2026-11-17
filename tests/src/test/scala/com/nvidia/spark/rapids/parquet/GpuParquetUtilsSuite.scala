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

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath}
import org.apache.parquet.schema.MessageTypeParser
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class GpuParquetUtilsSuite extends AnyFunSuite {
  @scala.annotation.nowarn("msg=method getPath in class ColumnChunkMetaData is deprecated")
  private def column(path: String*): ColumnChunkMetaData = {
    val result = mock(classOf[ColumnChunkMetaData])
    when(result.getPath).thenReturn(ColumnPath.get(path: _*))
    when(result.getTotalUncompressedSize).thenReturn(32L)
    result
  }

  for (caseSensitive <- Seq(false, true);
      (schema, selected) <- Seq(
        ("optional int32 a.b;", Seq(1)),
        ("optional group a { optional int32 b; }", Seq(0)),
        ("optional group a.b { optional int32 c; }", Seq(2)),
        ("optional group a { optional int32 b.c; }", Seq(3)))) {
    test(s"clip dotted path components: $schema, caseSensitive=$caseSensitive") {
      val columns = Seq(column("a", "b"), column("a.b"),
        column("a.b", "c"), column("a", "b.c"), column("unrelated"))
      val block = new BlockMetaData()
      block.setRowCount(13L)
      columns.foreach(block.addColumn)
      val readSchema = MessageTypeParser.parseMessageType(s"message root { $schema }")
      val clipped = GpuParquetUtils.clipBlocksToSchema(
        readSchema, Collections.singletonList(block), caseSensitive)
      assert(clipped.length == 1)
      assert(clipped.head.getRowCount == 13L)
      assert(clipped.head.getColumns.asScala.toSeq == selected.map(columns))
      assert(clipped.head.getTotalByteSize == 32L)
      assert(block.getColumns.asScala.toSeq == columns)
    }
  }

  test("case-insensitive clipping normalizes each component without changing column metadata") {
    val nested = column("A", "B")
    val flat = column("A.B")
    val block = new BlockMetaData()
    block.addColumn(nested)
    block.addColumn(flat)
    val readSchema = MessageTypeParser.parseMessageType("message root { optional int32 a.b; }")
    val blocks = Collections.singletonList(block)
    assert(GpuParquetUtils.clipBlocksToSchema(readSchema, blocks, isCaseSensitive = false)
      .head.getColumns.asScala.toSeq == Seq(flat))
    assert(GpuParquetUtils.clipBlocksToSchema(readSchema, blocks, isCaseSensitive = true)
      .head.getColumns.isEmpty)
    val uppercaseSchema = MessageTypeParser.parseMessageType(
      "message root { optional int32 A.B; }")
    assert(GpuParquetUtils.clipBlocksToSchema(uppercaseSchema, blocks, isCaseSensitive = true)
      .head.getColumns.asScala.toSeq == Seq(flat))
  }
  test("case-insensitive clipping handles repeated and different row-group paths") {
    val nested = column("A", "B")
    val flat = column("A.B")
    val lowerFlat = column("a.b")
    val unrelated = column("unrelated")
    val columnsByBlock = Seq(Seq(nested, flat), Seq(flat, nested, unrelated),
      Seq(lowerFlat, nested), Seq(unrelated, flat, nested))
    val blocks = columnsByBlock.map { columns =>
      val block = new BlockMetaData()
      columns.foreach(block.addColumn)
      block
    }
    val schema = MessageTypeParser.parseMessageType("message root { optional int32 a.b; }")
    val clipped = GpuParquetUtils.clipBlocksToSchema(
      schema, blocks.asJava, isCaseSensitive = false)
    assert(clipped.map(_.getColumns.asScala.toSeq) ==
      Seq(Seq(flat), Seq(flat), Seq(lowerFlat), Seq(flat)))
    assert(blocks.map(_.getColumns.asScala.toSeq) == columnsByBlock)
    // A second call with a different projection must not reuse the preceding decisions.
    val nestedSchema = MessageTypeParser.parseMessageType(
      "message root { optional group a { optional int32 b; } }")
    assert(GpuParquetUtils.clipBlocksToSchema(nestedSchema, blocks.asJava,
      isCaseSensitive = false).map(_.getColumns.asScala.toSeq) ==
      Seq.fill(4)(Seq(nested)))
  }

}
