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

import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}

class GpuParquetUtilsSuite extends AnyFunSuite {
  @scala.annotation.nowarn("msg=method getPath in class ColumnChunkMetaData is deprecated")
  private def column(path: String*): ColumnChunkMetaData = {
    val result = mock(classOf[ColumnChunkMetaData])
    when(result.getPath).thenReturn(ColumnPath.get(path: _*))
    when(result.getTotalUncompressedSize).thenReturn(32L)
    result
  }

  for ((schema, selected) <- Seq(
        ("optional int32 a.b;", Seq(1)),
        ("optional group a { optional int32 b; }", Seq(0)),
        ("optional group a.b { optional int32 c; }", Seq(2)),
        ("optional group a { optional int32 b.c; }", Seq(3)))) {
    test(s"clip dotted path components: $schema") {
      val columns = Seq(column("a", "b"), column("a.b"),
        column("a.b", "c"), column("a", "b.c"), column("unrelated"))
      val block = new BlockMetaData()
      block.setRowCount(13L)
      columns.foreach(block.addColumn)
      val readSchema = MessageTypeParser.parseMessageType(s"message root { $schema }")
      val clipped = GpuParquetUtils.clipBlocksToSchema(
        readSchema, Collections.singletonList(block))
      assert(clipped.length == 1)
      assert(clipped.head.getRowCount == 13L)
      assert(clipped.head.getColumns.asScala.toSeq == selected.map(columns))
      assert(clipped.head.getTotalByteSize == 32L)
      assert(block.getColumns.asScala.toSeq == columns)
    }
  }

  for (nested <- Seq(false, true)) {
    test(s"field-ID clipping excludes case-distinct physical siblings: nested=$nested") {
      val id = new MetadataBuilder().putLong("parquet.field.id", 1).build()
      val fields = "optional int32 A = 5; optional int32 a = 1;"
      val physicalFields = if (nested) s"optional group s { $fields }" else fields
      val fileSchema = MessageTypeParser.parseMessageType(s"message root { $physicalFields }")
      val requested = StructType(Seq(StructField("x", IntegerType, metadata = id)))
      val readSchema = if (nested) StructType(Seq(StructField("s", requested))) else requested
      val clippedSchema = ParquetSchemaUtils.clipParquetSchema(
        fileSchema, readSchema, caseSensitive = false, useFieldId = true)
      val columns = fileSchema.getPaths.asScala.map(path => column(path: _*)).toSeq
      val block = new BlockMetaData()
      block.setRowCount(2L)
      columns.foreach(block.addColumn)
      val clipped = GpuParquetUtils.clipBlocksToSchema(
        clippedSchema, Collections.singletonList(block))
      assert(clipped.head.getColumns.asScala.toSeq == Seq(columns(1)))
      assert(clipped.head.getRowCount == 2L)
      assert(clipped.head.getTotalByteSize == 32L)
      assert(block.getColumns.asScala.toSeq == columns)
    }
  }
}
