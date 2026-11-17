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
{"spark":["400","401","402","403","404"],"carryForward":"none"}
{"spark":["411","412","413"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.parquet._
import com.nvidia.spark.rapids.iceberg.parquet.metadata.GpuIcebergMetadataReader
import com.nvidia.spark.rapids.iceberg.parquet.metadata.GpuIcebergMetadataReader.FileMetadata
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{DataFile, DataFiles, DeleteFile, FileFormat, FileScanTask, MetadataColumns,
  PartitionSpec, Schema}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.types.Types
import org.mockito.Mockito.{mock, spy, when}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

class GpuIcebergMetadataReaderSuite extends RmmSparkRetrySuiteBase {
  private val schema = new Schema(Types.NestedField.optional(1, "p", Types.StringType.get()))

  private def conf(expected: Schema = schema, rows: Int = 100, bytes: Long = 1024 * 1024,
      columnBytes: Long = Int.MaxValue): GpuIcebergParquetReaderConf = {
    GpuIcebergParquetReaderConf(true, new Configuration(), rows, bytes, bytes, columnBytes,
      false, 0, false, None, false, Map.empty, SingleFile, expected, None, false)
  }

  private def newReader(files: Seq[FileMetadata], readerConf: GpuIcebergParquetReaderConf)
      : GpuIcebergMetadataReader = new GpuIcebergMetadataReader(files.asJava, readerConf)

  private def file(value: String, rows: Long, path: String = "/does-not-exist.parquet")
      : FileMetadata = {
    val constants = new JHashMap[Integer, Any]()
    constants.put(1, if (value == null) null else UTF8String.fromString(value))
    new FileMetadata(path, rows, constants)
  }

  private def task(start: Long = 0, length: Long = 100): FileScanTask = {
    val data: DataFile = spy[DataFile](DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath("/does-not-exist.parquet").withFileSizeInBytes(100).withRecordCount(5)
      .withSplitOffsets(Seq[java.lang.Long](4L, 50L).asJava).build())
    val scan = mock(classOf[FileScanTask])
    when(scan.file()).thenReturn(data)
    when(scan.start()).thenReturn(start)
    when(scan.length()).thenReturn(length)
    when(scan.deletes()).thenReturn(java.util.Collections.emptyList[DeleteFile]())
    when(scan.residual()).thenReturn(Expressions.alwaysTrue())
    scan
  }

  private def eligible(scan: FileScanTask, expected: Schema = schema): Boolean =
    GpuIcebergMetadataReader.plan(Seq(scan).asJava, expected,
      _ => file("a", 5).getConstants()).isPresent

  test("only complete files with exact unfiltered row counts are eligible") {
    assert(eligible(task()))
    assert(eligible(task(4, 96)))
    assert(!eligible(task(4, 46)))
    assert(!eligible(task(50, 50)))
    val filtered = task()
    when(filtered.residual()).thenReturn(Expressions.equal("p", "a"))
    assert(!eligible(filtered))
    val deleted = task()
    when(deleted.deletes()).thenReturn(Seq(mock(classOf[DeleteFile])).asJava)
    assert(!eligible(deleted))
    val negative = task()
    when(negative.file().recordCount()).thenReturn(-1L)
    assert(!eligible(negative))
    val orc = task()
    when(orc.file().format()).thenReturn(FileFormat.ORC)
    assert(!eligible(orc))
  }

  test("missing physical fields and row-dependent metadata retain the data reader") {
    assert(!eligible(task(), new Schema(Types.NestedField.optional(2, "data",
      Types.StringType.get()))))
    Seq(MetadataColumns.ROW_POSITION, MetadataColumns.IS_DELETED,
      Types.NestedField.optional(ShimUtils.rowIdFieldId(), "_row_id", Types.LongType.get()),
      Types.NestedField.optional(ShimUtils.lastUpdatedSequenceNumberFieldId(),
        "_last_updated_sequence_number", Types.LongType.get())).foreach { field =>
      assert(!eligible(task(), new Schema(field)))
    }
    assert(eligible(task(), new Schema()))
    assert(eligible(task(), new Schema(MetadataColumns.FILE_PATH)))
  }

  test("different constants combine after materialization without opening data files") {
    val files = Seq(file("a", 2), file("unused", 0), file("b", 3), file(null, 1), file("a", 1))
    withResource(newReader(files, conf())) { reader =>
      assert(reader.hasNext)
      assert(reader.hasNext)
      withResource(reader.next()) { batch =>
        assert(batch.numRows() == 7)
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          val values = (0 until batch.numRows()).map { i =>
            if (host.isNullAt(i)) null else host.getUTF8String(i).toString
          }
          assert(values == Seq("a", "a", "b", "b", "b", null, "a"))
        }
      }
      assert(!reader.hasNext)
      intercept[NoSuchElementException](reader.next())
    }
  }

  test("combining respects row limits across file boundaries") {
    withResource(newReader(Seq(file("a", 3), file("b", 3)),
        conf(rows = 4))) { reader =>
      val sizes = reader.map { batch => withResource(batch)(_.numRows()) }.toList
      assert(sizes == List(4, 2))
    }
  }

  test("identical partition constants cannot hide different projected file paths") {
    val expected = new Schema(schema.columns().get(0), MetadataColumns.FILE_PATH)
    val files = Seq(file("a", 2, "/first"), file("a", 1, "/first"), file("a", 2, "/second"))
    withResource(newReader(files, conf(expected))) { reader =>
      withResource(reader.next()) { batch =>
        withResource(batch.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          val paths = (0 until batch.numRows()).map(i => host.getUTF8String(i).toString)
          assert(paths == Seq("/first", "/first", "/first", "/second", "/second"))
        }
      }
      assert(!reader.hasNext)
    }
  }

  test("count-only reads combine files and retain Long row counts") {
    withResource(newReader(
        Seq(file("a", Int.MaxValue.toLong), file("b", 7)),
        conf(new Schema(), rows = Int.MaxValue))) { reader =>
      val sizes = reader.map { batch =>
        withResource(batch) { cb =>
          assert(cb.numCols() == 0)
          cb.numRows()
        }
      }.toList
      assert(sizes == List(Int.MaxValue, 7))
    }
  }

  test("nested strings and binary constants obey column limits after combining") {
    val nested = new Schema(Types.NestedField.optional(1, "partition", Types.StructType.of(
      Types.NestedField.optional(2, "s", Types.StringType.get()),
      Types.NestedField.optional(3, "b", Types.BinaryType.get()))))
    val constants = new JHashMap[Integer, Any]()
    constants.put(1, InternalRow(UTF8String.fromString("éé"), Array[Byte](1, 2, 3)))
    val files = Seq(new FileMetadata("/absent-a", 2, constants),
      new FileMetadata("/absent-b", 3, constants))
    withResource(newReader(files, conf(nested, columnBytes = 12))) { reader =>
      val sizes = reader.map { batch =>
        withResource(batch) { cb =>
          withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
            (0 until cb.numRows()).foreach { i =>
              val struct = host.getStruct(i)
              assert(struct.getUTF8String(0).toString == "éé")
              assert(struct.getBinary(1).toSeq == Seq[Byte](1, 2, 3))
            }
          }
          cb.numRows()
        }
      }.toList
      assert(sizes == List(3, 2))
    }
  }

  test("the estimated batch byte limit bounds materialization and concatenation") {
    withResource(newReader(Seq(file("abcd", 2), file("efgh", 3)),
        conf(bytes = 18))) { reader =>
      assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(2, 2, 1))
    }
  }

  for (skipAllocations <- Seq(0, 2, 4)) {
    test(s"split and retry preserves mixed constants after $skipAllocations allocations") {
      withResource(newReader(Seq(file("a", 3), file("b", 3)), conf())) { reader =>
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, skipAllocations)
        val values = ArrayBuffer.empty[String]
        reader.foreach { batch =>
          withResource(batch) { cb =>
            withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
              (0 until cb.numRows()).foreach(i => values += host.getUTF8String(i).toString)
            }
          }
        }
        assert(values.toSeq == Seq("a", "a", "a", "b", "b", "b"))
        assert(RmmSpark.getAndResetNumSplitRetryThrow(1) == 1)
      }
    }
  }

  test("a single row over the soft byte limit cannot make the next file's row budget negative") {
    withResource(newReader(Seq(file("a" * 100, 1), file("b", 2)),
        conf(bytes = 18))) { reader =>
      assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(1, 2))
    }
  }

  test("empty input and early close do not leave batches pending") {
    withResource(newReader(Seq(file("a", 0)), conf())) { reader =>
      assert(!reader.hasNext)
    }
    val reader = newReader(Seq(file("a", 20)), conf(rows = 3))
    withResource(reader) { r => withResource(r.next()) { cb => assert(cb.numRows() == 3) } }
    assert(!reader.hasNext)
  }
}
