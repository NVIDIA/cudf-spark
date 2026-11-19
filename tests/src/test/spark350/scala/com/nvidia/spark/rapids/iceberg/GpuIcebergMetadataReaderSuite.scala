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
{"spark":["400","411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{CombineConf, GpuColumnVector, RmmSparkRetrySuiteBase,
  ThreadPoolConfBuilder}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.parquet._
import com.nvidia.spark.rapids.iceberg.parquet.metadata.GpuIcebergMetadataReader
import com.nvidia.spark.rapids.iceberg.parquet.metadata.GpuIcebergMetadataReader.FileMetadata
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}
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

  private def multiThread(disabled: Boolean = false, threshold: Long = 1024 * 1024,
      filePath: Boolean = false, rowPosition: Boolean = false): MultiThread = {
    MultiThread(new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false), 2,
      CombineConf(threshold, 0), disabled, filePath, rowPosition)
  }

  private def coalescing(filePath: Boolean = false, rowPosition: Boolean = false): MultiFile =
    MultiFile(new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false), filePath, rowPosition)

  private def conf(expected: Schema = schema, rows: Int = 100, bytes: Long = 1024 * 1024,
      columnBytes: Long = Int.MaxValue, threadConf: ThreadConf = SingleFile)
      : GpuIcebergParquetReaderConf = {
    GpuIcebergParquetReaderConf(true, new Configuration(), rows, bytes, bytes, columnBytes,
      false, 0, false, None, false, Map.empty, threadConf, expected, None, false)
  }

  private def plan(files: Seq[FileMetadata], readerConf: GpuIcebergParquetReaderConf)
      : java.util.List[FileMetadata] = {
    val tasks = files.map(f => task(path = f.getPath(), rows = f.getRowCount()))
    val metadata = tasks.zip(files).toMap
    GpuIcebergMetadataReader.plan(tasks.asJava, readerConf, t => metadata(t).getConstants()).get()
  }

  private def newReader(files: Seq[FileMetadata], readerConf: GpuIcebergParquetReaderConf)
      : GpuIcebergMetadataReader = new GpuIcebergMetadataReader(plan(files, readerConf), readerConf)

  private def file(value: String, rows: Long, path: String = "/does-not-exist.parquet")
      : FileMetadata = {
    val constants = new JHashMap[Integer, Any]()
    constants.put(1, if (value == null) null else UTF8String.fromString(value))
    new FileMetadata(path, rows, constants)
  }

  private def task(start: Long = 0, length: Long = 100, path: String = "/does-not-exist.parquet",
      rows: Long = 5): FileScanTask = {
    val data: DataFile = spy[DataFile](DataFiles.builder(PartitionSpec.unpartitioned())
      .withFormat(FileFormat.PARQUET).withPath(path).withFileSizeInBytes(100).withRecordCount(rows)
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
    GpuIcebergMetadataReader.plan(Seq(scan).asJava, conf(expected),
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

  test("a late constant mismatch keeps every file separate and preserves nulls") {
    val files = Seq(file("a", 2), file("a", 1), file("unused", 0), file("b", 3), file(null, 1))
    withResource(newReader(files, conf(threadConf = multiThread()))) { reader =>
      assert(reader.hasNext)
      assert(reader.hasNext)
      val values = ArrayBuffer.empty[String]
      val sizes = reader.map { batch =>
        withResource(batch) { cb =>
          withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
            (0 until cb.numRows()).foreach { i =>
              values += (if (host.isNullAt(i)) null else host.getUTF8String(i).toString)
            }
          }
          cb.numRows()
        }
      }.toList
      assert(sizes == List(2, 1, 3, 1))
      assert(values.toSeq == Seq("a", "a", "a", "b", "b", "b", null))
      intercept[NoSuchElementException](reader.next())
    }
  }

  for ((mode, threadConf) <- Seq("multithreaded" -> multiThread(threshold = 1),
      "coalescing" -> coalescing())) {
    test(s"$mode planning sums all compatible files using the first path and constants") {
      val first = file("a", 3, "/first")
      val constants = new JHashMap[Integer, Any]()
      constants.put(1, UTF8String.fromString("a"))
      constants.put(99, "unprojected")
      val files = Seq(first, new FileMetadata("/second", 2, constants), file("a", 2, "/third"))
      val readerConf = conf(rows = 4, threadConf = threadConf)
      val planned = plan(files, readerConf)
      assert(planned.size() == 1)
      assert(planned.get(0).getRowCount() == 7)
      assert(planned.get(0).getPath() == first.getPath())
      assert(planned.get(0).getConstants() eq first.getConstants())
      withResource(new GpuIcebergMetadataReader(planned, readerConf)) { reader =>
        assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(4, 3))
      }
    }
  }

  for ((mode, threadConf) <- Seq("per-file" -> SingleFile,
      "multithreaded" -> multiThread(), "coalescing" -> coalescing())) {
    test(s"projected file paths preserve file boundaries in $mode mode") {
      val expected = new Schema(schema.columns().get(0), MetadataColumns.FILE_PATH)
      val files = Seq(file("a", 2, "/first"), file("a", 1, "/first"), file("a", 2, "/second"))
      withResource(newReader(files, conf(expected, threadConf = threadConf))) { reader =>
        val paths = ArrayBuffer.empty[String]
        val sizes = reader.map { batch =>
          withResource(batch) { cb =>
            withResource(cb.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
              (0 until cb.numRows()).foreach(i => paths += host.getUTF8String(i).toString)
            }
            cb.numRows()
          }
        }.toList
        assert(sizes == List(2, 1, 2))
        assert(paths.toSeq == Seq("/first", "/first", "/first", "/second", "/second"))
      }
    }
  }

  for ((mode, threadConf, sizes) <- Seq(
      ("per-file", SingleFile, List(Int.MaxValue, 7, 9)),
      ("combined", multiThread(), List(Int.MaxValue, 16)))) {
    test(s"$mode count-only reads support more than Int.MaxValue rows") {
      val files = Seq(file("a", Int.MaxValue.toLong + 7), file("b", 9))
      withResource(newReader(files,
          conf(new Schema(), rows = Int.MaxValue, threadConf = threadConf))) { reader =>
        val actual = reader.map { batch =>
          withResource(batch) { cb =>
            assert(cb.numCols() == 0)
            cb.numRows()
          }
        }.toList
        assert(actual == sizes)
      }
    }
  }

  test("keep per-file counts when their sum would overflow a descriptor") {
    val files = Seq(file("a", Long.MaxValue), file("a", 1))
    val planned = plan(files, conf(threadConf = multiThread()))
    assert(planned.asScala.map(_.getRowCount()).toList == List(Long.MaxValue, 1L))
  }

  test("nested strings and binary constants obey column limits after combining") {
    val nested = new Schema(Types.NestedField.optional(1, "partition", Types.StructType.of(
      Types.NestedField.optional(2, "s", Types.StringType.get()),
      Types.NestedField.optional(3, "b", Types.BinaryType.get()))))
    def nestedFile(path: String, rows: Long): FileMetadata = {
      val constants = new JHashMap[Integer, Any]()
      constants.put(1, InternalRow(UTF8String.fromString("éé"), Array[Byte](1, 2, 3)))
      new FileMetadata(path, rows, constants)
    }
    val files = Seq(nestedFile("/first", 2), nestedFile("/second", 3))
    withResource(newReader(files,
        conf(nested, columnBytes = 12, threadConf = multiThread()))) { reader =>
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

  test("different signed zeros in partition structs cannot use the first file's constant") {
    val nested = new Schema(Types.NestedField.optional(1, "partition", Types.StructType.of(
      Types.NestedField.optional(2, "d", Types.DoubleType.get()))))
    def partition(value: Double): FileMetadata = {
      val constants = new JHashMap[Integer, Any]()
      constants.put(1, InternalRow(value))
      new FileMetadata("/partition", 1, constants)
    }
    val planned = plan(Seq(partition(-0.0), partition(0.0)),
      conf(nested, threadConf = multiThread()))
    assert(planned.size() == 2)
  }

  test("materialization retries without changing the planned batch size") {
    withResource(newReader(Seq(file("a", 3), file("a", 3)),
        conf(threadConf = multiThread()))) { reader =>
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(reader.next()) { cb =>
        assert(cb.numRows() == 6)
        withResource(cb.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          assert((0 until cb.numRows()).forall(i => host.getUTF8String(i).toString == "a"))
        }
      }
      assert(!reader.hasNext)
      assert(RmmSpark.getAndResetNumRetryThrow(1) == 1)
      assert(RmmSpark.getAndResetNumSplitRetryThrow(1) == 0)
    }
  }

  test("the expected schema limits batch rows even for a short constant") {
    // Two nullable strings use an estimated 40 bytes of data, 12 of offsets and 64 of validity.
    withResource(newReader(Seq(file("a", 7)), conf(bytes = 116))) { reader =>
      assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(2, 2, 2, 1))
    }
  }

  test("long constants further limit the estimated batch rows") {
    withResource(newReader(Seq(file("a" * 100, 7)), conf(bytes = 250))) { reader =>
      assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(2, 2, 2, 1))
    }
  }

  test("one file's variable-width limit bounds batches before materialization") {
    withResource(newReader(Seq(file("éé", 8)), conf(columnBytes = 12))) { reader =>
      assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(3, 3, 2))
    }
  }

  for ((mode, threadConf) <- Seq(
      "per-file" -> SingleFile,
      "disabled" -> multiThread(disabled = true),
      "zero threshold" -> multiThread(threshold = 0),
      "file-path flag" -> multiThread(filePath = true),
      "row-position flag" -> multiThread(rowPosition = true),
      "coalescing file-path flag" -> coalescing(filePath = true),
      "coalescing row-position flag" -> coalescing(rowPosition = true))) {
    test(s"combining respects $mode") {
      withResource(newReader(Seq(file("a", 2), file("a", 2)),
          conf(threadConf = threadConf))) { reader =>
        assert(reader.map(batch => withResource(batch)(_.numRows())).toList == List(2, 2))
      }
    }
  }

  test("a value over the hard column limit is rejected before materialization") {
    withResource(newReader(Seq(file("abcd", 1)), conf(columnBytes = 3))) { reader =>
      val error = intercept[IllegalArgumentException](reader.next())
      assert(error.getMessage.contains("GPU column size limit"))
    }
  }

  test("a split-and-retry failure propagates from materialization") {
    withResource(newReader(Seq(file("a", 3)), conf())) { reader =>
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      intercept[GpuSplitAndRetryOOM](reader.next())
      assert(RmmSpark.getAndResetNumSplitRetryThrow(1) == 1)
    }
  }

  test("a single row over the soft byte limit still makes progress") {
    withResource(newReader(Seq(file("a" * 200, 1), file("b", 2)),
        conf(bytes = 116, threadConf = multiThread()))) { reader =>
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
