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

import java.util.concurrent.CompletableFuture

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.io.async.AsyncRunner
import com.nvidia.spark.rapids.parquet._
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.{MessageType, MessageTypeParser, Type}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StructType}

class ParquetCombineSuite extends RmmSparkRetrySuiteBase {
  private val dataSchema = MessageTypeParser.parseMessageType("message test { optional int32 b; }")
  private val emptySchema = new MessageType("test", java.util.Collections.emptyList[Type]())
  private val readSchema = new StructType().add("b", IntegerType)
  private var taskContext: MockTaskContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    taskContext = new MockTaskContext(1, 0) {
      private var completed = false

      override def isCompleted(): Boolean = completed

      override def markTaskComplete(): Unit = {
        if (!completed) {
          completed = true
          super.markTaskComplete()
        }
      }
    }
    TrampolineUtil.setTaskContext(taskContext)
  }

  override def afterEach(): Unit = {
    try {
      taskContext.markTaskComplete()
    } finally {
      try {
        TrampolineUtil.unsetTaskContext()
      } finally {
        super.afterEach()
      }
    }
  }

  private class Reader(
      ordered: Boolean,
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta =
        _ => throw new UnsupportedOperationException,
      files: Array[PartitionedFile] = Array.empty,
      ignoreCorrupt: Boolean = false,
      queryUsesInputFile: Boolean = false,
      combineThreshold: Long = 1L << 20) extends MultiFileCloudParquetPartitionReader(
      null, new Configuration(false), files, filterFunc,
      true, None, false, 1000, 1L << 20, 1L << 20, 1L << 20, false, 0,
      false, CpuCompressionConfig.disabled(), Map.empty, new StructType(),
      new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false).build(),
      1, false, ignoreCorrupt, false, queryUsesInputFile, ordered,
      CombineConf(combineThreshold, 0)) {
    var failRowCount = false
    var failBuild = false
    var numDeletedRows = 0L

    private def file(key: Int) = PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow(key), s"file:///tmp/part-$key", 0, 0)

    def empty(key: Int, rows: Long, schema: StructType = readSchema): HostMemoryEmptyMetaData =
      newHMEmptyMetadataForChunks(file(key), 0, 1, DateTimeRebaseCorrected,
        DateTimeRebaseCorrected, false, if (rows == 0) null else emptySchema,
        if (rows == 0) null else schema, rows, Seq.empty)

    def data(key: Int, rows: Long, schema: MessageType = dataSchema)
        : HostMemoryBuffersWithMetaData = {
      val partFile = file(key)
      val meta = ParquetFileInfoWithBlockMeta(new Path(partFile.filePath.toString()),
        Seq.empty, partFile.partitionValues, schema, readSchema,
        DateTimeRebaseCorrected, DateTimeRebaseCorrected, false)
      newHMBWithMetaDataForChunks(partFile, Array(SingleHMBAndMeta.empty(rows)), 1, meta)
    }

    def pending: Array[HostMemoryBuffersWithMetaDataBase] =
      combineLeftOverFiles.getOrElse(Array.empty)

    def combinePending(): HostMemoryBuffersWithMetaDataBase = {
      val input = pending
      combineLeftOverFiles = None
      combineHMBs(input)
    }

    override protected def computeNumRowsAlive(rows: Long,
        metadata: HostMemoryBuffersWithMetaDataBase): Int = {
      if (failRowCount) {
        assert(pending.isEmpty, "leftovers must not be published before combining succeeds")
      }
      require(!failRowCount, "injected row-count failure")
      super.computeNumRowsAlive(rows - numDeletedRows, metadata)
    }

    override protected def newCombinedHMBWithMetaData(meta: CombinedMeta,
        buffer: SingleHMBAndMeta, offset: Long): HostMemoryBuffersWithMetaData = {
      if (failBuild) {
        assert(pending.isEmpty, "leftovers must not be published before combining succeeds")
      }
      require(!failBuild, "injected combined-buffer failure")
      super.newCombinedHMBWithMetaData(meta, buffer, offset)
    }
  }

  private def keys(meta: HostMemoryBuffersWithMetaDataBase): Seq[Int] =
    meta.allPartValues.map(_.map(_._2.getInt(0)).toSeq)
      .getOrElse(Seq(meta.partitionedFile.partitionValues.getInt(0)))

  private def readRowCount(reader: Reader, meta: HostMemoryBuffersWithMetaDataBase): Int = {
    val batches = reader.readBatches(meta)
    assert(batches.hasNext)
    val rows = withResource(batches.next())(_.numRows())
    assert(!batches.hasNext)
    rows
  }

  for (ordered <- Seq(true, false)) {
    test(s"metadata-only compatibility splits preserve all rows, ordered=$ordered") {
      val checkedPairs = scala.collection.mutable.ArrayBuffer[(Int, Int)]()
      withResource(new Reader(ordered) {
        override protected def checkIfNeedToSplitEmpty(current: HostMemoryEmptyMetaData,
            next: HostMemoryEmptyMetaData): Boolean = {
          assert(current.numRows > 0 && next.numRows > 0)
          val currentKey = current.partitionedFile.partitionValues.getInt(0)
          val nextKey = next.partitionedFile.partitionValues.getInt(0)
          checkedPairs += ((currentKey, nextKey))
          // Files 0 and 2 model matching output constants; file 1 is incompatible.
          currentKey % 2 != nextKey % 2
        }
      }) { reader =>
        val input: Array[HostMemoryBuffersWithMetaDataBase] =
          Array(reader.empty(9, 0), reader.empty(0, 10, new StructType()),
            reader.empty(8, 0), reader.empty(1, 20, new StructType()),
            reader.empty(2, 30, new StructType()), reader.empty(7, 0))
        val firstRows = withResource(reader.combineHMBs(input)) { combined =>
          assert(combined.partitionedFile.filePath == input(1).partitionedFile.filePath)
          assert(keys(combined) == (if (ordered) Seq(9, 0, 8) else Seq(9, 0, 8, 2, 7)))
          readRowCount(reader, combined)
        }
        assert(checkedPairs.toSeq == (if (ordered) Seq((0, 1)) else Seq((0, 1), (0, 2))))
        val remainingRows = withResource(reader.combinePending()) { combined =>
          assert(keys(combined) == Seq(1))
          readRowCount(reader, combined)
        }
        val lastRows = if (ordered) {
          withResource(reader.combinePending()) { combined =>
            assert(keys(combined) == Seq(2, 7))
            readRowCount(reader, combined)
          }
        } else {
          0
        }
        assert(firstRows == (if (ordered) 10 else 40))
        assert(firstRows + remainingRows + lastRows == 60)
        assert(reader.pending.isEmpty)
      }
    }

    test(s"consecutive metadata-only files combine and preserve partitions, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        var released = 0
        val n1 = reader.empty(0, 10)
        val n2 = reader.empty(1, 20)
        n1.addReleaseResourceCallback(() => released += 1)
        n2.addReleaseResourceCallback(() => released += 1)
        val d = reader.data(2, 30)
        withResource(reader.combineHMBs(Array(n1, n2, d))) { combined =>
          assert(combined.isInstanceOf[HostMemoryEmptyMetaData])
          assert(combined.memBuffersAndSizes.map(_.numRows).sum == 30)
          assert(keys(combined) == Seq(0, 1))
          assert(combined.allPartValues.get.map(_._1).toSeq == Seq(10L, 20L))
          assert(released == 0)
          assert(reader.pending.toSeq == Seq(d))
        }
        assert(released == 2)
        withResource(reader.combinePending()) { combined => assert(combined eq d) }
      }
    }

    test(s"buffered groups defer metadata-only rows, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val d1 = reader.data(0, 10)
        val n = reader.empty(1, 20)
        val d2 = reader.data(2, 30)
        withResource(reader.combineHMBs(Array(d1, n, d2))) { combined =>
          assert(combined.isInstanceOf[HostMemoryBuffersWithMetaData])
          assert(keys(combined) == (if (ordered) Seq(0) else Seq(0, 2)))
          assert(reader.pending.toSeq == (if (ordered) Seq(n, d2) else Seq(n)))
        }
        withResource(reader.combinePending()) { combined =>
          assert(combined eq n)
        }
        if (ordered) {
          withResource(reader.combinePending()) { combined => assert(combined eq d2) }
        }
      }
    }

    test(s"zero-row sentinels do not split metadata-only groups, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        withResource(reader.combineHMBs(Array(reader.empty(0, 0), reader.empty(1, 10),
            reader.empty(2, 0), reader.empty(3, 20)))) { combined =>
          val meta = combined.asInstanceOf[HostMemoryEmptyMetaData]
          assert(meta.numRows == 30)
          assert(meta.readSchema == readSchema)
          assert(keys(meta) == Seq(0, 1, 2, 3))
          assert(reader.pending.isEmpty)
        }
      }
    }

    test(s"metadata-only groups accept exactly Int.MaxValue rows, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        // A count-only schema exercises batch construction without allocating row data.
        val countSchema = new StructType()
        val input = Array[HostMemoryBuffersWithMetaDataBase](
          reader.empty(0, Int.MaxValue.toLong - 1, countSchema), reader.empty(1, 0),
          reader.empty(2, 1, countSchema))
        withResource(reader.combineHMBs(input)) { combined =>
          assert(readRowCount(reader, combined) == Int.MaxValue)
          assert(keys(combined) == Seq(0, 1, 2))
        }
        assert(reader.pending.isEmpty)
      }
    }

    test(s"metadata-only overflow is deferred without losing rows, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val countSchema = new StructType()
        val n1 = reader.empty(0, Int.MaxValue.toLong - 10, countSchema)
        val n2 = reader.empty(1, 11, countSchema)
        val zero = reader.empty(2, 0)
        val n3 = reader.empty(3, 10, countSchema)
        val firstRows = withResource(reader.combineHMBs(Array(n1, n2, zero, n3))) { combined =>
          val rows = readRowCount(reader, combined)
          assert(rows == (if (ordered) Int.MaxValue - 10 else Int.MaxValue))
          assert(keys(combined) == (if (ordered) Seq(0) else Seq(0, 2, 3)))
          assert(reader.pending.toSeq == (if (ordered) Seq(n2, zero, n3) else Seq(n2)))
          rows
        }
        val remainingRows = withResource(reader.combinePending()) { combined =>
          assert(keys(combined) == (if (ordered) Seq(1, 2, 3) else Seq(1)))
          readRowCount(reader, combined)
        }
        assert(firstRows.toLong + remainingRows == Int.MaxValue.toLong + 11)
        assert(reader.pending.isEmpty)
      }
    }

    for (layout <- Seq("singleton", "zero-prefix", "buffered-suffix")) {
      test(s"oversized metadata-only files fail, ordered=$ordered, layout=$layout") {
        withResource(new Reader(ordered)) { reader =>
          val oversized = reader.empty(0, Int.MaxValue.toLong + 1)
          val input: Array[HostMemoryBuffersWithMetaDataBase] = layout match {
            case "singleton" => Array(oversized)
            case "zero-prefix" => Array(reader.empty(1, 0), oversized)
            case "buffered-suffix" => Array(oversized, reader.data(1, 10))
          }
          var released = 0
          input.foreach(_.addReleaseResourceCallback(() => released += 1))
          val error = intercept[ArithmeticException] {
            withResource(reader.combineHMBs(input)) { _ => () }
          }
          assert(error.getMessage.contains(oversized.partitionedFile.filePath.toString()))
          assert(error.getMessage.contains(s"row count ${oversized.numRows}"))
          assert(error.getMessage.contains(
            s"maximum supported batch row count of ${Int.MaxValue}"))
          assert(released == input.length)
          assert(reader.pending.isEmpty)
        }
      }
    }

    test(s"a deferred oversized metadata-only file fails on its own, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val first = reader.empty(0, 10)
        val oversized = reader.empty(1, Int.MaxValue.toLong + 1)
        var released = false
        oversized.addReleaseResourceCallback(() => released = true)
        withResource(reader.combineHMBs(Array(first, oversized))) { combined =>
          assert(combined eq first)
          assert(reader.pending.toSeq == Seq(oversized))
          assert(!released)
        }
        val error = intercept[ArithmeticException] {
          withResource(reader.combinePending()) { _ => () }
        }
        assert(error.getMessage.contains(oversized.partitionedFile.filePath.toString()))
        assert(error.getMessage.contains(s"row count ${oversized.numRows}"))
        assert(error.getMessage.contains(
          s"maximum supported batch row count of ${Int.MaxValue}"))
        assert(released)
        assert(reader.pending.isEmpty)
      }
    }

    test(s"existing schema splits remain compatible with metadata-only groups, ordered=$ordered") {
      withResource(new Reader(ordered)) { reader =>
        val otherSchema = MessageTypeParser.parseMessageType("message test { optional int64 b; }")
        val input = Array[HostMemoryBuffersWithMetaDataBase](reader.data(0, 10),
          reader.data(1, 20, otherSchema), reader.empty(2, 30), reader.data(3, 40))
        withResource(reader.combineHMBs(input)) { combined =>
          assert(keys(combined) == (if (ordered) Seq(0) else Seq(0, 3)))
        }
        withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(1)) }
        withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(2)) }
        if (ordered) {
          withResource(reader.combinePending()) { combined => assert(keys(combined) == Seq(3)) }
        }
        assert(reader.pending.isEmpty)
      }
    }
  }

  test("metadata-only file reading preserves oversized row counts for validation") {
    val rows = Int.MaxValue.toLong + 1
    val block = new BlockMetaData()
    block.setRowCount(rows)
    val filter = (file: PartitionedFile) => ParquetFileInfoWithBlockMeta(
      new Path(file.filePath.toString()), Seq(block), file.partitionValues, emptySchema,
      readSchema, DateTimeRebaseCorrected, DateTimeRebaseCorrected, false)
    withResource(new Reader(ordered = true, filterFunc = filter)) { reader =>
      val file = PartitionedFileUtilsShim.newPartitionedFile(
        InternalRow(0), "file:///tmp/oversized", 0, 1)
      val runner = reader.getBatchRunner(new MockTaskContext(1, 0), file, reader.conf, Array.empty)
      // The runner registers itself as a pool thread, so run it off the dedicated test thread.
      withResource(CompletableFuture.supplyAsync(() => runner.call()).get()) { result =>
        withResource(result.data) { metadata =>
          assert(metadata.memBuffersAndSizes.map(_.numRows).sum == rows)
          val error = intercept[ArithmeticException] {
            withResource(reader.combineHMBs(Array(metadata))) { _ => () }
          }
          assert(error.getMessage.contains(file.filePath.toString()))
          assert(error.getMessage.contains(s"row count $rows"))
          assert(error.getMessage.contains(
            s"maximum supported batch row count of ${Int.MaxValue}"))
        }
      }
    }
  }

  for (ignoreCorrupt <- Seq(false, true);
      mode <- Seq("threshold-zero", "input-file");
      rows <- Seq(Int.MaxValue.toLong, Int.MaxValue.toLong + 1)) {
    test(s"metadata-only row limits without combining, corrupt=$ignoreCorrupt, $mode, rows=$rows") {
      val file = PartitionedFileUtilsShim.newPartitionedFile(
        InternalRow(0), "file:///tmp/part-0", 0, 1)
      var released = 0
      val reader = new Reader(ordered = true, files = Array(file),
          ignoreCorrupt = ignoreCorrupt, queryUsesInputFile = mode == "input-file",
          combineThreshold = if (mode == "threshold-zero") 0 else 1L << 20) {
        private val metadata = empty(0, rows, new StructType())
        metadata.addReleaseResourceCallback(() => released += 1)

        override def getBatchRunner(tc: TaskContext, file: PartitionedFile,
            conf: Configuration, filters: Array[Filter])
            : AsyncRunner[HostMemoryBuffersWithMetaDataBase] =
          AsyncRunner.newUnboundedTask(() => metadata)
      }
      withResource(reader) { _ =>
        assert(!reader.canUseCombine)
        if (rows > Int.MaxValue) {
          val error = intercept[ArithmeticException](reader.next())
          assert(error.getMessage.contains(file.filePath.toString()))
          assert(error.getMessage.contains(s"row count $rows"))
          assert(error.getMessage.contains(
            s"maximum supported batch row count of ${Int.MaxValue}"))
        } else {
          assert(reader.next())
          withResource(reader.get()) { batch => assert(batch.numRows().toLong == rows) }
          assert(!reader.next())
        }
        assert(released == 1)
      }
      assert(released == 1)
    }
  }

  test("a selected singleton retains its original chunk metadata") {
    withResource(new Reader(ordered = true)) { reader =>
      val data = reader.data(0, Int.MaxValue.toLong + 1)
      withResource(reader.combineHMBs(Array(data, reader.empty(1, 10)))) { combined =>
        assert(combined eq data)
      }
    }
  }

  test("row-count failure releases selected and deferred inputs") {
    withResource(new Reader(ordered = true)) { reader =>
      var released = 0
      val input = Array[HostMemoryBuffersWithMetaDataBase](
        reader.empty(0, 10), reader.empty(1, 20), reader.data(2, 30))
      input.foreach(_.addReleaseResourceCallback(() => released += 1))
      reader.failRowCount = true
      intercept[IllegalArgumentException] { reader.combineHMBs(input) }
      assert(released == 3)
      assert(reader.pending.isEmpty)
    }
  }

  test("combined metadata-only groups reuse the precomputed alive-row counts") {
    withResource(new Reader(ordered = true)) { reader =>
      reader.numDeletedRows = 5
      val countSchema = new StructType()
      val input = Array[HostMemoryBuffersWithMetaDataBase](
        reader.empty(0, 10, countSchema), reader.empty(1, 20, countSchema))
      var released = 0
      input.foreach(_.addReleaseResourceCallback(() => released += 1))
      withResource(reader.combineHMBs(input)) { combined =>
        assert(combined.memBuffersAndSizes.map(_.numRows).sum == 30)
        assert(combined.allPartValues.get.map(_._1).toSeq == Seq(5L, 15L))
        reader.failRowCount = true
        assert(readRowCount(reader, combined) == 20)
        assert(released == 2)
      }
      assert(released == 2)
    }
  }

  test("uncombined metadata-only files compute their alive-row counts") {
    withResource(new Reader(ordered = true)) { reader =>
      reader.numDeletedRows = 5
      withResource(reader.empty(0, 10, new StructType())) { metadata =>
        assert(metadata.allPartValues.isEmpty)
        assert(readRowCount(reader, metadata) == 5)
      }
    }
  }

  test("combined-buffer failure releases selected and deferred inputs") {
    withResource(new Reader(ordered = true)) { reader =>
      var released = 0
      val input = Array[HostMemoryBuffersWithMetaDataBase](
        reader.data(0, 10), reader.data(1, 20), reader.empty(2, 30))
      input.foreach(_.addReleaseResourceCallback(() => released += 1))
      reader.failBuild = true
      intercept[IllegalArgumentException] { reader.combineHMBs(input) }
      assert(released == 3)
      assert(reader.pending.isEmpty)
    }
  }
}
