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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.parquet.async

import java.util.Collections

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{CombineConf, ThreadPoolConfBuilder}
import com.nvidia.spark.rapids.iceberg.parquet.{
  GpuAsyncIcebergParquetReader,
  GpuIcebergParquetReaderConf,
  IcebergPartitionedFile,
  MultiThread
}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Schema
import org.apache.iceberg.types.{Types => IcebergTypes}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.apache.parquet.schema.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class AsyncParquetPlannerSuite extends AnyFunSuite {
  private val MiB = 1024L * 1024L

  test("asynchronous decode support has a Hadoop configuration during trait initialization") {
    val hadoopConf = new Configuration(false)
    hadoopConf.setInt("parquet.read.allocation.size", 12345)
    val icebergSchema = new Schema(
      IcebergTypes.NestedField.required(1, "id", IcebergTypes.IntegerType.get()))
    val threadConf = MultiThread(
      new ThreadPoolConfBuilder(1, false, 0L, 0L, false),
      maxNumFilesProcessed = 1,
      CombineConf(1024L, 0),
      disableCombining = false,
      hasFilePathMetadata = false,
      hasRowPositionMetadata = false)
    val readerConf = GpuIcebergParquetReaderConf(
      caseSensitive = true,
      conf = hadoopConf,
      maxBatchSizeRows = 1024,
      maxBatchSizeBytes = 1024L * 1024L,
      targetBatchSizeBytes = 1024L * 1024L,
      maxGpuColumnSizeBytes = 1024L * 1024L,
      useChunkedReader = false,
      maxChunkedReaderMemoryUsageSizeBytes = 1024L * 1024L,
      parquetDebugDumpPrefix = None,
      parquetDebugDumpAlways = false,
      metrics = Map.empty,
      threadConf = threadConf,
      expectedSchema = icebergSchema,
      nameMapping = None)
    val reader = new GpuAsyncIcebergParquetReader(
      rapidsFileIO = null,
      files = Seq.empty,
      constantsProvider = (_: IcebergPartitionedFile) =>
        Collections.emptyMap[Integer, Object](),
      conf = readerConf)
    val readSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)))
    val parquetSchema = Types.buildMessage()
      .addField(Types.required(INT32).named("id"))
      .named("async_test")

    try {
      assert(reader.asyncParquetOptions(readSchema, parquetSchema) != null)
    } finally {
      reader.close()
    }
  }

  test("latency range planner coalesces adjacent chunks but never fetches holes") {
    val ranges = Seq(
      new ParquetDataReader.SourceRange(0L, 2L * MiB, 0L),
      new ParquetDataReader.SourceRange(2L * MiB, 3L * MiB, 2L * MiB),
      new ParquetDataReader.SourceRange(6L * MiB, 2L * MiB, 5L * MiB),
      new ParquetDataReader.SourceRange(8L * MiB, 1L * MiB, 7L * MiB))

    val plan = ParquetDataReader.planRanges(ranges.asJava, 64L * MiB)
    assert(plan.size() == 2)
    assert(plan.asScala.map(_.getInputOffset) == Seq(0L, 6L * MiB))
    assert(plan.asScala.map(_.getLength) == Seq(5L * MiB, 3L * MiB))
    assert(plan.asScala.map(_.getOutputOffset) == Seq(0L, 5L * MiB))
    assert(plan.asScala.map(_.getLength).sum == 8L * MiB)
    assert(plan.get(0).getInputOffset == 0L)
    assert(plan.get(1).getInputOffset == 6L * MiB)
  }

  test("latency range planner splits a coalesced run at the request-size ceiling") {
    val ranges = Seq(
      new ParquetDataReader.SourceRange(0L, 5L * MiB, 0L),
      new ParquetDataReader.SourceRange(5L * MiB, 7L * MiB, 5L * MiB),
      new ParquetDataReader.SourceRange(12L * MiB, 6L * MiB, 12L * MiB))

    val plan = ParquetDataReader.planRanges(ranges.asJava, 8L * MiB).asScala
    assert(plan.map(_.getInputOffset) == Seq(0L, 8L * MiB, 16L * MiB))
    assert(plan.map(_.getLength) == Seq(8L * MiB, 8L * MiB, 2L * MiB))
    assert(plan.map(_.getOutputOffset) == Seq(0L, 8L * MiB, 16L * MiB))
  }

  test("latency range planner preserves footer order instead of sorting") {
    val ranges = Seq(
      new ParquetDataReader.SourceRange(32L * MiB, 2L * MiB, 0L),
      new ParquetDataReader.SourceRange(0L, 2L * MiB, 2L * MiB))

    assertThrows[IllegalArgumentException] {
      ParquetDataReader.planRanges(ranges.asJava, 8L * MiB)
    }
  }

  test("latency range planner has no request-count limit") {
    val ranges = Seq(new ParquetDataReader.SourceRange(0L, 640L * MiB, 0L))

    val plan = ParquetDataReader.planRanges(ranges.asJava, 8L * MiB)
    assert(plan.size() == 80)
    assert(plan.asScala.forall(_.getLength == 8L * MiB))
  }

  test("range completion waits for every split request covering a column chunk") {
    val chunks = Seq(
      new ParquetDataReader.SourceRange(0L, 5L * MiB, 0L),
      new ParquetDataReader.SourceRange(5L * MiB, 7L * MiB, 5L * MiB),
      new ParquetDataReader.SourceRange(12L * MiB, 6L * MiB, 12L * MiB))
    val requests = ParquetDataReader.planRanges(chunks.asJava, 8L * MiB).asScala
    val completed = ArrayBuffer.empty[ParquetDataReader.SourceRange]
    val tracker = new ParquetDataReader.RangeCompletionTracker(
      chunks.asJava, requests.asJava, chunk => completed.synchronized(completed += chunk))

    // The middle request contains only partial pieces of the second and third chunks.
    tracker.requestCompleted(requests(1))
    assert(completed.isEmpty)
    assert(!tracker.isComplete)

    // The first request now completes chunks one and two, including the coalesced boundary.
    tracker.requestCompleted(requests.head)
    assert(completed.toSeq == Seq(chunks.head, chunks(1)))
    assert(!tracker.isComplete)

    tracker.requestCompleted(requests(2))
    assert(completed.toSeq == chunks)
    assert(tracker.isComplete)
  }

  test("range completion rejects a duplicate request callback") {
    val chunks = Seq(new ParquetDataReader.SourceRange(0L, 2L * MiB, 0L))
    val requests = ParquetDataReader.planRanges(chunks.asJava, 8L * MiB)
    val tracker = new ParquetDataReader.RangeCompletionTracker(
      chunks.asJava, requests, _ => ())

    tracker.requestCompleted(requests.get(0))
    assertThrows[IllegalArgumentException] {
      tracker.requestCompleted(requests.get(0))
    }
  }

  test("file-pipeline admission is asynchronous and executor-wide") {
    ParquetReaderThreadPool.resetForTesting()
    val pool = ParquetReaderThreadPool.getOrCreate(1, 2)
    val first = pool.acquireFilePermit().join()
    val second = pool.acquireFilePermit().join()
    val waiting = pool.acquireFilePermit()
    var third: ParquetReaderThreadPool.FilePermit = null

    try {
      assert(!waiting.isDone)
      first.close()
      third = waiting.join()
      assert(third != null)
    } finally {
      first.close()
      second.close()
      Option(third).foreach(_.close())
      ParquetReaderThreadPool.resetForTesting()
    }
  }
}
