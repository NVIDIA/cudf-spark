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
      conf = readerConf,
      workerThreads = 1)
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
}
