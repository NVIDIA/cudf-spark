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

import java.nio.file.Files

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{ScalableTaskCompletion, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext

class ParquetBufferConsumerSuite extends SparkQueryCompareTestSuite {
  private val rows = 10000

  private def parquetBytes(): Array[Byte] = withTempPath { file =>
    val schema = MessageTypeParser.parseMessageType("message test { required int64 id; }")
    val factory = new SimpleGroupFactory(schema)
    withResource(ExampleParquetWriter.builder(new Path(file.toURI))
        .withConf(new Configuration()).withType(schema).withRowGroupSize(8192L)
        .withDictionaryEncoding(false).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) { writer =>
      (0 until rows).foreach(row => writer.write(factory.newGroup().append("id", row.toLong)))
    }
    val bytes = Files.readAllBytes(file.toPath)
    assert(ParquetCachedBatchFiles.readFooter(ParquetCachedBatchFiles.fromBytes(bytes))
      .getBlocks.size > 1)
    bytes
  }

  // Each callback transfers its reference to the consumer. Retain only the Java wrappers
  // for refcount assertions, so the test does not keep native memory alive itself.
  private def addRegions(consumer: ParquetBufferConsumer,
      bytes: Array[Byte]): Seq[HostMemoryBuffer] = {
    bytes.grouped(math.max(1, bytes.length / 3)).map { region =>
      val buffer = closeOnExcept(HostMemoryBuffer.allocate(region.length)) { allocated =>
        allocated.setBytes(0, region, 0, region.length)
        allocated
      }
      consumer.handleBuffer(buffer, region.length)
      buffer
    }.toVector
  }

  test("malformed footer releases every callback region") {
    withGpuSparkSession { _ =>
      val handles = SpillFramework.stores.hostStore.numHandles
      withResource(new ParquetBufferConsumer(numRows = 1)) { consumer =>
        val buffers = addRegions(consumer, new Array[Byte](24))
        intercept[RuntimeException] {
          consumer.records.next()
        }
        assert(buffers.forall(_.getRefCount == 0))
        assert(SpillFramework.stores.hostStore.numHandles == handles)
      }
    }
  }

  test("task completion releases partially consumed callback regions") {
    withGpuSparkSession { _ =>
      val bytes = parquetBytes()
      val handles = SpillFramework.stores.hostStore.numHandles
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      TrampolineUtil.setTaskContext(context)
      try {
        withResource(new ParquetBufferConsumer(numRows = rows)) { consumer =>
          val buffers = addRegions(consumer, bytes)
          val records = consumer.records
          assert(records.next().numRows < rows)
          assert(buffers.exists(_.getRefCount > 0))
          context.markTaskComplete()
          assert(buffers.forall(_.getRefCount == 0))
          assert(SpillFramework.stores.hostStore.numHandles == handles)
        }
      } finally {
        TrampolineUtil.unsetTaskContext()
        ScalableTaskCompletion.reset()
      }
    }
  }
}
