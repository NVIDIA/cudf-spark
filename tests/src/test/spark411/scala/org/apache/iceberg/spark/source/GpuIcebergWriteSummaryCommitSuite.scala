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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import org.mockito.Mockito.mock
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, DeltaBatchWrite,
  DeltaWriterFactory, PhysicalWriteInfo, WriterCommitMessage, WriteSummary}

/**
 * Verifies production Iceberg GPU BatchWrite types forward WriteSummary via
 * GpuV2BatchWriteSummaryCommit (not only a synthetic trait mixin).
 */
class GpuIcebergWriteSummaryCommitSuite extends AnyFunSuite {

  test("GpuBatchAppend forwards WriteSummary to CPU BatchAppend delegate") {
    val cpu = new RecordingBatchWrite
    val gpu = new GpuBatchAppend(mock(classOf[GpuSparkWrite]), cpu)
    val summary = new WriteSummary {}
    gpu.commit(Array.empty, summary)
    assert(cpu.summaryCommitCount === 1)
    assert(cpu.plainCommitCount === 0)
    assert(cpu.lastSummary.contains(summary))
  }

  test("GpuPositionDeltaBatchWrite forwards WriteSummary to CPU delta delegate") {
    val cpu = new RecordingDeltaBatchWrite
    val gpu = new GpuPositionDeltaBatchWrite(mock(classOf[GpuSparkPositionDeltaWrite]), cpu)
    val summary = new WriteSummary {}
    gpu.commit(Array.empty, summary)
    assert(cpu.summaryCommitCount === 1)
    assert(cpu.plainCommitCount === 0)
    assert(cpu.lastSummary.contains(summary))
  }

  private class RecordingBatchWrite extends BatchWrite {
    var plainCommitCount = 0
    var summaryCommitCount = 0
    var lastSummary: Option[WriteSummary] = None

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      throw new UnsupportedOperationException
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      plainCommitCount += 1
    }

    override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit = {
      summaryCommitCount += 1
      lastSummary = Some(summary)
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = ()
  }

  private class RecordingDeltaBatchWrite extends RecordingBatchWrite with DeltaBatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
      throw new UnsupportedOperationException
    }
  }
}
