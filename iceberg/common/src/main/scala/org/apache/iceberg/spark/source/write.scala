/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import org.apache.spark.sql.connector.write._

abstract class GpuBaseBatchWrite(
    write: GpuSparkWrite,
    cpuBatchWrite: BatchWrite) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory
  = write.createDataWriterFactory

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit =
    cpuBatchWrite.abort(writerCommitMessages)

  override def useCommitCoordinator(): Boolean = cpuBatchWrite.useCommitCoordinator()
}

class GpuBatchAppend(write: GpuSparkWrite, cpuBatchWrite: BatchWrite)
  extends GpuBaseBatchWrite(write, cpuBatchWrite) {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    cpuBatchWrite.commit(messages)
  }
}

class GpuDynamicOverwrite(
    write: GpuSparkWrite,
    cpuBatchWrite: BatchWrite) extends GpuBaseBatchWrite(write, cpuBatchWrite) {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Delegate to the CPU version's commit method
    // The CPU version handles all the complex logic for dynamic partition overwrite
    cpuBatchWrite.commit(messages)
  }
}

class GpuOverwriteByFilter(write: GpuSparkWrite, cpuOverwrite: BatchWrite)
  extends GpuBaseBatchWrite(write, cpuOverwrite) {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Delegate to CPU OverwriteByFilter's commit method
    cpuOverwrite.commit(messages)
  }
}

/**
 * GPU version of copy-on-write operation's BatchWrite.
 * This wraps the CPU BatchRewrite for DELETE operations.
 */
class GpuCopyOnWriteOperation(write: GpuSparkWrite, cpuBatchWrite: BatchWrite)
  extends GpuBaseBatchWrite(write, cpuBatchWrite) {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Delegate to CPU BatchRewrite's commit method
    // The CPU version handles the complex logic for copy-on-write operations
    // including managing deleted and added files
    cpuBatchWrite.commit(messages)
  }
}

/**
 * GPU version of position delta batch write for merge-on-read DELETE operations.
 * This wraps the CPU PositionDeltaBatchWrite to handle position delete files.
 */
class GpuPositionDeltaBatchWrite(write: GpuSparkPositionDeltaWrite,
                                 cpuBatchWrite: DeltaBatchWrite)
  extends DeltaBatchWrite {
  

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Delegate to CPU PositionDeltaBatchWrite's commit method
    // The CPU version handles the complex logic for position deletes
    // including managing delete files and row delta transactions
    cpuBatchWrite.commit(messages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    cpuBatchWrite.abort(messages)
  }

  override def useCommitCoordinator(): Boolean = cpuBatchWrite.useCommitCoordinator()

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
    write.createDeltaWriterFactory
  }
}