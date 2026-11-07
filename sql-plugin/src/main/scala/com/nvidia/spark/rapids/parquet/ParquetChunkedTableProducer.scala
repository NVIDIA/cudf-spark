/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.parquet

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RetryableTableProducer

/**
 * Restores a chunked reader by reopening its encoded input and replaying completed chunks.
 * Replay decodes and discards tables without repeating output postprocessing.
 */
abstract class ParquetChunkedTableProducer extends RetryableTableProducer {
  protected def createReader(): ChunkedReader
  protected def readerResources: Seq[AutoCloseable]

  protected def decodeNext(reader: ChunkedReader): Table = reader.next
  protected def readNext(reader: ChunkedReader): Table = decodeNext(reader)

  private var activeReader: ChunkedReader = _
  private var completedChunks = 0
  private var checkpointedChunks = 0
  private var closed = false

  protected final def getReader: ChunkedReader = {
    if (activeReader == null) {
      require(!closed, "Parquet table reader is closed")
      val reader = createReader()
      closeOnExcept(reader) { _ =>
        var replayed = 0
        while (replayed < completedChunks) {
          require(reader.hasNext,
            s"Unable to restore Parquet reader to chunk $completedChunks")
          withResource(decodeNext(reader))(_ => ())
          replayed += 1
        }
        activeReader = reader
      }
    }
    activeReader
  }

  override def hasNext: Boolean = getReader.hasNext

  override def next: Table = {
    val result = readNext(getReader)
    completedChunks += 1
    result
  }

  override def checkpoint(): Unit = checkpointedChunks = completedChunks

  override def restore(): Unit = {
    completedChunks = checkpointedChunks
    val reader = activeReader
    activeReader = null
    Option(reader).foreach(_.close())
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      val reader = Option(activeReader).toSeq
      activeReader = null
      (reader ++ readerResources).safeClose()
    }
  }
}
