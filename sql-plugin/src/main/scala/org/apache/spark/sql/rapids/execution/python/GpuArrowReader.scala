/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import java.io.DataInputStream

import ai.rapids.cudf.{ArrowIPCOptions, HostBufferProvider, HostMemoryBuffer, StreamedTableReader, Table}
import com.nvidia.spark.rapids.{GpuSemaphore, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.TaskContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch


/** A helper class to read arrow data from the input stream to host buffer. */
private[rapids] class StreamToBufferProvider(
    inputStream: DataInputStream) extends HostBufferProvider {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def readInto(hostBuffer: HostMemoryBuffer, length: Long): Long = {
    var amountLeft = length
    var totalRead : Long = 0
    while (amountLeft > 0) {
      val amountToRead = Math.min(tempBuffer.length, amountLeft).toInt
      val amountRead = inputStream.read(tempBuffer, 0, amountToRead)
      if (amountRead <= 0) {
        // Reached EOF
        amountLeft = 0
      } else {
        amountLeft -= amountRead
        hostBuffer.setBytes(totalRead, tempBuffer, 0, amountRead)
        totalRead += amountRead
      }
    }
    totalRead
  }
}

trait GpuArrowOutput {
  /**
   * Update the expected rows number for next reading.
   */
  private[rapids] final def setMinReadTargetNumRows(numRows: Int): Unit = {
    minReadTargetNumRows = numRows
  }

  /** Convert the table received from the Python side to a batch. */
  protected def toBatch(table: Table): ColumnarBatch

  /**
   * Default to minimum one between "arrowMaxRecordsPerBatch" and 10000.
   * Change it by calling `setMinReadTargetNumRows` before a reading.
   */
  private var minReadTargetNumRows: Int = math.min(
    SQLConf.get.arrowMaxRecordsPerBatch, 10000)

  def newGpuArrowReader: GpuArrowReader = new GpuArrowReader

  class GpuArrowReader extends AutoCloseable {
    private[this] var tableReader: StreamedTableReader = _
    private[this] var batchLoaded: Boolean = true

    /** Make the reader ready to read data, should be called before reading any batch */
    final def start(stream: DataInputStream): Unit = {
      if (tableReader == null) {
        val builder = ArrowIPCOptions.builder().withCallback(
          () => GpuSemaphore.acquireIfNecessary(TaskContext.get()))
        tableReader = Table.readArrowIPCChunked(builder.build(), new StreamToBufferProvider(stream))
      }
    }

    final def isStarted: Boolean = tableReader != null

    final def mayHasNext: Boolean = batchLoaded

    final def readNext(): ColumnarBatch = {
      val table =
        NvtxRegistry.READ_PYTHON_BATCH {
          // The GpuSemaphore is acquired in a callback
          tableReader.getNextIfAvailable(minReadTargetNumRows)
        }
      if (table != null) {
        batchLoaded = true
        withResource(table)(toBatch)
      } else {
        batchLoaded = false
        null
      }
    }

    def close(): Unit = {
      if (tableReader != null) {
        tableReader.close()
        tableReader = null
      }
    }
  }
}
