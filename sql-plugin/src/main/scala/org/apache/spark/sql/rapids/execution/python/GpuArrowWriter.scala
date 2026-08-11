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

import java.io.DataOutputStream

import ai.rapids.cudf.{ArrowIPCWriterOptions, HostBufferConsumer, HostMemoryBuffer, Table, TableWriter}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuSemaphore, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/** A helper class to write arrow data from host buffer to the output stream */
private[rapids] class BufferToStreamWriter(
    outputStream: DataOutputStream) extends HostBufferConsumer {

  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def handleBuffer(hostBuffer: HostMemoryBuffer, length: Long): Unit = {
    withResource(hostBuffer) { buffer =>
      var len = length
      var offset: Long = 0
      while(len > 0) {
        val toCopy = math.min(tempBuffer.length, len).toInt
        buffer.getBytes(tempBuffer, 0, offset, toCopy)
        outputStream.write(tempBuffer, 0, toCopy)
        len = len - toCopy
        offset = offset + toCopy
      }
    }
  }
}

trait GpuArrowWriter extends AutoCloseable {

  protected[this] val inputSchema: StructType
  protected[this] val maxBatchSize: Long

  private[this] var tableWriter: TableWriter = _
  private[this] var writerOptions: ArrowIPCWriterOptions = _

  private def buildWriterOptions: ArrowIPCWriterOptions = {
    val builder = ArrowIPCWriterOptions.builder()
    builder.withMaxChunkSize(maxBatchSize)
    builder.withCallback((table: Table) => {
      table.close()
      GpuSemaphore.releaseIfNecessary(TaskContext.get())
    })
    // Flatten the names of nested struct columns, required by cudf arrow IPC writer.
    GpuArrowWriter.flattenNames(inputSchema).foreach { case (name, nullable) =>
      if (nullable) {
        builder.withColumnNames(name)
      } else {
        builder.withNotNullableColumnNames(name)
      }
    }
    builder.build()
  }

  /** Make the writer ready to write data, should be called before writing any batch */
  final def start(dataOut: DataOutputStream): Unit = {
    if (tableWriter == null) {
      if (writerOptions == null) {
        writerOptions = buildWriterOptions
      }
      tableWriter = Table.writeArrowIPCChunked(writerOptions, new BufferToStreamWriter(dataOut))
    }
  }

  final def writeAndClose(batch: ColumnarBatch): Unit = withResource(batch) { _ =>
    write(batch)
  }

  final def write(batch: ColumnarBatch): Unit = {
    NvtxRegistry.WRITE_PYTHON_BATCH {
      // The callback will handle closing table and releasing the semaphore
      tableWriter.write(GpuColumnVector.from(batch))
    }
  }

  /** This is design to reuse the writer options */
  final def reset(): Unit = {
    if (tableWriter != null) {
      tableWriter.close()
      tableWriter = null
    }
  }

  def close(): Unit = {
    if (tableWriter != null) {
      tableWriter.close()
      tableWriter = null
      writerOptions = null
    }
  }

}

object GpuArrowWriter {
  /**
   * Create a simple GpuArrowWriter in case you don't want to implement a new one
   * by extending from the trait.
   */
  def apply(schema: StructType, maxSize: Long): GpuArrowWriter = {
    new GpuArrowWriter {
      override protected val inputSchema: StructType = schema
      override protected val maxBatchSize: Long = maxSize
    }
  }

  def flattenNames(d: DataType, nullable: Boolean = true): Seq[(String, Boolean)] =
    d match {
      case s: StructType =>
        s.flatMap(sf => Seq((sf.name, sf.nullable)) ++ flattenNames(sf.dataType, sf.nullable))
      case m: MapType =>
        flattenNames(m.keyType, nullable) ++ flattenNames(m.valueType, nullable)
      case a: ArrayType => flattenNames(a.elementType, nullable)
      case _ => Nil
    }
}

abstract class GpuArrowPythonWriter(
    override val inputSchema: StructType,
    override val maxBatchSize: Long) extends GpuArrowWriter {

  protected def writeUDFs(dataOut: DataOutputStream): Unit

  def writeCommand(dataOut: DataOutputStream, confs: Map[String, String]): Unit = {
    // Write config for the worker as a number of key -> value pairs of strings
    dataOut.writeInt(confs.size)
    for ((k, v) <- confs) {
      PythonRDD.writeUTF(k, dataOut)
      PythonRDD.writeUTF(v, dataOut)
    }
    writeUDFs(dataOut)
  }

  /**
   * This is for writing the empty partition.
   * In this case CPU will still send the schema to Python workers by calling
   * the "start" API of the Java Arrow writer, but GPU will send out nothing,
   * leading to the IPC error. And it is not easy to do as what Spark does on
   * GPU, because the C++ Arrow writer used by GPU will only send out the schema
   * iff there is some data. Besides, it does not expose a "start" API to do this.
   * So here we leverage the Java Arrow writer to do similar things as Spark.
   * It is OK because sending out schema has nothing to do with GPU.
   * (Most code is copied from Spark)
   */
  final def writeEmptyIteratorOnCpu(dataOut: DataOutputStream,
      arrowSchema: Schema): Unit = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"stdout writer for empty partition", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    Utils.tryWithSafeFinally {
      val writer = new ArrowStreamWriter(root, null, dataOut)
      writer.start()
      // No data to write
      writer.end()
    } {
      root.close()
      allocator.close()
    }
  }

}
