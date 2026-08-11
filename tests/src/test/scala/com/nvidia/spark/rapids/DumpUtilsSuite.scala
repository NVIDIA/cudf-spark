/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, File, FileOutputStream}
import java.nio.file.Files

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, Rmm, RmmAllocationMode, Table}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.kudo.{KudoSerializer, KudoTableHeader}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class DumpUtilsSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SpillFramework.shutdown()
    Rmm.shutdown()
  }

  test("dumpToParquet handles SerializedTableColumn") {
    // Create a simple table
    withResource(
      new Table.TestBuilder()
        .column(
          1L.asInstanceOf[java.lang.Long],
          2L.asInstanceOf[java.lang.Long],
          3L.asInstanceOf[java.lang.Long])
        .column("a", "b", "c")
        .build()) { table =>
      // Serialize the table to a SerializedTableColumn
      val byteOut = new ByteArrayOutputStream()
      val out = new DataOutputStream(byteOut)
      JCudfSerialization.writeToStream(table, out, 0, table.getRowCount)
      out.close()
      val bytes = byteOut.toByteArray
      val in = new ByteArrayInputStream(bytes)
      val din = new DataInputStream(in)
      val header = new SerializedTableHeader(din)
      assert(header.wasInitialized())
      closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen)) { hostBuffer =>
        JCudfSerialization.readTableIntoBuffer(din, header, hostBuffer)
        assert(header.wasDataRead())
        // Create a ColumnarBatch with a SerializedTableColumn
        withResource(SerializedTableColumn.from(header, hostBuffer)) { batch =>
          // Create a temporary output file
          val tempFile = File.createTempFile("dumptest", ".parquet")
          tempFile.deleteOnExit()
          // Test dumping the SerializedTableColumn to parquet
          withResource(new FileOutputStream(tempFile)) { fileOut =>
            DumpUtils.dumpToParquet(batch, fileOut)
          }
          // Verify the file was created and has content
          assert(tempFile.exists())
          assert(tempFile.length() > 0)
          // Clean up
          Files.delete(tempFile.toPath)
        }
      }
    }
  }

  test("dumpToParquet handles KudoSerializedTableColumn") {
    // Create a simple table
    withResource(
      new Table.TestBuilder()
        .column(
          1L.asInstanceOf[java.lang.Long],
          2L.asInstanceOf[java.lang.Long],
          3L.asInstanceOf[java.lang.Long])
        .column("a", "b", "c")
        .build()) { table =>
      // Create a KudoSerializer
      val dataTypes: Array[DataType] = Array(LongType, StringType)
      val kudoSerializer = new KudoSerializer(GpuColumnVector.from(dataTypes))

      val byteOut = new ByteArrayOutputStream()
      val hostColumns = Array.range(0, table.getNumberOfColumns)
        .map(table.getColumn)
        .map(_.copyToHost())

      try {
        kudoSerializer.writeToStreamWithMetrics(hostColumns, byteOut, 0, table.getRowCount.toInt)
        // Create a KudoTableHeader from the serialized data
        val bytes = byteOut.toByteArray
        val in = new ByteArrayInputStream(bytes)
        val din = new DataInputStream(in)
        val headerOptional = KudoTableHeader.readFrom(din)
        if (!headerOptional.isPresent) {
          throw new NoSuchElementException("KudoTableHeader not found")
        }
        val header = headerOptional.get()
        val buffer = HostMemoryBuffer.allocate(header.getTotalDataLen())
        buffer.copyFromStream(0, din, header.getTotalDataLen())
        val spillableKudoTable = SpillableKudoTable(header, buffer)
        withResource(new KudoSerializedTableColumn(spillableKudoTable)) { column =>
          val batch = new ColumnarBatch(Array(column.asInstanceOf[GpuColumnVectorBase]),
            spillableKudoTable.header.getNumRows)

          // Create a temporary output file
          val tempFile = File.createTempFile("dumpkudotest", ".parquet")
          tempFile.deleteOnExit()
          // Test dumping the KudoSerializedTableColumn to parquet
          withResource(new FileOutputStream(tempFile)) { fileOut =>
            DumpUtils.dumpToParquet(batch, fileOut, Some(kudoSerializer))
          }
          // Verify the file was created and has content
          assert(tempFile.exists())
          assert(tempFile.length() > 0)
          // Clean up
          Files.delete(tempFile.toPath)
        }
      } finally {
        hostColumns.foreach(_.close())
        byteOut.close()
      }
    }
  }
}
