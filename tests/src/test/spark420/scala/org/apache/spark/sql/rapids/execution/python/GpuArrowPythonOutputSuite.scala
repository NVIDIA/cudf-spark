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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.RmmSparkRetrySuiteBase
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonRunner
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuArrowPythonOutputSuite extends RmmSparkRetrySuiteBase {
  private val schema = StructType(Seq(StructField("a", IntegerType, nullable = false)))

  private class TestRunner extends GpuArrowPythonRunner(
      funcs = Seq(ChainedPythonFunctions(Seq(new SimplePythonFunction(
        Array.emptyByteArray,
        Collections.emptyMap[String, String](),
        Collections.emptyList[String](),
        "python",
        "3",
        Collections.emptyList(),
        null))) -> 0L),
      evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      argOffsets = Array(Array(0)),
      pythonInSchema = schema,
      timeZoneId = "UTC",
      conf = Map.empty,
      maxBatchSize = 1024,
      pythonOutSchema = schema) {

    def newTestReader(stream: DataInputStream): Iterator[ColumnarBatch] = {
      setMinReadTargetNumRows(1)
      val writer = newWriter(null, null, Iterator.empty, 0, null)
      newReaderIterator(
        stream,
        writer,
        0L,
        null,
        null,
        None,
        new AtomicBoolean(true),
        null)
    }

    def batchesProcessed(reader: Iterator[ColumnarBatch]): Long = {
      readerMetric(reader, "batchesProcessed")
    }

    def totalDataReceived(reader: Iterator[ColumnarBatch]): Long = {
      readerMetric(reader, "totalDataReceived")
    }

    def totalBytesRead(reader: Iterator[ColumnarBatch]): Long = {
      val readerMethod = reader.getClass.getDeclaredMethod("gpuArrowReader")
      readerMethod.setAccessible(true)
      readerMetric(readerMethod.invoke(reader), "totalBytesRead")
    }

    private def readerMetric(target: AnyRef, name: String): Long = {
      target.getClass.getMethod(name).invoke(target).asInstanceOf[java.lang.Long]
    }
  }

  test("Spark 4.2+ Arrow reader tracks batch and data metrics through EOF") {
    withTestSparkEnv {
      val runner = new TestRunner
      val reader = runner.newTestReader(new DataInputStream(
        new ByteArrayInputStream(arrowStreamWithEndSignals())))

      assert(reader.hasNext)
      withResource(reader.next()) { batch =>
        assertResult(3)(batch.numRows())
        assertResult(1)(batch.numCols())
      }
      assertResult(1L)(runner.batchesProcessed(reader))
      val firstBatchBytes = runner.totalDataReceived(reader)
      assert(firstBatchBytes > 0)
      val bytesReadAfterFirstBatch = runner.totalBytesRead(reader)

      assert(reader.hasNext)
      withResource(reader.next()) { batch =>
        assertResult(2)(batch.numRows())
        assertResult(1)(batch.numCols())
      }
      assertResult(2L)(runner.batchesProcessed(reader))
      val secondBatchBytes = runner.totalBytesRead(reader) - bytesReadAfterFirstBatch
      assert(secondBatchBytes > 0)
      assertResult(firstBatchBytes + secondBatchBytes)(runner.totalDataReceived(reader))

      val dataReceived = runner.totalDataReceived(reader)
      assert(!reader.hasNext)
      assertResult(2L)(runner.batchesProcessed(reader))
      assertResult(dataReceived)(runner.totalDataReceived(reader))
    }
  }

  private def arrowStreamWithEndSignals(): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(output)
    dataOut.writeInt(SpecialLengths.START_ARROW_STREAM)
    withResource(GpuArrowWriter(schema, 1024)) { writer =>
      writer.start(dataOut)
      writeBatch(writer, Int.box(10), Int.box(20), Int.box(30))
      writeBatch(writer, Int.box(40), Int.box(50))
    }

    val endSignals = new DataOutputStream(output)
    endSignals.writeInt(SpecialLengths.END_OF_DATA_SECTION)
    endSignals.writeInt(0)
    endSignals.writeInt(SpecialLengths.END_OF_STREAM)
    endSignals.flush()
    output.toByteArray
  }

  private def writeBatch(writer: GpuArrowWriter, values: Integer*): Unit = {
    withResource(new Table.TestBuilder().column(values: _*).build()) { table =>
      writer.writeAndClose(GpuColumnVector.from(table, Array(IntegerType)))
    }
  }

  private def withTestSparkEnv(f: => Unit): Unit = {
    val previousEnv = SparkEnv.get
    val env = mock[SparkEnv]
    when(env.conf).thenReturn(new SparkConf(loadDefaults = false))
    SparkEnv.set(env)
    try {
      f
    } finally {
      SparkEnv.set(previousEnv)
    }
  }
}
