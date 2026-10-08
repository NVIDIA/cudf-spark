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
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonRunner
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuArrowPythonMetricsOutputSuite extends AnyFunSuite {
  private val memoryBytesSpilled = 123L
  private val diskBytesSpilled = 456L
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

    def newTestReader(
        stream: DataInputStream,
        context: TaskContext): Iterator[ColumnarBatch] = {
      val writer = newWriter(null, null, Iterator.empty, 0, context)
      newReaderIterator(
        stream,
        writer,
        0L,
        null,
        null,
        None,
        new AtomicBoolean(true),
        context)
    }
  }

  test("Spark 5 Arrow reader consumes metrics data before end marker") {
    val context = new MockTaskContext(taskAttemptId = 1L, partitionId = 0)
    withTestSparkEnv {
      val input = new ByteArrayInputStream(metricsStreamWithEndSignals())
      val reader = new TestRunner().newTestReader(new DataInputStream(input), context)

      assert(!reader.hasNext)
      assertResult(memoryBytesSpilled)(context.taskMetrics().memoryBytesSpilled)
      assertResult(diskBytesSpilled)(context.taskMetrics().diskBytesSpilled)
      assertResult(0)(input.available())
    }
  }

  private def metricsStreamWithEndSignals(): Array[Byte] = {
    val metrics =
      s"""{"bootTimestampMs":10,"initTimestampMs":20,"finishTimestampMs":30,""" +
        s""""pythonExecutionDurationMs":7,"memoryBytesSpilled":$memoryBytesSpilled,""" +
        s""""diskBytesSpilled":$diskBytesSpilled}"""
    val metricsBytes = metrics.getBytes(StandardCharsets.UTF_8)
    val output = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(output)

    dataOut.writeInt(SpecialLengths.METRICS_DATA)
    dataOut.writeInt(metricsBytes.length)
    dataOut.write(metricsBytes)
    dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
    dataOut.writeInt(0)
    dataOut.writeInt(SpecialLengths.END_OF_STREAM)
    dataOut.flush()
    output.toByteArray
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
