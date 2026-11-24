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
{"spark":["410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Collections

import ai.rapids.cudf.TableWriter
import com.nvidia.spark.rapids.FQSuiteName
import org.mockito.Mockito.{doThrow, mock, verify}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonWorkerUtils,
  SimplePythonFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.python.GpuArrowPythonWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CollectionAccumulator

class GpuGroupedPythonRunnerFactorySuite
    extends AnyFunSuite with BeforeAndAfterAll with FQSuiteName {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("GpuGroupedPythonRunnerFactorySuite")
      .config("spark.ui.enabled", "false")
      .config("spark.connect.grpc.binding.port", "0")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private val zeroConfKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.enabled"
  private val maxBytesKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.maxBytesPerSlice"
  private val arrowSlicingKey =
    "spark.databricks.execution.python.arrowBatchSize.slicing.enabled"
  private val udfLogMaxEntriesKey = "spark.sql.pyspark.udf.logging.maxEntries"
  private val udfLogLevelKey = "spark.sql.pyspark.udf.logging.logLevel"

  for {
    zeroConfEnabled <- Seq(false, true)
    maxBytes <- Seq(0L, 1L)
    arrowSlicingEnabled <- Seq(false, true)
  } {
    test(s"runner predicate: zeroConf=$zeroConfEnabled, maxBytes=$maxBytes, " +
        s"arrowSlicing=$arrowSlicingEnabled") {
      val actual = GpuGroupedPythonRunnerFactory.shouldUseGroupUdfRunner(
        zeroConfEnabled, maxBytes, arrowSlicingEnabled)
      assert(actual === (zeroConfEnabled && maxBytes > 0L))
    }
  }

  // Constructor-only fixtures: no Python worker is started.
  private def runnerFunctions: Seq[(ChainedPythonFunctions, Long)] = {
    val function = new SimplePythonFunction(
      Array.emptyByteArray,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python",
      "3",
      Collections.emptyList(),
      new CollectionAccumulator[Array[Byte]]())
    Seq(ChainedPythonFunctions(Seq(function)) -> 0L)
  }

  // One zero-argument function matches the empty input schemas used below.
  private def runnerArgOffsets: Array[Array[Int]] = Array(Array.emptyIntArray)

  private def getRunner(
      zeroConfEnabled: Boolean,
      arrowSlicingEnabled: Boolean,
      udfLogMaxEntries: Int = 0,
      udfLogLevel: String = "WARNING"): GpuBasePythonRunner[_] = {
    val conf = new SQLConf()
    conf.setConfString(zeroConfKey, zeroConfEnabled.toString)
    conf.setConfString(maxBytesKey, "1")
    conf.setConfString(arrowSlicingKey, arrowSlicingEnabled.toString)
    conf.setConfString(udfLogMaxEntriesKey, udfLogMaxEntries.toString)
    conf.setConfString(udfLogLevelKey, udfLogLevel)

    GpuGroupedPythonRunnerFactory(
      conf,
      chainedFunc = runnerFunctions,
      argOffsets = runnerArgOffsets,
      dedupAttrs = new StructType(),
      pythonOutputSchema = new StructType(),
      evalType = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF).getRunner()
  }

  private class TestArrowRunner extends GpuArrowPythonRunner(
      funcs = runnerFunctions,
      evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      argOffsets = runnerArgOffsets,
      pythonInSchema = new StructType(),
      timeZoneId = "UTC",
      conf = Map.empty,
      maxBatchSize = 1024,
      pythonOutSchema = new StructType()) {
    def writerForTest(input: Iterator[ColumnarBatch]): Writer =
      newWriter(SparkEnv.get, null, input, 0, null)
  }

  private def installOpenTableWriter(writer: AnyRef): TableWriter = {
    val arrowField = writer.getClass.getDeclaredFields
      .find(field => classOf[GpuArrowPythonWriter].isAssignableFrom(field.getType)).get
    arrowField.setAccessible(true)
    val arrowWriter = arrowField.get(writer).asInstanceOf[GpuArrowPythonWriter]
    // Simulate a stream left open by a previous batch without allocating GPU resources.
    val field = classOf[GpuArrowPythonWriter].getDeclaredFields
      .find(_.getType == classOf[TableWriter]).get
    field.setAccessible(true)
    val tableWriter = mock(classOf[TableWriter])
    field.set(arrowWriter, tableWriter)
    tableWriter
  }

  test("iterator hasNext failure closes the open Arrow writer") {
    val failure = new IllegalStateException("hasNext failed")
    var hasNextCalls = 0
    val input = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        hasNextCalls += 1
        throw failure
      }
      override def next(): ColumnarBatch = throw new AssertionError("next must not run")
    }
    val runner = new TestArrowRunner()
    val writer = runner.writerForTest(input)
    assert(hasNextCalls === 0)
    val tableWriter = installOpenTableWriter(writer)
    val out = new DataOutputStream(new ByteArrayOutputStream())
    assert(intercept[IllegalStateException](writer.writeNextInputToStream(out)) eq failure)
    verify(tableWriter).close()
  }

  test("Arrow cleanup failure does not replace an iterator failure") {
    val failure = new IllegalStateException("hasNext failed")
    val closeFailure = new IllegalStateException("close failed")
    val input = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = throw failure
      override def next(): ColumnarBatch = throw new AssertionError("next must not run")
    }
    val runner = new TestArrowRunner()
    val writer = runner.writerForTest(input)
    val tableWriter = installOpenTableWriter(writer)
    doThrow(closeFailure).when(tableWriter).close()
    val out = new DataOutputStream(new ByteArrayOutputStream())
    assert(intercept[IllegalStateException](writer.writeNextInputToStream(out)) eq failure)
    assert(failure.getSuppressed.contains(closeFailure))
    verify(tableWriter).close()
  }

  test("iterator next failure closes the open Arrow writer") {
    val failure = new IllegalStateException("next failed")
    val input = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = true
      override def next(): ColumnarBatch = throw failure
    }
    val runner = new TestArrowRunner()
    val writer = runner.writerForTest(input)
    val tableWriter = installOpenTableWriter(writer)
    val out = new DataOutputStream(new ByteArrayOutputStream())
    assert(intercept[IllegalStateException](writer.writeNextInputToStream(out)) eq failure)
    verify(tableWriter).close()
  }

  test("zero-conf with a positive slice size uses the grouped runner") {
    assert(getRunner(zeroConfEnabled = true, arrowSlicingEnabled = false)
      .isInstanceOf[GpuGroupUDFArrowPythonRunner])
  }

  test("Arrow slicing alone uses the window runner") {
    assert(getRunner(zeroConfEnabled = false, arrowSlicingEnabled = true)
      .isInstanceOf[GpuWindowArrowPythonRunner])
  }

  test("factory propagates Python UDF logging configuration") {
    val runner = getRunner(
      zeroConfEnabled = true,
      arrowSlicingEnabled = false,
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO").asInstanceOf[GpuGroupUDFArrowPythonRunner]

    assert(runner.udfLogMaxEntries === 37)
    assert(runner.udfLogLevel === "INFO")
  }

  test("non-grouped runners retain Python UDF logging configuration") {
    val arrowRunner = new GpuArrowPythonRunner(
      funcs = runnerFunctions,
      evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      argOffsets = runnerArgOffsets,
      pythonInSchema = new StructType(),
      timeZoneId = "UTC",
      conf = Map("framingTestKey" -> "framingTestValue"),
      maxBatchSize = 1024,
      pythonOutSchema = new StructType(),
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")
    assert(arrowRunner.udfLogMaxEntries === 37)
    assert(arrowRunner.udfLogLevel === "INFO")
    assert(arrowRunner.runnerConf("framingTestKey") === "framingTestValue")

    val coGroupedRunner = new GpuCoGroupedArrowPythonRunner(
      funcs = runnerFunctions,
      evalType = PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      argOffsets = runnerArgOffsets,
      leftSchema = new StructType(),
      rightSchema = new StructType(),
      timeZoneId = "UTC",
      conf = Map("framingTestKey" -> "framingTestValue"),
      batchSize = 1024,
      pythonOutSchema = new StructType(),
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")
    assert(coGroupedRunner.udfLogMaxEntries === 37)
    assert(coGroupedRunner.udfLogLevel === "INFO")
    assert(coGroupedRunner.runnerConf("framingTestKey") === "framingTestValue")
  }

  test("Python UDF logging configuration is serialized to the worker") {
    val bytes = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(bytes)
    WritePythonUDFUtils.writeUDFs(
      dataOut,
      funcs = Seq.empty,
      argOffsets = Array.empty,
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")

    val dataIn = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
    assert(dataIn.readInt() === 37)
    assert(PythonWorkerUtils.readUTF(dataIn) === "INFO")
    assert(dataIn.readInt() === 0)
    assert(dataIn.available() === 0)
  }
}
