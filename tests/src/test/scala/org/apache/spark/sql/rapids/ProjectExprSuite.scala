/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.File
import java.nio.file.Files

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito.{never, spy, verify}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.tests.datagen.DataGenExprShims
import org.apache.spark.sql.types._

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  private def astExpressions(expressions: Seq[Expression]): Seq[GpuProjectAstExpression] = {
    expressions.flatMap(_.collect {
      case astExpression: GpuProjectAstExpression => astExpression
    })
  }

  private def tierReferences(expression: Expression): Seq[GpuBoundReference] = {
    expression.collect {
      case reference: GpuBoundReference if reference.name.startsWith("tiered_input_") => reference
    }
  }

  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  test("rand is okay") {
    // We cannot test that the results are exactly equal because our random number
    // generator is not identical to spark, so just make sure it does not crash
    // and all of the numbers are in the proper range
    withGpuSparkSession(session => {
      val df = nullableFloatCsvDf(session)
      val data = df.select(col("floats"), rand().as("RANDOM")).collect()
      data.foreach(row => {
        val d = row.getDouble(1)
        assert(d < 1.0)
        assert(d >= 0.0)
      })
    }, conf = enableCsvConf())
  }

  private def buildProjectBatch(): SpillableColumnarBatch = {
    val projectTable = new Table.TestBuilder()
        .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
        .column(6L.asInstanceOf[java.lang.Long], 7L, 8L, 9L)
        .build()
    withResource(projectTable) { tbl =>
      val cb = GpuColumnVector.from(tbl, Seq(LongType, LongType).toArray[DataType])
      spy(SpillableColumnarBatch(cb, -1))
    }
  }

  test("basic retry") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      val expr = GpuAlias(GpuAdd(
        GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
        GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), false)(),
        "ret")()
      val sb = buildProjectBatch()

      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, Seq(expr))
      withResource(result) { cb =>
        assertResult(4)(cb.numRows)
        assertResult(1)(cb.numCols)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        withResource(gcv.getBase.copyToHost()) { hcv =>
          assert(!hcv.isNull(0))
          assertResult(11L)(hcv.getLong(0))
          assert(hcv.isNull(1))
          assert(!hcv.isNull(2))
          assertResult(11L)(hcv.getLong(2))
          assert(!hcv.isNull(3))
          assertResult(10L)(hcv.getLong(3))
        }
      }
    } finally {
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("tiered retry") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val simpleAdd = GpuAdd(a, b, false)()
      val fullAdd = GpuAlias(GpuAdd(simpleAdd, simpleAdd, false)(), "ret")()
      val tp = GpuBindReferences.bindGpuReferencesTiered(Seq(fullAdd), Seq(a, b), new SQLConf(),
        Map.empty)
      val sb = buildProjectBatch()

      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      val result = tp.projectAndCloseWithRetrySingleBatch(sb)
      withResource(result) { cb =>
        assertResult(4)(cb.numRows)
        assertResult(1)(cb.numCols)
        val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
        withResource(gcv.getBase.copyToHost()) { hcv =>
          assert(!hcv.isNull(0))
          assertResult(22L)(hcv.getLong(0))
          assert(hcv.isNull(1))
          assert(!hcv.isNull(2))
          assertResult(22L)(hcv.getLong(2))
          assert(!hcv.isNull(3))
          assertResult(20L)(hcv.getLong(3))
        }
      }
    } finally {
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("AST retry with split") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      val sb = buildProjectBatch()
      val astExpression = GpuProjectAstExpression(GpuAdd(
        GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
        GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), false)())
      val expr = GpuAlias(astExpression, "ret")()
      val tieredProject = GpuTieredProject(Seq(Seq(expr)))
      withResource(astExpression) { _ =>
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        val result = tieredProject.projectAndCloseStreamingWithSplitRetry(sb)
        withResource(result.next()) { cb =>
          assertResult(2)(cb.numRows)
          assertResult(1)(cb.numCols)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          withResource(gcv.getBase.copyToHost()) { hcv =>
            assert(!hcv.isNull(0))
            assertResult(11L)(hcv.getLong(0))
            assert(hcv.isNull(1))
          }
        }

        withResource(result.next()) { cb =>
          assertResult(2)(cb.numRows)
          assertResult(1)(cb.numCols)
          val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
          withResource(gcv.getBase.copyToHost()) { hcv =>
            assert(!hcv.isNull(0))
            assertResult(11L)(hcv.getLong(0))
            assert(!hcv.isNull(1))
            assertResult(10L)(hcv.getLong(1))
          }
        }
        assert(!result.hasNext)
      }
    } finally {
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("tiered project preserves AST across multi-level shared expressions") {
    val a = AttributeReference("a", LongType)()
    val b = AttributeReference("b", LongType)()
    val c = AttributeReference("c", LongType)()
    val d = AttributeReference("d", LongType)()
    val e = AttributeReference("e", LongType)()
    val f = AttributeReference("f", LongType)()
    def shared: GpuAdd = GpuAdd(a, b, failOnError = false)()
    def intermediate: GpuMultiply = GpuMultiply(shared, c)()
    def ast(expression: GpuExpression, name: String): GpuAlias =
      GpuAlias(GpuProjectAstExpression(expression), name)()
    val expressions = Seq(
      ast(GpuAdd(intermediate, d, failOnError = false)(), "first"),
      ast(GpuAdd(intermediate, e, failOnError = false)(), "second"),
      ast(shared, "shared_first"),
      ast(shared, "shared_second"),
      GpuAlias(GpuGreatest(Seq(shared, f)), "regular")())

    val tiered = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      expressions, Seq(a, b, c, d, e, f), new SQLConf())

    assertResult(Seq(1, 1, 2))(tiered.exprTiers.map(astExpressions(_).size))
    assert(astExpressions(tiered.exprTiers.head).head.child.isInstanceOf[GpuAdd])
    assert(astExpressions(tiered.exprTiers(1)).head.child.isInstanceOf[GpuMultiply])
    assertResult(1)(tierReferences(astExpressions(tiered.exprTiers(1)).head).size)
    val finalReferences = tiered.exprTiers.last.flatMap(tierReferences)
    assertResult(5)(finalReferences.size)
    assertResult(2)(finalReferences.map(_.exprId).distinct.size)
  }

  test("AST compiled expression closes at task completion") {
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    val astExpression = spy(GpuProjectAstExpression(GpuAdd(
      GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
      GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), false)()))
    TrampolineUtil.setTaskContext(context)
    try {
      withResource(buildProjectBatch()) { spillableBatch =>
        withResource(spillableBatch.getColumnarBatch()) { inputBatch =>
          withResource(GpuProjectExec.project(
            inputBatch, Seq(GpuAlias(astExpression, "sum")()))) { _ => }
        }
      }
      verify(astExpression, never()).close()
      context.markTaskComplete()
      verify(astExpression).close()
    } finally {
      TrampolineUtil.unsetTaskContext()
      ScalableTaskCompletion.reset()
      astExpression.close()
    }
  }

  testSparkResultsAreEqual("Test literal values in select", mixedFloatDf) {
    frame =>
      frame.select(col("floats"),
        lit(100), lit("hello, world!"),
        lit(BigDecimal(1234567890123L, 6)), lit(BigDecimal(0L)), lit(BigDecimal(1L, -3)),
        lit(BigDecimal(-2.12314e-8)),
        lit(BigDecimal("-123456789012345678901234567890")),
        lit(Decimal(BigDecimal("123456789012345678901234567890123"), 38, 5)),
        lit(Array(1, 2, 3, 4, 5)), lit(Array(1.2, 3.4, 5.6)),
        lit(Array("a", "b", null, "")),
        lit(Array(Array(1, 2), null, Array(3, 4))),
        lit(Array(Array(Array(1, 2), Array(2, 3), null), null)),
        DataGenExprShims.exprToColumn(
          Literal.create(Array(Row(1, "s1"), Row(2, "s2"), null),
            ArrayType(StructType(
              Array(StructField("id", IntegerType), StructField("name", StringType)))))),
        DataGenExprShims.exprToColumn(
          Literal.create(List(BigDecimal(123L, 2), BigDecimal(-1444L, 2)),
          ArrayType(DecimalType(10, 2)))),
        DataGenExprShims.exprToColumn(
          Literal.create(List(BigDecimal("1234567890123456789012345678")),
          ArrayType(DecimalType(30, 2))))
      )
          .selectExpr("*", "array(null)", "array(array(null))", "array()")
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.select("time")
  }

  // test GpuRowToColumnarExec + GpuProjectExec + GpuColumnarToRowExec
  testSparkResultsAreEqual("project decimal with row source", mixedDf(_),
    conf = new SparkConf(), repart = 0) {
    frame => frame.select("decimals")
  }

  // test HostColumnarToGpu + GpuProjectExec + GpuColumnarToRowExec
  test("project decimal with columnar source") {
    val dir = Files.createTempDirectory("spark-rapids-test").toFile
    val path = new File(dir,
      s"HostColumnarToGpu-${System.currentTimeMillis()}.parquet").getAbsolutePath

    try {
      withCpuSparkSession(spark => mixedDf(spark).write.parquet(path), new SparkConf())

      val createDF = (ss: SparkSession) => ss.read.parquet(path)
      val fun = (df: DataFrame) => df.withColumn("dec", df("decimals")).select("dec")
      val conf = new SparkConf()
          .set("spark.rapids.sql.exec.FileSourceScanExec", "false")
          .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "FileSourceScanExec")
      val (fromCpu, fromGpu) = runOnCpuAndGpu(createDF, fun, conf, repart = 0)
      compareResults(false, 0.0, fromCpu, fromGpu)
    } finally {
      dir.delete()
    }
  }

  testSparkResultsAreEqual("getMapValue", frameFromParquet("map_of_strings.snappy.parquet")) {
    frame => frame.selectExpr("mapField['foo']")
  }
}
