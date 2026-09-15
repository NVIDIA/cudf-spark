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
import com.nvidia.spark.rapids.ProjectAstTestUtils.{collectExpressions, tierReferences}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito.{never, spy, verify}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.tests.datagen.DataGenExprShims
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class ProjectExprSuite extends SparkQueryCompareTestSuite {
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
    // [AST((a+b)*c+d) AS first, AST((a+b)*c+e) AS second,
    //  AST(a+b) AS shared_first, AST(a+b) AS shared_second, greatest(a+b, f) AS regular]
    val expressions = Seq(
      ast(GpuAdd(intermediate, d, failOnError = false)(), "first"),
      ast(GpuAdd(intermediate, e, failOnError = false)(), "second"),
      ast(shared, "shared_first"),
      ast(shared, "shared_second"),
      GpuAlias(GpuGreatest(Seq(shared, f)), "regular")())

    val tiered = GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      expressions, Seq(a, b, c, d, e, f), new SQLConf())

    // After CSE:
    // tier 0: [AST(a+b) AS t1]
    // tier 1: [AST(t1*c) AS t2]
    // tier 2: [AST(t2+d) AS first, AST(t2+e) AS second,
    //          t1 AS shared_first, t1 AS shared_second, greatest(t1, f) AS regular]
    val astExpressionTiers = tiered.exprTiers.map(
      collectExpressions[GpuProjectAstExpression])
    assertResult(Seq(1, 1, 2))(astExpressionTiers.map(_.size))
    assert(astExpressionTiers.head.head.child.isInstanceOf[GpuAdd])
    assert(astExpressionTiers(1).head.child.isInstanceOf[GpuMultiply])
    assertResult(1)(tierReferences(astExpressionTiers(1).head).size)
    // Final references: [t2, t2, t1, t1, t1] (distinct: {t2, t1}).
    val finalReferences = tiered.exprTiers.last.flatMap(tierReferences)
    assertResult(5)(finalReferences.size)
    assertResult(2)(finalReferences.map(_.exprId).distinct.size)
  }

  test("AST compiled expression closes at task completion") {
    val astExpression = spy(GpuProjectAstExpression(GpuAdd(
      GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
      GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), false)()))
    try {
      TestUtils.withMockTaskContext(completesTask = true) {
        withResource(buildProjectBatch()) { spillableBatch =>
          withResource(spillableBatch.getColumnarBatch()) { inputBatch =>
            withResource(GpuProjectExec.project(
              inputBatch, Seq(GpuAlias(astExpression, "sum")()))) { _ => }
          }
        }
        verify(astExpression, never()).close()
      }
      verify(astExpression).close()
    } finally {
      astExpression.close()
    }
  }

  test("multiple multi-output AST JIT groups retry and preserve output order and nulls") {
    val left = GpuBoundReference(0, LongType, nullable = true)(
      NamedExpression.newExprId, "a")
    val right = GpuBoundReference(1, LongType, nullable = true)(
      NamedExpression.newExprId, "b")
    def shared = GpuAdd(left, GpuLiteral(1L, LongType), failOnError = false)()
    def shiftedRight = GpuAdd(right, GpuLiteral(1L, LongType), failOnError = false)()
    val jitExpressions = Seq(
      GpuAstJitExpression(GpuMultiply(shared, right, failOnError = false)(), groupId = 0),
      GpuAstJitExpression(GpuMultiply(shiftedRight, left, failOnError = false)(), groupId = 1),
      GpuAstJitExpression(GpuMultiply(shared, left, failOnError = false)(), groupId = 0),
      GpuAstJitExpression(GpuMultiply(shiftedRight, right, failOnError = false)(), groupId = 1))
    val expressions = Seq(
      GpuAlias(jitExpressions.head, "right_product")(),
      GpuAlias(jitExpressions(1), "shifted_right_times_left")(),
      GpuAlias(jitExpressions(2), "left_product")(),
      GpuAlias(jitExpressions(3), "shifted_right_times_right")())

    def assertColumn(result: ColumnarBatch, index: Int, expected: Seq[Option[Long]]): Unit = {
      val column = result.column(index).asInstanceOf[GpuColumnVector]
      withResource(column.getBase.copyToHost()) { hostColumn =>
        expected.zipWithIndex.foreach {
          case (None, row) => assert(hostColumn.isNull(row))
          case (Some(value), row) => assertResult(value)(hostColumn.getLong(row))
        }
      }
    }

    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      withResource(jitExpressions) { _ =>
        TestUtils.withMockTaskContext(completesTask = true) {
          val spillableBatch = buildProjectBatch()
          RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
          withResource(GpuProjectExec.projectAndCloseWithRetrySingleBatch(
              spillableBatch, expressions)) { result =>
            assertResult(4)(result.numCols())
            assertColumn(result, 0, Seq(Some(36L), None, Some(32L), Some(18L)))
            assertColumn(result, 1, Seq(Some(35L), None, Some(27L), Some(10L)))
            assertColumn(result, 2, Seq(Some(30L), None, Some(12L), Some(2L)))
            assertColumn(result, 3, Seq(Some(42L), Some(56L), Some(72L), Some(90L)))
          }
        }
      }
    } finally {
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
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
