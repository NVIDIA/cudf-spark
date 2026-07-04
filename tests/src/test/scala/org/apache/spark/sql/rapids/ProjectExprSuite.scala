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
import org.mockito.Mockito.{mock, spy, when}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.tests.datagen.DataGenExprShims
import org.apache.spark.sql.types._

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

  private def buildFromBufferProjectBatch(): org.apache.spark.sql.vectorized.ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5L, null.asInstanceOf[java.lang.Long], 3L, 1L)
        .column(6L.asInstanceOf[java.lang.Long], 7L, 8L, 9L)
        .build()) { table =>
      withResource(table.contiguousSplit()) { contiguousTables =>
        GpuColumnVectorFromBuffer.from(
          contiguousTables.head, Array[DataType](LongType, LongType))
      }
    }
  }

  private def assertLongColumn(
      batch: org.apache.spark.sql.vectorized.ColumnarBatch,
      columnIndex: Int,
      expected: Seq[Option[Long]]): Unit = {
    val gcv = batch.column(columnIndex).asInstanceOf[GpuColumnVector]
    withResource(gcv.getBase.copyToHost()) { hcv =>
      expected.zipWithIndex.foreach {
        case (Some(value), rowIndex) =>
          assert(!hcv.isNull(rowIndex))
          assertResult(value)(hcv.getLong(rowIndex))
        case (None, rowIndex) =>
          assert(hcv.isNull(rowIndex))
      }
    }
  }

  private def withDebugMetrics[T](body: => T): T = {
    withGpuSparkSession(_ => body,
      new SparkConf().set(RapidsConf.METRICS_LEVEL.key, "DEBUG"))
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

  test("AST pass-through project reorders and duplicates columns") {
    withDebugMetrics {
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val boundA = GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a")
      val boundB = GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b")
      val exprs = List(
        GpuAlias(boundB, "x")(),
        GpuAlias(boundA, "y")(),
        GpuAlias(boundB, "z")())
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(exprs.map(_.asInstanceOf[Expression]), mockPlan)()

      withResource(sb) { sb =>
        withResource(ast.buildRetryableAstIterator(Seq(sb.getColumnarBatch).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(4)(cb.numRows)
            assertResult(3)(cb.numCols)
            assertLongColumn(cb, 0, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
            assertLongColumn(cb, 1, Seq(Some(5L), None, Some(3L), Some(1L)))
            assertLongColumn(cb, 2, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
          }
          assert(!result.hasNext)
        }
      }
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPILE_ASTS_TIME).value)
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPUTE_ASTS_TIME).value)
    }
  }

  test("AST pass-through no-op keeps the returned batch alive") {
    withDebugMetrics {
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val exprs = List(
        GpuAlias(GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"), "a")(),
        GpuAlias(GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), "b")())
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(exprs.map(_.asInstanceOf[Expression]), mockPlan)()

      withResource(sb) { sb =>
        val input = sb.getColumnarBatch
        withResource(ast.buildRetryableAstIterator(Seq(input).iterator)) { result =>
          val output = result.next()
          assert(output eq input)
          withResource(output) { cb =>
            assertLongColumn(cb, 0, Seq(Some(5L), None, Some(3L), Some(1L)))
            assertLongColumn(cb, 1, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
          }
          assert(!result.hasNext)
        }
      }
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPILE_ASTS_TIME).value)
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPUTE_ASTS_TIME).value)
    }
  }

  test("AST empty project preserves row count") {
    withDebugMetrics {
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(Nil, mockPlan)()

      withResource(sb) { sb =>
        withResource(ast.buildRetryableAstIterator(Seq(sb.getColumnarBatch).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(4)(cb.numRows)
            assertResult(0)(cb.numCols)
          }
          assert(!result.hasNext)
        }
      }
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPILE_ASTS_TIME).value)
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPUTE_ASTS_TIME).value)
    }
  }

  test("AST literal project avoids JIT for rows-only input") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      RmmSpark.getAndResetNumSplitRetryThrow(0)
      withDebugMetrics {
        val exprs = List(
          GpuAlias(GpuLiteral(7L, LongType), "literal")(),
          GpuAlias(GpuLiteral(null, LongType), "null_literal")(),
          GpuAlias(GpuLiteral(7L, LongType), "duplicate_literal")())
        val mockPlan = mock(classOf[SparkPlan])
        when(mockPlan.output).thenReturn(Seq.empty)
        val ast = GpuProjectAstExec(exprs.map(_.asInstanceOf[Expression]), mockPlan)()
        val input = new org.apache.spark.sql.vectorized.ColumnarBatch(
          Array.empty[org.apache.spark.sql.vectorized.ColumnVector], 5)
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)

        withResource(ast.buildRetryableAstIterator(Seq(input).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(5)(cb.numRows)
            assertResult(3)(cb.numCols)
            assertLongColumn(cb, 0, Seq.fill(5)(Some(7L)))
            assertLongColumn(cb, 1, Seq.fill(5)(None))
            assertLongColumn(cb, 2, Seq.fill(5)(Some(7L)))
          }
          assert(!result.hasNext)
        }
        assertResult(0)(ast.metrics(GpuProjectAstExec.COMPILE_ASTS_TIME).value)
        assertResult(0)(ast.metrics(GpuProjectAstExec.COMPUTE_ASTS_TIME).value)
      }
      assert(RmmSpark.getAndResetNumSplitRetryThrow(0) > 0,
        "expected a split OOM during literal projection")
    } finally {
      RmmSpark.getAndResetNumSplitRetryThrow(0)
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("AST pass-through and literal project converts from-buffer columns") {
    withDebugMetrics {
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val boundB = GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b")
      val exprs = List(
        GpuAlias(GpuLiteral(7L, LongType), "literal")(),
        GpuAlias(boundB, "b")(),
        GpuAlias(GpuLiteral(7L, LongType), "duplicate_literal")(),
        GpuAlias(GpuLiteral(null, LongType), "null_literal")())
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(exprs.map(_.asInstanceOf[Expression]), mockPlan)()

      withResource(SpillableColumnarBatch(
          buildFromBufferProjectBatch(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)) { sb =>
        val input = sb.getColumnarBatch()
        assert(input.column(1).isInstanceOf[GpuColumnVectorFromBuffer])
        withResource(ast.buildRetryableAstIterator(Seq(input).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(4)(cb.numRows)
            assertResult(4)(cb.numCols)
            assertLongColumn(cb, 0, Seq.fill(4)(Some(7L)))
            assertLongColumn(cb, 1, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
            assertLongColumn(cb, 2, Seq.fill(4)(Some(7L)))
            assertLongColumn(cb, 3, Seq.fill(4)(None))
            assert(!cb.column(1).isInstanceOf[GpuColumnVectorFromBuffer])
          }
          assert(!result.hasNext)
        }
      }
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPILE_ASTS_TIME).value)
      assertResult(0)(ast.metrics(GpuProjectAstExec.COMPUTE_ASTS_TIME).value)
    }
  }

  test("AST mixed pass-through project retries with split") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      RmmSpark.getAndResetNumSplitRetryThrow(0)
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val boundA = GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a")
      val boundB = GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b")
      val exprs = List(
        GpuAlias(boundB, "x")(),
        GpuAlias(GpuAdd(boundA, boundB, false)(), "sum")(),
        GpuAlias(boundA, "y")(),
        GpuAlias(GpuLiteral(7L, LongType), "literal")(),
        GpuAlias(GpuLiteral(7L, LongType), "duplicate_literal")(),
        GpuAlias(GpuMultiply(boundA, boundB, failOnError = false)(), "product")(),
        GpuAlias(GpuAdd(boundB, boundA, false)(), "sum_again")(),
        GpuAlias(boundB, "z")())
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(exprs.map(_.asInstanceOf[Expression]), mockPlan)()
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(sb) { sb =>
        withResource(ast.buildRetryableAstIterator(Seq(sb.getColumnarBatch).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(4)(cb.numRows)
            assertResult(8)(cb.numCols)
            assertLongColumn(cb, 0, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
            assertLongColumn(cb, 1, Seq(Some(11L), None, Some(11L), Some(10L)))
            assertLongColumn(cb, 2, Seq(Some(5L), None, Some(3L), Some(1L)))
            assertLongColumn(cb, 3, Seq.fill(4)(Some(7L)))
            assertLongColumn(cb, 4, Seq.fill(4)(Some(7L)))
            assertLongColumn(cb, 5, Seq(Some(30L), None, Some(24L), Some(9L)))
            assertLongColumn(cb, 6, Seq(Some(11L), None, Some(11L), Some(10L)))
            assertLongColumn(cb, 7, Seq(Some(6L), Some(7L), Some(8L), Some(9L)))
          }
          assert(!result.hasNext)
        }
      }
      assert(RmmSpark.getAndResetNumSplitRetryThrow(0) > 0,
        "expected a split OOM during AST evaluation")
    } finally {
      RmmSpark.getAndResetNumSplitRetryThrow(0)
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("AST compute retry recompiles expressions") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      RmmSpark.getAndResetNumRetryThrow(0)
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val expr = GpuAlias(GpuAdd(
        GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
        GpuBoundReference(1, LongType, true)(NamedExpression.newExprId, "b"), false)(),
        "ret")()
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(List(expr.asInstanceOf[Expression]), mockPlan)()
      withResource(sb) { sb =>
        val input = sb.getColumnarBatch
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        withResource(ast.buildRetryableAstIterator(Seq(input).iterator)) { result =>
          withResource(result.next()) { cb =>
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
          assert(!result.hasNext)
        }
      }
      assert(RmmSpark.getAndResetNumRetryThrow(0) > 0,
        "expected an OOM during AST evaluation")
    } finally {
      RmmSpark.getAndResetNumRetryThrow(0)
      RmmSpark.removeCurrentDedicatedThreadAssociation(0)
    }
  }

  test("AST compile retry with literal") {
    RmmSpark.currentThreadIsDedicatedToTask(0)
    try {
      RmmSpark.getAndResetNumRetryThrow(0)
      val a = AttributeReference("a", LongType)()
      val b = AttributeReference("b", LongType)()
      val sb = buildProjectBatch()
      val expr = GpuAlias(GpuAdd(
        GpuBoundReference(0, LongType, true)(NamedExpression.newExprId, "a"),
        GpuLiteral(1L, LongType), false)(), "ret")()
      val mockPlan = mock(classOf[SparkPlan])
      when(mockPlan.output).thenReturn(Seq(a, b))
      val ast = GpuProjectAstExec(List(expr.asInstanceOf[Expression]), mockPlan)()
      withResource(sb) { sb =>
        val input = sb.getColumnarBatch
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        withResource(ast.buildRetryableAstIterator(Seq(input).iterator)) { result =>
          withResource(result.next()) { cb =>
            assertResult(4)(cb.numRows)
            assertResult(1)(cb.numCols)
            val gcv = cb.column(0).asInstanceOf[GpuColumnVector]
            withResource(gcv.getBase.copyToHost()) { hcv =>
              assert(!hcv.isNull(0))
              assertResult(6L)(hcv.getLong(0))
              assert(hcv.isNull(1))
              assert(!hcv.isNull(2))
              assertResult(4L)(hcv.getLong(2))
              assert(!hcv.isNull(3))
              assertResult(2L)(hcv.getLong(3))
            }
          }
          assert(!result.hasNext)
        }
      }
      assert(RmmSpark.getAndResetNumRetryThrow(0) > 0,
        "expected an OOM during AST compilation")
    } finally {
      RmmSpark.getAndResetNumRetryThrow(0)
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
