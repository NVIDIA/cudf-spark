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

package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId,
  NamedExpression}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuAdd
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskCompletionListener

class ProjectSplitRetrySuite extends RmmSparkRetrySuiteBase {
  private val NUM_ROWS = 500
  private val RAND_SEED = 10
  private val intAttr = AttributeReference("int", IntegerType)(ExprId(10))
  private val batchAttrs = Seq(intAttr)

  private case class TestColumnarLeafExec(
      override val output: Seq[Attribute],
      rdd: RDD[ColumnarBatch]) extends LeafExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("TestColumnarLeafExec only supports columnar")
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = rdd
  }

  // Reset retry counters so a leaked count from one test cannot mask a
  // missed injection in the next.
  override def afterEach(): Unit = {
    RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
    RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1)
    super.afterEach()
  }

  private def buildBatch(): ColumnarBatch = {
    val ints = 0 until NUM_ROWS
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)),
      ints.length)
  }

  private def newSpillable(): SpillableColumnarBatch =
    SpillableColumnarBatch(buildBatch(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)

  private def assertClosed(sb: SpillableColumnarBatch): Unit = {
    val wasClosed = try {
      sb.incRefCount()
      sb.close()
      sb.close()
      false
    } catch {
      case _: IllegalStateException => true
    }
    assert(wasClosed)
  }

  // GpuAdd(int, 1) — pure, deterministic, retryable.
  private def addOneExprs(): Seq[GpuExpression] = Seq(
    GpuAlias(GpuAdd(
      GpuBoundReference(0, IntegerType, true)(NamedExpression.newExprId, "int"),
      GpuLiteral(1, IntegerType),
      failOnError = false)(),
      "plus_one")())

  private case class GpuNonRetryablePassthrough(ordinal: Int, dataType: DataType)
      extends GpuLeafExpression {
    override lazy val deterministic: Boolean = false
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      batch.column(ordinal).asInstanceOf[GpuColumnVector].incRefCount()
  }

  private def mixedNonRetryableExprs(): Seq[GpuExpression] =
    addOneExprs() :+ GpuAlias(GpuNonRetryablePassthrough(0, IntegerType),
      "non_retryable")()

  private def mixedRandExprs(doContextCheck: Boolean): Seq[GpuExpression] = Seq(
    GpuAlias(GpuAdd(
      GpuBoundReference(0, IntegerType, true)(NamedExpression.newExprId, "int"),
      GpuLiteral(1, IntegerType),
      failOnError = false)(), "plus_one")(),
    GpuAlias(GpuRand(GpuLiteral(RAND_SEED, IntegerType), doContextCheck), "rnd")())

  private def collectInts(cb: ColumnarBatch, col: Int): Array[Int] = {
    val gcv = cb.column(col).asInstanceOf[GpuColumnVector]
    withResource(gcv.copyToHost()) { hcv =>
      (0 until cb.numRows()).map(hcv.getInt).toArray
    }
  }

  private def collectDoubles(cb: ColumnarBatch, col: Int): Array[Double] = {
    val gcv = cb.column(col).asInstanceOf[GpuColumnVector]
    withResource(gcv.copyToHost()) { hcv =>
      (0 until cb.numRows()).map(hcv.getDouble).toArray
    }
  }

  private def assertMixedRandBatchesEqual(retried: ColumnarBatch, ref: ColumnarBatch): Unit = {
    assertResult(ref.numRows())(retried.numRows())
    assertResult(ref.numCols())(retried.numCols())
    val refPlus = collectInts(ref, 0)
    val retPlus = collectInts(retried, 0)
    val refRand = collectDoubles(ref, 1)
    val retRand = collectDoubles(retried, 1)
    (0 until NUM_ROWS).foreach { i =>
      assertResult(refPlus(i))(retPlus(i))
      assertResult(refRand(i))(retRand(i))
    }
  }

  // Helper: build the SpillableColumnarBatch BEFORE injecting the OOM, so
  // that the alloc inside ColumnVector.fromInts doesn't accidentally absorb
  // the injection. Only the projection itself should trip the OOM.
  private def withInjectedOOM[T](inject: => Unit)(body: SpillableColumnarBatch => T): T = {
    val sb = newSpillable()
    inject
    body(sb)
  }

  test("split-retry produces same output as a single-batch projection") {
    val out = withInjectedOOM {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs())
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      assertResult(1)(cb.numCols())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0,
      "expected at least one SplitAndRetryOOM to have been observed")
  }

  test("conf=false routes split-retry OOM to legacy path which fails") {
    val sqlConf = new SQLConf()
    sqlConf.setConfString(RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.key, "false")
    SQLConf.withExistingConf(sqlConf) {
      val sb = newSpillable()
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertThrows[GpuSplitAndRetryOOM] {
        GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs()).close()
      }
    }
  }

  test("tiered project split-retry produces correct output") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(tier.areAllRetryable)
    val out = withInjectedOOM {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      tier.projectAndCloseWithRetrySingleBatch(sb)
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("mixed deterministic + GpuRand supports plain retry in split-retry path") {
    val batches = Seq(true, false).safeMap { forceRetry =>
      val tier = GpuBindReferences.bindGpuReferencesTiered(
        mixedRandExprs(doContextCheck = true), batchAttrs, new SQLConf(), Map.empty)
      assert(tier.areAllRetryable)
      val sb = newSpillable()
      if (forceRetry) {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      tier.projectAndCloseWithRetrySingleBatch(sb)
    }
    withResource(batches) { case Seq(retried, ref) =>
      assertMixedRandBatchesEqual(retried, ref)
    }
  }

  test("flat mixed deterministic + GpuRand supports split-retry path") {
    val batches = Seq(true, false).safeMap { forceSplit =>
      val sb = newSpillable()
      if (forceSplit) {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(
        sb, mixedRandExprs(doContextCheck = true))
    }
    withResource(batches) { case Seq(retried, ref) =>
      assertMixedRandBatchesEqual(retried, ref)
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("tiered mixed deterministic + GpuRand supports split-retry path") {
    val batches = Seq(true, false).safeMap { forceSplit =>
      val tier = GpuBindReferences.bindGpuReferencesTiered(
        mixedRandExprs(doContextCheck = true), batchAttrs, new SQLConf(), Map.empty)
      assert(tier.areAllRetryable)
      val sb = newSpillable()
      if (forceSplit) {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      tier.projectAndCloseWithRetrySingleBatch(sb)
    }
    withResource(batches) { case Seq(retried, ref) =>
      assertMixedRandBatchesEqual(retried, ref)
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("flat mixed retryable + non-retryable stays on no-split path") {
    val exprs = mixedNonRetryableExprs()
    assert(!exprs.forall(_.retryable))
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, exprs).close()
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("tiered mixed retryable + non-retryable stays on no-split path") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      mixedNonRetryableExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(!tier.areAllRetryable)
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      tier.projectAndCloseWithRetrySingleBatch(sb).close()
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  // A plain GpuRetryOOM under the new path is resolved before the splitter
  // is invoked, so the result comes back as a single piece — exercising the
  // single-piece path through ConcatAndConsumeAll.buildNonEmptyBatchFromTypes.
  test("plain GpuRetryOOM under split-retry path returns a single piece") {
    val out = withInjectedOOM {
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs())
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assertResult(0)(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1))
    assert(RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1) > 0)
  }

  private def runProjectExecSplitRetry(enablePreSplit: Boolean): Int = {
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("ProjectSplitRetrySuite")
        .config(RapidsConf.METRICS_LEVEL.key, "DEBUG")
        .getOrCreate()
    try {
      val input = buildBatch()
      closeOnExcept(input) { _ =>
        val inputRdd = spark.sparkContext.parallelize(Seq(input), numSlices = 1)
        val project = GpuProjectExec(
          addOneExprs().map(_.asInstanceOf[NamedExpression]).toList,
          TestColumnarLeafExec(batchAttrs, inputRdd),
          enablePreSplit = enablePreSplit)
        val outputRdd = project.doExecuteColumnar()
        val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
        TrampolineUtil.setTaskContext(context)
        try {
          RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
          val output = drainPieces(outputRdd.compute(outputRdd.partitions.head, context))
          val numBatches = output.size
          withResource(output) { _ =>
            assertResult(NUM_ROWS)(output.map(_.numRows()).sum)
          }
          assertResult(numBatches)(project.metrics(GpuMetric.NUM_OUTPUT_BATCHES).value)
          assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
          numBatches
        } finally {
          TrampolineUtil.unsetTaskContext()
          ScalableTaskCompletion.reset()
        }
      }
    } finally {
      spark.stop()
    }
  }

  // Owns drained batches and closes partial output on failure.
  private def drainPieces(
      it: Iterator[ColumnarBatch]): scala.collection.mutable.ArrayBuffer[ColumnarBatch] = {
    val buf = scala.collection.mutable.ArrayBuffer[ColumnarBatch]()
    closeOnExcept(buf) { _ =>
      while (it.hasNext) {
        buf += it.next()
      }
    }
    buf
  }

  test("streaming split-retry emits multiple pieces and matches reference") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(tier.areAllRetryable)
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val pieces = drainPieces(tier.projectAndCloseStreamingWithSplitRetry(sb))
    withResource(pieces) { _ =>
      assert(pieces.size >= 2,
        s"expected >= 2 pieces from streaming split-retry, got ${pieces.size}")
      val total = pieces.map(_.numRows()).sum
      assertResult(NUM_ROWS)(total)
      val got = pieces.flatMap(collectInts(_, 0)).toArray
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("GpuProjectExec streams split-retry pieces and counts output batches") {
    val numBatches = runProjectExecSplitRetry(enablePreSplit = true)
    assert(numBatches >= 2,
      s"expected >= 2 pieces from GpuProjectExec, got $numBatches")
  }

  test("GpuProjectExec keeps one output batch when pre-split is disabled") {
    assertResult(1)(runProjectExecSplitRetry(enablePreSplit = false))
  }

  test("streaming entry yields one piece when no split occurs") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
    val sb = newSpillable()
    val pieces = drainPieces(tier.projectAndCloseStreamingWithSplitRetry(sb))
    withResource(pieces) { _ =>
      assertResult(1)(pieces.size)
      assertResult(NUM_ROWS)(pieces.head.numRows())
    }
    assertResult(0)(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1))
  }

  test("streaming entry preserves one output batch when requested") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val pieces = drainPieces(tier.projectAndCloseStreamingWithSplitRetry(
      sb, allowMultipleOutputBatches = false))
    withResource(pieces) { _ =>
      assertResult(1)(pieces.size)
      assertResult(NUM_ROWS)(pieces.head.numRows())
      val got = collectInts(pieces.head, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("streaming entry closes input when abandoned before next") {
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    TrampolineUtil.setTaskContext(context)
    try {
      val tier = GpuBindReferences.bindGpuReferencesTiered(
        addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
      val sb = newSpillable()
      val pieces = tier.projectAndCloseStreamingWithSplitRetry(sb)
      assert(pieces.hasNext)
      context.markTaskComplete()
      assertClosed(sb)
    } finally {
      TrampolineUtil.unsetTaskContext()
      ScalableTaskCompletion.reset()
    }
  }

  test("fallback streaming entry closes input when abandoned before next") {
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    TrampolineUtil.setTaskContext(context)
    try {
      val tier = GpuBindReferences.bindGpuReferencesTiered(
        addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
      val sb = newSpillable()
      val pieces = tier.projectAndCloseStreamingWithSplitRetry(
        sb, allowMultipleOutputBatches = false)
      assert(pieces.hasNext)
      context.markTaskComplete()
      assertClosed(sb)
    } finally {
      TrampolineUtil.unsetTaskContext()
      ScalableTaskCompletion.reset()
    }
  }

  test("streaming entry closes input when iterator construction fails") {
    Seq(true, false).foreach { allowMultipleOutputBatches =>
      val expected = new RuntimeException("task completion registration failed")
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0) {
        override def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext =
          throw expected
      }
      TrampolineUtil.setTaskContext(context)
      try {
        val tier = GpuBindReferences.bindGpuReferencesTiered(
          addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
        val sb = newSpillable()
        val thrown = intercept[RuntimeException] {
          tier.projectAndCloseStreamingWithSplitRetry(sb, allowMultipleOutputBatches)
        }
        assert(thrown eq expected)
        assertClosed(sb)
      } finally {
        TrampolineUtil.unsetTaskContext()
        ScalableTaskCompletion.reset()
      }
    }
  }

  test("streaming entry falls back to single piece for multi-tier projection") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      mixedNonRetryableExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(!tier.areAllRetryable)
    val sb = newSpillable()
    val pieces = drainPieces(tier.projectAndCloseStreamingWithSplitRetry(sb))
    withResource(pieces) { _ =>
      assertResult(1)(pieces.size)
      assertResult(NUM_ROWS)(pieces.head.numRows())
    }
  }
}
