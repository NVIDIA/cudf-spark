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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.FQSuiteName
import com.nvidia.spark.rapids.shims.{GpuMergeRowsKeepShims, GpuV2BatchWriteSummaryCommit}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Copy, Delete, Insert, Keep, Update}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, MergeSummary,
  MergeSummaryImpl, PhysicalWriteInfo, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType}

class GpuV2WriteSummarySuite extends AnyFunSuite with FQSuiteName {

  test("GpuMergeRowsKeepShims maps Keep.context to action tags") {
    val cond = Literal(true, BooleanType)
    val out = Seq(Literal(1, IntegerType))
    assert(GpuMergeRowsKeepShims.actionOf(Keep(Copy, cond, out)) ===
      GpuMergeRowsExec.ACTION_COPY)
    assert(GpuMergeRowsKeepShims.actionOf(Keep(Insert, cond, out)) ===
      GpuMergeRowsExec.ACTION_INSERT)
    assert(GpuMergeRowsKeepShims.actionOf(Keep(Update, cond, out)) ===
      GpuMergeRowsExec.ACTION_UPDATE)
    assert(GpuMergeRowsKeepShims.actionOf(Keep(Delete, cond, out)) ===
      GpuMergeRowsExec.ACTION_DELETE)
  }

  test("commitWithOptionalSummary forwards WriteSummary when present") {
    val recorder = new RecordingBatchWrite
    val summary: WriteSummary = MergeSummaryImpl(1, 2, 3, 4, 5, 6, 7, 8)
    GpuV2WriteCommitShims.commitWithOptionalSummary(
      recorder, Array.empty, Some(summary))
    assert(recorder.summaryCommitCount === 1)
    assert(recorder.plainCommitCount === 0)
    val merge = recorder.lastSummary.get.asInstanceOf[MergeSummary]
    assert(merge.numTargetRowsCopied === 1)
    assert(merge.numTargetRowsDeleted === 2)
    assert(merge.numTargetRowsUpdated === 3)
    assert(merge.numTargetRowsInserted === 4)
  }

  test("commitWithOptionalSummary falls back to plain commit without summary") {
    val recorder = new RecordingBatchWrite
    GpuV2WriteCommitShims.commitWithOptionalSummary(recorder, Array.empty, None)
    assert(recorder.summaryCommitCount === 0)
    assert(recorder.plainCommitCount === 1)
  }

  test("mergeSummaryFromMetrics maps all eight fields and missing-key sentinel") {
    withSparkSession { spark =>
      val sc = spark.sparkContext
      val metrics = Map(
        GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED ->
          SQLMetrics.createMetric(sc, "copied"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED ->
          SQLMetrics.createMetric(sc, "deleted"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED ->
          SQLMetrics.createMetric(sc, "updated"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED ->
          SQLMetrics.createMetric(sc, "inserted"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED ->
          SQLMetrics.createMetric(sc, "matchedUpdated"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED ->
          SQLMetrics.createMetric(sc, "matchedDeleted"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED ->
          SQLMetrics.createMetric(sc, "nmbsUpdated"),
        GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED ->
          SQLMetrics.createMetric(sc, "nmbsDeleted"))
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED).add(1L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED).add(2L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED).add(3L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED).add(4L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED).add(5L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED).add(6L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED).add(7L)
      metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED).add(8L)
      val summary = GpuV2WriteCommitShims.mergeSummaryFromMetrics(metrics)
      assert(summary.numTargetRowsCopied === 1L)
      assert(summary.numTargetRowsDeleted === 2L)
      assert(summary.numTargetRowsUpdated === 3L)
      assert(summary.numTargetRowsInserted === 4L)
      assert(summary.numTargetRowsMatchedUpdated === 5L)
      assert(summary.numTargetRowsMatchedDeleted === 6L)
      assert(summary.numTargetRowsNotMatchedBySourceUpdated === 7L)
      assert(summary.numTargetRowsNotMatchedBySourceDeleted === 8L)

      val missingDeleted = metrics - GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED
      val withMissing = GpuV2WriteCommitShims.mergeSummaryFromMetrics(missingDeleted)
      assert(withMissing.numTargetRowsDeleted === -1L)
      assert(withMissing.numTargetRowsCopied === 1L)
    }
  }

  test("commit traverses GpuMergeRowsExec and uses summary-aware overload") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val output = Seq(AttributeReference("id", LongType, nullable = false)())
      val merge = GpuMergeRowsExec(
        Literal(true, BooleanType),
        Literal(true, BooleanType),
        Nil, Nil, Nil,
        checkCardinality = false,
        output,
        child)
      seedAllMergeMetrics(merge)
      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, merge)
      assert(recorder.summaryCommitCount === 1)
      assert(recorder.plainCommitCount === 0)
      assertAllMergeSummaryFields(recorder.lastSummary.get.asInstanceOf[MergeSummary])
    }
  }

  test("commit preserves summary for CPU MergeRowsExec fallback child") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val output = Seq(AttributeReference("id", LongType, nullable = false)())
      val merge = MergeRowsExec(
        Literal(true, BooleanType),
        Literal(true, BooleanType),
        Nil, Nil, Nil,
        checkCardinality = false,
        output,
        child)
      seedAllMergeMetrics(merge)
      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, merge)
      assert(recorder.summaryCommitCount === 1)
      assert(recorder.plainCommitCount === 0)
      assertAllMergeSummaryFields(recorder.lastSummary.get.asInstanceOf[MergeSummary])
    }
  }

  test("GpuV2BatchWriteSummaryCommit forwards summary to CPU delegate") {
    val cpu = new RecordingBatchWrite
    val gpu = new ForwardingGpuBatchWrite(cpu)
    val summary: WriteSummary = MergeSummaryImpl(9, 0, 0, 1, 0, 0, 0, 0)
    gpu.commit(Array.empty, summary)
    assert(cpu.summaryCommitCount === 1)
    assert(cpu.lastSummary.get.asInstanceOf[MergeSummary].numTargetRowsCopied === 9)
    assert(cpu.lastSummary.get.asInstanceOf[MergeSummary].numTargetRowsInserted === 1)
  }

  private def withSparkSession[T](f: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  private def seedAllMergeMetrics(plan: org.apache.spark.sql.execution.SparkPlan): Unit = {
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED).add(1L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED).add(2L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED).add(3L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED).add(4L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED).add(5L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED).add(6L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED).add(7L)
    plan.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED).add(8L)
  }

  private def assertAllMergeSummaryFields(summary: MergeSummary): Unit = {
    assert(summary.numTargetRowsCopied === 1L)
    assert(summary.numTargetRowsDeleted === 2L)
    assert(summary.numTargetRowsUpdated === 3L)
    assert(summary.numTargetRowsInserted === 4L)
    assert(summary.numTargetRowsMatchedUpdated === 5L)
    assert(summary.numTargetRowsMatchedDeleted === 6L)
    assert(summary.numTargetRowsNotMatchedBySourceUpdated === 7L)
    assert(summary.numTargetRowsNotMatchedBySourceDeleted === 8L)
  }

  private class RecordingBatchWrite extends BatchWrite {
    var plainCommitCount = 0
    var summaryCommitCount = 0
    var lastSummary: Option[WriteSummary] = None

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      throw new UnsupportedOperationException
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      plainCommitCount += 1
    }

    override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit = {
      summaryCommitCount += 1
      lastSummary = Some(summary)
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = ()
  }

  private class ForwardingGpuBatchWrite(cpu: BatchWrite)
    extends BatchWrite with GpuV2BatchWriteSummaryCommit {
    override protected def summaryCommitDelegate: BatchWrite = cpu

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      cpu.createBatchWriterFactory(info)
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = cpu.commit(messages)

    override def abort(messages: Array[WriterCommitMessage]): Unit = cpu.abort(messages)
  }
}
