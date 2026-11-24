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
{"spark":["411","420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.{FQSuiteName, GpuRowToColumnarExec, RapidsConf, TargetSize,
  VersionUtils}
import com.nvidia.spark.rapids.shims.GpuMergeRowsKeepShims
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Copy, Delete, Insert, Keep, Update}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, MergeSummary,
  MergeSummaryImpl, PhysicalWriteInfo, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.ProjectExec
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

  test("commit finds GpuMergeRowsExec under an ancestor via plan traversal") {
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
      // Root is not the MERGE node; commit must walk descendants.
      val root = ProjectExec(merge.output, merge)
      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, root)
      assert(recorder.summaryCommitCount === 1)
      assert(recorder.plainCommitCount === 0)
      assertAllMergeSummaryFields(recorder.lastSummary.get.asInstanceOf[MergeSummary])
    }
  }

  test("commit finds CPU MergeRowsExec under GpuRowToColumnarExec fallback wrapper") {
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
      // Mixed plan: GPU writer above a CPU MERGE child wrapped for columnar output.
      val root = GpuRowToColumnarExec(merge, TargetSize(1L << 20))
      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, root)
      assert(recorder.summaryCommitCount === 1)
      assert(recorder.plainCommitCount === 0)
      assertAllMergeSummaryFields(recorder.lastSummary.get.asInstanceOf[MergeSummary])
    }
  }

  test("MERGE commit counts all GPU summary fields once when the shuffle map stage is retried") {
    assume(VersionUtils.cmpSparkVersion(4, 2, 0) >= 0, "SPARK-56743 starts in Spark 4.2")
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val merge = GpuMergeRowsExec(
        Literal(true, BooleanType), Literal(true, BooleanType), Nil, Nil, Nil,
        checkCardinality = false, child.output, child)
      val mergeMetrics = gpuMergeMetrics(merge)
      val trueLit = Literal(true, BooleanType)

      runWithRetriedMapStage(spark) { () =>
        val attempt = GpuMergeRowsExec.MergeRowMetrics.forAttempt()
        // Split across two batches so every SQLLastAttemptMetric also receives += 0L via addAll.
        attempt.reset()
        attempt.record(GpuMergeRowsExec.GpuKeep(trueLit, Nil, GpuMergeRowsExec.ACTION_COPY),
          1L, sourcePresent = true)
        attempt.record(GpuMergeRowsExec.GpuKeep(trueLit, Nil, GpuMergeRowsExec.ACTION_INSERT),
          2L, sourcePresent = true)
        attempt.record(GpuMergeRowsExec.GpuDiscard(trueLit), 3L, sourcePresent = true)
        attempt.record(GpuMergeRowsExec.GpuSplit(trueLit, Nil, Nil), 5L, sourcePresent = true)
        mergeMetrics.addAll(attempt)

        attempt.reset()
        attempt.record(GpuMergeRowsExec.GpuDiscard(trueLit), 4L, sourcePresent = false)
        attempt.record(GpuMergeRowsExec.GpuSplit(trueLit, Nil, Nil), 6L, sourcePresent = false)
        mergeMetrics.addAll(attempt)
      }

      val expectedAttemptCounts = Seq(
        GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED -> 1L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED -> 2L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED -> 7L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED -> 11L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED -> 5L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED -> 3L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED -> 6L,
        GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED -> 4L)
      expectedAttemptCounts.foreach { case (name, perAttempt) =>
        assert(merge.metrics(name).value === perAttempt * 2L, s"cumulative value for $name")
      }

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, merge)
      val summary = recorder.lastSummary.get.asInstanceOf[MergeSummary]
      assert(summary.numTargetRowsCopied === 1L)
      assert(summary.numTargetRowsInserted === 2L)
      assert(summary.numTargetRowsDeleted === 7L)
      assert(summary.numTargetRowsUpdated === 11L)
      assert(summary.numTargetRowsMatchedUpdated === 5L)
      assert(summary.numTargetRowsMatchedDeleted === 3L)
      assert(summary.numTargetRowsNotMatchedBySourceUpdated === 6L)
      assert(summary.numTargetRowsNotMatchedBySourceDeleted === 4L)
    }
  }

  test("MERGE commit counts CPU fallback MergeRowsExec fields once after stage retry") {
    assume(VersionUtils.cmpSparkVersion(4, 2, 0) >= 0, "SPARK-56743 starts in Spark 4.2")
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val merge = MergeRowsExec(
        Literal(true, BooleanType), Literal(true, BooleanType), Nil, Nil, Nil,
        checkCardinality = false, child.output, child)
      val cpuMetrics = merge.metrics

      runWithRetriedMapStage(spark) { () =>
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED).add(1L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED).add(2L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED).add(3L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED).add(4L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED).add(5L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED).add(6L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED).add(7L)
        cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED).add(8L)
      }

      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED).value === 2L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED).value === 4L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED).value === 6L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED).value === 8L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED).value === 10L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED).value === 12L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED).value ===
        14L)
      assert(cpuMetrics(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED).value ===
        16L)

      val root = GpuRowToColumnarExec(merge, TargetSize(1L << 20))
      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(recorder, Array.empty, root)
      assertAllMergeSummaryFields(recorder.lastSummary.get.asInstanceOf[MergeSummary])
    }
  }

  private def gpuMergeMetrics(merge: GpuMergeRowsExec): GpuMergeRowsExec.MergeRowMetrics =
    GpuMergeRowsExec.MergeRowMetrics(
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED),
      merge.gpuLongMetric(GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED))

  private def runWithRetriedMapStage(spark: SparkSession)(recordAttempt: () => Unit): Unit = {
    // Put the MERGE counters upstream of the shuffle so fetch failure repeats their updates.
    val rows = spark.sparkContext.parallelize(1 to 4, 1).mapPartitions { iter =>
      recordAttempt()
      iter.map(i => (0, i))
    }
    val shuffled = rows.reduceByKey(_ + _, 1)
    val shuffleId = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    val result = shuffled.mapPartitions { iter =>
      if (TaskContext.get().stageAttemptNumber() == 0) {
        throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
          shuffleId, 0L, 0, 0, "Retry the stage that counted the MERGE rows")
      }
      iter
    }
    assert(result.collect().toSeq === Seq(0 -> 10))
  }

  private def withSparkSession[T](f: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      // ESSENTIAL must still expose all MergeSummary fields used as commit metadata.
      .config(RapidsConf.METRICS_LEVEL.key, "ESSENTIAL")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  private def seedAllMergeMetrics(plan: org.apache.spark.sql.execution.SparkPlan): Unit = {
    // Under ESSENTIAL, every MergeSummary field must still be present (not NoopMetric).
    Seq(
      GpuMergeRowsExec.NUM_TARGET_ROWS_COPIED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_INSERTED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_UPDATED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_MATCHED_DELETED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED,
      GpuMergeRowsExec.NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED).foreach { name =>
      assert(plan.metrics.contains(name),
        s"$name missing from plan.metrics at ESSENTIAL metrics level")
    }
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
}
