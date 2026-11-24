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
{"spark":["420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.shims.{GpuBatchScanExec, GpuMergeRowMetricsShims}

import org.apache.spark.sql.connector.write.{BatchWrite, DeleteSummaryImpl, MergeSummaryImpl,
  RowLevelOperation, RowLevelOperationTable, UpdateSummaryImpl, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec._
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Spark 4.2+ forwards MergeSummary, UpdateSummary, and DeleteSummary via
 * BatchWrite.commit(messages, summary). Lives under org.apache.spark.sql so it can construct
 * private[sql] *SummaryImpl classes.
 */
object GpuV2WriteCommitShims extends AdaptiveSparkPlanHelper {
  def commit(
      writeExec: GpuV2TableWriteExec,
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan): Unit = {
    commitWithOptionalSummary(batchWrite, messages, getWriteSummary(writeExec, query))
  }

  def commit(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan): Unit = {
    commitWithOptionalSummary(batchWrite, messages, getWriteSummary(query))
  }

  /** Visible for tests. */
  private[v2] def commitWithOptionalSummary(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      summary: Option[WriteSummary]): Unit = {
    summary match {
      case Some(s) => batchWrite.commit(messages, s)
      case None => batchWrite.commit(messages)
    }
  }

  /** Visible for tests. */
  private[v2] def getWriteSummary(
      writeExec: GpuV2TableWriteExec,
      query: SparkPlan): Option[WriteSummary] = {
    writeExec match {
      case r: GpuRowLevelWriteExec =>
        r.rowLevelCommand match {
          case Some(RowLevelOperation.Command.MERGE) =>
            getMergeSummary(query)
          case Some(RowLevelOperation.Command.UPDATE) =>
            getUpdateSummary(r.metrics)
          case Some(RowLevelOperation.Command.DELETE) =>
            getDeleteSummary(r, query)
          case None =>
            getMergeSummary(query)
        }
      case _ =>
        getMergeSummary(query)
    }
  }

  /** Visible for tests. */
  private[v2] def getWriteSummary(query: SparkPlan): Option[WriteSummary] = {
    getMergeSummary(query)
  }

  private def getMergeSummary(query: SparkPlan): Option[MergeSummaryImpl] = {
    collectFirst(query) {
      case m: GpuMergeRowsExec => m.metrics
      case m: MergeRowsExec => m.metrics
    }.map(mergeSummaryFromMetrics)
  }

  /** Visible for tests. */
  private[v2] def getUpdateSummary(metrics: Map[String, SQLMetric]): Option[UpdateSummaryImpl] = {
    Some(UpdateSummaryImpl(
      getMetricValue(metrics, GpuRowLevelWriteExec.NUM_UPDATED_ROWS),
      getMetricValue(metrics, GpuRowLevelWriteExec.NUM_COPIED_ROWS)))
  }

  /** Visible for tests. */
  private[v2] def getDeleteSummary(
      writeExec: GpuRowLevelWriteExec,
      query: SparkPlan): Option[DeleteSummaryImpl] = {
    val metrics = writeExec.metrics
    writeExec match {
      case _: GpuReplaceDataExec =>
        // Copy-on-write DELETE filters out deleted rows before ReplaceDataExec; mirror Spark by
        // deriving numDeletedRows = numScannedRows - numCopiedRows.
        val numScannedRows = collectFirst(query) {
          case b: GpuBatchScanExec if b.table.isInstanceOf[RowLevelOperationTable] =>
            getMetricValue(b.metrics, GpuMetric.NUM_OUTPUT_ROWS)
          case b: BatchScanExec if b.table.isInstanceOf[RowLevelOperationTable] =>
            getMetricValue(b.metrics, GpuMetric.NUM_OUTPUT_ROWS)
        }
        val numCopiedRows = getMetricValue(metrics, GpuRowLevelWriteExec.NUM_COPIED_ROWS)
        val numDeletedRows = if (numScannedRows.exists(_ >= 0L) && numCopiedRows >= 0L) {
          numScannedRows.get - numCopiedRows
        } else {
          -1L
        }
        // SQLMetric.set(-1) is a no-op, so pass numDeletedRows directly into DeleteSummaryImpl.
        metrics.get(GpuRowLevelWriteExec.NUM_DELETED_ROWS).foreach(_.set(numDeletedRows))
        Some(DeleteSummaryImpl(numDeletedRows, numCopiedRows))
      case _ =>
        Some(DeleteSummaryImpl(
          getMetricValue(metrics, GpuRowLevelWriteExec.NUM_DELETED_ROWS),
          getMetricValue(metrics, GpuRowLevelWriteExec.NUM_COPIED_ROWS)))
    }
  }

  /** Visible for tests. */
  private[v2] def mergeSummaryFromMetrics(metrics: Map[String, SQLMetric]): MergeSummaryImpl = {
    MergeSummaryImpl(
      getMetricValue(metrics, NUM_TARGET_ROWS_COPIED),
      getMetricValue(metrics, NUM_TARGET_ROWS_DELETED),
      getMetricValue(metrics, NUM_TARGET_ROWS_UPDATED),
      getMetricValue(metrics, NUM_TARGET_ROWS_INSERTED),
      getMetricValue(metrics, NUM_TARGET_ROWS_MATCHED_UPDATED),
      getMetricValue(metrics, NUM_TARGET_ROWS_MATCHED_DELETED),
      getMetricValue(metrics, NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_UPDATED),
      getMetricValue(metrics, NUM_TARGET_ROWS_NOT_MATCHED_BY_SOURCE_DELETED))
  }

  private def getMetricValue(metrics: Map[String, SQLMetric], name: String): Long = {
    metrics.get(name).map(GpuMergeRowMetricsShims.metricValue).getOrElse(-1L)
  }
}
