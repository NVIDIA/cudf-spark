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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.connector.write.{BatchWrite, MergeSummaryImpl, WriterCommitMessage,
  WriteSummary}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.GpuMergeRowsExec._
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Spark 4.1+ passes a WriteSummary to BatchWrite.commit. For MERGE, collect metrics from
 * GpuMergeRowsExec (or a CPU MergeRowsExec fallback child) the same way Spark CPU does.
 *
 * Lives under org.apache.spark.sql so it can construct private[sql] MergeSummaryImpl.
 */
object GpuV2WriteCommitShims extends AdaptiveSparkPlanHelper {
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
  private[v2] def getWriteSummary(query: SparkPlan): Option[WriteSummary] = {
    collectFirst(query) {
      case m: GpuMergeRowsExec => m.metrics
      case m: MergeRowsExec => m.metrics
    }.map(mergeSummaryFromMetrics)
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
    metrics.get(name).map(_.value).getOrElse(-1L)
  }
}
