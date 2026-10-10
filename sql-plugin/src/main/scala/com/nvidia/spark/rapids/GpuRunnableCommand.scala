/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import java.net.URI

import com.nvidia.spark.rapids.shims.ShimUnaryCommand
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.{SparkSession => GpuSparkSession}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * An extension of `RunnableCommand` that allows columnar execution.
 */
trait GpuRunnableCommand extends RunnableCommand with ShimUnaryCommand {
  lazy val basicMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.basicMetrics
  lazy val taskMetrics: Map[String, GpuMetric] = GpuWriteJobStatsTracker.taskMetrics

  override lazy val metrics: Map[String, SQLMetric] = GpuMetric.unwrap(basicMetrics ++ taskMetrics)

  override final def run(sparkSession: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  def runColumnar(sparkSession: GpuSparkSession, child: SparkPlan): Seq[ColumnarBatch]

  def gpuWriteJobStatsTracker(
      hadoopConf: Configuration): GpuWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    GpuWriteJobStatsTracker(serializableHadoopConf, this.basicMetrics, this.taskMetrics)
  }

  def requireSingleBatch: Boolean
}


object GpuRunnableCommand {
  private val allowNonEmptyLocationInCTASKey = "spark.sql.legacy.allowNonEmptyLocationInCTAS"

  private def getAllowNonEmptyLocationInCTAS: Boolean = {
    // config only exists in Spark 3.2+, so looking it up manually for now.
    val key = allowNonEmptyLocationInCTASKey
    val v = SQLConf.get.getConfString(key, "false")
    try {
      v.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $v")
    }
  }

  def assertEmptyRootPath(tablePath: URI, saveMode: SaveMode, hadoopConf: Configuration): Unit = {
    if (saveMode == SaveMode.ErrorIfExists && !getAllowNonEmptyLocationInCTAS) {
      val filePath = new org.apache.hadoop.fs.Path(tablePath)
      val fs = filePath.getFileSystem(hadoopConf)
      if (fs.exists(filePath) &&
          fs.getFileStatus(filePath).isDirectory &&
          fs.listStatus(filePath).length != 0) {
        throw RapidsErrorUtils.
          createTableAsSelectWithNonEmptyDirectoryError(tablePath.toString,
            allowNonEmptyLocationInCTASKey)
      }
    }
  }

  /**
   * When execute CTAS operators, the write can be delegated to a sub-command
   * and we need to propagate the metrics from that sub-command to the
   * parent command.
   * Derived from Spark's DataWritingCommand.propagateMetrics
   */
  def propogateMetrics(
      sparkContext: SparkContext,
      command: GpuDataWritingCommand,
      metrics: Map[String, SQLMetric]): Unit = {
    command.metrics.foreach { case (key, metric) => metrics(key).set(metric.value) }
    SQLMetrics.postDriverMetricUpdates(sparkContext,
      sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      metrics.values.toSeq)
  }
}
