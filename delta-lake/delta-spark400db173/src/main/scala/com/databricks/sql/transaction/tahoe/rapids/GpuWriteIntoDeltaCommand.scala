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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand
import com.databricks.sql.transaction.tahoe.stats.{DeltaJobStatisticsTracker,
  StatisticsOnLoadJobTracker}
import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta,
  GpuDataWritingCommand, GpuMetric, GpuParquetFileFormat, RapidsConf, RapidsMeta}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker,
  GpuFileFormatWriter}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class GpuWriteIntoDeltaCommandMeta(
    cmd: WriteIntoDeltaCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[WriteIntoDeltaCommand](cmd, conf, parent, rule) {

  override protected def tagSelfForGpuInternal(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled")
    }
    if (cmd.fileFormat.getClass != classOf[DeltaParquetFileFormat]) {
      willNotWorkOnGpu(s"Delta file format ${cmd.fileFormat.getClass.getName} is not supported")
    }
    if (cmd.bucketSpec.nonEmpty) {
      willNotWorkOnGpu("Bucketed Delta writes are not supported")
    }
    if (cmd.staticPartitions.nonEmpty) {
      willNotWorkOnGpu("Static partition Delta writes are not supported by this command path")
    }
    cmd.statsTrackers.foreach {
      case _: BasicWriteJobStatsTracker =>
      case _: DeltaJobStatisticsTracker =>
      case _: StatisticsOnLoadJobTracker =>
        willNotWorkOnGpu("DBR StatisticsOnLoadJobTracker is not supported on GPU")
      case tracker =>
        willNotWorkOnGpu(s"Delta write statistics tracker ${tracker.getClass.getName} " +
          "is not supported on GPU")
    }
  }

  override def convertToGpu(): GpuDataWritingCommand =
    GpuWriteIntoDeltaCommand(cmd, conf)
}

case class GpuWriteIntoDeltaCommand(
    cpuCmd: WriteIntoDeltaCommand,
    @transient rapidsConf: RapidsConf) extends GpuDataWritingCommand {

  override def query: LogicalPlan = cpuCmd.query

  override def outputColumnNames: Seq[String] = cpuCmd.outputColumnNames

  override lazy val metrics: Map[String, SQLMetric] = cpuCmd.writeJobMetrics

  override def requireSingleBatch: Boolean = false

  private def columnarStatsTrackers: Seq[ColumnarWriteJobStatsTracker] = {
    val serializableConf = new SerializableConfiguration(cpuCmd.hadoopConf)
    cpuCmd.statsTrackers.map {
      case tracker: BasicWriteJobStatsTracker =>
        new BasicColumnarWriteJobStatsTracker(
          serializableConf, GpuMetric.wrap(tracker.driverSideMetrics))
      case tracker: DeltaJobStatisticsTracker =>
        GpuWriteIntoDeltaCommandStats(cpuCmd, tracker)
      case tracker =>
        throw new IllegalStateException(
          s"Unsupported Delta write statistics tracker ${tracker.getClass.getName}")
    }
  }

  override def runColumnar(
      sparkSession: SparkSession,
      child: SparkPlan): Seq[ColumnarBatch] = {
    val partitionColumns = cpuCmd.partitionColExprIds.map { exprId =>
      child.output.find(_.exprId == exprId).getOrElse {
        throw new IllegalStateException(s"Missing Delta partition column expression $exprId")
      }
    }
    val outputSpec = cpuCmd.outputSpec.copy(outputColumns = child.output)
    val writePartitionColumns = WriteIntoDeltaCommand.writePartitionColumns(
      cpuCmd.protocol, cpuCmd.metadata, sparkSession)
    val effectivePartitions = if (writePartitionColumns) partitionColumns else Seq.empty[Attribute]

    GpuFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = child,
      fileFormat = new GpuParquetFileFormat,
      committer = cpuCmd.committer,
      outputSpec = outputSpec,
      hadoopConf = cpuCmd.hadoopConf,
      partitionColumns = effectivePartitions,
      bucketSpec = None,
      statsTrackers = columnarStatsTrackers,
      options = cpuCmd.options,
      useStableSort = rapidsConf.stableSort,
      concurrentWriterPartitionFlushSize = rapidsConf.concurrentWriterPartitionFlushSize,
      baseDebugOutputPath = rapidsConf.outputDebugDumpPrefix)
    Seq.empty
  }
}
