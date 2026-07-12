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
import com.databricks.sql.transaction.tahoe.schema.InnerInvariantViolationException
import com.databricks.sql.transaction.tahoe.stats.{DeltaJobStatisticsTracker,
  StatisticsOnLoadJobTracker}
import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta,
  GpuDataWritingCommand, GpuMetric, GpuParquetFileFormat, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, GpuWriteFiles}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker,
  GpuFileFormatWriter}
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class GpuWriteIntoDeltaCommandMeta(
    cmd: WriteIntoDeltaCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[WriteIntoDeltaCommand](cmd, conf, parent, rule) {

  private var fileFormat: Option[GpuParquetFileFormat] = None

  private def tagAttributeMapping(attribute: Attribute, description: String): Unit = {
    val matches = cmd.query.output.count(_.exprId == attribute.exprId)
    if (matches != 1) {
      willNotWorkOnGpu(s"Delta $description ${attribute.exprId} has $matches query matches")
    }
  }

  override protected def tagSelfForGpuInternal(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled")
    }
    if (cmd.fileFormat.getClass != classOf[DeltaParquetFileFormat]) {
      willNotWorkOnGpu(s"Delta file format ${cmd.fileFormat.getClass.getName} is not supported")
    } else {
      fileFormat = GpuParquetFileFormat.tagGpuSupport(
        this, TrampolineConnectShims.getActiveSession, cmd.options, cmd.query.schema)
    }
    if (cmd.bucketSpec.nonEmpty) {
      willNotWorkOnGpu("Bucketed Delta writes are not supported")
    }
    if (cmd.staticPartitions.nonEmpty) {
      willNotWorkOnGpu("Static partition Delta writes are not supported by this command path")
    }
    if (cmd.partitionColExprIds.nonEmpty) {
      willNotWorkOnGpu("Partitioned DBR Delta writes require DeltaFileFormatWriter's private " +
        "partitioned task-attempt context")
    }
    cmd.outputSpec.outputColumns.foreach(tagAttributeMapping(_, "output column"))
    cmd.partitionColExprIds.foreach { exprId =>
      val matches = cmd.query.output.count(_.exprId == exprId)
      if (matches != 1) {
        willNotWorkOnGpu(s"Delta partition column $exprId has $matches query matches")
      }
    }
    cmd.statsTrackers.foreach {
      case tracker: BasicWriteJobStatsTracker =>
        val metrics = tracker.driverSideMetrics ++ cmd.writeJobMetrics
        if (!metrics.contains(TASK_COMMIT_TIME)) {
          willNotWorkOnGpu(s"Delta basic statistics tracker is missing $TASK_COMMIT_TIME")
        }
      case _: DeltaJobStatisticsTracker =>
      case _: StatisticsOnLoadJobTracker =>
        willNotWorkOnGpu("DBR StatisticsOnLoadJobTracker is not supported on GPU")
      case tracker =>
        willNotWorkOnGpu(s"Delta write statistics tracker ${tracker.getClass.getName} " +
          "is not supported on GPU")
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val gpuFileFormat = fileFormat.getOrElse(
      throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))
    GpuWriteIntoDeltaCommand(cmd, conf, gpuFileFormat)
  }
}

case class GpuWriteIntoDeltaCommand(
    cpuCmd: WriteIntoDeltaCommand,
    @transient rapidsConf: RapidsConf,
    fileFormat: GpuParquetFileFormat) extends GpuDataWritingCommand {

  override def query: LogicalPlan = cpuCmd.query

  override def outputColumnNames: Seq[String] = cpuCmd.outputColumnNames

  override lazy val metrics: Map[String, SQLMetric] = cpuCmd.writeJobMetrics

  override def requireSingleBatch: Boolean = false

  private def columnarStatsTrackers(
      sparkSession: SparkSession): Seq[ColumnarWriteJobStatsTracker] = {
    val serializableConf = new SerializableConfiguration(cpuCmd.hadoopConf)
    cpuCmd.statsTrackers.map {
      case tracker: BasicWriteJobStatsTracker =>
        val metrics = tracker.driverSideMetrics ++ cpuCmd.writeJobMetrics
        new BasicColumnarWriteJobStatsTracker(
          serializableConf, GpuMetric.wrap(metrics))
      case tracker: DeltaJobStatisticsTracker =>
        GpuWriteIntoDeltaCommandStats(cpuCmd, tracker, sparkSession)
      case tracker =>
        throw new IllegalStateException(
          s"Unsupported Delta write statistics tracker ${tracker.getClass.getName}")
    }
  }

  override def runColumnar(
      sparkSession: SparkSession,
      child: SparkPlan): Seq[ColumnarBatch] = {
    val dataPlan = GpuWriteFiles.getWriteFilesOpt(child).map(_.child).getOrElse(child)

    def resolveAttribute(exprId: ExprId,
        description: String): Attribute = {
      val queryMatches = cpuCmd.query.output.zipWithIndex.filter(_._1.exprId == exprId)
      queryMatches match {
        case Seq((_, ordinal)) if ordinal < dataPlan.output.size => dataPlan.output(ordinal)
        case Seq((_, ordinal)) => throw new IllegalStateException(
          s"Delta $description $exprId maps to missing data output ordinal $ordinal")
        case matches => throw new IllegalStateException(
          s"Delta $description $exprId has ${matches.size} query output matches")
      }
    }

    val outputColumns = cpuCmd.outputSpec.outputColumns.map { attribute =>
      resolveAttribute(attribute.exprId, "output column")
    }
    val partitionColumns = cpuCmd.partitionColExprIds.map { exprId =>
      resolveAttribute(exprId, "partition column")
    }
    val outputSpec = cpuCmd.outputSpec.copy(outputColumns = outputColumns)
    val writePartitionColumns = WriteIntoDeltaCommand.writePartitionColumns(
      cpuCmd.protocol, cpuCmd.metadata, sparkSession)
    if (partitionColumns.nonEmpty && writePartitionColumns) {
      throw new IllegalStateException(
        "Writing partition columns into Delta Parquet data files is not supported")
    }

    try {
      GpuFileFormatWriter.write(
        sparkSession = sparkSession,
        plan = child,
        fileFormat = fileFormat,
        committer = cpuCmd.committer,
        outputSpec = outputSpec,
        hadoopConf = cpuCmd.hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = None,
        statsTrackers = columnarStatsTrackers(sparkSession),
        options = cpuCmd.options,
        useStableSort = rapidsConf.stableSort,
        concurrentWriterPartitionFlushSize = rapidsConf.concurrentWriterPartitionFlushSize,
        baseDebugOutputPath = rapidsConf.outputDebugDumpPrefix)
    } catch {
      case InnerInvariantViolationException(violation) => throw violation
    }
    Seq.empty
  }
}
