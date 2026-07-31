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

import com.databricks.sql.transaction.tahoe.{DeltaColumnMapping, DeltaParquetFileFormat}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaCommand
import com.databricks.sql.transaction.tahoe.schema.InnerInvariantViolationException
import com.databricks.sql.transaction.tahoe.stats.{DeltaJobStatisticsTracker,
  StatisticsOnLoadJobTracker}
import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta,
  GpuDataWritingCommand, GpuMetric, GpuParquetFileFormat, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.delta.RapidsDeltaUtils

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, GpuWriteFiles}
import org.apache.spark.sql.execution.datasources.v2.rapids.GpuAtomicDeltaWriteContext
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker,
  GpuFileFormatWriter}
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

private object DeltaWriteAttributeMapping {
  private def structurallyMatches(left: Attribute, right: Attribute): Boolean = {
    left.name == right.name && left.dataType == right.dataType
  }

  private def structurallyMatches(left: Attribute, right: StructField): Boolean = {
    left.name == right.name && left.dataType == right.dataType
  }

  def validateQueryAttributeAtOrdinal(
      queryOutput: Seq[Attribute],
      logicalField: StructField,
      physicalField: StructField,
      attribute: Attribute,
      ordinal: Int,
      description: String): Either[String, Unit] = {
    if (ordinal >= queryOutput.size) {
      Left(s"Delta $description ${attribute.exprId} maps to missing query output ordinal $ordinal")
    } else {
      val queryAttribute = queryOutput(ordinal)
      val exprIdOrdinal = queryOutput.indexWhere(_.exprId == attribute.exprId)
      val matchesExpectedSchema = structurallyMatches(attribute, queryAttribute) ||
        (structurallyMatches(queryAttribute, logicalField) &&
          structurallyMatches(attribute, physicalField))
      if (attribute.exprId == queryAttribute.exprId ||
          (exprIdOrdinal == -1 && matchesExpectedSchema)) {
        Right(())
      } else if (exprIdOrdinal != -1) {
        Left(s"Delta $description ${attribute.exprId} maps to query output ordinal " +
          s"$exprIdOrdinal instead of native output ordinal $ordinal")
      } else {
        Left(s"Delta $description ${attribute.exprId} has no query ExprId match and does not " +
          s"match the logical or physical Delta schema at query output ordinal $ordinal")
      }
    }
  }

  def remapToDataAttribute(
      outputAttribute: Attribute,
      queryAttribute: Attribute,
      dataAttribute: Attribute,
      description: String,
      ordinal: Int): Either[String, Attribute] = {
    if (structurallyMatches(queryAttribute, dataAttribute)) {
      Right(outputAttribute.withExprId(dataAttribute.exprId))
    } else {
      Left(s"Delta $description at ordinal $ordinal does not structurally match the data plan: " +
        s"query=${queryAttribute.name}:${queryAttribute.dataType.catalogString}, " +
        s"data=${dataAttribute.name}:${dataAttribute.dataType.catalogString}")
    }
  }
}

class GpuWriteIntoDeltaCommandMeta(
    cmd: WriteIntoDeltaCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[WriteIntoDeltaCommand](cmd, conf, parent, rule) {

  private var fileFormat: Option[GpuParquetFileFormat] = None
  private lazy val logicalDataFields = cmd.metadata.dataSchema.fields
  private lazy val physicalDataFields = DeltaColumnMapping.createPhysicalSchema(
    cmd.metadata.dataSchema,
    cmd.metadata.schema,
    cmd.metadata.columnMappingMode).fields

  private def tagAttributeMapping(
      attribute: Attribute,
      ordinal: Int,
      description: String): Unit = {
    DeltaWriteAttributeMapping.validateQueryAttributeAtOrdinal(
      cmd.query.output,
      logicalDataFields(ordinal),
      physicalDataFields(ordinal),
      attribute,
      ordinal,
      description) match {
      case Left(reason) => willNotWorkOnGpu(reason)
      case Right(_) => ()
    }
  }

  override protected def tagSelfForGpuInternal(): Unit = {
    if (!GpuAtomicDeltaWriteContext.isActive) {
      willNotWorkOnGpu(
        "DBR WriteIntoDeltaCommand GPU support is limited to atomic CTAS/RTAS")
    }
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled")
    }
    val spark = TrampolineConnectShims.getActiveSession
    RapidsDeltaUtils.tagForDeltaWrite(
      this, cmd.query.schema, Some(cmd.deltaLog), cmd.options, spark)
    if (cmd.fileFormat.getClass != classOf[DeltaParquetFileFormat]) {
      willNotWorkOnGpu(s"Delta file format ${cmd.fileFormat.getClass.getName} is not supported")
    } else {
      fileFormat = GpuParquetFileFormat.tagGpuSupport(
        this, spark, cmd.options, cmd.hadoopConf, cmd.query.schema)
    }
    if (cmd.bucketSpec.nonEmpty) {
      willNotWorkOnGpu("Bucketed Delta writes are not supported")
    }
    if (cmd.staticPartitions.nonEmpty) {
      willNotWorkOnGpu("Static partition Delta writes are not supported by this command path")
    }
    if (cmd.partitionColExprIds.nonEmpty) {
      willNotWorkOnGpu("Partitioned DBR Delta writes are not supported by this command path " +
        "until native partition materialization and partition-evolution semantics are validated")
    }
    val columnCounts = Seq(
      "output specification" -> cmd.outputSpec.outputColumns.size,
      "query" -> cmd.query.output.size,
      "logical data schema" -> logicalDataFields.length,
      "physical data schema" -> physicalDataFields.length)
    if (columnCounts.map(_._2).distinct.size != 1) {
      willNotWorkOnGpu(s"Delta column count mismatch: ${columnCounts.map {
        case (description, count) => s"$description=$count"
      }.mkString(", ")}")
    } else {
      cmd.outputSpec.outputColumns.zipWithIndex.foreach { case (attribute, ordinal) =>
        tagAttributeMapping(attribute, ordinal, "output column")
      }
      cmd.partitionColExprIds.foreach { exprId =>
        val matches = cmd.outputSpec.outputColumns.zipWithIndex.filter(_._1.exprId == exprId)
        matches match {
          case Seq((attribute, ordinal)) =>
            tagAttributeMapping(attribute, ordinal, "partition column")
          case _ =>
            willNotWorkOnGpu(
              s"Delta partition column $exprId has ${matches.size} output specification matches")
        }
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

  private lazy val logicalDataFields = cpuCmd.metadata.dataSchema.fields
  private lazy val physicalDataFields = DeltaColumnMapping.createPhysicalSchema(
    cpuCmd.metadata.dataSchema,
    cpuCmd.metadata.schema,
    cpuCmd.metadata.columnMappingMode).fields

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

    if (cpuCmd.outputSpec.outputColumns.size != cpuCmd.query.output.size ||
        dataPlan.output.size != cpuCmd.query.output.size) {
      throw new IllegalStateException(
        s"Delta output size mismatch: outputSpec=${cpuCmd.outputSpec.outputColumns.size}, " +
          s"query=${cpuCmd.query.output.size}, dataPlan=${dataPlan.output.size}")
    }

    def resolveAttribute(
        attribute: Attribute,
        ordinal: Int,
        description: String): Attribute = {
      DeltaWriteAttributeMapping.validateQueryAttributeAtOrdinal(
        cpuCmd.query.output,
        logicalDataFields(ordinal),
        physicalDataFields(ordinal),
        attribute,
        ordinal,
        description) match {
        case Right(_) => ()
        case Left(reason) => throw new IllegalStateException(reason)
      }
      DeltaWriteAttributeMapping.remapToDataAttribute(
        attribute,
        cpuCmd.query.output(ordinal),
        dataPlan.output(ordinal),
        description,
        ordinal) match {
        case Right(value) => value
        case Left(reason) => throw new IllegalStateException(reason)
      }
    }

    val outputColumns = cpuCmd.outputSpec.outputColumns.zipWithIndex.map {
      case (attribute, ordinal) =>
        resolveAttribute(attribute, ordinal, "output column")
    }
    val partitionColumns = cpuCmd.partitionColExprIds.map { exprId =>
      val matches = cpuCmd.outputSpec.outputColumns.zipWithIndex.filter(_._1.exprId == exprId)
      matches match {
        case Seq((attribute, ordinal)) =>
          resolveAttribute(attribute, ordinal, "partition column")
        case _ => throw new IllegalStateException(
          s"Delta partition column $exprId has ${matches.size} output specification matches")
      }
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
