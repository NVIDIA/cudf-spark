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

import com.databricks.sql.io.skipping.liquid.ClusteredTableUtils
import com.databricks.sql.transaction.tahoe.DeltaIdentityColumnStatsTracker
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, WriteIntoDeltaCommand}
import com.databricks.sql.transaction.tahoe.stats.{DeltaJobStatisticsTracker,
  StatisticsOnLoadJobTracker}
import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta,
  GpuDataWritingCommand, GpuMetric, GpuParquetFileFormat, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.delta.{GpuDeltaJobStatisticsTracker, GpuStatisticsCollection,
  RapidsDeltaUtils}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker,
  WriteJobStatsTracker}
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker,
  ColumnarWriteJobStatsTracker}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.{
  SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/** Limits the generic DBR V1 write rule to the native liquid OPTIMIZE call stack. */
object GpuLiquidOptimizeWriteContext {
  private val activeKey = "spark.rapids.sql.delta.liquidOptimizeWrite.active"

  def isActive: Boolean = SparkContext.getActive
    .exists(_.getLocalProperty(activeKey) == "true")

  def withOptimize[T](spark: SparkSession)(body: => T): T = {
    val sparkContext = spark.sparkContext
    val previous = sparkContext.getLocalProperty(activeKey)
    sparkContext.setLocalProperty(activeKey, "true")
    try {
      body
    } finally {
      sparkContext.setLocalProperty(activeKey, previous)
    }
  }
}

/** Metadata for the DBR write command used by the native liquid OPTIMIZE framework. */
class GpuWriteIntoDeltaCommandMeta(
    cmd: WriteIntoDeltaCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends DataWritingCommandMeta[WriteIntoDeltaCommand](cmd, conf, parent, rule) {

  override protected def tagSelfForGpuInternal(): Unit = {
    val snapshot = cmd.deltaLog.unsafeVolatileSnapshot
    if (!GpuLiquidOptimizeWriteContext.isActive ||
        !ClusteredTableUtils.isSupported(cmd.protocol)) {
      willNotWorkOnGpu(
        "DBR WriteIntoDeltaCommand GPU support is limited to native liquid OPTIMIZE")
    }
    if (DeletionVectorUtils.deletionVectorsWritable(snapshot) ||
        !DeletionVectorUtils.isTableDVFree(snapshot)) {
      willNotWorkOnGpu(
        "DBR liquid OPTIMIZE with deletion vectors is not supported on GPU")
    }
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    RapidsDeltaUtils.tagForDeltaWrite(
      this, cmd.query.schema, Some(cmd.deltaLog), cmd.options, SparkSession.active)
    cmd.statsTrackers.foreach {
      case _: DeltaIdentityColumnStatsTracker =>
        willNotWorkOnGpu("DBR identity-column write statistics are not supported by the " +
          "GPU WriteIntoDeltaCommand")
      case _: StatisticsOnLoadJobTracker =>
        willNotWorkOnGpu("DBR statistics-on-load are not supported by the GPU " +
          "WriteIntoDeltaCommand")
      case _: BasicWriteJobStatsTracker | _: DeltaJobStatisticsTracker =>
      case tracker =>
        willNotWorkOnGpu(s"DBR write statistics tracker ${tracker.getClass.getName} is not " +
          "supported by the GPU WriteIntoDeltaCommand")
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = GpuWriteIntoDeltaCommand(
    cmd,
    conf.stableSort,
    conf.concurrentWriterPartitionFlushSize,
    conf.outputDebugDumpPrefix)
}

/**
 * GPU file-writing equivalent of DBR's [[WriteIntoDeltaCommand]].
 *
 * The command deliberately reuses the native output specification and commit protocol. The
 * enclosing DBR transaction therefore remains responsible for AddFile creation, liquid domain
 * metadata, and the final commit; only FileFormatWriter's row writer is replaced here.
 */
case class GpuWriteIntoDeltaCommand(
    cpuCmd: WriteIntoDeltaCommand,
    useStableSort: Boolean,
    concurrentWriterPartitionFlushSize: Long,
    baseDebugOutputPath: Option[String])
    extends GpuDataWritingCommand {

  override def query: LogicalPlan = cpuCmd.query

  override def outputColumnNames: Seq[String] = cpuCmd.outputColumnNames

  override def requireSingleBatch: Boolean = false

  override def runColumnar(
      sparkSession: ClassicSparkSession,
      child: SparkPlan): Seq[ColumnarBatch] = {
    val outputColumns = cpuCmd.outputSpec.outputColumns
    val partitionColumns = cpuCmd.partitionColExprIds.map { exprId =>
      outputColumns.find(_.exprId == exprId).get
    }
    val convertedTrackers = cpuCmd.statsTrackers.map(convertTracker)

    GpuDeltaFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = child,
      fileFormat = new GpuParquetFileFormat,
      committer = cpuCmd.committer,
      outputSpec = cpuCmd.outputSpec,
      hadoopConf = cpuCmd.hadoopConf,
      partitionColumns = partitionColumns,
      bucketSpec = cpuCmd.bucketSpec,
      statsTrackers = convertedTrackers.map(_.gpu),
      options = cpuCmd.options,
      useStableSort = useStableSort,
      concurrentWriterPartitionFlushSize = concurrentWriterPartitionFlushSize,
      baseDebugOutputPath = baseDebugOutputPath)
    convertedTrackers.foreach(_.copyResultToCpu())
    Seq.empty
  }

  private case class ConvertedTracker(
      gpu: ColumnarWriteJobStatsTracker,
      copyResultToCpu: () => Unit)

  private def convertTracker(tracker: WriteJobStatsTracker): ConvertedTracker = tracker match {
    case identity: DeltaIdentityColumnStatsTracker =>
      throw new IllegalStateException(
        s"Unsupported identity-column statistics tracker ${identity.getClass.getName}")
    case delta: DeltaJobStatisticsTracker =>
      val statsSchema = StructType(delta.dataCols.map { attr =>
        StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
      })
      val explodedDataSchema = GpuStatisticsCollection.explode(statsSchema).toMap
      val statsColExpr = delta.statsColExpr.transform {
        case runtime: RuntimeReplaceable => runtime.replacement
      }
      val batchStatsToRow = (batch: ColumnarBatch, row: InternalRow) => {
        GpuStatisticsCollection.batchStatsToRow(
          statsSchema, explodedDataSchema, batch, row)
      }
      val gpu = new GpuDeltaJobStatisticsTracker(
        delta.dataCols, statsColExpr, batchStatsToRow)
      ConvertedTracker(gpu, () => delta.recordedStats = gpu.recordedStats)
    case basic: BasicWriteJobStatsTracker =>
      val gpu = new BasicColumnarWriteJobStatsTracker(
        new SerializableConfiguration(cpuCmd.hadoopConf),
        GpuMetric.wrap(basic.driverSideMetrics))
      ConvertedTracker(gpu, () => ())
    case statsOnLoad: StatisticsOnLoadJobTracker =>
      throw new IllegalStateException(
        s"Unsupported statistics-on-load tracker ${statsOnLoad.getClass.getName}")
    case other =>
      throw new IllegalStateException(s"Unsupported write statistics tracker " +
        other.getClass.getName)
  }
}
