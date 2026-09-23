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

package org.apache.spark.sql.delta.rapids.delta43x

import scala.util.Try

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}
import com.nvidia.spark.rapids.delta.DeltaWriteUtils.toBooleanOption
import com.nvidia.spark.rapids.delta.delta43x.{Delta43xConfigChecker, Delta43xProvider,
  GpuDeltaCatalog}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaOptions,
  NumRecordsStats, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeltaReorgOperation, UpdateCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.hooks.GpuAutoCompact43x
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShimBase, GpuDeltaLog,
  GpuOptimisticTransactionBase, GpuWriteIntoDeltaLike, StartTransactionArg}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.StructType

class Delta43xRuntimeShim extends DeltaRuntimeShimBase {

  override def groupOptimizeFilesByPartition(
      spark: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile]): Seq[(Map[String, String], Seq[AddFile])] = {
    files
      .groupBy(_.normalizedPartitionValues(
        spark,
        snapshot.metadata.physicalPartitionSchema))
      .map { case (_, partitionFiles) =>
        (partitionFiles.head.partitionValues, partitionFiles)
      }
      .toSeq
  }

  override def reportSomeZeroMetrics(
      spark: SparkSession,
      numCopiedRows: Option[Long],
      numDeletedRows: Option[Long]): (Option[Long], Option[Long]) = {
    val alwaysReportSomeZero = spark.sessionState.conf.getConf(
      DeltaSQLConf.METRICS_ALWAYS_REPORT_SOME_ZERO_METRICS)
    def reportSomeZero(metric: Option[Long]): Option[Long] = {
      if (alwaysReportSomeZero) {
        Some(metric.getOrElse(0L))
      } else {
        metric
      }
    }
    (reportSomeZero(numCopiedRows), reportSomeZero(numDeletedRows))
  }

  override def validateDeleteNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit = {
    validateNumRecords(spark, deltaLog, numRecordsStats, "DELETE", _ > _)
  }

  override def validateUpdateNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit = {
    validateNumRecords(spark, deltaLog, numRecordsStats, "UPDATE", _ != _)
  }

  private def validateNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats,
      operation: String,
      isMismatch: (Long, Long) => Boolean): Unit = {
    (numRecordsStats.numLogicalRecordsAdded,
      numRecordsStats.numLogicalRecordsRemoved,
      numRecordsStats.numLogicalRecordsAddedInFilesWithDeletionVectors) match {
      case (Some(numAddedRecords), Some(numRemovedRecords), Some(_))
          if isMismatch(numAddedRecords, numRemovedRecords) &&
            spark.sessionState.conf.getConf(DeltaSQLConf.NUM_RECORDS_VALIDATION_ENABLED) =>
        throw DeltaErrors.numRecordsMismatch(operation, numAddedRecords, numRemovedRecords)
      case _ if numRecordsStats.numLogicalRecordsAdded.isEmpty ||
          numRecordsStats.numLogicalRecordsRemoved.isEmpty ||
          numRecordsStats.numLogicalRecordsAddedInFilesWithDeletionVectors.isEmpty =>
        recordDeltaEvent(
          deltaLog, opType = "delta.assertions.statsNotPresentForNumRecordsCheck")
      case _ =>
    }
  }

  override def runDeltaOperation[A](
      deltaLog: DeltaLog,
      opType: String)(thunk: => A): A = {
    recordDeltaOperation(deltaLog, opType)(thunk)
  }

  override def emitDeltaEvent(
      deltaLog: DeltaLog,
      opType: String,
      data: AnyRef): Unit = {
    recordDeltaEvent(deltaLog, opType, data = data)
  }

  override def assertRemovable(snapshot: Snapshot): Unit = DeltaLog.assertRemovable(snapshot)

  override def filterFilesToReorg(
      operation: DeltaReorgOperation,
      spark: SparkSession,
      snapshot: Snapshot,
      candidates: Seq[AddFile]): Seq[AddFile] = {
    operation.filterFilesToReorg(spark, snapshot, candidates)
  }

  override def preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns: DataFrame,
      snapshot: Snapshot,
      targetOutput: Seq[Attribute],
      updateExpressions: Seq[Expression]): (DataFrame, Seq[Attribute], Seq[Expression]) = {
    UpdateCommand.preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns, snapshot, targetOutput, updateExpressions)
  }

  override def getDeltaConfigChecker: DeltaConfigChecker = Delta43xConfigChecker

  override def getDeltaProvider: DeltaProvider = Delta43xProvider

  override def getGpuDeltaCatalog(
      cpuCatalog: DeltaCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = {
    new GpuDeltaCatalog(cpuCatalog, rapidsConf)
  }

  override protected def constructOptimisticTransaction(
      arg: StartTransactionArg): GpuOptimisticTransactionBase =
    new GpuOptimisticTransaction43x(
      arg.log, arg.catalogTable, arg.snapshot, arg.conf, GpuAutoCompact43x)

  override def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    WriteIntoDelta(
      deltaLog,
      mode,
      options,
      partitionColumns,
      configuration,
      data,
      catalogTableOpt,
      schemaInCatalog,
      isInsertReplaceUsingByName = false)
  }

  override def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike = {
    GpuWriteIntoDelta43x(gpuDeltaLog, cpuWrite)
  }

  override def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    DeltaOperations.Write(
      mode,
      Option(partitionColumns),
      options.replaceWhere,
      options.userMetadata,
      dynamicPartitionOverwriteOption(options),
      toBooleanOption(options.canOverwriteSchema),
      toBooleanOption(options.canMergeSchema))
  }

  override def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    DeltaOperations.ReplaceTable(
      metadata,
      isManaged,
      orCreate,
      asSelect,
      options.flatMap(_.userMetadata),
      clusterBy,
      options.flatMap(_.replaceWhere),
      options.flatMap(dynamicPartitionOverwriteOption),
      toBooleanOption(options.exists(_.canOverwriteSchema)),
      toBooleanOption(options.exists(_.canMergeSchema)),
      isV1SaveAsTableOverwrite)
  }

  private def dynamicPartitionOverwriteOption(options: DeltaOptions): Option[Boolean] = {
    toBooleanOption(Try(options.isDynamicPartitionOverwriteMode).getOrElse(false))
  }
}
