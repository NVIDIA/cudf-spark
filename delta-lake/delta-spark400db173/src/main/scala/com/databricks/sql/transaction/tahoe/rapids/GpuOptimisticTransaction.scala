/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from OptimisticTransaction.scala and TransactionalWrite.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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
import com.databricks.sql.transaction.tahoe.{CommittedTransaction, DeltaLog, DeltaOptions, OptimizeExecutionObserver, Snapshot}
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, DeltaOptimizeContext}
import com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeMetrics
import com.databricks.sql.transaction.tahoe.constraints.Constraint
import com.databricks.sql.transaction.tahoe.files.TransactionalWriteOptions
import com.databricks.sql.transaction.tahoe.hooks.{AutoCompact, AutoCompactPartitionReserve}
import com.databricks.sql.transaction.tahoe.hooks.{AutoCompactRequest, AutoCompactType, AutoCompactUtils, PostCommitHook}
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.databricks.sql.transaction.tahoe.stats.AutoCompactPartitionStats
import com.databricks.sql.transaction.tahoe.util.TestBarrier
import com.nvidia.spark.rapids.{ColumnarFileFormat, RapidsConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, RuntimeReplaceable}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.ColumnarWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.util.Clock

class GpuOptimisticTransaction(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)(implicit clock: Clock)
    extends GpuOptimisticTransactionWriteBase(deltaLog, catalogTable, snapshot, rapidsConf)(clock) {

  def this(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], snapshot, rapidsConf)
  }

  def this(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      snapshotOpt: Option[Snapshot],
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, catalogTable, snapshotOpt.getOrElse(deltaLog.update()), rapidsConf)
  }

  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], deltaLog.update(), rapidsConf)
  }

  override protected def normalizeGpuStatsColExpr(expr: Expression): Expression = {
    expr.transform {
      case rr: RuntimeReplaceable => rr.replacement
    }
  }

  override protected def handlePostWriteAutoCompact(
      spark: SparkSession,
      isOptimize: Boolean,
      fileActions: Seq[FileAction]): Unit = {
    val autoCompactEnabled =
      AutoCompact.getAutoCompactType(spark.sessionState.conf, metadata).isDefined
    if (!isOptimize && autoCompactEnabled && fileActions.nonEmpty) {
      registerPostCommitHook(GpuAutoCompact)
    }
  }

  override def registerSQLMetrics(spark: SparkSession, metrics: Map[String, SQLMetric]): Unit = {
    val extended = metrics.get("numSourceRows").fold(metrics) { sourceRows =>
      // Alias for 4.0 commit-time transformMetrics expectations.
      metrics + ("operationNumSourceRows" -> sourceRows)
    }
    super.registerSQLMetrics(spark, extended)
  }

  override protected def getGpuWriteCommitter(
      outputPath: Path,
      keepPartitionIdTag: Boolean,
      forcePreserveInputOrder: Boolean) = {
    getCommitter(
      outputPath,
      keepPartitionIdTag,
      forcePreserveInputOrder,
      false,
      false,
      false)
  }

  override protected def writeUsingGpuFileFormatWriter(
      sparkSession: TrampolineConnectShims.SparkSession,
      plan: SparkPlan,
      fileFormat: ColumnarFileFormat,
      committer: FileCommitProtocol,
      outputSpec: FileFormatWriter.OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      options: Map[String, String]): Unit = {
    GpuDeltaFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = fileFormat,
      committer = committer,
      outputSpec = outputSpec,
      hadoopConf = hadoopConf,
      partitionColumns = partitionColumns,
      bucketSpec = None,
      statsTrackers = statsTrackers,
      options = options,
      useStableSort = rapidsConf.stableSort,
      concurrentWriterPartitionFlushSize = rapidsConf.concurrentWriterPartitionFlushSize,
      baseDebugOutputPath = rapidsConf.outputDebugDumpPrefix)
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    val (fileActions, _) =
      gpuWriteFiles(
        inputData,
        writeOptions,
        additionalConstraints,
        Some(isOptimize),
        keepPartitionIdTag = false,
        forcePreserveInputOrder = false,
        context = None)
    fileActions
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: TransactionalWriteOptions,
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      isCDCWritePhase: Boolean,
      context: Option[String]): Seq[FileAction] = {
    if (isLiquidClustering || isCDCWritePhase) {
      super.writeFiles(inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, isCDCWritePhase, context)
    } else {
      val (fileActions, _) =
        gpuWriteFiles(
          inputData,
          Option(writeOptions).flatMap(_.deltaOptions),
          additionalConstraints,
          Some(isOptimize),
          keepPartitionIdTag = false,
          writeOptions.forcePreserveInputOrder,
          context)
      fileActions
    }
  }

  override def writeFilesAndGetQueryExecution(
      inputData: Dataset[_],
      writeOptions: TransactionalWriteOptions,
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      isCDCWritePhase: Boolean,
      context: Option[String],
      trailing: Boolean): (Seq[FileAction], QueryExecution) = {
    if (isLiquidClustering || isCDCWritePhase) {
      super.writeFilesAndGetQueryExecution(
        inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, isCDCWritePhase, context, trailing)
    } else {
      gpuWriteFiles(
        inputData,
        Option(writeOptions).flatMap(_.deltaOptions),
        additionalConstraints,
        Some(isOptimize),
        trailing,
        writeOptions.forcePreserveInputOrder,
        context)
    }
  }

  override def writeFilesAndGetExecutedPlan(
      inputData: Dataset[_],
      writeOptions: Either[Option[DeltaOptions], TransactionalWriteOptions],
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      context: Option[String],
      trailing: Boolean): (Seq[FileAction], SparkPlan) = {
    if (isLiquidClustering) {
      super.writeFilesAndGetExecutedPlan(
        inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, context, trailing)
    } else {
      val deltaOpts = writeOptions match {
        case Left(opt) => opt
        case Right(opts) => Option(opts).flatMap(_.deltaOptions)
      }
      val forcePreserveInputOrder = writeOptions match {
        case Left(_) => false
        case Right(opts) => opts.forcePreserveInputOrder
      }
      val (fileActions, qe) =
        gpuWriteFiles(
          inputData,
          deltaOpts,
          additionalConstraints,
          Some(isOptimize),
          trailing,
          forcePreserveInputOrder,
          context)
      (fileActions, qe.executedPlan)
    }
  }
}

private object GpuAutoCompact extends PostCommitHook with DeltaLogging with Serializable {
  override val name: String = AutoCompact.name

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    val autoCompactTypeOpt = AutoCompact.getAutoCompactType(
      spark.sessionState.conf,
      txn.postCommitSnapshot.metadata)
    autoCompactTypeOpt.foreach { autoCompactType =>
      if (!shouldSkipAutoCompact(spark, txn, autoCompactType)) {
        if (autoCompactType.shouldRunInBackground ||
            DeletionVectorUtils.deletionVectorsWritable(txn.postCommitSnapshot) ||
            !DeletionVectorUtils.isTableDVFree(txn.postCommitSnapshot)) {
          GpuAutoCompactCpuFallback.run(spark, txn)
        } else {
          val request = AutoCompactUtils.prepareAutoCompactRequest(
            spark,
            txn,
            AutoCompact.OP_TYPE,
            false,
            AutoCompact.maxDeletedRowsRatio)
          val tableId = txn.deltaLog.tableId
          if (request.shouldCompact) {
            OptimizeExecutionObserver.getObserver.beginInlineAutoCompact()
            TestBarrier.waitIfEnabled("autoCompact.inline.start")
            try {
              compact(spark, txn, request)
              AutoCompactPartitionStats.instance(spark).markPartitionsAsCompacted(
                tableId,
                request.allowedPartitions)
            } catch {
              case e: Throwable =>
                recordDeltaEvent(txn.deltaLog, "delta.autoCompaction.error", data = getErrorData(e))
                throw e
            } finally {
              if (AutoCompactUtils.reservePartitionEnabled(spark)) {
                AutoCompactPartitionReserve.releasePartitions(tableId, request.allowedPartitions)
              }
              TestBarrier.waitIfEnabled("autoCompact.inline.end")
              OptimizeExecutionObserver.getObserver.endInlineAutoCompact()
            }
          } else {
            TestBarrier.waitIfEnabled("autoCompact.inline.shouldNotCompact")
          }
        }
      }
    }
  }

  private def shouldSkipAutoCompact(
      spark: SparkSession,
      txn: CommittedTransaction,
      autoCompactType: AutoCompactType): Boolean = {
    !AutoCompactUtils.isQualifiedForAutoCompact(spark, txn) ||
      (ClusteredTableUtils.isSupported(txn.committedProtocol) &&
        (!spark.conf.get(DeltaSQLConf.DELTA_LIQUID_BACKGROUND_AUTO_COMPACTION_ENABLED) ||
          !autoCompactType.shouldRunInBackground)) ||
      (autoCompactType == AutoCompactType.BackgroundAutoCompactUCTables &&
        !txn.deltaLog.unsafeVolatileIsUCManagedTable)
  }

  private def compact(
      spark: SparkSession,
      txn: CommittedTransaction,
      request: AutoCompactRequest): Seq[OptimizeMetrics] = {
    recordDeltaOperation(txn.deltaLog, AutoCompact.OP_TYPE) {
      recordDeltaOperation(txn.deltaLog, s"${AutoCompact.OP_TYPE}.execute") {
        val maxFileSize = spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE)
        val minFileSize = spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_FILE_SIZE)
          .orElse(maxFileSize.map(_ / 2))
        val optimizeContext = DeltaOptimizeContext(
          minFileSize = minFileSize,
          maxFileSize = maxFileSize,
          maxDeletedRowsRatio = AutoCompact.maxDeletedRowsRatio)
        val rows = new GpuOptimizeExecutor(
          spark,
          txn.deltaLog.update(catalogTableOpt = txn.catalogTable),
          txn.catalogTable,
          request.targetPartitionsPredicate,
          optimizeContext,
          isAutoCompact = true).optimize()
        val metrics = rows.map(_.getAs[OptimizeMetrics](1))
        metrics.headOption.foreach { metric =>
          recordDeltaEvent(txn.deltaLog, s"${AutoCompact.OP_TYPE}.execute.metrics", data = metric)
        }
        metrics
      }
    }
  }
}

private object GpuAutoCompactCpuFallback extends PostCommitHook {
  override val name: String = AutoCompact.name

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    GpuDeltaCpuFallback.withRapidsDisabled(spark) {
      AutoCompact.run(spark, txn)
    }
  }
}
