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

import java.net.URI

import scala.collection.mutable.ListBuffer

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.actions.{AddFile, FileAction}
import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.databricks.sql.transaction.tahoe.constraints.{Constraint, Constraints}
import com.databricks.sql.transaction.tahoe.schema.InvariantViolationException
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker, GpuFileFormatWriter, GpuWriteJobStatsTracker}
import org.apache.spark.sql.rapids.delta.GpuIdentityColumn
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{Clock, SerializableConfiguration}

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the DeltaLog, MUST go through this instance rather
 * than directly to the DeltaLog otherwise they will not be check for logical conflicts
 * with concurrent updates.
 *
 * This class is not thread-safe.
 *
 * @param deltaLog The Delta Log for the table this transaction is modifying.
 * @param catalogTable catalog table for commit routing.
 * @param snapshot The snapshot that this transaction is reading at.
 * @param rapidsConf RAPIDS Accelerator config settings.
 */
abstract class GpuOptimisticTransactionWriteBase(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)(implicit clock: Clock)
    extends GpuOptimisticTransactionBase(deltaLog, catalogTable, snapshot, rapidsConf)(clock) {

  /** Creates a new OptimisticTransaction.
   *
   * @param deltaLog   The Delta Log for the table this transaction is modifying.
   * @param rapidsConf RAPIDS Accelerator config settings
   */
  def this(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], snapshot, rapidsConf)
  }

  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], deltaLog.update(), rapidsConf)
  }

  protected def getGpuWriteCommitter(
      outputPath: Path,
      keepPartitionIdTag: Boolean,
      forcePreserveInputOrder: Boolean) = {
    getCommitter(outputPath)
  }

  protected def writeUsingGpuFileFormatWriter(
      sparkSession: TrampolineConnectShims.SparkSession,
      plan: SparkPlan,
      fileFormat: ColumnarFileFormat,
      committer: FileCommitProtocol,
      outputSpec: FileFormatWriter.OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      options: Map[String, String]): Unit = {
    GpuFileFormatWriter.write(
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

  protected def normalizeGpuStatsColExpr(expr: Expression): Expression = expr

  protected def handlePostWriteAutoCompact(
      spark: SparkSession,
      isOptimize: Boolean,
      fileActions: Seq[FileAction]): Unit = {}

  private def getGpuStatsColExpr(
      statsDataSchema: Seq[Attribute],
      statsCollection: GpuStatisticsCollection): Expression = {
    val classicSpark = TrampolineConnectShims.getActiveSession
    val expr = TrampolineConnectShims.createDataFrame(classicSpark, LocalRelation(statsDataSchema))
        .select(to_json(statsCollection.statsCollector))
        .queryExecution.analyzed.expressions.head
    normalizeGpuStatsColExpr(expr)
  }

  /** Return the pair of optional stats tracker and stats collection class */
  private def getOptionalGpuStatsTrackerAndStatsCollection(
      output: Seq[Attribute],
      partitionSchema: StructType, data: DataFrame): (
      Option[GpuDeltaJobStatisticsTracker],
          Option[GpuStatisticsCollection]) = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {

      val (statsDataSchema, statsCollectionSchema) = getStatsSchema(output, partitionSchema)

      val indexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)
      val prefixLength =
        spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
      val tableSchema = {
        // If collecting stats using the table schema, then pass in statsCollectionSchema.
        // Otherwise pass in statsDataSchema to collect stats using the DataFrame schema.
        if (spark.sessionState.conf.getConf(DeltaSQLConf
            .DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)) {
          statsCollectionSchema.toStructType
        } else {
          statsDataSchema.toStructType
        }
      }

      val _spark = spark
      val statsCollection = new GpuStatisticsCollection {
        override val spark = _spark
        override val deletionVectorsSupported: Boolean =
          DeletionVectorUtils.deletionVectorsWritable(snapshot, newProtocol, newMetadata)
        override val tableDataSchema = tableSchema
        override val dataSchema = statsDataSchema.toStructType
        override val numIndexedCols = indexedCols
        override val stringPrefixLength: Int = prefixLength
      }

      val statsColExpr = getGpuStatsColExpr(statsDataSchema, statsCollection)

      val statsSchema = statsCollection.statCollectionSchema
      val explodedDataSchema = statsCollection.explodedDataSchema
      val batchStatsToRow = (batch: ColumnarBatch, row: InternalRow) => {
        GpuStatisticsCollection.batchStatsToRow(statsSchema, explodedDataSchema, batch, row)
      }
      (Some(new GpuDeltaJobStatisticsTracker(statsDataSchema, statsColExpr, batchStatsToRow)),
          Some(statsCollection))
    } else {
      (None, None)
    }
  }

  protected def gpuWriteFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint],
      isOptimizeOverride: Option[Boolean],
      keepPartitionIdTag: Boolean,
      forcePreserveInputOrder: Boolean,
      context: Option[String]): (Seq[FileAction], QueryExecution) = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (normalizedQueryExecution, output, generatedColumnConstraints, dataHighWaterMarks) = {
      // The path-style/V1 write entry point has no CatalogTable handle to pass here.
      // DB-14.3 uses the same normalizeData(deltaLog, None, data) pattern.
      normalizeData(deltaLog, None, data)
    }
    val highWaterMarks = trackHighWaterMarks.getOrElse(dataHighWaterMarks)

    // Build a new plan with a stub GpuDeltaWrite node to work around undesired transitions between
    // columns and rows when AQE is involved. Without this node in the plan, AdaptiveSparkPlanExec
    // could be the root node of the plan. In that case we do not have enough context to know
    // whether the AdaptiveSparkPlanExec should be columnar or not, since the GPU overrides do not
    // see how the parent is using the AdaptiveSparkPlanExec outputs. By using this stub node that
    // appears to be a data writing node to AQE (it derives from V2CommandExec), the
    // AdaptiveSparkPlanExec will be planned as a child of this new node. That provides enough
    // context to plan the AQE sub-plan properly with respect to columnar and row transitions.
    // We could force the AQE node to be columnar here by explicitly replacing the node, but that
    // breaks the connection between the queryExecution and the node that will actually execute.
    val gpuWritePlan = TrampolineConnectShims.createDataFrame(
      spark.asInstanceOf[TrampolineConnectShims.SparkSession],
      RapidsDeltaWrite(normalizedQueryExecution.logical))
    val queryExecution = gpuWritePlan.queryExecution

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getGpuWriteCommitter(
      outputPath,
      keepPartitionIdTag,
      forcePreserveInputOrder)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val (optionalStatsTracker, _) = getOptionalGpuStatsTrackerAndStatsCollection(output,
      partitionSchema, data)

    // schema should be normalized, therefore we can do an equality check
    val (statsDataSchema, _) = getStatsSchema(output, partitionSchema)
    val identityTracker = GpuIdentityColumn.createIdentityColumnStatsTracker(
      spark,
      statsDataSchema,
      metadata.schema,
      highWaterMarks)

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    val isOptimize = isOptimizeOverride.getOrElse(isOptimizeCommand(queryExecution.analyzed))

    SQLExecution.withNewExecutionId(
        queryExecution,
        context.orElse(Option("deltaTransactionalWrite"))) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      // Remove any unnecessary row conversions added as part of Spark planning
      val queryPhysicalPlan = queryExecution.executedPlan match {
        case GpuColumnarToRowExec(child, _) => child
        case p => p
      }
      val gpuRapidsWrite = queryPhysicalPlan match {
        case g: GpuRapidsDeltaWriteExec => Some(g)
        case _ => None
      }

      val empty2NullPlan = convertEmptyToNullIfNeeded(queryPhysicalPlan,
        partitioningColumns, constraints)
      val optimizedPlan =
        applyOptimizeWriteIfNeeded(spark, empty2NullPlan, partitionSchema, isOptimize, writeOptions)
      val planWithInvariants = addInvariantChecks(optimizedPlan, constraints)
      val physicalPlan = convertToGpu(planWithInvariants)

      val statsTrackers: ListBuffer[ColumnarWriteJobStatsTracker] = ListBuffer()

      val hadoopConf = spark.sessionState.newHadoopConfWithOptions(
        metadata.configuration ++ deltaLog.options)

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
        val basicWriteJobStatsTracker = new BasicColumnarWriteJobStatsTracker(
          serializableHadoopConf,
          GpuMetric.wrap(BasicWriteJobStatsTracker.metrics))
        registerSQLMetrics(spark, GpuMetric.unwrap(basicWriteJobStatsTracker.driverSideMetrics))
        statsTrackers.append(basicWriteJobStatsTracker)
        gpuRapidsWrite.foreach { grw =>
          val tracker = new GpuWriteJobStatsTracker(serializableHadoopConf,
            grw.basicMetrics, grw.taskMetrics)
          statsTrackers.append(tracker)
        }
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filter { case (key, _) =>
            key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
                key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }
      }
      val deltaFileFormat = deltaLog.fileFormat(deltaLog.unsafeVolatileSnapshot.protocol, metadata)
      val gpuFileFormat = if (deltaFileFormat.getClass == classOf[DeltaParquetFileFormat]) {
        new GpuParquetFileFormat
      } else {
        throw new IllegalStateException(s"file format $deltaFileFormat is not supported")
      }

      try {
        logDebug(s"Physical plan for write:\n$physicalPlan")
        writeUsingGpuFileFormatWriter(
          sparkSession = spark.asInstanceOf[TrampolineConnectShims.SparkSession],
          plan = physicalPlan,
          fileFormat = gpuFileFormat,
          committer = committer,
          outputSpec = outputSpec,
          hadoopConf = hadoopConf,
          partitionColumns = partitioningColumns,
          statsTrackers = optionalStatsTracker.toSeq ++ identityTracker.toSeq ++ statsTrackers,
          options = options)
      } catch {
        case s: SparkException =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      }
    }

    val resultFiles = committer.addedStatuses.map { a =>
      a.copy(stats = optionalStatsTracker.map(
        _.recordedStats(new Path(new URI(a.path)).getName)).getOrElse(a.stats))
    }.filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    }

    identityTracker.foreach { tracker =>
      updatedIdentityHighWaterMarks.appendAll(tracker.highWaterMarks.toSeq)
    }
    val fileActions = resultFiles.toSeq ++ committer.changeFiles
    handlePostWriteAutoCompact(spark, isOptimize, fileActions)

    (fileActions, queryExecution)
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    val (fileActions, _) =
      gpuWriteFiles(inputData, writeOptions, additionalConstraints, None, false, false, None)
    fileActions
  }
}
