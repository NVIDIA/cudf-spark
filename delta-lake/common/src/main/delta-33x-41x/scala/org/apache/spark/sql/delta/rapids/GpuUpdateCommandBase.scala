/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * This file was derived from UpdateCommand.scala
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

package org.apache.spark.sql.delta.rapids

import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.delta.GpuDeltaMetricUpdateUDF
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTableUtils, DeltaUDF, NumRecordsStats, RowTracking}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.{DeletionVectorUtils, DeltaCommand, TouchedFileWithDV, UpdateCommand, UpdateMetric}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.LongType

/**
 * Base class for GPU version of Delta UpdateCommand.
 * Version-specific API differences are handled by DeltaCommandShims trait.
 */
abstract class GpuUpdateCommandBase(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    catalogTable: Option[CatalogTable],
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
    extends LeafRunnableCommand
    with DeltaCommand
    with DeltaCommandShims {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("num_affected_rows", LongType)())
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numAddedBytes" -> createMetric(sc, "number of bytes added"),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed"),
    "numUpdatedRows" -> createMetric(sc, "number of rows updated."),
    "numCopiedRows" -> createMetric(sc, "number of rows copied."),
    "executionTimeMs" ->
        createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
        createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
        createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated"),
    "numTouchedRows" -> createMetric(sc, "number of rows touched (copied + updated)"),
    "numDeletionVectorsAdded" -> createMetric(sc, "number of deletion vectors added"),
    "numDeletionVectorsRemoved" -> createMetric(sc, "number of deletion vectors removed"),
    "numDeletionVectorsUpdated" -> createMetric(sc, "number of deletion vectors updated")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      gpuDeltaLog.withNewTransaction(catalogTable) { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        if (hasBeenExecuted(txn, sparkSession.asInstanceOf[ShimSparkSession])) {
          sendDriverMetrics(sparkSession, metrics)
          return Seq.empty
        }
        val opSpark = toOperationSparkSession(sparkSession.asInstanceOf[ShimSparkSession])
        performUpdate(opSpark, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      recacheByPlan(sparkSession.asInstanceOf[ShimSparkSession], target)
    }
    Seq(Row(metrics("numUpdatedRows").value))
  }

  private def performUpdate(
      sparkSession: OperationSparkSession,
      deltaLog: DeltaLog,
      txn: GpuOptimisticTransactionBase): Unit = {
    import org.apache.spark.sql.delta.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var numAddedBytes: Long = 0
    var numRemovedBytes: Long = 0
    var numAddedChangeFiles: Long = 0
    var changeFileBytes: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    // Deletion vector not supported yet
    val numDeletionVectorsAdded: Long = 0
    val numDeletionVectorsRemoved: Long = 0
    val numDeletionVectorsUpdated: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)

    // Should we write the DVs to represent updated rows?
    val shouldWriteDeletionVectors = shouldWritePersistentDeletionVectors(sparkSession, txn)
    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)

    val filesToRewrite: Seq[TouchedFileWithDV] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      candidateFiles
        .map(f => TouchedFileWithDV(f.path, f, newDeletionVector = null, deletedRows = 0L))
    } else {
      // Case 3: Find all the affected files using the user-specified condition
      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)

      val touchedFilesWithDV = if (shouldWriteDeletionVectors) {
        // this should be unreachable because we fall back to CPU
        // if deletion vectors are enabled. The tracking issue for adding deletion vector
        // support is https://github.com/NVIDIA/spark-rapids/issues/8554
        throw new IllegalStateException("Deletion vectors are not supported on GPU")
      } else {
        // Case 3.2: Find all the affected files using the non-DV path
        // Keep everything from the resolved target except a new TahoeFileIndex
        // that only involves the affected files instead of all files.
        val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
        val data = createDataFrame(sparkSession, newTarget)
        val updatedRowCount = metrics("numUpdatedRows")
        val updatedRowUdf = DeltaUDF.boolean {
          new GpuDeltaMetricUpdateUDF(updatedRowCount)
        }.asNondeterministic()
        val pathsToRewrite =
          withStatusCode("DELTA", UpdateCommand.FINDING_TOUCHED_FILES_MSG) {
            data.filter(exprToColumn(updateCondition))
              .select(input_file_name())
              .filter(updatedRowUdf())
              .distinct()
              .as[String]
              .collect()
          }

        // Wrap AddFile into TouchedFileWithDV that has empty DV.
        pathsToRewrite
          .map(getTouchedFile(deltaLog.dataPath, _, nameToAddFile))
          .map(f => TouchedFileWithDV(f.path, f, newDeletionVector = null, deletedRows = 0L))
          .toSeq
      }
      // Refresh scan time for Case 3, since we performed scan here.
      scanTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
      touchedFilesWithDV
    }

    val totalActions = {
      // When DV is on, we first mask removed rows with DVs and generate (remove, add) pairs.
      val actionsForExistingFiles = if (shouldWriteDeletionVectors) {
        // this should be unreachable because we fall back to CPU
        // if deletion vectors are enabled. The tracking issue for adding deletion vector
        // support is https://github.com/NVIDIA/spark-rapids/issues/8554
        throw new IllegalStateException("Deletion vectors are not supported on GPU")
      } else {
        // Without DV we'll leave the job to `rewriteFiles`.
        Nil
      }

      // When DV is on, we write out updated rows only. The return value will be only `add` actions.
      // When DV is off, we write out updated rows plus unmodified rows from the same file, then
      // return `add` and `remove` actions.
      val rewriteStartNs = System.nanoTime()
      val actionsForNewFiles =
        withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(filesToRewrite.size)) {
          if (filesToRewrite.nonEmpty) {
            rewriteFiles(
              sparkSession,
              txn,
              rootPath = tahoeFileIndex.path,
              inputLeafFiles = filesToRewrite.map(_.fileLogEntry),
              nameToAddFileMap = nameToAddFile,
              condition = updateCondition,
              generateRemoveFileActions = !shouldWriteDeletionVectors,
              copyUnmodifiedRows = !shouldWriteDeletionVectors)
          } else {
            Nil
          }
        }
      rewriteTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rewriteStartNs)

      numTouchedFiles = filesToRewrite.length
      val (addActions, removeActions) = actionsForNewFiles.partition(_.isInstanceOf[AddFile])
      numRewrittenFiles = addActions.size
      numAddedBytes = addActions.map(_.getFileSize).sum
      numRemovedBytes = removeActions.map(_.getFileSize).sum

      actionsForExistingFiles ++ actionsForNewFiles
    }

    val changeActions = totalActions.collect { case f: AddCDCFile => f }
    numAddedChangeFiles = changeActions.size
    changeFileBytes = changeActions.map(_.size).sum

    metrics("numAddedFiles").set(numRewrittenFiles)
    metrics("numAddedBytes").set(numAddedBytes)
    metrics("numAddedChangeFiles").set(numAddedChangeFiles)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numRemovedFiles").set(numTouchedFiles)
    metrics("numRemovedBytes").set(numRemovedBytes)
    metrics("executionTimeMs").set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    // In the case where the numUpdatedRows is not captured, we can siphon out the metrics from
    // the BasicWriteStatsTracker. This is for case 2 where the update condition contains only
    // metadata predicates and so the entire partition is re-written.
    val outputRows = txn.getMetric("numOutputRows").map(_.value).getOrElse(-1L)
    if (metrics("numUpdatedRows").value == 0 && outputRows != 0 &&
      metrics("numCopiedRows").value == 0) {
      // We know that numTouchedRows = numCopiedRows + numUpdatedRows.
      // Since an entire partition was re-written, no rows were copied.
      // So numTouchedRows == numUpdateRows
      metrics("numUpdatedRows").set(metrics("numTouchedRows").value)
    } else {
      // This is for case 3 where the update condition contains both metadata and data predicates
      // so relevant files will have some rows updated and some rows copied. We don't need to
      // consider case 1 here, where no files match the update condition, as we know that
      // `totalActions` is empty.
      metrics("numCopiedRows").set(
        metrics("numTouchedRows").value - metrics("numUpdatedRows").value)
      metrics("numDeletionVectorsAdded").set(numDeletionVectorsAdded)
      metrics("numDeletionVectorsRemoved").set(numDeletionVectorsRemoved)
      metrics("numDeletionVectorsUpdated").set(numDeletionVectorsUpdated)
    }
    txn.registerSQLMetrics(sparkSession, metrics)

    val finalActions = createSetTransaction(sparkSession, deltaLog).toSeq ++ totalActions
    val numRecordsStats = NumRecordsStats.fromActions(finalActions)
    val commitVersion = txn.commitIfNeeded(
      actions = finalActions,
      op = DeltaOperations.Update(condition),
      tags = RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot))
    sendDriverMetrics(sparkSession, metrics)

    recordDeltaEvent(
      deltaLog,
      "delta.dml.update.stats",
      data = UpdateMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        numAddedChangeFiles,
        changeFileBytes,
        scanTimeMs,
        rewriteTimeMs,
        numDeletionVectorsAdded,
        numDeletionVectorsRemoved,
        numDeletionVectorsUpdated,
        commitVersion = commitVersion,
        numLogicalRecordsAdded = numRecordsStats.numLogicalRecordsAdded,
        numLogicalRecordsRemoved = numRecordsStats.numLogicalRecordsRemoved)
    )
  }

  /**
   * Scan all the affected files and write out the updated files.
   *
   * When CDF is enabled, includes the generation of CDC preimage and postimage columns for
   * changed rows.
   *
   * @return the list of [[AddFile]]s and [[AddCDCFile]]s that have been written.
   */
  private def rewriteFiles(
      spark: OperationSparkSession,
      txn: GpuOptimisticTransactionBase,
      rootPath: Path,
      inputLeafFiles: Seq[AddFile],
      nameToAddFileMap: Map[String, AddFile],
      condition: Expression,
      generateRemoveFileActions: Boolean,
      copyUnmodifiedRows: Boolean): Seq[FileAction] = {

    val touchedRowCount = metrics("numTouchedRows")
    val touchedRowUdf = DeltaUDF.boolean {
      new GpuDeltaMetricUpdateUDF(touchedRowCount)
    }.asNondeterministic()

    // Containing the map from the relative file path to AddFile
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles.map(_.path), nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val (targetDf, finalOutput, finalUpdateExpressions) = UpdateCommand.preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns = createDataFrame(spark, newTarget),
      snapshot = txn.snapshot,
      targetOutput = target.output,
      updateExpressions)

    val targetDfWithEvaluatedCondition = {
      val evalDf = targetDf.withColumn(UpdateCommand.CONDITION_COLUMN_NAME, exprToColumn(condition))
      val copyAndUpdateRowsDf = if (copyUnmodifiedRows) {
        evalDf
      } else {
        import org.apache.spark.sql.functions.col
        evalDf.filter(col(UpdateCommand.CONDITION_COLUMN_NAME))
      }
      copyAndUpdateRowsDf.filter(touchedRowUdf())
    }


    val updatedDataFrame = UpdateCommand.withUpdatedColumns(
      finalOutput,
      finalUpdateExpressions,
      condition,
      targetDfWithEvaluatedCondition,
      UpdateCommand.shouldOutputCdc(txn))

    val addFiles = txn.writeFiles(updatedDataFrame)

    val removeFiles = if (generateRemoveFileActions) {
      val operationTimestamp = System.currentTimeMillis()
      inputLeafFiles.map(_.removeWithTimestamp(operationTimestamp))
    } else {
      Nil
    }

    addFiles ++ removeFiles
  }

  def shouldWritePersistentDeletionVectors(
      spark: OperationSparkSession, txn: GpuOptimisticTransactionBase): Boolean = {
    spark.conf.get(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }
}
