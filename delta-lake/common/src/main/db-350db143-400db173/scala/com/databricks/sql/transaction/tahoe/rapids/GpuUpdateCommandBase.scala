/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOperations, DeltaTableUtils, DeltaUDF, OptimisticTransaction, RowTracking}
import com.databricks.sql.transaction.tahoe.DeltaCommitTag._
import com.databricks.sql.transaction.tahoe.actions.{AddCDCFile, AddFile, FileAction}
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, DeltaCommand, DMLUtils, UpdateCommand, UpdateMetric}
import com.databricks.sql.transaction.tahoe.files.{TahoeBatchFileIndex, TahoeFileIndex}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.delta.GpuDeltaMetricUpdateUDF
import com.nvidia.spark.rapids.delta.shims.UpdateCommandShims
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.types.LongType

abstract class GpuUpdateCommandBase(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
    extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("num_affected_rows", LongType)())
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
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
    "numDeletionVectorsAdded" -> createMetric(sc, "number of deletion vectors added."),
    "numDeletionVectorsRemoved" -> createMetric(sc, "number of deletion vectors removed."),
    "numDeletionVectorsUpdated" -> createMetric(sc, "number of deletion vectors updated.")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      gpuDeltaLog.withNewTransaction { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        performUpdate(sparkSession, deltaLog, txn)
      }
      recacheByPlan(sparkSession, target)
    }
    Seq(Row(metrics("numUpdatedRows").value))
  }

  private def performUpdate(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction): Unit = {
    import com.databricks.sql.transaction.tahoe.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var numAddedChangeFiles: Long = 0
    var changeFileBytes: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)
    val shouldWriteDVs = shouldWritePersistentDeletionVectors(txn)
    if (shouldWriteDVs) {
      // This should be unreachable because the meta shim falls back to CPU if deletion vector
      // writes are enabled.
      throw new IllegalStateException("Deletion vectors are not supported on GPU")
    }

    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates, true)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

    val filesToRewrite: Seq[AddFile] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition.
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty.
      candidateFiles
    } else {
      // Case 3: Find all the affected files using the user-specified condition.
      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)
      // Keep everything from the resolved target except a new TahoeFileIndex that only involves
      // the affected files instead of all files.
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

      scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

      pathsToRewrite.map(getTouchedFile(deltaLog.dataPath, _, nameToAddFile)).toSeq
    }

    numTouchedFiles = filesToRewrite.length

    val newActions = if (filesToRewrite.isEmpty) {
      // Do nothing if no row qualifies the UPDATE condition.
      Nil
    } else {
      // Generate the new files containing the updated values.
      withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(filesToRewrite.size)) {
        rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
          filesToRewrite.map(_.path), nameToAddFile, updateCondition)
      }
    }

    rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

    val (changeActions, addActions) = newActions.partition(_.isInstanceOf[AddCDCFile])
    numRewrittenFiles = addActions.size
    numAddedChangeFiles = changeActions.size
    changeFileBytes = changeActions.collect { case f: AddCDCFile => f.size }.sum

    val totalActions = if (filesToRewrite.isEmpty) {
      // Do nothing if no row qualifies the UPDATE condition.
      Nil
    } else {
      // Delete the old files and return those delete actions along with the new AddFile actions for
      // files containing the updated values.
      val operationTimestamp = System.currentTimeMillis()
      val deleteActions = filesToRewrite.map(_.removeWithTimestamp(operationTimestamp))

      deleteActions ++ newActions
    }

    if (totalActions.nonEmpty) {
      metrics("numAddedFiles").set(numRewrittenFiles)
      metrics("numAddedChangeFiles").set(numAddedChangeFiles)
      metrics("changeFileBytes").set(changeFileBytes)
      metrics("numRemovedFiles").set(numTouchedFiles)
      metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
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
        // So numTouchedRows == numUpdateRows.
        metrics("numUpdatedRows").set(metrics("numTouchedRows").value)
      } else {
        // This is for case 3 where the update condition contains both metadata and data predicates
        // so relevant files will have some rows updated and some rows copied. We don't need to
        // consider case 1 here, where no files match the update condition, as we know that
        // `totalActions` is empty.
        metrics("numCopiedRows").set(
          metrics("numTouchedRows").value - metrics("numUpdatedRows").value)
      }
      metrics("numDeletionVectorsAdded").set(0)
      metrics("numDeletionVectorsRemoved").set(0)
      metrics("numDeletionVectorsUpdated").set(0)
      txn.registerSQLMetrics(sparkSession, metrics)
      val tags = DMLUtils.TaggedCommitData(Seq.empty[FileAction])
        .withTag(PreservedRowTrackingTag, RowTracking.isEnabled(txn.protocol, txn.metadata))
        .withTag(NoRowsCopiedTag, metrics("numCopiedRows").value == 0)
      txn.commitIfNeeded(totalActions, DeltaOperations.Update(condition), tags.stringTags)
      // This is needed to make the SQL metrics visible in the Spark UI.
      val executionId = sparkSession.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sparkSession.sparkContext, executionId, metrics.values.toSeq)
    }

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
        numDeletionVectorsAdded = 0,
        numDeletionVectorsRemoved = 0,
        numDeletionVectorsUpdated = 0)
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
      spark: SparkSession,
      txn: OptimisticTransaction,
      rootPath: Path,
      inputLeafFiles: Seq[String],
      nameToAddFileMap: Map[String, AddFile],
      condition: Expression): Seq[FileAction] = {
    // Containing the map from the relative file path to AddFile.
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles, nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val (targetDf, finalOutput, finalUpdateExpressions) =
      UpdateCommandShims.preserveRowTrackingColumns(
        createDataFrame(spark, newTarget), txn.snapshot, target.output, updateExpressions)

    // Number of total rows that we have seen, i.e. are either copying or updating (sum of both).
    // This will be used later, along with numUpdatedRows, to determine numCopiedRows.
    val numTouchedRows = metrics("numTouchedRows")
    val numTouchedRowsUdf = DeltaUDF.boolean {
      new GpuDeltaMetricUpdateUDF(numTouchedRows)
    }.asNondeterministic()

    val updatedDataFrame = UpdateCommand.withUpdatedColumns(
      finalOutput,
      finalUpdateExpressions,
      condition,
      targetDf
          .filter(numTouchedRowsUdf())
          .withColumn(UpdateCommand.CONDITION_COLUMN_NAME, exprToColumn(condition)),
      UpdateCommand.shouldOutputCdc(txn))

    txn.writeFiles(updatedDataFrame)
  }

  private def shouldWritePersistentDeletionVectors(txn: OptimisticTransaction): Boolean = {
    conf.getConf(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }

  private def createDataFrame(sparkSession: SparkSession, plan: LogicalPlan): DataFrame = {
    TrampolineConnectShims.createDataFrame(
      sparkSession.asInstanceOf[TrampolineConnectShims.SparkSession], plan)
  }

  private def exprToColumn(expr: Expression): Column = DFUDFShims.exprToColumn(expr)

  private def recacheByPlan(sparkSession: SparkSession, plan: LogicalPlan): Unit = {
    val classic = sparkSession.asInstanceOf[TrampolineConnectShims.SparkSession]
    classic.sharedState.cacheManager.recacheByPlan(classic, plan)
  }
}
