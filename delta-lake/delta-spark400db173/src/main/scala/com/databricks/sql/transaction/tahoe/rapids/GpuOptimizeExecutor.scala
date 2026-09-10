/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from:
 *  1. DoAutoCompaction.scala from PR#1156 at https://github.com/delta-io/delta/pull/1156,
 *  2. OptimizeTableCommand.scala from the Delta Lake project at https://github.com/delta-io/delta.
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

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.DeltaCommitTag.PreservedRowTrackingTag
import com.databricks.sql.transaction.tahoe.DeltaOperations.Operation
import com.databricks.sql.transaction.tahoe.actions.{Action, AddFile, FileAction, RemoveFile}
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, DeltaOptimizeContext}
import com.databricks.sql.transaction.tahoe.commands.optimize.OptimizeStats
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Clock

class GpuOptimizeExecutor(
    sparkSession: SparkSession,
    snapshot: Snapshot,
    catalogTable: Option[CatalogTable],
    partitionPredicate: Seq[Expression],
    optimizeContext: DeltaOptimizeContext,
    isAutoCompact: Boolean = false)
  extends GpuOptimizeExecutorBase(sparkSession, partitionPredicate) {

  override protected val deltaLog: DeltaLog = snapshot.deltaLog
  private val rapidsConf = new RapidsConf(sparkSession.sessionState.conf)
  private implicit val clock: Clock = deltaLog.clock

  private def ensureDeletionVectorsUnsupported(): Unit = {
    if (DeletionVectorUtils.deletionVectorsWritable(snapshot) ||
        !DeletionVectorUtils.isTableDVFree(snapshot)) {
      throw new IllegalStateException(
        "Delta OPTIMIZE on tables with deletion vectors is not supported on GPU")
    }
  }

  ensureDeletionVectorsUnsupported()

  override protected def createTransaction(): OptimisticTransaction =
    new GpuOptimisticTransaction(deltaLog, catalogTable, snapshot, rapidsConf)

  override protected def beforeOptimize(txn: OptimisticTransaction): Unit =
    DeltaLog.assertRemovable(txn.snapshot)

  override protected def getMinFileSize: Long = {
    optimizeContext.minFileSize.getOrElse(
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE))
  }

  override protected def getMaxFileSize: Long = {
    optimizeContext.maxFileSize.getOrElse(
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE))
  }

  override protected def optimizeOperation: Operation =
    DeltaOperations.Optimize(partitionPredicate, Nil, auto = isAutoCompact)

  override protected def getCandidateFiles(txn: OptimisticTransaction): Seq[AddFile] =
    txn.filterFiles(partitionPredicate, true)

  override protected def prepareInput(
      txn: OptimisticTransaction,
      bin: Seq[AddFile]): DataFrame = {
    RowTracking.preserveRowTrackingColumns(
      txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize")),
      txn.snapshot)
  }

  override protected def writeOptimizeOutput(
      txn: OptimisticTransaction,
      repartitionDF: DataFrame): Seq[FileAction] = {
    txn.writeFiles(repartitionDF, None, isOptimize = true, Nil)
  }

  override protected def commitTransaction(
      txn: OptimisticTransaction,
      optimizeOperation: Operation,
      actions: Seq[Action]): Unit = {
    txn.commit(actions, optimizeOperation, getCommitTags(txn))
  }

  override protected def startRetryTransaction(txn: OptimisticTransaction): OptimisticTransaction =
    txn.deltaLog.startTransaction(catalogTable)

  override protected def extraMetrics(
      sparkContext: SparkContext,
      setAndReturnMetric: (String, Long) => SQLMetric): Map[String, SQLMetric] = {
    Map(
      "numDeletionVectorsRemoved" -> setAndReturnMetric(
        "number of deletion vectors removed", 0L))
  }

  override protected def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      jobs: Seq[PartitionedBin],
      candidateFiles: Seq[AddFile],
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile],
      committedTxn: OptimisticTransaction): Unit = {
    optimizeStats.numBins = jobs.size
    optimizeStats.totalScheduledTasks = jobs.size
    val committedMetadata = committedTxn.metadata
    val numTableColumns = committedMetadata.schema.size.toLong
    optimizeStats.numTableColumns = numTableColumns
    optimizeStats.numTableColumnsWithStats = Math.min(
      DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(committedMetadata).toLong,
      numTableColumns)
  }

  private def getCommitTags(txn: OptimisticTransaction): Map[String, String] = {
    val tags = RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot, Map.empty)
    val notPreservedTag = PreservedRowTrackingTag.withValue(false).stringPair
    if (tags.contains(notPreservedTag._1)) {
      tags
    } else {
      tags + notPreservedTag
    }
  }
}
