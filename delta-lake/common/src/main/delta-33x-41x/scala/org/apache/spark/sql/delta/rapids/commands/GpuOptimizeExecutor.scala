/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids.commands

import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, FileAction, RemoveFile}
import org.apache.spark.sql.delta.actions.InMemoryLogReplay.UniqueFileActionTuple
import org.apache.spark.sql.delta.commands.{Batch, Bin, ClusteringStrategy, DeletionVectorUtils, DeltaCommand, DeltaOptimizeContext, OptimizeTableStrategy, ZOrderStrategy}
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.files.SQLMetricsReporting
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuOptimisticTransactionBase}
import org.apache.spark.sql.delta.rapids.DeltaMdcShims.mdc
import org.apache.spark.sql.delta.skipping.MultiDimClustering
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.BinPackingUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.util.{SystemClock, ThreadUtils}

/**
 * Optimize job which compacts small files into larger files to reduce
 * the number of files and potentially allow more efficient reads.
 *
 * @param sparkSession Spark environment reference.
 * @param snapshot The snapshot of the table to optimize
 * @param partitionPredicate List of partition predicates to select subset of files to optimize.
 */
class GpuOptimizeExecutor(
    sparkSession: SparkSession,
    snapshot: Snapshot,
    catalogTable: Option[CatalogTable],
    partitionPredicate: Seq[Expression],
    zOrderByColumns: Seq[String],
    isAutoCompact: Boolean,
    optimizeContext: DeltaOptimizeContext)
  extends DeltaCommand with SQLMetricsReporting with Serializable {
  private def ensureDeletionVectorDisabled(): Unit = {
    val dvFeatureEnabled = DeletionVectorUtils.deletionVectorsWritable(snapshot)

    // Currently optimize executor will only be triggered by auto compaction, and we should
    // already fallback to cpu when deletion vector enabled. This check ensures that the fallback
    // actually works.
    if (dvFeatureEnabled) {
      throw new IllegalStateException("Deletion vector not supported in gpu, we should have " +
        "fallback to cpu in GpuOptimizeExecutor")
    }
  }

  ensureDeletionVectorDisabled()

  private val rapidsConf = new RapidsConf(sparkSession.sessionState.conf)

  /**
   * In which mode the Optimize command is running. There are three valid modes:
   * 1. Compaction
   * 2. ZOrder
   * 3. Clustering
   */
  private val optimizeStrategy =
    OptimizeTableStrategy(sparkSession, snapshot, optimizeContext, zOrderByColumns)

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = new SystemClock().getTimeMillis()

  private val isClusteredTable = ClusteredTableUtils.isSupported(snapshot.protocol)

  private val isMultiDimClustering =
    optimizeStrategy.isInstanceOf[ClusteringStrategy] ||
      optimizeStrategy.isInstanceOf[ZOrderStrategy]

  private val clusteringColumns: Seq[String] = {
    if (zOrderByColumns.nonEmpty) {
      zOrderByColumns
    } else if (isClusteredTable) {
      ClusteringColumnInfo.extractLogicalNames(snapshot)
    } else {
      Nil
    }
  }

  private val partitionSchema = snapshot.metadata.partitionSchema

  def optimize(): Seq[Row] = {
    recordDeltaOperation(snapshot.deltaLog, "delta.optimize") {
      val minFileSize = optimizeContext.minFileSize.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE))
      val maxFileSize = optimizeContext.maxFileSize.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE))
      val maxDeletedRowsRatio = optimizeContext.maxDeletedRowsRatio.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO))
      val batchSize = sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_BATCH_SIZE)

      // Get all the files from the snapshot, we will register them with the individual
      // transactions later
      val candidateFiles = snapshot.filesForScan(partitionPredicate, keepNumRecords = true).files

      val filesToProcess = optimizeContext.reorg match {
        case Some(reorgOperation) =>
          reorgOperation.filterFilesToReorg(sparkSession, snapshot, candidateFiles)
        case None =>
          filterCandidateFileList(minFileSize, maxDeletedRowsRatio, candidateFiles)
      }
      val partitionsToCompact = filesToProcess.groupBy(_.partitionValues).toSeq

      val jobs = groupFilesIntoBins(partitionsToCompact)

      val batchResults = batchSize match {
        case Some(size) =>
          val batches = BinPackingUtils.binPackBySize[Bin, Bin](
            jobs,
            bin => bin.files.map(_.size).sum,
            bin => bin,
            size)
          batches.map(batch => runOptimizeBatch(Batch(batch), maxFileSize))
        case None =>
          Seq(runOptimizeBatch(Batch(jobs), maxFileSize))
      }

      val addedFiles = batchResults.map(_._1).flatten
      val removedFiles = batchResults.map(_._2).flatten
      val removedDVs = batchResults.map(_._3).flatten

      val optimizeStats = OptimizeStats()
      optimizeStats.addedFilesSizeStats.merge(addedFiles)
      optimizeStats.removedFilesSizeStats.merge(removedFiles)
      optimizeStats.numPartitionsOptimized = jobs.map(j => j.partitionValues).distinct.size
      optimizeStats.numBins = jobs.size
      optimizeStats.numBatches = batchResults.size
      optimizeStats.totalConsideredFiles = candidateFiles.size
      optimizeStats.totalFilesSkipped = optimizeStats.totalConsideredFiles - removedFiles.size
      optimizeStats.totalClusterParallelism = sparkSession.sparkContext.defaultParallelism
      val numTableColumns = snapshot.metadata.schema.size
      optimizeStats.numTableColumns = numTableColumns
      optimizeStats.numTableColumnsWithStats =
        DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(snapshot.metadata)
          .min(numTableColumns)
      if (removedDVs.size > 0) {
        optimizeStats.deletionVectorStats = Some(DeletionVectorStats(
          numDeletionVectorsRemoved = removedDVs.size,
          numDeletionVectorRowsRemoved = removedDVs.map(_.cardinality).sum))
      }

      optimizeStrategy.updateOptimizeStats(optimizeStats, removedFiles, jobs)

      return Seq(Row(snapshot.deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
    }
  }

  /**
   * Helper method to prune the list of selected files based on fileSize and ratio of
   * deleted rows according to the deletion vector in [[AddFile]].
   */
  private def filterCandidateFileList(
      minFileSize: Long, maxDeletedRowsRatio: Double, files: Seq[AddFile]): Seq[AddFile] = {

    // Select all files in case of multi-dimensional clustering
    if (isMultiDimClustering) return files

    def shouldCompactBecauseOfDeletedRows(file: AddFile): Boolean = {
      // Always compact files with DVs but without numRecords stats.
      // This may be overly aggressive, but it fixes the problem in the long-term,
      // as the compacted files will have stats.
      (file.deletionVector != null && file.numPhysicalRecords.isEmpty) ||
        file.deletedToPhysicalRecordsRatio.getOrElse(0d) > maxDeletedRowsRatio
    }

    // Select files that are small or have too many deleted rows
    files.filter(
      addFile => addFile.size < minFileSize || shouldCompactBecauseOfDeletedRows(addFile))
  }

  /**
   * Utility methods to group files into bins for optimize.
   *
   * @param partitionsToCompact List of files to compact group by partition.
   *                            Partition is defined by the partition values (partCol -> partValue)
   * @return Sequence of bins. Each bin contains one or more files from the same
   *         partition and targeted for one output file.
   */
  private def groupFilesIntoBins(
      partitionsToCompact: Seq[(Map[String, String], Seq[AddFile])])
  : Seq[Bin] = {
    val maxBinSize = optimizeStrategy.maxBinSize
    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        val preparedFiles = optimizeStrategy.prepareFilesPerPartition(files)
        preparedFiles.foreach { file =>
          // Generally, a bin is a group of existing files, whose total size does not exceed the
          // desired maxBinSize. The output file size depends on the mode:
          // 1. Compaction: Files in a bin will be coalesced into a single output file.
          // 2. ZOrder:  all files in a partition will be read by the
          //    same job, the data will be range-partitioned and
          //    numFiles = totalFileSize / maxFileSize will be produced.
          // 3. Clustering: Files in a bin belongs to one ZCUBE, the data will be
          //    range-partitioned and numFiles = totalFileSize / maxFileSize.
          if (file.size + currentBinSize > maxBinSize) {
            bins += currentBin.toVector
            currentBin.clear()
            currentBin += file
            currentBinSize = file.size
          } else {
            currentBin += file
            currentBinSize += file.size
          }
        }

        if (currentBin.nonEmpty) {
          bins += currentBin.toVector
        }

        bins.filter { bin =>
          bin.size > 1 || // bin has more than one file or
            bin.size == 1 && optimizeContext.reorg.nonEmpty || // always rewrite files during reorg
            isMultiDimClustering // multi-clustering
        }.map(b => Bin(partition, b))
    }
  }

  private def runOptimizeBatch(
      batch: Batch,
      maxFileSize: Long
  ): (Seq[AddFile], Seq[RemoveFile], Seq[DeletionVectorDescriptor]) = {
    // transaction should already be gpu.
    val txn = new GpuDeltaLog(snapshot.deltaLog, rapidsConf)
      .startTransaction(catalogTable, Some(snapshot))

    val filesToProcess = batch.bins.flatMap(_.files)

    txn.trackFilesRead(filesToProcess)
    txn.trackReadPredicates(partitionPredicate)

    val maxThreads =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS)
    val updates = ThreadUtils.parmap(batch.bins, "OptimizeJob", maxThreads) { partitionBinGroup =>
      runOptimizeBinJob(txn, partitionBinGroup.partitionValues, partitionBinGroup.files,
        maxFileSize)
    }.flatten

    val addedFiles = updates.collect { case a: AddFile => a }
    val removedFiles = updates.collect { case r: RemoveFile => r }
    val removedDVs = filesToProcess.filter(_.deletionVector != null).map(_.deletionVector).toSeq
    if (addedFiles.size > 0) {
      val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles, removedDVs)
      commitAndRetry(txn, getOperation(), updates, metrics) { newTxn =>
        val newPartitionSchema = newTxn.metadata.partitionSchema
        // Note: When checking if the candidate set is the same, we need to consider (Path, DV)
        //       as the key.
        val candidateSetOld = filesToProcess.
          map(f => UniqueFileActionTuple(f.pathAsUri, f.getDeletionVectorUniqueId)).toSet

        // We specifically don't list the files through the transaction since we are potentially
        // only processing a subset of them below. If the transaction is still valid, we will
        // register the files and predicate below
        val candidateSetNew =
          newTxn.snapshot.filesForScan(partitionPredicate).files
            .map(f => UniqueFileActionTuple(f.pathAsUri, f.getDeletionVectorUniqueId)).toSet

        // As long as all of the files that we compacted are still part of the table,
        // and the partitioning has not changed it is valid to continue to try
        // and commit this checkpoint.
        if (candidateSetOld.subsetOf(candidateSetNew) && partitionSchema == newPartitionSchema) {
          // Make sure the files we are processing are registered with the transaction
          newTxn.trackFilesRead(filesToProcess)
          newTxn.trackReadPredicates(partitionPredicate)
          true
        } else {
          val deleted = candidateSetOld -- candidateSetNew
          logWarning(log"The following compacted files were deleted " +
            log"during checkpoint ${mdc(DeltaLogKeys.PATHS, deleted.mkString(","))}. " +
            log"Aborting the compaction.")
          false
        }
      }
    }
    (addedFiles, removedFiles, removedDVs)
  }

  /**
   * Utility method to run a Spark job to compact the files in given bin
   *
   * @param txn [[OptimisticTransaction]] instance in use to commit the changes to DeltaLog.
   * @param partition Partition values of the partition that files in [[bin]] belongs to.
   * @param bin List of files to compact into one large file.
   * @param maxFileSize Targeted output file size in bytes
   */
  private def runOptimizeBinJob(
      txn: GpuOptimisticTransactionBase,
      partition: Map[String, String],
      bin: Seq[AddFile],
      maxFileSize: Long): Seq[FileAction] = {
    val baseTablePath = txn.deltaLog.dataPath

    var input = txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))
    input = RowTracking.preserveRowTrackingColumns(input, txn.snapshot)
    val repartitionDF = if (isMultiDimClustering) {
      val totalSize = bin.map(_.size).sum
      val approxNumFiles = Math.max(1, totalSize / maxFileSize).toInt
      MultiDimClustering.cluster(
        input,
        approxNumFiles,
        clusteringColumns,
        optimizeStrategy.curve)
    } else {
      val useRepartition = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED)
      if (useRepartition) {
        input.repartition(numPartitions = 1)
      } else {
        input.coalesce(numPartitions = 1)
      }
    }

    val partitionDesc = partition.toSeq.map(entry => entry._1 + "=" + entry._2).mkString(",")

    val partitionName = if (partition.isEmpty) "" else s" in partition ($partitionDesc)"
    val description = s"$baseTablePath<br/>Optimizing ${bin.size} files" + partitionName
    sparkSession.sparkContext.setJobGroup(
      sparkSession.sparkContext.getLocalProperty(SPARK_JOB_GROUP_ID),
      description)

    val binInfo = optimizeStrategy.initNewBin
    val addFiles = txn.writeFiles(repartitionDF, None, isOptimize = true, Nil).collect {
      case a: AddFile => optimizeStrategy.tagAddFile(a, binInfo)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass}. File compaction job output" +
            s"should only have AddFiles")
    }
    val removeFiles = bin.map(f => f.removeWithTimestamp(operationTimestamp, dataChange = false))
    val updates = addFiles ++ removeFiles
    updates
  }

  /**
   * Attempts to commit the given actions to the log. In the case of a concurrent update,
   * the given function will be invoked with a new transaction to allow custom conflict
   * detection logic to indicate it is safe to try again, by returning `true`.
   *
   * This function will continue to try to commit to the log as long as `f` returns `true`,
   * otherwise throws a subclass of [[ConcurrentModificationException]].
   */
  private def commitAndRetry(
      txn: GpuOptimisticTransactionBase,
      optimizeOperation: Operation,
      actions: Seq[Action],
      metrics: Map[String, SQLMetric])(f: OptimisticTransaction => Boolean): Unit = {
    try {
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(actions, optimizeOperation,
        RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot))
    } catch {
      case e: ConcurrentModificationException =>
        val newTxn = new GpuDeltaLog(txn.deltaLog, rapidsConf).startTransaction(txn.catalogTable,
          snapshotOpt = None)
        if (f(newTxn)) {
          logInfo(
            log"Retrying commit after checking for semantic conflicts with concurrent updates.")
          commitAndRetry(newTxn, optimizeOperation, actions, metrics)(f)
        } else {
          logWarning(log"Semantic conflicts detected. Aborting operation.")
          throw e
        }
    }
  }

  /** Create the appropriate [[Operation]] object for txn commit history */
  private def getOperation(): Operation = {
    if (optimizeContext.reorg.nonEmpty) {
      DeltaOperations.Reorg(partitionPredicate)
    } else {
      DeltaOperations.Optimize(
        predicate = partitionPredicate,
        zOrderBy = zOrderByColumns,
        auto = isAutoCompact,
        clusterBy = if (isClusteredTable) Option(clusteringColumns).filter(_.nonEmpty) else None,
        isFull = optimizeContext.isFull)
    }
  }

  /** Create a map of SQL metrics for adding to the commit history. */
  private def createMetrics(
      sparkContext: SparkContext,
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile],
      removedDVs: Seq[DeletionVectorDescriptor]): Map[String, SQLMetric] = {

    def setAndReturnMetric(description: String, value: Long) = {
      val metric = createMetric(sparkContext, description)
      metric.set(value)
      metric
    }

    def totalSize(actions: Seq[FileAction]): Long = {
      var totalSize = 0L
      actions.foreach { file =>
        val fileSize = file match {
          case addFile: AddFile => addFile.size
          case removeFile: RemoveFile => removeFile.size.getOrElse(0L)
          case default =>
            throw new IllegalArgumentException(s"Unknown FileAction type: ${default.getClass}")
        }
        totalSize += fileSize
      }
      totalSize
    }

    val (deletionVectorRowsRemoved, deletionVectorBytesRemoved) =
      removedDVs.map(dv => (dv.cardinality, dv.sizeInBytes.toLong))
        .reduceLeftOption((dv1, dv2) => (dv1._1 + dv2._1, dv1._2 + dv2._2))
        .getOrElse((0L, 0L))

    val dvMetrics: Map[String, SQLMetric] = Map(
      "numDeletionVectorsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vectors removed",
          removedDVs.size),
      "numDeletionVectorRowsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vector rows removed",
          deletionVectorRowsRemoved),
      "numDeletionVectorBytesRemoved" ->
        setAndReturnMetric(
          "total number of bytes of removed deletion vectors",
          deletionVectorBytesRemoved))

    val sizeStats = FileSizeStatsWithHistogram.create(addedFiles.map(_.size).sorted)
    Map[String, SQLMetric](
      "minFileSize" -> setAndReturnMetric("minimum file size", sizeStats.get.min),
      "p25FileSize" -> setAndReturnMetric("25th percentile file size", sizeStats.get.p25),
      "p50FileSize" -> setAndReturnMetric("50th percentile file size", sizeStats.get.p50),
      "p75FileSize" -> setAndReturnMetric("75th percentile file size", sizeStats.get.p75),
      "maxFileSize" -> setAndReturnMetric("maximum file size", sizeStats.get.max),
      "numAddedFiles" -> setAndReturnMetric("total number of files added.", addedFiles.size),
      "numRemovedFiles" -> setAndReturnMetric("total number of files removed.", removedFiles.size),
      "numAddedBytes" -> setAndReturnMetric("total number of bytes added", totalSize(addedFiles)),
      "numRemovedBytes" ->
        setAndReturnMetric("total number of bytes removed", totalSize(removedFiles))
    ) ++ dvMetrics
  }
}
