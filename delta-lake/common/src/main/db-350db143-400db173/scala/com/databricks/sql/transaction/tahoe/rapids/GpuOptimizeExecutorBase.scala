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

import java.util.ConcurrentModificationException

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.DeltaOperations.Operation
import com.databricks.sql.transaction.tahoe.actions.{Action, AddFile, FileAction, RemoveFile}
import com.databricks.sql.transaction.tahoe.commands.DeltaCommand
import com.databricks.sql.transaction.tahoe.commands.optimize.{FileSizeStatsWithHistogram, OptimizeStats}
import com.databricks.sql.transaction.tahoe.files.SQLMetricsReporting
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.util.ThreadUtils

abstract class GpuOptimizeExecutorBase(
    sparkSession: SparkSession,
    partitionPredicate: Seq[Expression])
  extends DeltaCommand with SQLMetricsReporting with Serializable {

  protected type PartitionedBin = (Map[String, String], Seq[AddFile])

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = System.currentTimeMillis

  protected def deltaLog: DeltaLog

  protected def createTransaction(): OptimisticTransaction

  protected def beforeOptimize(txn: OptimisticTransaction): Unit = {}

  protected def getMinFileSize: Long

  protected def getMaxFileSize: Long

  protected def optimizeOperation: Operation

  protected def getCandidateFiles(txn: OptimisticTransaction): Seq[AddFile] =
    txn.filterFiles(partitionPredicate)

  protected def prepareInput(txn: OptimisticTransaction, bin: Seq[AddFile]): DataFrame =
    txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))

  protected def writeOptimizeOutput(
      txn: OptimisticTransaction,
      repartitionDF: DataFrame): Seq[FileAction]

  protected def commitTransaction(
      txn: OptimisticTransaction,
      optimizeOperation: Operation,
      actions: Seq[Action]): Unit

  protected def startRetryTransaction(txn: OptimisticTransaction): OptimisticTransaction

  protected def extraMetrics(
      sparkContext: SparkContext,
      setAndReturnMetric: (String, Long) => SQLMetric): Map[String, SQLMetric] = Map.empty

  protected def updateOptimizeStats(
      optimizeStats: OptimizeStats,
      jobs: Seq[PartitionedBin],
      candidateFiles: Seq[AddFile],
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile],
      committedTxn: OptimisticTransaction): Unit = {}

  def optimize(): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.optimize") {
      val txn = createTransaction()
      beforeOptimize(txn)

      val maxFileSize = getMaxFileSize
      require(maxFileSize > 0, "maxFileSize must be > 0")
      val minFileSize = getMinFileSize
      require(minFileSize > 0, "minFileSize must be > 0")

      val candidateFiles = getCandidateFiles(txn)
      val filesToProcess = candidateFiles.filter(_.size < minFileSize)
      val partitionSchema = txn.metadata.partitionSchema

      val partitionsToCompact = filesToProcess
        .groupBy(_.partitionValues)
        .filter { case (_, filesInPartition) => filesInPartition.size >= 2 }
        .toSeq

      val jobs = groupFilesIntoBins(partitionsToCompact, maxFileSize)
      val maxThreads = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS)
      val updates = ThreadUtils.parmap(jobs, "GpuOptimizeJob", maxThreads) {
        case (partition, bin) => runOptimizeBinJob(txn, partition, bin)
      }.flatten

      val addedFiles = updates.collect { case a: AddFile => a }
      val removedFiles = updates.collect { case r: RemoveFile => r }
      val committedTxn = if (addedFiles.nonEmpty) {
        val operation = optimizeOperation
        val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles)
        commitAndRetry(txn, operation, updates, metrics) { newTxn =>
          val newPartitionSchema = newTxn.metadata.partitionSchema
          val candidateSetOld = candidateFiles.map(_.path).toSet
          val candidateSetNew = newTxn.filterFiles(partitionPredicate).map(_.path).toSet

          // It is safe to retry when the files compacted by this attempt are still present
          // and the table partitioning has not changed.
          if (candidateSetOld.subsetOf(candidateSetNew) && partitionSchema == newPartitionSchema) {
            true
          } else {
            val deleted = candidateSetOld -- candidateSetNew
            logWarning(s"The following compacted files were deleted " +
              s"during checkpoint ${deleted.mkString(",")}. Aborting the compaction.")
            false
          }
        }
      } else {
        txn
      }

      val optimizeStats = OptimizeStats()
      optimizeStats.addedFilesSizeStats.merge(addedFiles)
      optimizeStats.removedFilesSizeStats.merge(removedFiles)
      optimizeStats.numPartitionsOptimized = jobs.map(_._1).distinct.size
      optimizeStats.numBatches = jobs.size
      optimizeStats.totalConsideredFiles = candidateFiles.size
      optimizeStats.totalFilesSkipped = optimizeStats.totalConsideredFiles - removedFiles.size
      optimizeStats.totalClusterParallelism = sparkSession.sparkContext.defaultParallelism
      updateOptimizeStats(
        optimizeStats, jobs, candidateFiles, addedFiles, removedFiles, committedTxn)

      Seq(Row(deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
    }
  }

  /**
   * Utility methods to group files into bins for optimize.
   *
   * @param partitionsToCompact List of files to compact group by partition.
   *                            Partition is defined by the partition values (partCol -> partValue)
   * @param maxTargetFileSize Max size (in bytes) of the compaction output file.
   * @return Sequence of bins. Each bin contains one or more files from the same
   *         partition and targeted for one output file.
   */
  private def groupFilesIntoBins(
      partitionsToCompact: Seq[PartitionedBin],
      maxTargetFileSize: Long): Seq[PartitionedBin] = {
    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        files.sortBy(_.size).foreach { file =>
          if (file.size + currentBinSize > maxTargetFileSize) {
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

        bins.map(b => (partition, b)).filter(_._2.size > 1)
    }
  }

  /**
   * Utility method to run a Spark job to compact the files in given bin.
   *
   * @param txn [[OptimisticTransaction]] instance in use to commit the changes to DeltaLog.
   * @param partition Partition values of the partition that files in [[bin]] belongs to.
   * @param bin List of files to compact into one large file.
   */
  private def runOptimizeBinJob(
      txn: OptimisticTransaction,
      partition: Map[String, String],
      bin: Seq[AddFile]): Seq[FileAction] = {
    val baseTablePath = txn.deltaLog.dataPath

    val input = prepareInput(txn, bin)
    val useRepartition = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED)
    val repartitionDF = if (useRepartition) {
      input.repartition(numPartitions = 1)
    } else {
      input.coalesce(numPartitions = 1)
    }

    val partitionDesc = partition.toSeq.map(entry => entry._1 + "=" + entry._2).mkString(",")
    val partitionName = if (partition.isEmpty) "" else s" in partition ($partitionDesc)"
    val description = s"$baseTablePath<br/>Optimizing ${bin.size} files" + partitionName
    sparkSession.sparkContext.setJobGroup(
      sparkSession.sparkContext.getLocalProperty(SPARK_JOB_GROUP_ID),
      description)

    val addFiles = writeOptimizeOutput(txn, repartitionDF).collect {
      case a: AddFile =>
        a.copy(dataChange = false)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass}. File compaction job output " +
            "should only have AddFiles")
    }
    val removeFiles = bin.map(_.removeWithTimestamp(operationTimestamp, dataChange = false))
    addFiles ++ removeFiles
  }

  /**
   * Attempts to commit the given actions to the log. In the case of a concurrent update,
   * the given function will be invoked with a new transaction to allow custom conflict
   * detection logic to indicate it is safe to try again, by returning `true`.
   *
   * This function will continue to try to commit to the log as long as `f` returns `true`,
   * otherwise throws a subclass of [[ConcurrentModificationException]].
   */
  @tailrec
  private def commitAndRetry(
      txn: OptimisticTransaction,
      optimizeOperation: Operation,
      actions: Seq[Action],
      metrics: Map[String, SQLMetric])(f: OptimisticTransaction => Boolean)
      : OptimisticTransaction = {
    try {
      txn.registerSQLMetrics(sparkSession, metrics)
      commitTransaction(txn, optimizeOperation, actions)
      txn
    } catch {
      case e: ConcurrentModificationException =>
        val newTxn = startRetryTransaction(txn)
        if (f(newTxn)) {
          logInfo("Retrying commit after checking for semantic conflicts with concurrent updates.")
          commitAndRetry(newTxn, optimizeOperation, actions, metrics)(f)
        } else {
          logWarning("Semantic conflicts detected. Aborting operation.")
          throw e
        }
    }
  }

  /** Create a map of SQL metrics for adding to the commit history. */
  private def createMetrics(
      sparkContext: SparkContext,
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile]): Map[String, SQLMetric] = {
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

    val sizeStats = FileSizeStatsWithHistogram.create(addedFiles.map(_.size).sorted).getOrElse {
      throw new IllegalStateException(
        s"FileSizeStatsWithHistogram.create returned None for ${addedFiles.size} files")
    }
    Map[String, SQLMetric](
      "minFileSize" -> setAndReturnMetric("minimum file size", sizeStats.min),
      "p25FileSize" -> setAndReturnMetric("25th percentile file size", sizeStats.p25),
      "p50FileSize" -> setAndReturnMetric("50th percentile file size", sizeStats.p50),
      "p75FileSize" -> setAndReturnMetric("75th percentile file size", sizeStats.p75),
      "maxFileSize" -> setAndReturnMetric("maximum file size", sizeStats.max),
      "numAddedFiles" -> setAndReturnMetric("total number of files added", addedFiles.size),
      "numRemovedFiles" -> setAndReturnMetric(
        "total number of files removed", removedFiles.size),
      "numAddedBytes" -> setAndReturnMetric("total number of bytes added", totalSize(addedFiles)),
      "numRemovedBytes" -> setAndReturnMetric(
        "total number of bytes removed", totalSize(removedFiles))) ++
      extraMetrics(sparkContext, setAndReturnMetric)
  }
}
