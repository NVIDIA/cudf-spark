/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import com.databricks.sql.io.skipping.MultiDimClustering
import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.DeltaOperations.Operation
import com.databricks.sql.transaction.tahoe.actions.{Action, AddFile, FileAction, RemoveFile}
import com.databricks.sql.transaction.tahoe.commands.DeltaCommand
import com.databricks.sql.transaction.tahoe.commands.optimize._
import com.databricks.sql.transaction.tahoe.files.SQLMetricsReporting
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.delta.RapidsDeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.util.ThreadUtils

class GpuOptimizeExecutor(
                        sparkSession: SparkSession,
                        txn: OptimisticTransaction,
                        partitionPredicate: Seq[Expression],
                        zOrderByColumns: Seq[String],
                        prevCommitActions: Seq[Action])
  extends DeltaCommand with SQLMetricsReporting with Serializable {

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = System.currentTimeMillis

  private val isMultiDimClustering = zOrderByColumns.nonEmpty
  private val isAutoCompact = prevCommitActions.nonEmpty
  private val optimizeType = GpuOptimizeType(isMultiDimClustering, isAutoCompact)

  def optimize(): Seq[Row] = {
    recordDeltaOperation(txn.deltaLog, "delta.optimize") {
      val maxFileSize = optimizeType.maxFileSize
      require(maxFileSize > 0, "maxFileSize must be > 0")

      val minNumFilesInDir = optimizeType.minNumFiles
      val (candidateFiles, filesToProcess) = optimizeType.targetFiles
      val partitionSchema = txn.metadata.partitionSchema

      // select all files in case of multi-dimensional clustering
      val partitionsToCompact = filesToProcess
        .groupBy(_.partitionValues)
        .filter { case (_, filesInPartition) => filesInPartition.size >= minNumFilesInDir }
        .toSeq

      val groupedJobs = groupFilesIntoBins(partitionsToCompact, maxFileSize)
      val jobs = optimizeType.targetBins(groupedJobs)

      val maxThreads =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS)
      val updates = ThreadUtils.parmap(jobs, "OptimizeJob", maxThreads) { partitionBinGroup =>
        runOptimizeBinJob(txn, partitionBinGroup._1, partitionBinGroup._2, maxFileSize)
      }.flatten

      val addedFiles = updates.collect { case a: AddFile => a }
      val removedFiles = updates.collect { case r: RemoveFile => r }
      if (addedFiles.nonEmpty) {
        val operation = DeltaOperations.Optimize(partitionPredicate, zOrderByColumns)
        val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles)
        commitAndRetry(txn, operation, updates, metrics) { newTxn =>
          val newPartitionSchema = newTxn.metadata.partitionSchema
          val candidateSetOld = candidateFiles.map(_.path).toSet
          val candidateSetNew = newTxn.filterFiles(partitionPredicate).map(_.path).toSet

          // As long as all of the files that we compacted are still part of the table,
          // and the partitioning has not changed it is valid to continue to try
          // and commit this checkpoint.
          if (candidateSetOld.subsetOf(candidateSetNew) && partitionSchema == newPartitionSchema) {
            true
          } else {
            val deleted = candidateSetOld -- candidateSetNew
            logWarning(s"The following compacted files were delete " +
              s"during checkpoint ${deleted.mkString(",")}. Aborting the compaction.")
            false
          }
        }
      }

      val optimizeStats = OptimizeStats()
      optimizeStats.addedFilesSizeStats.merge(addedFiles)
      optimizeStats.removedFilesSizeStats.merge(removedFiles)
      optimizeStats.numPartitionsOptimized = jobs.map(j => j._1).distinct.size
      optimizeStats.numBatches = jobs.size
      optimizeStats.totalConsideredFiles = candidateFiles.size
      optimizeStats.totalFilesSkipped = optimizeStats.totalConsideredFiles - removedFiles.size
      optimizeStats.totalClusterParallelism = sparkSession.sparkContext.defaultParallelism

      if (isMultiDimClustering) {
        val inputFileStats =
          ZOrderFileStats(removedFiles.size, removedFiles.map(_.size.getOrElse(0L)).sum)
        optimizeStats.zOrderStats = Some(ZOrderStats(
          strategyName = "all", // means process all files in a partition
          inputCubeFiles = ZOrderFileStats(0, 0),
          inputOtherFiles = inputFileStats,
          inputNumCubes = 0,
          mergedFiles = inputFileStats,
          // There will one z-cube for each partition
          numOutputCubes = optimizeStats.numPartitionsOptimized))
      }

      return Seq(Row(txn.deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
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
    partitionsToCompact: Seq[(Map[String, String], Seq[AddFile])],
    maxTargetFileSize: Long): Seq[(Map[String, String], Seq[AddFile])] = {

    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        files.sortBy(_.size).foreach { file =>
          // Generally, a bin is a group of existing files, whose total size does not exceed the
          // desired maxFileSize. They will be coalesced into a single output file.
          // However, if isMultiDimClustering = true, all files in a partition will be read by the
          // same job, the data will be range-partitioned and numFiles = totalFileSize / maxFileSize
          // will be produced. See below.
          if (file.size + currentBinSize > maxTargetFileSize && !isMultiDimClustering) {
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

        bins.map(b => (partition, b))
          // select bins that have at least two files or in case of multi-dim clustering
          // select all bins
          .filter(_._2.size > 1 || isMultiDimClustering)
    }
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
                                 txn: OptimisticTransaction,
                                 partition: Map[String, String],
                                 bin: Seq[AddFile],
                                 maxFileSize: Long): Seq[FileAction] = {
    val baseTablePath = txn.deltaLog.dataPath

    val input = txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))
    val repartitionDF = if (isMultiDimClustering) {
      val totalSize = bin.map(_.size).sum
      val approxNumFiles = Math.max(1, totalSize / maxFileSize).toInt
      MultiDimClustering.cluster(
        input,
        approxNumFiles,
        zOrderByColumns)
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

    val addFiles = txn.writeFiles(repartitionDF).collect {
      case a: AddFile =>
        a.copy(dataChange = false)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass}. File compaction job output" +
            s"should only have AddFiles")
    }
    val removeFiles = bin.map(f => f.removeWithTimestamp(operationTimestamp, dataChange = false))
    val updates = addFiles ++ removeFiles
    updates
  }

  private type PartitionedBin = (Map[String, String], Seq[AddFile])

  private trait GpuOptimizeType {
    def minNumFiles: Long

    def maxFileSize: Long =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE)

    def targetFiles: (Seq[AddFile], Seq[AddFile])

    def targetBins(jobs: Seq[PartitionedBin]): Seq[PartitionedBin] = jobs
  }

  private case class GpuCompaction() extends GpuOptimizeType {
    def minNumFiles: Long = 2

    def targetFiles: (Seq[AddFile], Seq[AddFile]) = {
      val minFileSize = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE)
      require(minFileSize > 0, "minFileSize must be > 0")
      val candidateFiles = txn.filterFiles(partitionPredicate)
      val filesToProcess = candidateFiles.filter(_.size < minFileSize)
      (candidateFiles, filesToProcess)
    }
  }

  private case class GpuMultiDimOrdering() extends GpuOptimizeType {
    def minNumFiles: Long = 1

    def targetFiles: (Seq[AddFile], Seq[AddFile]) = {
      // select all files in case of multi-dimensional clustering
      val candidateFiles = txn.filterFiles(partitionPredicate)
      (candidateFiles, candidateFiles)
    }
  }

  private case class GpuAutoCompaction() extends GpuOptimizeType {
    def minNumFiles: Long = {
      val minNumFiles =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES)
      require(minNumFiles > 0, "minNumFiles must be > 0")
      minNumFiles
    }

    override def maxFileSize: Long =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE)
        .getOrElse(128 * 1024 * 1024)

    override def targetFiles: (Seq[AddFile], Seq[AddFile]) = {
      val autoCompactTarget =
        sparkSession.sessionState.conf.getConf(RapidsDeltaSQLConf.AUTO_COMPACT_TARGET)
      // Filter the candidate files according to autoCompact.target config.
      lazy val addedFiles = prevCommitActions.collect { case a: AddFile => a }
      val candidateFiles = autoCompactTarget match {
        case "table" =>
          txn.filterFiles()
        case "commit" =>
          addedFiles
        case "partition" =>
          val eligiblePartitions = addedFiles.map(_.partitionValues).toSet
          txn.filterFiles().filter(f => eligiblePartitions.contains(f.partitionValues))
        case _ =>
          logError(s"Invalid config for autoCompact.target: $autoCompactTarget. " +
            s"Falling back to the default value 'table'.")
          txn.filterFiles()
      }
      val filesToProcess = candidateFiles.filter(_.size < maxFileSize)
      (candidateFiles, filesToProcess)
    }

    override def targetBins(jobs: Seq[PartitionedBin]): Seq[PartitionedBin] = {
      var acc = 0L
      val maxCompactBytes =
        sparkSession.sessionState.conf.getConf(RapidsDeltaSQLConf.AUTO_COMPACT_MAX_COMPACT_BYTES)
      // bins with more files are prior to less files.
      jobs
        .sortBy { case (_, filesInBin) => -filesInBin.length }
        .takeWhile { case (_, filesInBin) =>
          acc += filesInBin.map(_.size).sum
          acc <= maxCompactBytes
        }
    }
  }

  private object GpuOptimizeType {

    def apply(isMultiDimClustering: Boolean, isAutoCompact: Boolean): GpuOptimizeType = {
      if (isMultiDimClustering) {
        GpuMultiDimOrdering()
      } else if (isAutoCompact) {
        GpuAutoCompaction()
      } else {
        GpuCompaction()
      }
    }
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
  : Unit = {
    try {
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(actions, optimizeOperation)
    } catch {
      case e: ConcurrentModificationException =>
        val newTxn = txn.deltaLog.startTransaction()
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
        setAndReturnMetric("total number of bytes removed", totalSize(removedFiles)))
  }
}
