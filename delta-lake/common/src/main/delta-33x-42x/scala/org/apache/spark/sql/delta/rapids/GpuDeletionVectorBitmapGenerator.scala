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

package org.apache.spark.sql.delta.rapids

import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.{DeletionVectorBitmapGenerator,
  DeletionVectorData, DeletionVectorResult, DMLWithDeletionVectorsHelper, FileToDvDescriptor,
  TouchedFileWithDV}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat
import org.apache.spark.sql.delta.util.{DeltaFileOperations, Utils => DeltaUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.nvidia.DFUDFShims

private[rapids] case class GpuTargetScan(
    dataFrame: DataFrame,
    filePathColumn: Column,
    rowIndexColumn: Column)

private[rapids] object GpuDeletionVectorBitmapGenerator {

  /**
   * Finds candidate files containing rows that satisfy `condition` and writes a replacement
   * deletion vector for each touched file. The scan, predicate, and projection remain eligible
   * for GPU execution; Delta's CPU `BitmapAggregator` builds the compact Roaring bitmaps.
   *
   * @param spark active Spark session
   * @param txn active GPU Delta transaction
   * @param hasReadableDVs whether existing deletion vectors must be applied and merged
   * @param targetScan scan and analyzed attributes for Delta's internal metadata columns
   * @param candidateFiles files selected by Delta data skipping
   * @param condition predicate selecting rows to invalidate
   * @param nameToAddFileMap canonical file-path lookup used to construct touched-file results
   * @param operationName DML operation name used for Delta operation recording
   * @return touched files and their replacement deletion vectors
   */
  def findTouchedFiles(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      hasReadableDVs: Boolean,
      targetScan: GpuTargetScan,
      candidateFiles: Seq[AddFile],
      condition: Column,
      nameToAddFileMap: Map[String, AddFile],
      operationName: String): Seq[TouchedFileWithDV] = {
    // Delta 4.3 changed the DeltaLogging signature; use the runtime-specific shim.
    DeltaRuntimeShim33x.runDeltaOperation(txn.deltaLog, s"$operationName.findTouchedFiles") {
      val gpuTargetDf = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(
        spark, targetScan.dataFrame)
      val candidatesHaveDVs =
        hasReadableDVs && candidateFiles.exists(_.deletionVector != null)
      val matchedRows = matchedRowsWithDVs(
        spark,
        txn.deltaLog.dataPath,
        candidatesHaveDVs,
        gpuTargetDf,
        candidateFiles,
        condition,
        targetScan)
      val storedResults = persistDeletionVectors(
        spark,
        matchedRows,
        txn.deltaLog.newDeltaHadoopConf(),
        txn.deltaLog.dataPath,
        DeltaUtils.getRandomPrefixLength(txn.metadata))

      DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(txn, nameToAddFileMap, storedResults)
    }
  }

  /**
   * The rows matching `condition` with their file path, row index and the file's existing
   * deletion vector (if any). Mirrors the first half of Delta's
   * `buildRowIndexSetsForFilesMatchingCondition` in Delta 3.3 through 4.2. It is kept here
   * because only Delta 4.1+ exposes this step apart from the DV persistence, so compare it with
   * Delta's when moving to a new version.
   */
  private def matchedRowsWithDVs(
      spark: SparkSession,
      tablePath: Path,
      tableHasDVs: Boolean,
      targetDf: DataFrame,
      candidateFiles: Seq[AddFile],
      condition: Column,
      targetScan: GpuTargetScan): DataFrame = {
    // The scan always supplies the file path and row index columns, see GpuTargetScan.
    val matchedRows = targetDf
      .withColumn(FileNameCol, targetScan.filePathColumn)
      // Filter after getting the file name as the filter might introduce a join.
      .filter(condition)
      .withColumn(RowIndexCol, targetScan.rowIndexColumn)
    if (tableHasDVs) {
      // Attach each matched file's existing deletion vector. Metadata paths are canonicalized,
      // so the lookup paths must be too.
      val filePathToDV = candidateFiles.map { add =>
        FileToDvDescriptor(
          SparkPath.fromPath(DeltaFileOperations.absolutePath(tablePath.toString, add.path))
            .urlEncoded,
          Option(add.deletionVector).map(_.serializeToBase64()))
      }
      val lookup = spark.createDataset(filePathToDV)(FileToDvDescriptor.encoder)
        .withColumnRenamed("path", LookupPathCol)
      // A left outer join keeps rows whose path was not matched, so that the bitmap
      // persistence fails on them instead of silently dropping deletes.
      matchedRows.join(lookup, lookup(LookupPathCol) === matchedRows(FileNameCol), "leftOuter")
        .drop(FileNameCol)
        .withColumnRenamed(LookupPathCol, FileNameCol)
    } else {
      matchedRows.withColumn(DvIdCol, lit(null))
    }
  }

  /**
   * Builds one bitmap per file with Delta's `BitmapAggregator`, then persists the packed
   * deletion vector files with [[GpuDeletionVectorWriter]]. Mirrors Delta's
   * `DeletionVectorSet.computeResult`, with the same column names and result encoders.
   */
  private def persistDeletionVectors(
      spark: SparkSession,
      matchedRows: DataFrame,
      hadoopConf: org.apache.hadoop.conf.Configuration,
      tablePath: Path,
      prefixLength: Int): Seq[DeletionVectorResult] = {
    val bitmaps = matchedRows
      .groupBy(col(FileNameCol), col(DvIdCol))
      .agg(DFUDFShims.exprToColumn(new BitmapAggregator(
        DFUDFShims.columnToExpr(col(RowIndexCol)), RoaringBitmapArrayFormat.Portable)
        .toAggregateExpression(isDistinct = false)).as(BitmapStruct))
      .select(
        col(FileNameCol),
        col(DvIdCol),
        col(s"$BitmapStruct.bitmap").as(DeletionVectorBitmapGenerator.DELETED_ROW_INDEX_BITMAP),
        col(s"$BitmapStruct.cardinality")
          .as(DeletionVectorBitmapGenerator.DELETED_ROW_INDEX_COUNT))
    val mapper = GpuDeletionVectorWriter.createMapper(spark, hadoopConf, tablePath, prefixLength)
    bitmaps.as[DeletionVectorData](DeletionVectorData.encoder)
      .mapPartitions(mapper)(DeletionVectorResult.encoder)
      .collect().toSeq
  }

  private val FileNameCol = DeletionVectorBitmapGenerator.FILE_NAME_COL
  private val DvIdCol = DeletionVectorBitmapGenerator.FILE_DV_ID_COL
  private val RowIndexCol = DeletionVectorBitmapGenerator.ROW_INDEX_COL
  private val LookupPathCol = "dvFilePath"
  private val BitmapStruct = "CardinalityAndBitmapStruct"
}
