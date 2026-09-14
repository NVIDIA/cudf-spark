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

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.{DeletionVectorBitmapGenerator,
  DMLWithDeletionVectorsHelper, TouchedFileWithDV}
import org.apache.spark.sql.nvidia.DFUDFShims

private[rapids] object GpuDeletionVectorBitmapGenerator extends GpuDeltaCommandLike {

  /**
   * Finds candidate files containing rows that satisfy `condition` and writes a replacement
   * deletion vector for each touched file. The scan, predicate, and projection remain eligible
   * for GPU execution; Delta's CPU `BitmapAggregator` builds the compact Roaring bitmaps.
   *
   * @param spark active Spark session
   * @param txn active GPU Delta transaction
   * @param hasReadableDVs whether existing deletion vectors must be applied and merged
   * @param targetDf scan of the candidate files with Delta metadata columns
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
      targetDf: DataFrame,
      candidateFiles: Seq[AddFile],
      condition: Column,
      nameToAddFileMap: Map[String, AddFile],
      operationName: String): Seq[TouchedFileWithDV] = {
    recordDeltaOperation(txn.deltaLog, s"$operationName.findTouchedFiles") {
      val gpuTargetDf = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(spark, targetDf)
      val candidatesHaveDVs =
        hasReadableDVs && candidateFiles.exists(_.deletionVector != null)
      val storedResults = DeletionVectorBitmapGenerator.buildRowIndexSetsForFilesMatchingCondition(
        spark,
        txn,
        candidatesHaveDVs,
        gpuTargetDf,
        candidateFiles,
        DFUDFShims.columnToExpr(condition),
        Some(DMLWithDeletionVectorsHelperShims.filePathColumnForGpuScanning(spark)),
        Some(DMLWithDeletionVectorsHelperShims.rowIndexColumnForGpuScanning(spark)))

      DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(txn, nameToAddFileMap, storedResults)
    }
  }
}
