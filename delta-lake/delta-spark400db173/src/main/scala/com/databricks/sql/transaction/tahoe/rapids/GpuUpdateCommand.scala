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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.OptimisticTransaction
import com.databricks.sql.transaction.tahoe.actions.{AddFile, FileAction}
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, TouchedFileWithDV}
import com.databricks.sql.transaction.tahoe.files.TahoeBatchFileIndex
import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.nvidia.DFUDFShims

case class GpuUpdateCommand(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
    extends GpuUpdateCommandBase(
      gpuDeltaLog,
      tahoeFileIndex,
      target,
      updateExpressions,
      condition) {

  private class DbrPersistentDvTouchedFiles(
      val touchedFiles: Seq[TouchedFileWithDV]) extends PersistentDvTouchedFiles {
    override val files: Seq[AddFile] = touchedFiles.map(_.fileLogEntry)
  }

  override protected def findTouchedFilesWithPersistentDVs(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      candidateFiles: Seq[AddFile],
      fileIndex: TahoeBatchFileIndex,
      updateCondition: Expression,
      nameToAddFileMap: Map[String, AddFile]):
      Option[PersistentDvTouchedFiles] = {
    val targetScan = DMLWithDeletionVectorsHelperShims.createTargetDfForGpuScanningForMatches(
      sparkSession, target, fileIndex)
    val touchedFiles = GpuDeletionVectorBitmapGenerator.findTouchedFiles(
      sparkSession,
      txn,
      hasReadableDVs = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot),
      targetScan,
      candidateFiles,
      DFUDFShims.exprToColumn(updateCondition),
      nameToAddFileMap)
    Some(new DbrPersistentDvTouchedFiles(touchedFiles))
  }

  override protected def processUnmodifiedDataWithPersistentDVs(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      touchedFiles: PersistentDvTouchedFiles):
      Option[(Seq[FileAction], Map[String, Long])] = touchedFiles match {
    case dbTouchedFiles: DbrPersistentDvTouchedFiles =>
      Some(GpuDeletionVectorBitmapGenerator.processUnmodifiedData(
        sparkSession, dbTouchedFiles.touchedFiles, txn))
    case other =>
      throw new IllegalStateException(
        s"Unexpected persistent-DV touched-file container: ${other.getClass.getName}")
  }
}
