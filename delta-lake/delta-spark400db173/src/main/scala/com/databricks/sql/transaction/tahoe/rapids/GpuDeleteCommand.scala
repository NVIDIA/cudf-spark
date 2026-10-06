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
import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.databricks.sql.transaction.tahoe.files.TahoeBatchFileIndex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.nvidia.DFUDFShims

case class GpuDeleteCommand(
    gpuDeltaLog: GpuDeltaLog,
    target: LogicalPlan,
    condition: Option[Expression])
    extends GpuDeleteCommandBase(gpuDeltaLog, target, condition) {

  override protected def deleteWithPersistentDeletionVectors(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      candidateFiles: Seq[AddFile],
      fileIndex: TahoeBatchFileIndex,
      deleteCondition: Expression,
      nameToAddFileMap: Map[String, AddFile]):
      Option[(Seq[FileAction], Map[String, Long])] = {
    val targetScan = DMLWithDeletionVectorsHelperShims.createTargetDfForGpuScanningForMatches(
      sparkSession, target, fileIndex)
    val touchedFiles = GpuDeletionVectorBitmapGenerator.findTouchedFilesForDelete(
      sparkSession,
      txn,
      hasReadableDVs = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot),
      targetScan,
      DFUDFShims.exprToColumn(deleteCondition),
      nameToAddFileMap)
    if (touchedFiles.nonEmpty) {
      Some(GpuDeletionVectorBitmapGenerator.processUnmodifiedData(
        sparkSession, touchedFiles, txn))
    } else {
      Some(Nil -> Map(
        "numModifiedRows" -> 0L,
        "numDeletionVectorsAdded" -> 0L,
        "numDeletionVectorsRemoved" -> 0L,
        "numDeletionVectorsUpdated" -> 0L,
        "numRemovedFiles" -> 0L))
    }
  }
}

object GpuDeleteCommand {
  val FINDING_TOUCHED_FILES_MSG: String = "Finding files to rewrite for DELETE operation"

  def rewritingFilesMsg(numFilesToRewrite: Long): String =
    s"Rewriting $numFilesToRewrite files for DELETE operation"
}
