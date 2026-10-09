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

import scala.collection.mutable

import com.databricks.sql.transaction.tahoe.OptimisticTransaction
import com.databricks.sql.transaction.tahoe.actions.{AddFile, FileAction, RemoveFile}
import com.databricks.sql.transaction.tahoe.commands.{
  DeletionVectorBitmapGenerator,
  DeletionVectorResult,
  DMLWithDeletionVectorsHelper,
  TouchedFileWithDV
}
import com.databricks.sql.transaction.tahoe.deletionvectors.{RoaringBitmapArray,
  RoaringBitmapArrayFormat}
import com.databricks.sql.transaction.tahoe.util.{Utils => DeltaUtils}
import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuColumnVector, GpuExec, GpuSemaphore}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, Encoder, SparkSession}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.nvidia.DFUDFShims

private[rapids] object GpuDeletionVectorBitmapGenerator {
  private val FilePathColumn = "__delta_gpu_dv_file_path"
  private val FileIdColumn = "__delta_gpu_dv_file_id"
  private val RowIndexColumn = "__delta_gpu_dv_row_index"
  private val DvDataClassName =
    "com.databricks.sql.transaction.tahoe.commands.DeletionVectorData"
  private val DvWriterClassName =
    "com.databricks.sql.transaction.tahoe.commands.DeletionVectorWriter$"

  // DBR's DeletionVectorData signature references a Scala 2.12-only Sizing trait in some
  // compile jars. Resolve this private API at runtime so the Scala 2.13 compiler need not load it.
  private def dvDataEncoder: Encoder[AnyRef] =
    Class.forName(DvDataClassName).getMethod("encoder").invoke(null)
      .asInstanceOf[Encoder[AnyRef]]

  private def newDvData(path: String, existingDv: Option[String], bitmap: Array[Byte],
      cardinality: Long): AnyRef = {
    val constructor = Class.forName(DvDataClassName).getConstructor(
      classOf[String], classOf[Option[_]], classOf[Array[Byte]], java.lang.Long.TYPE)
    constructor.newInstance(path, existingDv, bitmap, Long.box(cardinality))
  }

  private def dvWriter(spark: SparkSession, conf: Configuration, dataPath: Path,
      prefixLength: Int): Iterator[AnyRef] => Iterator[DeletionVectorResult] = {
    val writerClass = Class.forName(DvWriterClassName)
    val module = writerClass.getField("MODULE$").get(null)
    writerClass.getMethod("createMapperToStoreDeletionVectors", classOf[SparkSession],
      classOf[Configuration], classOf[Path], java.lang.Integer.TYPE)
      .invoke(module, spark, conf, dataPath, Int.box(prefixLength))
      .asInstanceOf[Iterator[AnyRef] => Iterator[DeletionVectorResult]]
  }

  private class PartialBitmap {
    private val bitmap = new RoaringBitmapArray()
    private var rangeStart = -1L
    private var rangeEnd = -1L

    private def flushRange(): Unit = {
      if (rangeStart >= 0) {
        if (rangeStart == rangeEnd) {
          bitmap.add(rangeStart)
        } else {
          bitmap.addRange(rangeStart to rangeEnd)
        }
        rangeStart = -1L
      }
    }

    def add(index: Long): Unit = {
      require(index >= 0, s"Invalid DELETE physical row index: $index")
      // NumericRange has an Int-sized length, so split exceptionally long consecutive runs.
      if (rangeStart < 0) {
        rangeStart = index
        rangeEnd = index
      } else if (rangeEnd != Long.MaxValue && index == rangeEnd + 1 &&
          rangeEnd - rangeStart < Int.MaxValue - 1) {
        rangeEnd = index
      } else {
        flushRange()
        rangeStart = index
        rangeEnd = index
      }
    }

    def toBytes: Array[Byte] = {
      flushRange()
      bitmap.runOptimize()
      bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    }
  }

  /**
   * DELETE keeps its predicate and file-ID lookup columnar on GPU. Each scan task copies only the
   * matched file IDs and row indexes to host and builds compact partial bitmaps before the shuffle.
   * DBR's writer still merges any old DV and persists the replacement DV.
   */
  def findTouchedFilesForDelete(
      spark: SparkSession,
      txn: OptimisticTransaction,
      hasReadableDVs: Boolean,
      targetScan: GpuTargetScan,
      condition: Column,
      nameToAddFileMap: Map[String, AddFile]): Seq[TouchedFileWithDV] = {
    import spark.implicits._

    val fileMetadata = nameToAddFileMap.toSeq.sortBy(_._1).map { case (path, add) =>
      val existingDv = if (hasReadableDVs) {
        Option(add.deletionVector).map(_.serializeToBase64())
      } else {
        None
      }
      // Match the encoded file-path key used by Delta's existing-DV lookup join.
      (SparkPath.fromPath(new Path(path)).urlEncoded, path, existingDv)
    }.toArray
    require(fileMetadata.map(_._1).distinct.length == fileMetadata.length,
      "Different DELETE candidate files have the same encoded scan path")
    val fileIds = spark.createDataset(fileMetadata.indices.map { id =>
      (fileMetadata(id)._1, id.toLong)
    }).toDF(FilePathColumn, FileIdColumn)

    val gpuTargetDf = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(
      spark, targetScan.dataFrame)
    val matchedRows = gpuTargetDf.filter(condition)
      .select(targetScan.filePathColumn.as(FilePathColumn),
        targetScan.rowIndexColumn.as(RowIndexColumn))
      .join(broadcast(fileIds), Seq(FilePathColumn), "left_outer")
      .select(col(FileIdColumn), col(RowIndexColumn))
    val queryExecution = matchedRows.queryExecution
    val columnarPlan = queryExecution.executedPlan match {
      case GpuColumnarToRowExec(child: GpuExec, _) => Some(child)
      case plan: GpuExec if plan.supportsColumnar => Some(plan)
      case _ => None
    }

    columnarPlan match {
      case Some(plan) =>
        val storedResults = SQLExecution.withNewExecutionId(
          queryExecution, Some("DELETE partial deletion vectors")) {
          val fileCount = fileMetadata.length
          val partialBitmaps = plan.executeColumnar().mapPartitions { batches =>
            val bitmaps = mutable.HashMap.empty[Long, PartialBitmap]
            batches.foreach { batch =>
              withResource(batch) { gpuBatch =>
                val columns = GpuColumnVector.extractBases(gpuBatch)
                require(columns.length == 2, "DELETE bitmap scan must return file ID and row index")
                withResource(columns(0).copyToHost()) { ids =>
                  withResource(columns(1).copyToHost()) { indexes =>
                    GpuSemaphore.releaseIfNecessary(TaskContext.get())
                    for (row <- 0 until gpuBatch.numRows()) {
                      require(!ids.isNull(row),
                        "A matched DELETE file was absent from the candidate-file map")
                      require(!indexes.isNull(row),
                        "The DELETE scan produced a null physical row index")
                      val fileId = ids.getLong(row)
                      require(fileId >= 0 && fileId < fileCount,
                        s"Invalid DELETE file ID: $fileId")
                      bitmaps.getOrElseUpdate(fileId, new PartialBitmap()).add(
                        indexes.getLong(row))
                    }
                  }
                }
              }
            }
            bitmaps.iterator.map { case (fileId, bitmap) => fileId -> bitmap.toBytes }
          }
          val rowIndexData = partialBitmaps
            .groupByKey(spark.sessionState.conf.numShufflePartitions)
            .map { case (fileId, parts) =>
              val bitmap = new RoaringBitmapArray()
              parts.foreach(bytes => bitmap.or(RoaringBitmapArray.readFrom(bytes)))
              bitmap.runOptimize()
              val (_, path, existingDv) = fileMetadata(fileId.toInt)
              newDvData(path, existingDv,
                bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable), bitmap.cardinality)
            }
          val prefixLength = DeltaUtils.getRandomPrefixLength(txn.metadata)
          spark.createDataset(rowIndexData)(dvDataEncoder)
            .mapPartitions(dvWriter(spark, txn.deltaLog.newDeltaHadoopConf(),
              txn.deltaLog.dataPath, prefixLength))(DeletionVectorResult.encoder)
            .collect().toSeq
        }
        DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(
          txn, nameToAddFileMap, storedResults)
      case None =>
        findTouchedFiles(spark, txn, hasReadableDVs, targetScan,
          nameToAddFileMap.values.toSeq, condition, nameToAddFileMap)
    }
  }

  /**
   * Finds candidate files containing matching rows and writes replacement deletion vectors.
   * Scanning and predicate evaluation remain GPU-eligible; DBR's native bitmap helper performs
   * the compact bitmap construction and DV persistence.
   */
  def findTouchedFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      hasReadableDVs: Boolean,
      targetScan: GpuTargetScan,
      candidateFiles: Seq[AddFile],
      condition: Column,
      nameToAddFileMap: Map[String, AddFile]): Seq[TouchedFileWithDV] = {
    val gpuTargetDf = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(
      spark, targetScan.dataFrame)
    val candidatesHaveDVs =
      hasReadableDVs && candidateFiles.exists(_.deletionVector != null)
    val storedResults = DeletionVectorBitmapGenerator
      .buildRowIndexSetsForFilesMatchingCondition(
        spark,
        txn,
        candidatesHaveDVs,
        gpuTargetDf,
        candidateFiles,
        DFUDFShims.columnToExpr(condition),
        Some(targetScan.filePathColumn),
        Some(targetScan.rowIndexColumn))

    DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(
      txn, nameToAddFileMap, storedResults)
  }

  def processUnmodifiedData(
      spark: SparkSession,
      touchedFiles: Seq[TouchedFileWithDV],
      txn: OptimisticTransaction): (Seq[FileAction], Map[String, Long]) = {
    // This helper works on file actions and snapshot statistics, not table rows. Keeping its
    // metadata joins on CPU avoids GPU broadcasts/transfers and CPU JSON bridges for small
    // driver-originated datasets. Bitmap generation above and subsequent DML remain GPU-enabled.
    val (actions, metrics) = GpuDeltaCpuFallback.withRapidsDisabled(spark) {
      DMLWithDeletionVectorsHelper.processUnmodifiedData(spark, touchedFiles, txn.snapshot)
    }
    // DBR rehydrates the replacement AddFile stats from the snapshot, but the paired RemoveFile
    // can retain the numRecords-only stats from the data-skipping candidate. Both actions describe
    // the same logical file state, so carry DBR's native wide-bound stats onto the remove action.
    val addStatsByPath = actions.collect {
      case add: AddFile if add.stats != null => add.path -> add.stats
    }.toMap
    val actionsWithStats = actions.map {
      case remove: RemoveFile if addStatsByPath.contains(remove.path) =>
        remove.copy(stats = addStatsByPath(remove.path))
      case action => action
    }
    (actionsWithStats, metrics)
  }
}
