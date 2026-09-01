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

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuBringBackToHost, GpuColumnarToRowExec, RapidsHostColumnVector}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.{DeletionVectorData, DeletionVectorWriter, DMLWithDeletionVectorsHelper, TouchedFileWithDV}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.spark.sql.functions.{broadcast, col, collect_list}

private[rapids] object GpuDeletionVectorBitmapGenerator {
  private val FileNameColumn = "filePath"
  private val FileIdColumn = "fileId"
  private val FileNameKeyColumn = "fileNameKey"
  private val RowIndexColumn = "rowIndexCol"
  private val RowIndexListColumn = "rowIndexList"

  case class FileDictionaryRow(fileNameKey: String, fileId: Long)

  private object FileDictionaryRow {
    implicit val encoder: Encoder[FileDictionaryRow] = Encoders.product[FileDictionaryRow]
  }

  case class GroupedRowIndexes(
      fileId: Long,
      rowIndexList: Seq[Long])

  private object GroupedRowIndexes {
    implicit val encoder: Encoder[GroupedRowIndexes] = Encoders.product[GroupedRowIndexes]
  }

  def findTouchedFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      tableHasDVs: Boolean,
      rowsArePartitionedByFile: Boolean,
      targetDf: DataFrame,
      candidateFiles: Seq[AddFile],
      condition: Column,
      fileNameColumn: Column,
      rowIndexColumn: Column,
      nameToAddFileMap: Map[String, AddFile]): Seq[TouchedFileWithDV] = {
    val matchedRows = targetDf
      .withColumn(FileNameColumn, fileNameColumn)
      .filter(condition)
      .withColumn(RowIndexColumn, rowIndexColumn)

    val basePath = txn.deltaLog.dataPath.toString
    val candidateFilePaths = candidateFiles.map { addFile =>
      SparkPath.fromPath(absolutePath(basePath, addFile.path)).urlEncoded
    }
    require(candidateFilePaths.distinct.size == candidateFilePaths.size,
      "Cannot safely match duplicate deletion-vector candidate paths")
    val fileDictionaryRows = candidateFilePaths.zipWithIndex.map { case (filePath, fileId) =>
      FileDictionaryRow(filePath, fileId.toLong)
    }
    val fileInfoById = candidateFiles.zipWithIndex.map { case (addFile, fileId) =>
      val canonicalPath = SparkPath.fromPath(absolutePath(basePath, addFile.path)).urlEncoded
      val serializedDv = if (tableHasDVs) {
        Option(addFile.deletionVector).map(_.serializeToBase64())
      } else {
        None
      }
      (fileId.toLong, (canonicalPath, serializedDv))
    }.toMap
    val fileInfoBroadcast = spark.sparkContext.broadcast(fileInfoById)

    import FileDictionaryRow.encoder
    val fileDictionaryDf = broadcast(spark.createDataset(fileDictionaryRows))
    val joinExpr = fileDictionaryDf(FileNameKeyColumn) === matchedRows(FileNameColumn)
    val matchedRowsWithIndexes = matchedRows
      .join(fileDictionaryDf, joinExpr, "inner")
      .filter(col(RowIndexColumn).isNotNull)
      .select(fileDictionaryDf(FileIdColumn), matchedRows(RowIndexColumn))

    val prefixLength = DeltaUtils.getRandomPrefixLength(txn.metadata)
    val storeDvs = DeletionVectorWriter.createMapperToStoreDeletionVectors(
      spark,
      txn.deltaLog.newDeltaHadoopConf(),
      txn.deltaLog.dataPath,
      prefixLength)

    val storedResults = try {
      if (rowsArePartitionedByFile) {
        // Preserve the GPU scan/filter/project and cross the CPU boundary in columnar batches.
        // Direct DML scans are unsplit, so all matches for a file stay in one Spark partition.
        // The write stub gives AQE the parent context it needs to plan columnar output instead of
        // placing a row-producing AdaptiveSparkPlanExec at the root of this manually-run plan.
        val gpuRead = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(
          spark, matchedRowsWithIndexes)
        val columnarPlan = gpuRead.queryExecution.executedPlan match {
          case transition: GpuColumnarToRowExec => transition.child
          case plan if plan.supportsColumnar => plan
          case plan => throw new IllegalStateException(
            "GPU deletion-vector match plan is not columnar: " + plan)
        }
        GpuBringBackToHost(columnarPlan).executeColumnar().mapPartitions { batches =>
          val bitmaps = mutable.LinkedHashMap.empty[Long, RoaringBitmapArray]
          val fileInfo = fileInfoBroadcast.value
          // GpuBringBackToHost returns an auto-closing iterator; it owns each host batch.
          batches.foreach { hostBatch =>
            val fileIds = hostBatch.column(0).asInstanceOf[RapidsHostColumnVector]
            val rowIndexes = hostBatch.column(1).asInstanceOf[RapidsHostColumnVector]
            var row = 0
            while (row < hostBatch.numRows()) {
              val fileId = fileIds.getLong(row)
              bitmaps.getOrElseUpdate(fileId, new RoaringBitmapArray())
                .add(rowIndexes.getLong(row))
              row += 1
            }
          }
          val deletionVectorData = bitmaps.iterator.map { case (fileId, bitmap) =>
            val (filePath, deletionVectorId) = fileInfo(fileId)
            bitmap.runOptimize()
            DeletionVectorData(
              filePath,
              deletionVectorId,
              bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
              bitmap.cardinality)
          }
          storeDvs(deletionVectorData)
        }.collect().toSeq
      } else {
        import GroupedRowIndexes.encoder
        val groupedRows = matchedRowsWithIndexes
          .groupBy(col(FileIdColumn))
          .agg(collect_list(col(RowIndexColumn)).as(RowIndexListColumn))
          .as[GroupedRowIndexes]

        groupedRows.mapPartitions { rows =>
          val fileInfo = fileInfoBroadcast.value
          val deletionVectorData = rows.map { row =>
            val (filePath, deletionVectorId) = fileInfo(row.fileId)
            val bitmap = new RoaringBitmapArray()
            row.rowIndexList.foreach(bitmap.add)
            bitmap.runOptimize()
            DeletionVectorData(
              filePath,
              deletionVectorId,
              bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
              bitmap.cardinality)
          }
          storeDvs(deletionVectorData)
        }.collect().toSeq
      }
    } finally {
      fileInfoBroadcast.destroy()
    }

    DMLWithDeletionVectorsHelper.findFilesWithMatchingRows(
      txn,
      nameToAddFileMap,
      storedResults)
  }
}
