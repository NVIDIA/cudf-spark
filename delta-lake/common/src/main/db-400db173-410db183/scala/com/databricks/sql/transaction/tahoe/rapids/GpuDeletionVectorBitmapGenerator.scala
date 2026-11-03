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

import ai.rapids.cudf.HostColumnVector
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
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.{broadcast, col, input_file_name, lit, raise_error, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private[rapids] object GpuDeletionVectorBitmapGenerator {
  private val TargetFilesPerBitmapPartition = 64
  private val FilePathColumn = "__delta_gpu_dv_file_path"
  private val FileIdColumn = "__delta_gpu_dv_file_id"
  private val RowIndexColumn = "__delta_gpu_dv_row_index"
  private val DvDataClassName =
    "com.databricks.sql.transaction.tahoe.commands.DeletionVectorData"
  private val DvWriterClassName =
    "com.databricks.sql.transaction.tahoe.commands.DeletionVectorWriter$"

  private def isolatedSession(spark: SparkSession): SparkSession = {
    val isolated = spark.newSession()
    spark.sessionState.conf.getAllConfs.foreach { case (key, value) =>
      isolated.sessionState.conf.setConfString(key, value)
    }
    isolated
  }

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
      require(index >= 0, s"Invalid DML physical row index: $index")
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
   * input_file_name() reflects the file of the batch being processed. Python UDFs may merge batches
   * from different files and subqueries may plan as shuffled joins, so conditions containing them
   * must be evaluated after the file path has been attached to each row.
   */
  private def canFilterBeforeFileName(condition: Column): Boolean = {
    DFUDFShims.columnToExpr(condition).find {
      case _: PythonUDF | _: SubqueryExpression => true
      case _ => false
    }.isEmpty
  }

  /**
   * DELETE and UPDATE keep their predicates and file-ID lookup columnar on GPU. Each scan task
   * copies only matched file IDs and row indexes to host and builds compact partial bitmaps before
   * the shuffle.
   * DBR's writer still merges any old DV and persists the replacement DV.
   */
  def findTouchedFilesWithGpuBitmap(
      spark: SparkSession,
      txn: OptimisticTransaction,
      hasReadableDVs: Boolean,
      targetScan: GpuTargetScan,
      condition: Column,
      nameToAddFileMap: Map[String, AddFile]): Seq[TouchedFileWithDV] = {
    // This scan's broadcast join must be planned without AQE. A thread-local SQLConf override
    // does not control QueryExecution's session-owned planner on DBR, so use an isolated session.
    val scanSpark = isolatedSession(spark)
    scanSpark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)
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
    // Keep the small file-ID lookup local instead of scanning an ExternalRDD on the broadcast
    // side of the matched-row query.
    val filePathAttr = AttributeReference(FilePathColumn, StringType, nullable = false)()
    val fileIdAttr = AttributeReference(FileIdColumn, LongType, nullable = false)()
    val fileIdRows = fileMetadata.indices.map { id =>
      InternalRow(UTF8String.fromString(fileMetadata(id)._1), id.toLong)
    }
    val fileIds = TrampolineConnectShims.createDataFrame(
      scanSpark.asInstanceOf[TrampolineConnectShims.SparkSession],
      LocalRelation(Seq(filePathAttr, fileIdAttr), fileIdRows))

    val filteredTargetDf = if (canFilterBeforeFileName(condition)) {
      // Keep the predicate below input_file_name(), whose non-deterministic projection otherwise
      // prevents Spark from pushing the predicate into the Parquet scan.
      targetScan.dataFrameWithoutFilePath.filter(condition)
        .withColumn(targetScan.filePathColumnName, input_file_name())
    } else {
      targetScan.dataFrame.filter(condition)
    }
    val gpuTargetDf = DMLWithDeletionVectorsHelperShims.withGpuExecutionContext(
      scanSpark, filteredTargetDf)
    val matchedRows = gpuTargetDf
      .select(col(targetScan.filePathColumnName).as(FilePathColumn),
        targetScan.rowIndexColumn.as(RowIndexColumn))
      .join(broadcast(fileIds), Seq(FilePathColumn), "left_outer")
      .select(col(FileIdColumn), col(RowIndexColumn))
    def gpuColumnarPlan(plan: SparkPlan): Option[GpuExec] = plan match {
      case adaptive: AdaptiveSparkPlanExec => gpuColumnarPlan(adaptive.executedPlan)
      case GpuColumnarToRowExec(child, _) => gpuColumnarPlan(child)
      case gpu: GpuExec if gpu.supportsColumnar => Some(gpu)
      case _ => None
    }
    val queryExecution = matchedRows.queryExecution
    val columnarPlan = gpuColumnarPlan(queryExecution.executedPlan)

    columnarPlan match {
      case Some(plan) =>
        val storedResults = SQLExecution.withNewExecutionId(
          queryExecution, Some("DML partial deletion vectors")) {
          val fileCount = fileMetadata.length
          val partialBitmaps = plan.executeColumnar().mapPartitions { batches =>
            val bitmaps = mutable.HashMap.empty[Long, PartialBitmap]
            batches.foreach { batch =>
              var hostColumns = Array.empty[HostColumnVector]
              val rowCount = try {
                withResource(batch) { gpuBatch =>
                  val columns = GpuColumnVector.extractBases(gpuBatch)
                  require(columns.length == 2,
                    "DML bitmap scan must return file ID and row index")
                  hostColumns = columns.safeMap(_.copyToHost())
                  gpuBatch.numRows()
                }
              } catch {
                case t: Throwable =>
                  hostColumns.safeClose(t)
                  throw t
              }
              // Only host columns remain when another task can acquire the GPU semaphore.
              withResource(hostColumns) { copiedColumns =>
                GpuSemaphore.releaseIfNecessary(TaskContext.get())
                val ids = copiedColumns(0)
                val indexes = copiedColumns(1)
                for (row <- 0 until rowCount) {
                  require(!ids.isNull(row),
                    "A matched DML file was absent from the candidate-file map")
                  require(!indexes.isNull(row),
                    "The DML scan produced a null physical row index")
                  val fileId = ids.getLong(row)
                  require(fileId >= 0 && fileId < fileCount,
                    s"Invalid DML file ID: $fileId")
                  bitmaps.getOrElseUpdate(fileId, new PartialBitmap()).add(
                    indexes.getLong(row))
                }
              }
            }
            bitmaps.iterator.map { case (fileId, bitmap) => fileId -> bitmap.toBytes }
          }
          // The shuffle carries one compact bitmap per touched file, not the matched rows.
          // Avoid hundreds of near-empty reduce tasks while retaining enough parallelism to
          // persist larger sets of deletion vectors.
          val filePartitions = math.ceil(
            fileMetadata.length.toDouble / TargetFilesPerBitmapPartition).toInt
          val bitmapPartitions = math.min(spark.sessionState.conf.numShufflePartitions,
            math.max(spark.sparkContext.defaultParallelism, filePartitions))
          val rowIndexData = partialBitmaps.groupByKey(bitmapPartitions)
            .map { case (fileId, parts) =>
              val bitmap = new RoaringBitmapArray()
              parts.foreach(bytes => bitmap.or(RoaringBitmapArray.readFrom(bytes)))
              bitmap.runOptimize()
              val (_, path, existingDv) = fileMetadata(fileId.toInt)
              newDvData(path, existingDv,
                bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
                bitmap.cardinality)
            }
          val prefixLength = DeltaUtils.getRandomPrefixLength(txn.metadata)
          GpuDeltaCpuFallback.withRapidsDisabled(scanSpark) {
            scanSpark.createDataset(rowIndexData)(dvDataEncoder)
              .mapPartitions(dvWriter(scanSpark, txn.deltaLog.newDeltaHadoopConf(),
                txn.deltaLog.dataPath, prefixLength))(DeletionVectorResult.encoder)
              .collect().toSeq
          }
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
    // Every row reaching the condition comes from the target scan. Only the GPU reader fills the
    // row index, and DBR's bitmap aggregation silently skips null indexes, which would leave
    // matched rows undeleted. Fail instead of writing a wrong result.
    val guardedCondition = when(targetScan.rowIndexColumn.isNull,
      raise_error(lit("The DML target scan did not produce physical row indexes, so it must " +
        "run on the GPU"))).otherwise(condition)
    val storedResults = DeletionVectorBitmapGenerator
      .buildRowIndexSetsForFilesMatchingCondition(
        spark,
        txn,
        candidatesHaveDVs,
        gpuTargetDf,
        candidateFiles,
        DFUDFShims.columnToExpr(guardedCondition),
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
    // The snapshot's DataFrames are bound to the command's session, so RAPIDS must be disabled
    // there; an isolated session would not stop those joins from being planned on the GPU.
    val (actions, metrics) = GpuDeltaCpuFallback.withRapidsDisabled(spark) {
      DMLProcessUnmodifiedDataShim.processUnmodifiedData(spark, touchedFiles, txn)
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
