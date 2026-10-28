/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from WriteIntoDelta.scala
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

package org.apache.spark.sql.delta.rapids

import scala.collection.mutable

import com.nvidia.spark.rapids._

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaErrors, DeltaLog,
  DeltaOperations, DeltaOptions, DeltaTableUtils, IdentityColumn, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.{DeleteCommand, DeleteMetric, WriteIntoDelta}
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.types.StructType

/**
 * Shared implementation of the GPU WriteIntoDeltaLike contract for Delta 4.0 through 4.3.
 */
abstract class GpuWriteIntoDeltaBase(
    val gpuDeltaLog: GpuDeltaLog,
    val cpuWrite: WriteIntoDelta)
    extends LeafRunnableCommand
      with ImplicitMetadataOperation {

  // The self-type deliberately keeps the version-specific GpuWriteIntoDeltaLike contract off this
  // base class's JVM interface list. Delta 4.1 through 4.3 adapters share this base class name in
  // the aggregate JAR, while their WriteIntoDeltaLike interfaces are not binary compatible.
  self: GpuWriteIntoDeltaLike =>

  override protected val canMergeSchema: Boolean = cpuWrite.options.canMergeSchema

  protected def isOverwriteOperation: Boolean = cpuWrite.mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    cpuWrite.options.canOverwriteSchema && isOverwriteOperation &&
      !isInsertAtomicReplaceOp

  /** Version-specific classification and expression resolution for atomic replacements. */
  protected def isInsertAtomicReplaceOp: Boolean = cpuWrite.options.replaceWhere.isDefined

  protected def isReplaceOnOrUsing: Boolean = false

  protected def validateAtomicReplaceOptions(): Unit = {}

  protected def getAtomicReplaceExpressions(
      spark: SqlSparkSession,
      txn: OptimisticTransaction,
      replaceOnDataColsEnabled: Boolean): (Option[Seq[Expression]], Boolean) = {
    var containsDataFilters = false
    val replaceWhere = cpuWrite.options.replaceWhere.flatMap { replace =>
      val parsed = parsePredicates(spark, replace)
      if (replaceOnDataColsEnabled) {
        val (metadataPredicates, dataFilters) = DeltaTableUtils.splitMetadataAndDataPredicates(
          parsed.head, txn.metadata.partitionColumns, spark)
        if (cpuWrite.options.rearrangeOnly && dataFilters.nonEmpty) {
          throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters.mkString(","))
        }
        containsDataFilters = dataFilters.nonEmpty
        Some(metadataPredicates ++ dataFilters)
      } else if (isOverwriteOperation) {
        verifyPartitionPredicates(spark, txn.metadata.partitionColumns, parsed)
        Some(parsed)
      } else {
        None
      }
    }
    (replaceWhere, containsDataFilters)
  }

  protected def constraintPredicates(
      spark: SqlSparkSession,
      condition: Seq[Expression]): Seq[Expression] = condition

  protected def constraintViolation(e: InvariantViolationException): Throwable =
    DeltaErrors.replaceWhereMismatchException(cpuWrite.options.replaceWhere.get, e)

  protected def observeCommit[T](body: => T): T = body

  protected def observeDelete[T](body: => T): T = body

  protected def observeInsert[T](body: => T): T = body

  protected def recordDeleteMetrics(deleteMetrics: DeleteMetric): Unit = {
    DeltaRuntimeShim33x.emitDeltaEvent(
      deltaLog, "delta.dml.write.removeFiles.stats", data = deleteMetrics)
  }

  protected def prepareDeleteCommand(
      spark: ClassicSparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): (ClassicSparkSession, DeleteCommand) = {
    val relation = LogicalRelation(
      txn.deltaLog.createRelation(snapshotToUseOpt = Some(txn.snapshot),
        catalogTableOpt = txn.catalogTable))
    val command = spark.sessionState.analyzer.execute(
      DeleteFromTable(relation, condition.reduceOption(And).getOrElse(Literal.TrueLiteral)))
    spark.sessionState.analyzer.checkAnalysis(command)
    (spark, command.asInstanceOf[DeleteCommand])
  }

  /**
   * Build the Delta commit metadata object for this runtime's `WriteIntoDelta` implementation.
   *
   * Delta 4.1 extends `DeltaOperations.Write` with additional overwrite/schema flags while Delta
   * 4.0 still uses the shorter constructor, so subclasses supply the version-specific metadata
   * object here.
   */
  protected def buildCommitMetadata: DeltaOperations.Operation

  /**
   * Recreate the GPU write command around an updated CPU `WriteIntoDelta` while preserving the
   * concrete GPU subclass used by the current Delta runtime.
   */
  protected def copyWithCpuWrite(newCpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike

  /** Select files removed by overwrite using the selected Delta runtime API. */
  protected def getFilesToRemoveForOverwrite(
      txn: OptimisticTransaction,
      addFiles: Seq[AddFile],
      useDynamicPartitionOverwriteMode: Boolean): Seq[Action]

  /** Register write metrics using the APIs provided by the selected Delta runtime. */
  protected def registerWriteOperationMetrics(
      sparkSession: SqlSparkSession,
      txn: OptimisticTransaction,
      newFiles: Seq[FileAction],
      deletedFiles: Seq[Action],
      replaceWhere: Option[Seq[Expression]],
      replaceOnDataColsEnabled: Boolean): Unit

  override def run(sparkSession: SqlSparkSession): Seq[Row] = {
    gpuDeltaLog.withNewTransaction(cpuWrite.catalogTableOpt) { txn =>
      if (hasBeenExecuted(txn, sparkSession, Some(cpuWrite.options))) {
        return Seq.empty
      }

      val taggedCommitData = writeAndReturnCommitData(
        txn, sparkSession, None, false
      )
      observeCommit {
        txn.commitIfNeeded(
          taggedCommitData.actions,
          buildCommitMetadata,
          tags = taggedCommitData.stringTags)
      }
    }
    Seq.empty
  }

  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SqlSparkSession,
      clusterBySpecOpt: Option[ClusterBySpec],
      isTableReplace: Boolean): TaggedCommitData[Action] = {
    import org.apache.spark.sql.delta.implicits._

    require(txn.isInstanceOf[GpuOptimisticTransactionBase], "Only gpu transaction supported")

    validateAtomicReplaceOptions()

    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (cpuWrite.mode == SaveMode.ErrorIfExists) {
        throw DeltaErrors.pathAlreadyExistsException(deltaLog.dataPath)
      } else if (cpuWrite.mode == SaveMode.Ignore) {
        return TaggedCommitData.empty
      } else if (cpuWrite.mode == SaveMode.Overwrite) {
        DeltaRuntimeShim33x.assertRemovable(txn.snapshot)
      }
    }
    val finalClusterBySpecOpt =
      if (cpuWrite.mode == SaveMode.Append || (isOverwriteOperation && isInsertAtomicReplaceOp)) {
        clusterBySpecOpt.foreach { clusterBySpec =>
          ClusteredTableUtils.validateClusteringColumnsInSnapshot(txn.snapshot, clusterBySpec)
        }
        // Append and atomic partial overwrites cannot update the clustering columns.
        None
      } else {
        clusterBySpecOpt
      }
    val rearrangeOnly = cpuWrite.options.rearrangeOnly
    val charPadding = sparkSession.sessionState.conf.getConf(SQLConf.READ_SIDE_CHAR_PADDING)
    val charAsVarchar = sparkSession.sessionState.conf.getConf(SQLConf.CHAR_AS_VARCHAR)
    val dataSchema = if (!charAsVarchar && charPadding) {
      data.schema
    } else {
      // If READ_SIDE_CHAR_PADDING is not enabled, CHAR type is the same as VARCHAR. The change
      // below makes DESC TABLE to show VARCHAR instead of CHAR.
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(
        CharVarcharUtils.replaceCharWithVarchar(CharVarcharUtils.getRawSchema(data.schema))
          .asInstanceOf[StructType])
    }
    val finalSchema = cpuWrite.schemaInCatalog.getOrElse(dataSchema)
    if (txn.metadata.schemaString != null) {
      // In cases other than CTAS (INSERT INTO, DataFrame write), block if values are provided for
      // GENERATED ALWAYS AS IDENTITY columns.
      IdentityColumn.blockExplicitIdentityColumnInsert(
        txn.metadata.schema,
        data.queryExecution.analyzed)
    }
    // We need to cache this canUpdateMetadata before calling updateMetadata, as that will update
    // it to true. This is unavoidable as getNewDomainMetadata uses information generated by
    // updateMetadata, so it needs to be run after that.
    val canUpdateMetadata = txn.canUpdateMetadata
    updateMetadata(data.sparkSession, txn, finalSchema,
      cpuWrite.partitionColumns, configuration, isOverwriteOperation, rearrangeOnly
    )
    val newDomainMetadata = getNewDomainMetadata(
      txn,
      canUpdateMetadata,
      isReplacingTable = isOverwriteOperation && !isInsertAtomicReplaceOp,
      finalClusterBySpecOpt
    )

    val replaceOnDataColsEnabled =
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED)

    val useDynamicPartitionOverwriteMode = {
      if (txn.metadata.partitionColumns.isEmpty) {
        // We ignore dynamic partition overwrite mode for non-partitioned tables
        false
      } else if (isTableReplace) {
        // A replace table command should always replace the table, not just some partitions.
        false
      } else if (cpuWrite.options.replaceWhere.nonEmpty) {
        if (cpuWrite.options.partitionOverwriteModeInOptions &&
          cpuWrite.options.isDynamicPartitionOverwriteMode) {
          // replaceWhere and dynamic partition overwrite conflict because they both specify which
          // data to overwrite. We throw an error when:
          // 1. replaceWhere is provided in a DataFrameWriter option
          // 2. partitionOverwriteMode is set to "dynamic" in a DataFrameWriter option
          throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
        } else {
          // If replaceWhere is provided, we do not use dynamic partition overwrite, even if it's
          // enabled in the spark session configuration, since generally query-specific configs take
          // precedence over session configs
          false
        }
      } else {
        cpuWrite.options.isDynamicPartitionOverwriteMode
      }
    }

    if (useDynamicPartitionOverwriteMode && canOverwriteSchema) {
      throw DeltaErrors.overwriteSchemaUsedWithDynamicPartitionOverwrite()
    }

    val (atomicReplaceExpressions, containsDataFilters) =
      getAtomicReplaceExpressions(sparkSession, txn, replaceOnDataColsEnabled)

    if (txn.readVersion < 0) {
      // Initialize the log path
      deltaLog.createLogDirectoriesIfNotExists()
    }

    val (newFiles, addFiles, deletedFiles) = (cpuWrite.mode, atomicReplaceExpressions) match {
      case (SaveMode.Overwrite, Some(predicates))
          if cpuWrite.options.replaceWhere.isDefined && !replaceOnDataColsEnabled =>
        // fall back to match on partition cols only when replaceArbitrary is disabled.
        val newFiles = txn.writeFiles(data, Some(cpuWrite.options))
        val addFiles = newFiles.collect { case a: AddFile => a }
        // Check to make sure the files we wrote out were actually valid.
        val partitionPredicates = constraintPredicates(sparkSession, predicates)
        val matchingFiles = DeltaLog.filterFileList(
            txn.metadata.partitionSchema, addFiles.toDF(sparkSession), partitionPredicates)
          .as[AddFile].collect()
        val invalidFiles = addFiles.toSet -- matchingFiles
        if (invalidFiles.nonEmpty) {
          val badPartitions = invalidFiles
            .map(_.partitionValues)
            .map { _.map { case (k, v) => s"$k=$v" }.mkString("/") }
            .mkString(", ")
          throw DeltaErrors.replaceWhereMismatchException(cpuWrite.options.replaceWhere.get,
            badPartitions)
        }
        (newFiles, addFiles, txn.filterFiles(partitionPredicates).map(_.remove))
      case (SaveMode.Overwrite, Some(condition)) if txn.snapshot.version >= 0 =>
        val constraints = if (isReplaceOnOrUsing) {
          Seq.empty
        } else {
          extractConstraints(sparkSession, constraintPredicates(sparkSession, condition))
        }

        val removedFileActions = observeDelete {
          removeFiles(sparkSession.asInstanceOf[ClassicSparkSession], txn, condition)
        }
        val cdcExistsInRemoveOp = removedFileActions.exists(_.isInstanceOf[AddCDCFile])

        // The above REMOVE will not produce explicit CDF data when persistent DV is enabled.
        // Therefore here we need to decide whether to produce explicit CDF for INSERTs, because
        // the CDF protocol requires either (i) all CDF data are generated explicitly as AddCDCFile,
        // or (ii) all CDF data can be deduced from [[AddFile]] and [[RemoveFile]].
        val dataToWrite =
          if (containsDataFilters &&
            CDCReader.isCDCEnabledOnTable(txn.metadata, sparkSession) &&
            (isReplaceOnOrUsing ||
              sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_WITH_CDF_ENABLED)) &&
            cdcExistsInRemoveOp) {
            var dataWithDefaultExprs = data
            // Add identity columns if they are not in `data`.
            // Column names for which we will track identity column high water marks.
            val trackHighWaterMarks = mutable.Set.empty[String]
            val topLevelOutputNames = CaseInsensitiveMap(data.schema.map(f => f.name -> f).toMap)
            val selectExprs = txn.metadata.schema.map { f =>
              if (ColumnWithDefaultExprUtils.isIdentityColumn(f) &&
                !topLevelOutputNames.contains(f.name)) {
                // Track high water marks for generated IDENTITY values.
                trackHighWaterMarks += f.name
                IdentityColumn.createIdentityColumnGenerationExprAsColumn(f)
              } else {
                SchemaUtils.fieldToColumn(f).alias(f.name)
              }
            }
            if (trackHighWaterMarks.nonEmpty) {
              txn.setTrackHighWaterMarks(trackHighWaterMarks.toSet)
              dataWithDefaultExprs = data.select(selectExprs: _*)
            }

            // pack new data and cdc data into an array of structs and unpack them into rows
            // to share values in outputCols on both branches, avoiding re-evaluating
            // non-deterministic expression twice.
            val outputCols = dataWithDefaultExprs.schema.map(SchemaUtils.fieldToColumn(_))
            val insertCols = outputCols :+
              lit(CDCReader.CDC_TYPE_INSERT).as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val insertDataCols = outputCols :+
              DFUDFShims.exprToColumn(CDCReader.CDC_TYPE_NOT_CDC)
                .as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val packedInsertsExpr = array(
              struct(insertCols: _*),
              struct(insertDataCols: _*)
            )

            dataWithDefaultExprs
              .select(explode(packedInsertsExpr).as("packedData"))
              .select(
                (dataWithDefaultExprs.schema.map(_.name) :+ CDCReader.CDC_TYPE_COLUMN_NAME)
                  .map { n => col(s"packedData.`$n`").as(n) }: _*)
          } else {
            data
          }
        val newFiles = observeInsert {
          try txn.writeFiles(dataToWrite, Some(cpuWrite.options), constraints) catch {
            case e: InvariantViolationException =>
              throw constraintViolation(e)
          }
        }
        (newFiles,
          newFiles.collect { case a: AddFile => a },
          removedFileActions)
      case (SaveMode.Overwrite, None) =>
        val newFiles = writeFiles(
          txn, data, cpuWrite.options
        )
        val addFiles = newFiles.collect { case a: AddFile => a }
        val deletedFiles = getFilesToRemoveForOverwrite(
          txn, addFiles, useDynamicPartitionOverwriteMode)
        (newFiles, addFiles, deletedFiles)
      case _ =>
        val newFiles = writeFiles(
          txn, data, cpuWrite.options
        )
        (newFiles, newFiles.collect { case a: AddFile => a }, Nil)
    }

    registerWriteOperationMetrics(
      sparkSession, txn, newFiles, deletedFiles, atomicReplaceExpressions, replaceOnDataColsEnabled)

    val fileActions = if (rearrangeOnly) {
      val changeFiles = newFiles.collect { case c: AddCDCFile => c }
      if (changeFiles.nonEmpty) {
        throw DeltaErrors.unexpectedChangeFilesFound(changeFiles.mkString("\n"))
      }
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map {
          case add: AddFile => add.copy(dataChange = !rearrangeOnly)
          case remove: RemoveFile => remove.copy(dataChange = !rearrangeOnly)
          case other => throw DeltaErrors.illegalFilesFound(other.toString)
        }
    } else {
      newFiles ++ deletedFiles
    }
    val allActions =
      newDomainMetadata ++
        createSetTransaction(sparkSession, deltaLog, Some(cpuWrite.options)).toSeq ++
        fileActions

    // DV updates remove and re-add the same physical file. They do not count as file removals.
    val readdedPaths = deletedFiles.collect { case add: AddFile => add.path }.toSet
    val removedFilesCount = fileActions.count {
      case remove: RemoveFile => !readdedPaths.contains(remove.path)
      case _ => false
    }
    val m: SQLMetric = SQLMetrics.createMetric(
      sparkSession.sparkContext, "number of files removed.")
    m.set(removedFilesCount)
    txn.registerSQLMetrics(sparkSession, Map("numRemovedFiles" -> m))
    TaggedCommitData(allActions)
  }

  private def writeFiles(
      txn: OptimisticTransaction,
      data: DataFrame,
      options: DeltaOptions
  ): Seq[FileAction] = {
    txn.writeFiles(data, Some(options))
  }

  private def removeFiles(
      spark: ClassicSparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): Seq[Action] = {
    val (deleteSpark, command) = prepareDeleteCommand(spark, txn, condition)
    val deleteCommandMeta = GpuOverrides
      .wrapRunnableCmd(command, gpuDeltaLog.rapidsConf, None)
    deleteCommandMeta.initReasons()
    deleteCommandMeta.tagSelfForGpu()

    val deleteResult = if (deleteCommandMeta.canThisBeReplaced) {
      val gpuTxn = txn.asInstanceOf[GpuOptimisticTransactionBase]
      deleteCommandMeta.convertToGpu().asInstanceOf[GpuDeleteCommand]
        .performDelete(deleteSpark, gpuTxn.deltaLog, gpuTxn)
    } else {
      command.performDelete(deleteSpark, txn.deltaLog, txn)
    }
    val deleteActions = deleteResult._1
    val deleteMetrics = deleteResult._2

    recordDeleteMetrics(deleteMetrics)
    deleteActions
  }

  override def withNewWriterConfiguration(updatedConfiguration: Map[String, String])
  : GpuWriteIntoDeltaLike = {
    val newCpuWrite = cpuWrite.copy(configuration = updatedConfiguration)
    copyWithCpuWrite(newCpuWrite)
  }

  override val configuration: Map[String, String] = cpuWrite.configuration
  override val data: DataFrame = cpuWrite.data
  override val deltaLog: DeltaLog = gpuDeltaLog.deltaLog
}
