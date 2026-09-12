/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from MergeIntoCommand.scala
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

package com.databricks.sql.transaction.tahoe.rapids

import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.collection.mutable

import com.databricks.sql.io.RowIndexFilterType
import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.DeltaOperations.MergePredicate
import com.databricks.sql.transaction.tahoe.actions.{AddCDCFile, AddFile,
  DeletionVectorDescriptor, FileAction}
import com.databricks.sql.transaction.tahoe.commands.{DeltaCommand,
  DMLWithDeletionVectorsHelper}
import com.databricks.sql.transaction.tahoe.commands.cdc.CDCReader._
import com.databricks.sql.transaction.tahoe.commands.merge.MergeIntoMaterializeSource
import com.databricks.sql.transaction.tahoe.deletionvectors.{RoaringBitmapArray,
  RoaringBitmapArrayFormat}
import com.databricks.sql.transaction.tahoe.files.{TahoeBatchFileIndex, TahoeFileIndex}
import com.databricks.sql.transaction.tahoe.rapids.MergeExecutor.{
  totalBytesAndDistinctPartitionValues,
  CDC_TYPE_NOT_CDC_LITERAL,
  FILE_PATH_COL,
  INCR_METRICS_COL,
  INCR_METRICS_FIELD,
  INCR_ROW_COUNT_COL,
  ROW_DROPPED_COL,
  ROW_DROPPED_FIELD,
  SOURCE_ROW_PRESENT_COL,
  SOURCE_ROW_PRESENT_FIELD,
  TARGET_ROW_PRESENT_COL,
  TARGET_ROW_PRESENT_FIELD}
import com.databricks.sql.transaction.tahoe.schema.ImplicitMetadataOperation
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.databricks.sql.transaction.tahoe.util.{AnalysisHelper, DeltaFileOperations}
import com.nvidia.spark.rapids.{BaseExprMeta, GpuOverrides, RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.RapidsConf.DELTA_LOW_SHUFFLE_MERGE_DEL_VECTOR_BROADCAST_THRESHOLD
import com.nvidia.spark.rapids.delta._
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.METADATA_ROW_IDX_COL
import com.nvidia.spark.rapids.shims.FileSourceScanExecMeta
import org.apache.hadoop.conf.Configuration
import org.roaringbitmap.longlong.Roaring64Bitmap

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference,
  CaseWhen, EqualNullSafe, Expression, If, IsNull, Literal, NamedExpression, Not, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeAction, DeltaMergeIntoClause,
  DeltaMergeIntoMatchedClause, DeltaMergeIntoMatchedDeleteClause,
  DeltaMergeIntoMatchedUpdateClause, DeltaMergeIntoNotMatchedBySourceClause,
  DeltaMergeIntoNotMatchedBySourceDeleteClause, DeltaMergeIntoNotMatchedBySourceUpdateClause,
  DeltaMergeIntoNotMatchedClause, DeltaMergeIntoNotMatchedInsertClause, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}

/**
 * GPU version of Delta Lake's low shuffle merge implementation.
 *
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 * Different from the original implementation, it optimized writing touched unmodified target files.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 * the condition and verify that no two source rows match with the same target row.
 * This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 * for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows
 * without copying unmodified rows.
 *
 * Phase 3: Read the touched files again and write new files with unmodified rows in target table,
 * trying to keep its original order and avoid shuffle as much as possible.
 *
 * Phase 4: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source            Source data to merge from
 * @param target            Target table to merge into
 * @param gpuDeltaLog       Delta log to use
 * @param condition         Condition for a source row to match with a target row
 * @param matchedClauses    All info related to matched clauses.
 * @param notMatchedClauses All info related to not matched clause.
 * @param migratedSchema    The final schema of the target - may be changed by schema evolution.
 */
case class GpuLowShuffleMergeCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient catalogTable: Option[CatalogTable],
    @transient targetFileIndex: TahoeFileIndex,
    @transient gpuDeltaLog: GpuDeltaLog,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
    notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
    migratedSchema: Option[StructType],
    trackHighWaterMarks: Set[String] = Set.empty,
    schemaEvolutionEnabled: Boolean = false,
    snapshotAtAnalysis: Option[Snapshot] = None)(
    @transient val rapidsConf: RapidsConf)
  extends LeafRunnableCommand
    with DeltaCommand
    with PredicateHelper
    with AnalysisHelper
    with ImplicitMetadataOperation
    with MergeIntoMaterializeSource {

  import SQLMetrics._

  override val otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

  override val canMergeSchema: Boolean = schemaEvolutionEnabled
  override val canOverwriteSchema: Boolean = false

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient lazy val targetDeltaLog: DeltaLog = gpuDeltaLog.deltaLog

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numSourceRowsInSecondScan" ->
      createMetric(sc, "number of source rows (during repeated scan)"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsMatchedUpdated" -> createMetric(sc, "number of target rows updated when matched"),
    "numTargetRowsMatchedDeleted" -> createMetric(sc, "number of target rows deleted when matched"),
    "numTargetRowsNotMatchedBySourceUpdated" -> createMetric(sc,
      "number of target rows updated when not matched by source"),
    "numTargetRowsNotMatchedBySourceDeleted" -> createMetric(sc,
      "number of target rows deleted when not matched by source"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"),
    "numTargetChangeFilesAdded" ->
      createMetric(sc, "number of change data capture files generated"),
    "numTargetChangeFileBytes" ->
      createMetric(sc, "total size of change data capture files generated"),
    "numTargetBytesBeforeSkipping" -> createMetric(sc, "number of target bytes before skipping"),
    "numTargetBytesAfterSkipping" -> createMetric(sc, "number of target bytes after skipping"),
    "numTargetBytesRemoved" -> createMetric(sc, "number of target bytes removed"),
    "numTargetBytesAdded" -> createMetric(sc, "number of target bytes added"),
    "numTargetPartitionsAfterSkipping" ->
      createMetric(sc, "number of target partitions after skipping"),
    "numTargetPartitionsRemovedFrom" ->
      createMetric(sc, "number of target partitions from which files were removed"),
    "numTargetPartitionsAddedTo" ->
      createMetric(sc, "number of target partitions to which files were added"),
    "executionTimeMs" ->
      createMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createMetric(sc, "time taken to rewrite the matched files"))

  /** Whether this merge statement has only a single insert (NOT MATCHED) clause. */
  protected def isSingleInsertOnly: Boolean = matchedClauses.isEmpty &&
    notMatchedClauses.length == 1

  private[rapids] def mergeSourceDF: DataFrame = getMergeSource.df

  /**
   * Validates that identity-column metadata has not changed since the merge was analyzed and that
   * insert actions do not explicitly populate identity columns that disallow explicit values.
   */
  private def checkIdentityColumnHighWaterMarks(deltaTxn: OptimisticTransaction): Unit = {
    notMatchedClauses.foreach { clause =>
      val schema = deltaTxn.metadata.schema
      if (schema.length != clause.resolvedActions.length) {
        throw new IllegalStateException()
      }
      schema.zip(clause.resolvedActions.map(_.expr)).foreach {
        case (field, expr: GenerateIdentityValues) =>
          val highWaterMark = IdentityColumn.getIdentityInfo(field).highWaterMark
          if (highWaterMark != expr.generator.highWaterMarkOpt) {
            IdentityColumn.logTransactionAbort(deltaTxn.deltaLog)
            throw DeltaErrors.metadataChangedException(None)
          }
        case (field, _) =>
          if (ColumnWithDefaultExprUtils.isIdentityColumn(field) &&
              !IdentityColumn.allowExplicitInsert(field)) {
            throw new IllegalStateException()
          }
      }
    }
  }

  private def runMerge(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.lowshufflemerge") {
      val startTime = System.nanoTime()
      val result = gpuDeltaLog.withNewTransaction(catalogTable, snapshotAtAnalysis) { deltaTxn =>
        if (hasBeenExecuted(deltaTxn, spark)) {
          val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
          SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
          return Seq.empty
        }

        if (target.schema.size != deltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = deltaTxn.metadata.schema)
        }

        TypeWidening.ensureFeatureConsistentlyEnabled(
          protocol = targetFileIndex.protocol,
          metadata = targetFileIndex.metadata,
          otherProtocol = deltaTxn.protocol,
          otherMetadata = deltaTxn.metadata)

        if (canMergeSchema) {
          updateMetadata(
            spark, deltaTxn, migratedSchema.getOrElse(target.schema),
            deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }

        checkIdentityColumnHighWaterMarks(deltaTxn)
        deltaTxn.setTrackHighWaterMarks(trackHighWaterMarks)

        prepareMergeSource(
          spark,
          source,
          condition,
          matchedClauses,
          notMatchedClauses,
          isSingleInsertOnly)

        val executor: MergeExecutor = {
          val context = MergeExecutorContext(this, spark, deltaTxn, rapidsConf)
          if (isSingleInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            new InsertOnlyMergeExecutor(context)
          } else {
            new LowShuffleMergeExecutor(context)
          }
        }

        try {
          val fallback = executor match {
            case lowShuffle: LowShuffleMergeExecutor => lowShuffle.shouldFallback()
            case _ => false
          }
          if (fallback) {
            None
          } else {
            Some(runLowShuffleMerge(spark, startTime, deltaTxn, executor))
          }
        } finally {
          executor.close()
        }
      }

      result match {
        case Some(row) => row
        case None =>
          // We should rollback to normal gpu
          new GpuMergeIntoCommand(source, target, catalogTable, targetFileIndex, gpuDeltaLog,
            condition, matchedClauses, notMatchedClauses, notMatchedBySourceClauses,
            migratedSchema, trackHighWaterMarks, schemaEvolutionEnabled,
            snapshotAtAnalysis)(rapidsConf)
            .run(spark)
      }
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val (materializeSource, _) = shouldMaterializeSource(spark, source, isSingleInsertOnly)
    if (materializeSource) {
      runWithMaterializedSourceLostRetries(spark, targetDeltaLog, metrics, runMerge)
    } else {
      runMerge(spark)
    }
  }


  private def runLowShuffleMerge(
      spark: SparkSession,
      startTime: Long,
      deltaTxn: GpuOptimisticTransactionBase,
      mergeExecutor: MergeExecutor): Seq[Row] = {
    val deltaActions = mergeExecutor.execute()
    // Metrics should be recorded before commit (where they are written to delta logs).
    metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
    deltaTxn.registerSQLMetrics(spark, metrics)

    // This is a best-effort sanity check.
    if (metrics("numSourceRowsInSecondScan").value >= 0 &&
      metrics("numSourceRows").value != metrics("numSourceRowsInSecondScan").value) {
      log.warn(s"Merge source has ${metrics("numSourceRows").value} rows in initial scan but " +
        s"${metrics("numSourceRowsInSecondScan").value} rows in second scan")
      if (conf.getConf(DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED)) {
        throw DeltaErrors.sourceNotDeterministicInMergeException(spark)
      }
    }

    val finalActions = createSetTransaction(spark, targetDeltaLog).toSeq ++ deltaActions
    deltaTxn.commitIfNeeded(
      finalActions,
      DeltaOperations.Merge(
        Option(condition),
        matchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
        // The command shim selects traditional GPU merge when these clauses are present.
        notMatchedBySourcePredicates = Seq.empty[MergePredicate]
      ),
      RowTracking.addPreservedRowTrackingTagIfNotSet(deltaTxn.snapshot))

    // Record metrics
    val stats = GpuMergeStats.fromMergeSQLMetrics(
      metrics,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      deltaTxn.metadata.partitionColumns.nonEmpty)
    recordDeltaEvent(targetDeltaLog, "delta.dml.merge.stats", data = stats)


    spark.sharedState.cacheManager.recacheByPlan(spark, target)

    // This is needed to make the SQL metrics visible in the Spark UI. Also this needs
    // to be outside the recordMergeOperation because this method will update some metric.
    val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
    Seq(Row(metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value +
      metrics("numTargetRowsInserted").value, metrics("numTargetRowsUpdated").value,
      metrics("numTargetRowsDeleted").value, metrics("numTargetRowsInserted").value))
  }

  /**
   * Execute the given `thunk` and return its result while recording the time taken to do it.
   *
   * @param sqlMetricName name of SQL metric to update with the time taken by the thunk
   * @param thunk         the code to execute
   */
  def recordMergeOperation[A](sqlMetricName: String)(thunk: => A): A = {
    val startTimeNs = System.nanoTime()
    val r = thunk
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
    if (sqlMetricName != null && timeTakenMs > 0) {
      metrics(sqlMetricName) += timeTakenMs
    }
    r
  }

  /** Expressions to increment SQL metrics */
  def makeMetricUpdateUDF(name: String, deterministic: Boolean = false): Column = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    var u = DeltaUDF.boolean(new GpuDeltaMetricUpdateUDF(metric))
    if (!deterministic) {
      u = u.asNondeterministic()
    }
    u()
  }

  @nowarn("cat=deprecation")
  def metricUpdateExpr(name: String, deterministic: Boolean): Expression = {
    makeMetricUpdateUDF(name, deterministic).expr
  }
}

/**
 * Context merge execution.
 */
case class MergeExecutorContext(cmd: GpuLowShuffleMergeCommand,
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    rapidsConf: RapidsConf)

trait MergeExecutor extends AnalysisHelper with PredicateHelper with Logging with AutoCloseable {

  val context: MergeExecutorContext


  /**
   * Map to get target output attributes by name.
   * The case sensitivity of the map is set accordingly to Spark configuration.
   */
  @transient private lazy val targetOutputAttributesMap: Map[String, Attribute] = {
    val attrMap: Map[String, Attribute] = context.cmd.target
      .outputSet.view
      .map(attr => attr.name -> attr).toMap
    if (context.cmd.conf.caseSensitiveAnalysis) {
      attrMap
    } else {
      CaseInsensitiveMap(attrMap)
    }
  }

  def execute(): Seq[FileAction]

  override def close(): Unit = {}

  protected def targetOutputCols: Seq[NamedExpression] = {
    context.deltaTxn.metadata.schema.map { col =>
      targetOutputAttributesMap
        .get(col.name)
        .map { a =>
          AttributeReference(col.name, col.dataType, col.nullable)(a.exprId)
        }
        .getOrElse(Alias(Literal(null, col.dataType), col.name)())
    }
  }

  /**
   * Build a DataFrame using the given `files` that has the same output columns (exprIds)
   * as the `target` logical plan, so that existing update/insert expressions can be applied
   * on this new plan.
   */
  protected def buildTargetDFWithFiles(files: Seq[AddFile]): DataFrame = {
    val targetOutputColsMap = {
      val colsMap: Map[String, NamedExpression] = targetOutputCols.view
        .map(col => col.name -> col).toMap
      if (context.cmd.conf.caseSensitiveAnalysis) {
        colsMap
      } else {
        CaseInsensitiveMap(colsMap)
      }
    }

    val plan = {
      // We have to do surgery to use the attributes from `targetOutputCols` to scan the table.
      // In cases of schema evolution, they may not be the same type as the original attributes.
      val original =
        context.deltaTxn.deltaLog.createDataFrame(context.deltaTxn.snapshot, files)
          .queryExecution
          .analyzed
      val transformed = original.transform {
        case r: LogicalRelation =>
          r.copy(
            // We can ignore the new columns which aren't yet AttributeReferences.
            output = targetOutputCols.collect { case a: AttributeReference => a })
      }

      // In case of schema evolution & column mapping, we would also need to rebuild the file
      // format because under column mapping, the reference schema within DeltaParquetFileFormat
      // that is used to populate metadata needs to be updated
      if (context.deltaTxn.metadata.columnMappingMode != NoMapping) {
        val updatedFileFormat = context.deltaTxn.deltaLog.fileFormat(
          context.deltaTxn.deltaLog.unsafeVolatileSnapshot.protocol, context.deltaTxn.metadata)
        DeltaTableUtils.replaceFileFormat(transformed, updatedFileFormat)
      } else {
        transformed
      }
    }

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = targetOutputColsMap.getOrElse(newAttrib.name,
          throw new AnalysisException(
            s"Could not find ${newAttrib.name} among the existing target output " +
              targetOutputCols.mkString(","))).asInstanceOf[AttributeReference]

        if (existingTargetAttrib.exprId == newAttrib.exprId) {
          // It's not valid to alias an expression to its own exprId (this is considered a
          // non-unique exprId by the analyzer), so we just use the attribute directly.
          newAttrib
        } else {
          Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
        }
    }

    Dataset.ofRows(context.spark, Project(aliases, plan))
  }


  /**
   * Repartitions the output DataFrame by the partition columns if table is partitioned
   * and `merge.repartitionBeforeWrite.enabled` is set to true.
   */
  protected def repartitionIfNeeded(df: DataFrame): DataFrame = {
    val partitionColumns = context.deltaTxn.metadata.partitionColumns
    // TODO: We should remove this method and use optimized write instead, see
    // https://github.com/NVIDIA/spark-rapids/issues/10417
    if (partitionColumns.nonEmpty && context.spark.conf.get(DeltaSQLConf
      .MERGE_REPARTITION_BEFORE_WRITE)) {
      df.repartition(partitionColumns.map(col): _*)
    } else {
      df
    }
  }

  protected def sourceDF: DataFrame = {
    // UDF to increment metrics
    val incrSourceRowCountCol = context.cmd.makeMetricUpdateUDF("numSourceRows")
    context.cmd.mergeSourceDF.filter(incrSourceRowCountCol)
  }

  /** Whether this merge statement has no insert (NOT MATCHED) clause. */
  protected def hasNoInserts: Boolean = context.cmd.notMatchedClauses.isEmpty


}

/**
 * This is an optimization of the case when there is no update clause for the merge.
 * We perform an left anti join on the source data to find the rows to be inserted.
 *
 * This will currently only optimize for the case when there is a _single_ notMatchedClause.
 */
class InsertOnlyMergeExecutor(override val context: MergeExecutorContext) extends MergeExecutor {
  override def execute(): Seq[FileAction] = {
    context.cmd.recordMergeOperation(sqlMetricName = "rewriteTimeMs") {

      // UDFs to update metrics
      val incrSourceRowCountCol = context.cmd.makeMetricUpdateUDF("numSourceRows")
      val incrInsertedCountCol = context.cmd.makeMetricUpdateUDF("numTargetRowsInserted")

      val outputColNames = targetOutputCols.map(_.name)
      // we use head here since we know there is only a single notMatchedClause
      val outputExprs = context.cmd.notMatchedClauses.head.resolvedActions.map(_.expr)
      val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
        DFUDFShims.exprToColumn(Alias(expr, name)())
      }

      // source DataFrame
      val sourceDF = context.cmd.mergeSourceDF
        .filter(incrSourceRowCountCol)
        .filter(DFUDFShims.exprToColumn(context.cmd.notMatchedClauses.head.condition
          .getOrElse(Literal.TrueLiteral)))

      // Skip data based on the merge condition
      val conjunctivePredicates = splitConjunctivePredicates(context.cmd.condition)
      val targetOnlyPredicates =
        conjunctivePredicates.filter(_.references.subsetOf(context.cmd.target.outputSet))
      val dataSkippedFiles = context.deltaTxn.filterFiles(targetOnlyPredicates)

      // target DataFrame
      val targetDF = buildTargetDFWithFiles(dataSkippedFiles)

      val insertDf = sourceDF.join(
          targetDF, DFUDFShims.exprToColumn(context.cmd.condition), "leftanti")
        .select(outputCols: _*)
        .filter(incrInsertedCountCol)

      val newFiles = context.deltaTxn.writeFiles(repartitionIfNeeded(insertDf))

      // Update metrics
      context.cmd.metrics("numTargetFilesBeforeSkipping") += context.deltaTxn.snapshot.numOfFiles
      context.cmd.metrics("numTargetBytesBeforeSkipping") += context.deltaTxn.snapshot.sizeInBytes
      val (afterSkippingBytes, afterSkippingPartitions) =
        totalBytesAndDistinctPartitionValues(dataSkippedFiles)
      context.cmd.metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
      context.cmd.metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
      context.cmd.metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
      context.cmd.metrics("numTargetFilesRemoved") += 0
      context.cmd.metrics("numTargetBytesRemoved") += 0
      context.cmd.metrics("numTargetPartitionsRemovedFrom") += 0
      val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
      context.cmd.metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
      context.cmd.metrics("numTargetBytesAdded") += addedBytes
      context.cmd.metrics("numTargetPartitionsAddedTo") += addedPartitions
      newFiles
    }
  }
}


/**
 * This is an optimized algorithm for merge statement, where we avoid shuffling the unmodified
 * target data.
 *
 * The algorithm is as follows:
 * 1. Find touched target files in the target table by joining the source and target data, with
 * collecting joined row identifiers as (`__metadata_file_path`, `__metadata_row_idx`) pairs.
 * 2. Read the touched files again and write new files with updated and/or inserted rows
 * without coping unmodified data from target table, but filtering target table with collected
 * rows mentioned above.
 * 3. Read the touched files again, filtering unmodified rows with collected row identifiers
 * collected in first step, and saving them without shuffle.
 */
class LowShuffleMergeExecutor(override val context: MergeExecutorContext) extends MergeExecutor {

  // We over-count numTargetRowsDeleted when there are multiple matches;
  // this is the amount of the overcount, so we can subtract it to get a correct final metric.
  private var multipleMatchDeleteOnlyOvercount: Option[Long] = None

  // UDFs to update metrics
  private val incrSourceRowCountExpr: Expression = context.cmd
    .metricUpdateExpr("numSourceRowsInSecondScan", deterministic = false)
  private val incrUpdatedCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsUpdated", deterministic = false)
  private val incrUpdatedMatchedCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsMatchedUpdated", deterministic = false)
  private val incrUpdatedNotMatchedBySourceCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsNotMatchedBySourceUpdated", deterministic = false)
  private val incrInsertedCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsInserted", deterministic = false)
  private val incrDeletedCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsDeleted", deterministic = false)
  private val incrDeletedMatchedCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsMatchedDeleted", deterministic = false)
  private val incrDeletedNotMatchedBySourceCountExpr: Expression = context.cmd
    .metricUpdateExpr("numTargetRowsNotMatchedBySourceDeleted", deterministic = false)

  private def updateOutput(resolvedActions: Seq[DeltaMergeAction], incrExpr: Expression)
  : Seq[Expression] = {
    resolvedActions.map(_.expr) :+
      Literal.FalseLiteral :+
      UnresolvedAttribute(TARGET_ROW_PRESENT_COL) :+
      UnresolvedAttribute(SOURCE_ROW_PRESENT_COL) :+
      incrExpr
  }

  private def deleteOutput(incrExpr: Expression): Seq[Expression] = {
    targetOutputCols :+
      TrueLiteral :+
      UnresolvedAttribute(TARGET_ROW_PRESENT_COL) :+
      UnresolvedAttribute(SOURCE_ROW_PRESENT_COL) :+
      incrExpr
  }

  private def insertOutput(resolvedActions: Seq[DeltaMergeAction], incrExpr: Expression)
  : Seq[Expression] = {
    resolvedActions.map(_.expr) :+
      Literal.FalseLiteral :+
      UnresolvedAttribute(TARGET_ROW_PRESENT_COL) :+
      UnresolvedAttribute(SOURCE_ROW_PRESENT_COL) :+
      incrExpr
  }

  private def clauseOutput(clause: DeltaMergeIntoClause): Seq[Expression] = clause match {
    case u: DeltaMergeIntoMatchedUpdateClause =>
      updateOutput(u.resolvedActions, And(incrUpdatedCountExpr, incrUpdatedMatchedCountExpr))
    case _: DeltaMergeIntoMatchedDeleteClause =>
      deleteOutput(And(incrDeletedCountExpr, incrDeletedMatchedCountExpr))
    case i: DeltaMergeIntoNotMatchedInsertClause =>
      insertOutput(i.resolvedActions, incrInsertedCountExpr)
    case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
      updateOutput(u.resolvedActions,
        And(incrUpdatedCountExpr, incrUpdatedNotMatchedBySourceCountExpr))
    case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
      deleteOutput(And(incrDeletedCountExpr, incrDeletedNotMatchedBySourceCountExpr))
  }

  private def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
    // if condition is None, then expression always evaluates to true
    clause.condition.getOrElse(TrueLiteral)
  }

  /**
   * Though low shuffle merge algorithm performs better than traditional merge algorithm in some
   * cases, there are some case we should fallback to traditional merge executor:
   *
   * 1. Low shuffle merge requires GPU file scans for both Databricks' metadata row-index scan and
   * the temporary deletion-vector scan used to retain unmodified rows.
   * 2. The temporary deletion vectors introduce extra overhead, so it may be better to fall back
   * when the changeset is too large.
   */
  def shouldFallback(): Boolean = {
    // Trying to detect if we can execute finding touched files.
    val touchFilePlanOverrideSucceed = verifyGpuPlan(planForFindingTouchedFiles()) { planMeta =>
      def check(meta: SparkPlanMeta[SparkPlan]): Boolean = {
        meta match {
          case scan if scan.isInstanceOf[FileSourceScanExecMeta] &&
              isLowShuffleTargetScan(scan.asInstanceOf[FileSourceScanExecMeta]) =>
            scan.asInstanceOf[FileSourceScanExecMeta].canThisBeReplaced
          case m => m.childPlans.exists(check)
        }
      }

      check(planMeta)
    }
    if (!touchFilePlanOverrideSucceed) {
      logWarning("Unable to override file scan for low shuffle merge for finding touched files " +
        "plan, fallback to tradition merge.")
      return true
    }

    // Trying to detect if we can execute the merge plan.
    val mergePlanOverrideSucceed = verifyGpuPlan(planForMergeExecution(touchedFiles)) { planMeta =>
      var targetScanCount = 0
      var gpuTargetScanCount = 0
      def count(meta: SparkPlanMeta[SparkPlan]): Unit = {
        meta match {
          case scan if scan.isInstanceOf[FileSourceScanExecMeta] &&
              isLowShuffleTargetScan(scan.asInstanceOf[FileSourceScanExecMeta]) =>
            val fileScan = scan.asInstanceOf[FileSourceScanExecMeta]
            targetScanCount += 1
            if (fileScan.canThisBeReplaced) {
              gpuTargetScanCount += 1
            }
          case m => m.childPlans.foreach(count)
        }
      }

      count(planMeta)
      targetScanCount == 2 && gpuTargetScanCount == targetScanCount
    }

    if (!mergePlanOverrideSucceed) {
      logWarning("Unable to override file scan for low shuffle merge for merge plan, fallback to " +
        "tradition merge.")
      return true
    }

    val deletionVectorSize = touchedFiles.values.map(_._1.serializedSizeInBytes()).sum
    val maxDelVectorSize = context.rapidsConf
      .get(DELTA_LOW_SHUFFLE_MERGE_DEL_VECTOR_BROADCAST_THRESHOLD)
    if (deletionVectorSize > maxDelVectorSize) {
      logWarning(
        s"""Low shuffle merge can't be executed because broadcast deletion vector count
           |$deletionVectorSize is large than max value $maxDelVectorSize """.stripMargin)
      return true
    }

    false
  }

  private def isLowShuffleTargetScan(scan: FileSourceScanExecMeta): Boolean = {
    scan.wrapped.relation.location match {
      case index: TahoeBatchFileIndex => index.deltaLog == context.deltaTxn.deltaLog
      case _ => false
    }
  }

  private def verifyGpuPlan(input: DataFrame)(checkPlanMeta: SparkPlanMeta[SparkPlan] => Boolean)
  : Boolean = {
    val overridePlan = GpuOverrides.wrapAndTagPlan(input.queryExecution.sparkPlan,
      context.rapidsConf)
    checkPlanMeta(overridePlan)
  }

  override def execute(): Seq[FileAction] = {
    val newFiles = context.cmd.withStatusCode("DELTA",
      s"Rewriting ${touchedFiles.size} files and saving modified data") {
      val df = planForMergeExecution(touchedFiles)
      context.deltaTxn.writeFiles(df)
    }

    // Update metrics
    val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
    context.cmd.metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
    context.cmd.metrics("numTargetChangeFilesAdded") += newFiles.count(_.isInstanceOf[AddCDCFile])
    context.cmd.metrics("numTargetChangeFileBytes") += newFiles.collect {
        case f: AddCDCFile => f.size
      }
      .sum
    context.cmd.metrics("numTargetBytesAdded") += addedBytes
    context.cmd.metrics("numTargetPartitionsAddedTo") += addedPartitions

    if (multipleMatchDeleteOnlyOvercount.isDefined) {
      // Compensate for counting duplicates during the query.
      val actualRowsDeleted =
        context.cmd.metrics("numTargetRowsDeleted").value - multipleMatchDeleteOnlyOvercount.get
      assert(actualRowsDeleted >= 0)
      context.cmd.metrics("numTargetRowsDeleted").set(actualRowsDeleted)
    }

    touchedFiles.values.map(_._2).map(_.remove).toSeq ++ newFiles
  }

  private lazy val dataSkippedFiles: Seq[AddFile] = {
    // Skip data based on the merge condition
    val targetOnlyPredicates = splitConjunctivePredicates(context.cmd.condition)
      .filter(_.references.subsetOf(context.cmd.target.outputSet))
    context.deltaTxn.filterFiles(targetOnlyPredicates)
  }

  private lazy val dataSkippedTargetDF: DataFrame = {
    addRowIndexMetaColumn(dataSkippedFiles)
  }

  private lazy val touchedFiles: Map[String, (Roaring64Bitmap, AddFile)] = this.findTouchedFiles()

  private def planForFindingTouchedFiles(): DataFrame = {

    // Apply inner join to between source and target using the merge condition to find matches
    // In addition, we attach two columns
    // - METADATA_ROW_IDX column to identify target row in file
    // - FILE_PATH_COL the target file name the row is from to later identify the files touched
    // by matched rows
    val targetDF = dataSkippedTargetDF.withColumn(FILE_PATH_COL, input_file_name())

    sourceDF.join(targetDF, DFUDFShims.exprToColumn(context.cmd.condition), "inner")
  }

  private def planForMergeExecution(touchedFiles: Map[String, (Roaring64Bitmap, AddFile)])
  : DataFrame = {
    getModifiedDF(touchedFiles).unionAll(getUnmodifiedDF(touchedFiles))
  }

  /**
   * Find the target table files that contain the rows that satisfy the merge condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the merge condition.
   */
  private def findTouchedFiles(): Map[String, (Roaring64Bitmap, AddFile)] =
    context.cmd.recordMergeOperation(sqlMetricName = "scanTimeMs") {
      context.spark.udf.register("row_index_set", udaf(RoaringBitmapUDAF))
      // Process the matches from the inner join to record touched files and find multiple matches
      val collectTouchedFiles = planForFindingTouchedFiles()
        .select(col(FILE_PATH_COL), col(METADATA_ROW_IDX_COL))
        .groupBy(FILE_PATH_COL)
        .agg(
          expr(s"row_index_set($METADATA_ROW_IDX_COL) as row_idxes"),
          count("*").as("count"))
        .collect().map(row => {
          val filename = row.getAs[String](FILE_PATH_COL)
          val rowIdxSet = row.getAs[RoaringBitmapWrapper]("row_idxes").inner
          val count = row.getAs[Long]("count")
          (filename, (rowIdxSet, count))
        })
        .toMap

      val duplicateCount = {
        val distinctMatchedRowCounts = collectTouchedFiles.values
          .map(_._1.getLongCardinality).sum
        val allMatchedRowCounts = collectTouchedFiles.values.map(_._2).sum
        allMatchedRowCounts - distinctMatchedRowCounts
      }

      val hasMultipleMatches = duplicateCount > 0

      // Throw error if multiple matches are ambiguous or cannot be computed correctly.
      val canBeComputedUnambiguously = {
        // Multiple matches are not ambiguous when there is only one unconditional delete as
        // all the matched row pairs in the 2nd join in `writeAllChanges` will get deleted.
        val isUnconditionalDelete = context.cmd.matchedClauses.headOption match {
          case Some(DeltaMergeIntoMatchedDeleteClause(None)) => true
          case _ => false
        }
        context.cmd.matchedClauses.size == 1 && isUnconditionalDelete
      }

      if (hasMultipleMatches && !canBeComputedUnambiguously) {
        throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(context.spark)
      }

      if (hasMultipleMatches) {
        // This is only allowed for delete-only queries.
        // This query will count the duplicates for numTargetRowsDeleted in Job 2,
        // because we count matches after the join and not just the target rows.
        // We have to compensate for this by subtracting the duplicates later,
        // so we need to record them here.
        multipleMatchDeleteOnlyOvercount = Some(duplicateCount)
      }

      // Get the AddFiles using the touched file names.
      val touchedFileNames = collectTouchedFiles.keys.toSeq

      val nameToAddFileMap = context.cmd.generateCandidateFileMap(
        context.cmd.targetDeltaLog.dataPath,
        dataSkippedFiles)

      val touchedAddFiles = touchedFileNames.map(f =>
          context.cmd.getTouchedFile(context.cmd.targetDeltaLog.dataPath, f, nameToAddFileMap))
        .map(f => (DeltaFileOperations
          .absolutePath(context.cmd.targetDeltaLog.dataPath.toString, f.path)
          .toString, f)).toMap

      // When the target table is empty, and the optimizer optimized away the join entirely
      // numSourceRows will be incorrectly 0.
      // We need to scan the source table once to get the correct
      // metric here.
      if (context.cmd.metrics("numSourceRows").value == 0 &&
        (dataSkippedFiles.isEmpty || dataSkippedTargetDF.take(1).isEmpty)) {
        val numSourceRows = sourceDF.count()
        context.cmd.metrics("numSourceRows").set(numSourceRows)
      }

      // Update metrics
      context.cmd.metrics("numTargetFilesBeforeSkipping") += context.deltaTxn.snapshot.numOfFiles
      context.cmd.metrics("numTargetBytesBeforeSkipping") += context.deltaTxn.snapshot.sizeInBytes
      val (afterSkippingBytes, afterSkippingPartitions) =
        totalBytesAndDistinctPartitionValues(dataSkippedFiles)
      context.cmd.metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
      context.cmd.metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
      context.cmd.metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
      val (removedBytes, removedPartitions) =
        totalBytesAndDistinctPartitionValues(touchedAddFiles.values.toSeq)
      context.cmd.metrics("numTargetFilesRemoved") += touchedAddFiles.size
      context.cmd.metrics("numTargetBytesRemoved") += removedBytes
      context.cmd.metrics("numTargetPartitionsRemovedFrom") += removedPartitions

      collectTouchedFiles.map(kv => (kv._1, (kv._2._1, touchedAddFiles(kv._1))))
    }


  /**
   * Uses Databricks' deletion-vector scan preparation to expose the file metadata column, then
   * copies its file-relative row index into
   * [[GpuDeltaParquetFileFormatUtils.METADATA_ROW_IDX_COL]].
   */
  private def addRowIndexMetaColumn(files: Seq[AddFile]): DataFrame = {
    val fileIndex = context.deltaTxn.deltaLog.createDataFrame(context.deltaTxn.snapshot, files)
      .queryExecution.analyzed.collectFirst {
        case relation: LogicalRelation
            if relation.relation.isInstanceOf[HadoopFsRelation] &&
              relation.relation.asInstanceOf[HadoopFsRelation]
                .location.isInstanceOf[TahoeFileIndex] =>
          relation.relation.asInstanceOf[HadoopFsRelation]
            .location.asInstanceOf[TahoeFileIndex]
      }.getOrElse {
        throw new IllegalStateException("Unable to find the Delta file index for low shuffle merge")
      }
    DMLWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
      context.spark, context.cmd.target, fileIndex)
      .withColumn(METADATA_ROW_IDX_COL, col("_metadata.row_index"))
  }

  private def uniqueColumnName(base: String, existing: Seq[String]): String = {
    val resolver = context.cmd.conf.resolver
    Iterator.from(0)
        .map(i => if (i == 0) base else s"$base$i")
        .find(candidate => !existing.exists(name => resolver(name, candidate)))
        .get
  }

  private def addMergeJoinProcessor(
      joinedPlan: LogicalPlan,
      outputRowSchema: StructType,
      targetRowHasNoMatch: Expression,
      sourceRowHasNoMatch: Expression,
      matchedConditions: Seq[Expression],
      matchedOutputs: Seq[Seq[Seq[Expression]]],
      notMatchedConditions: Seq[Expression],
      notMatchedOutputs: Seq[Seq[Seq[Expression]]],
      notMatchedBySourceConditions: Seq[Expression],
      notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
      noopCopyOutput: Seq[Expression],
      deleteRowOutput: Seq[Expression],
      rowDroppedColumnIndex: Int): Dataset[Row] = {
    def wrap(e: Expression): BaseExprMeta[Expression] = {
      GpuOverrides.wrapExpr(e, context.rapidsConf, None)
    }

    val targetRowHasNoMatchMeta = wrap(targetRowHasNoMatch)
    val sourceRowHasNoMatchMeta = wrap(sourceRowHasNoMatch)
    val matchedConditionsMetas = matchedConditions.map(wrap)
    val matchedOutputsMetas = matchedOutputs.map(_.map(_.map(wrap)))
    val notMatchedConditionsMetas = notMatchedConditions.map(wrap)
    val notMatchedOutputsMetas = notMatchedOutputs.map(_.map(_.map(wrap)))
    val notMatchedBySourceConditionsMetas = notMatchedBySourceConditions.map(wrap)
    val notMatchedBySourceOutputsMetas = notMatchedBySourceOutputs.map(_.map(_.map(wrap)))
    val noopCopyOutputMetas = noopCopyOutput.map(wrap)
    val deleteRowOutputMetas = deleteRowOutput.map(wrap)
    val allMetas = Seq(targetRowHasNoMatchMeta, sourceRowHasNoMatchMeta) ++
        matchedConditionsMetas ++ matchedOutputsMetas.flatten.flatten ++
        notMatchedConditionsMetas ++ notMatchedOutputsMetas.flatten.flatten ++
        notMatchedBySourceConditionsMetas ++ notMatchedBySourceOutputsMetas.flatten.flatten ++
        noopCopyOutputMetas ++ deleteRowOutputMetas
    allMetas.foreach(_.tagForGpu())
    val canReplace = allMetas.forall(_.canExprTreeBeReplaced) &&
        context.rapidsConf.isOperatorEnabled(
          "spark.rapids.sql.exec.RapidsProcessDeltaMergeJoinExec", false, false)
    if (context.rapidsConf.shouldExplainAll || (context.rapidsConf.shouldExplain && !canReplace)) {
      val exprExplains = allMetas.map(_.explain(context.rapidsConf.shouldExplainAll))
      val execWorkInfo = if (canReplace) {
        "will run on GPU"
      } else {
        "cannot run on GPU because not all merge processing expressions can be replaced"
      }
      logWarning(s"<RapidsProcessDeltaMergeJoinExec> $execWorkInfo:\n" +
          s"  ${exprExplains.mkString("  ")}")
    }

    if (canReplace) {
      val processedJoinPlan = RapidsProcessDeltaMergeJoin(
        joinedPlan,
        toAttributes(outputRowSchema),
        targetRowHasNoMatch = targetRowHasNoMatch,
        sourceRowHasNoMatch = sourceRowHasNoMatch,
        matchedConditions = matchedConditions,
        matchedOutputs = matchedOutputs,
        notMatchedConditions = notMatchedConditions,
        notMatchedOutputs = notMatchedOutputs,
        notMatchedBySourceConditions = notMatchedBySourceConditions,
        notMatchedBySourceOutputs = notMatchedBySourceOutputs,
        noopCopyOutput = noopCopyOutput,
        deleteRowOutput = deleteRowOutput,
        rowDroppedColumnIndex = Some(rowDroppedColumnIndex))
      Dataset.ofRows(context.spark, processedJoinPlan)
    } else {
      val joinedRowEncoder = ExpressionEncoder(RowEncoder.encoderFor(joinedPlan.schema))
      val outputRowEncoder = ExpressionEncoder(RowEncoder.encoderFor(outputRowSchema))
          .resolveAndBind()
      val processor = new GpuMergeIntoCommand.JoinedRowProcessor(
        targetRowHasNoMatch = targetRowHasNoMatch,
        sourceRowHasNoMatch = sourceRowHasNoMatch,
        matchedConditions = matchedConditions,
        matchedOutputs = matchedOutputs,
        notMatchedConditions = notMatchedConditions,
        notMatchedOutputs = notMatchedOutputs,
        notMatchedBySourceConditions = notMatchedBySourceConditions,
        notMatchedBySourceOutputs = notMatchedBySourceOutputs,
        noopCopyOutput = noopCopyOutput,
        deleteRowOutput = deleteRowOutput,
        joinedAttributes = joinedPlan.output,
        joinedRowEncoder = joinedRowEncoder,
        outputRowEncoder = outputRowEncoder,
        rowDroppedColumnIndex = rowDroppedColumnIndex)
      Dataset.ofRows(context.spark, joinedPlan)
          .mapPartitions(processor.processPartition)(outputRowEncoder)
    }
  }

  /** Generate both rewritten table rows and explicit change-data-feed rows. */
  private def getModifiedDFWithCdf(
      touchedFiles: Map[String, (Roaring64Bitmap, AddFile)]): DataFrame = {
    import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}

    val isDeleteWithDuplicateMatches = multipleMatchDeleteOnlyOvercount.nonEmpty
    val sourcePlanDF = this.sourceDF
    val targetPlanDF = buildTargetDFWithFiles(touchedFiles.values.map(_._2).toSeq)
    val userColumns = sourcePlanDF.columns.toSeq ++ targetPlanDF.columns.toSeq
    val sourceRowPresentCol = uniqueColumnName(SOURCE_ROW_PRESENT_COL, userColumns)
    val targetRowPresentCol = uniqueColumnName(
      TARGET_ROW_PRESENT_COL, userColumns :+ sourceRowPresentCol)
    val taken = userColumns ++ Seq(sourceRowPresentCol, targetRowPresentCol)
    val targetRowIdCol = uniqueColumnName(GpuMergeIntoCommand.TARGET_ROW_ID_COL, taken)
    val sourceRowIdCol = uniqueColumnName(
      GpuMergeIntoCommand.SOURCE_ROW_ID_COL, taken :+ targetRowIdCol)

    var sourceDF = sourcePlanDF.withColumn(
      sourceRowPresentCol, DFUDFShims.exprToColumn(incrSourceRowCountExpr))
    var targetDF = targetPlanDF.withColumn(targetRowPresentCol, lit(true))
    if (isDeleteWithDuplicateMatches) {
      targetDF = targetDF.withColumn(targetRowIdCol, monotonically_increasing_id())
      if (context.cmd.notMatchedClauses.nonEmpty) {
        sourceDF = sourceDF.withColumn(sourceRowIdCol, monotonically_increasing_id())
      }
    }

    val joinType = if (hasNoInserts &&
        context.spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
      "inner"
    } else {
      "leftOuter"
    }
    val joinedPlan = sourceDF
        .join(targetDF, DFUDFShims.exprToColumn(context.cmd.condition), joinType)
        .queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      tryResolveReferencesForExpressions(context.spark, exprs, joinedPlan)
    }

    val incrUpdatedCount = context.cmd.metricUpdateExpr(
      "numTargetRowsUpdated", deterministic = true)
    val incrUpdatedMatchedCount = context.cmd.metricUpdateExpr(
      "numTargetRowsMatchedUpdated", deterministic = true)
    val incrInsertedCount = context.cmd.metricUpdateExpr(
      "numTargetRowsInserted", deterministic = true)
    val incrDeletedCount = context.cmd.metricUpdateExpr(
      "numTargetRowsDeleted", deterministic = true)
    val incrDeletedMatchedCount = context.cmd.metricUpdateExpr(
      "numTargetRowsMatchedDeleted", deterministic = true)

    var cdfTargetOutputCols: Seq[Expression] = targetOutputCols
    var outputRowSchema = context.deltaTxn.metadata.schema
    if (isDeleteWithDuplicateMatches) {
      cdfTargetOutputCols = cdfTargetOutputCols :+ UnresolvedAttribute(targetRowIdCol)
      outputRowSchema = outputRowSchema.add(targetRowIdCol, LongType)
      if (context.cmd.notMatchedClauses.nonEmpty) {
        cdfTargetOutputCols = cdfTargetOutputCols :+
            Alias(Literal(null, LongType), sourceRowIdCol)()
        outputRowSchema = outputRowSchema.add(sourceRowIdCol, LongType)
      }
    }
    val rowDroppedColumnIndex = cdfTargetOutputCols.size
    outputRowSchema = outputRowSchema
        .add(ROW_DROPPED_COL, BooleanType)
        .add(INCR_ROW_COUNT_COL, BooleanType)
        .add(CDC_TYPE_COLUMN_NAME, StringType)

    val materializedValues = mutable.ArrayBuffer[NamedExpression]()
    def materializeNonDeterministic(
        exprs: Seq[Expression],
        takesClause: Expression): Seq[Expression] = exprs.map {
      case e if !e.deterministic =>
        val resolved = resolveOnJoinedPlan(Seq(e)).head
        val existing = joinedPlan.output.map(_.name) ++ materializedValues.map(_.name)
        val alias = Alias(If(takesClause, resolved, Literal(null, resolved.dataType)),
          uniqueColumnName(GpuMergeIntoCommand.NON_DETERMINISTIC_VALUE_COL, existing))()
        materializedValues += alias
        alias.toAttribute
      case e => e
    }

    def clauseRouting(
        rowKind: Expression,
        conditions: Seq[Expression],
        index: Int): Expression = {
      val earlierNotTaken = conditions.take(index)
          .map(condition => Not(EqualNullSafe(condition, TrueLiteral)))
      (rowKind +: earlierNotTaken :+ EqualNullSafe(conditions(index), TrueLiteral)).reduce(And)
    }

    def updateOutput(
        updateExprs: Seq[Expression],
        incrMetricExpr: Expression): Seq[Seq[Expression]] = {
      val mainDataOutput = updateExprs :+ FalseLiteral :+ incrMetricExpr :+
          CDC_TYPE_NOT_CDC_LITERAL
      val preImageOutput = cdfTargetOutputCols :+ FalseLiteral :+ TrueLiteral :+
          Literal(CDC_TYPE_UPDATE_PREIMAGE)
      val postImageOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+
          Literal(CDC_TYPE_UPDATE_POSTIMAGE)
      Seq(mainDataOutput, preImageOutput, postImageOutput).map(resolveOnJoinedPlan)
    }

    def deleteOutput(incrMetricExpr: Expression): Seq[Seq[Expression]] = {
      val mainDataOutput = cdfTargetOutputCols :+ TrueLiteral :+ incrMetricExpr :+
          CDC_TYPE_NOT_CDC_LITERAL
      val deleteCdfOutput = cdfTargetOutputCols :+ FalseLiteral :+ TrueLiteral :+
          Literal(CDC_TYPE_DELETE)
      Seq(mainDataOutput, deleteCdfOutput).map(resolveOnJoinedPlan)
    }

    def insertOutput(
        insertExprs: Seq[Expression],
        incrMetricExpr: Expression): Seq[Seq[Expression]] = {
      val outputExprs = if (isDeleteWithDuplicateMatches) {
        insertExprs :+ Alias(Literal(null, LongType), targetRowIdCol)() :+
            UnresolvedAttribute(sourceRowIdCol)
      } else {
        insertExprs
      }
      val mainDataOutput = resolveOnJoinedPlan(
        outputExprs :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL)
      val insertCdfOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+
          Literal(CDC_TYPE_INSERT)
      Seq(mainDataOutput, insertCdfOutput)
    }

    def clauseOutput(clause: DeltaMergeIntoClause, routing: Expression)
        : Seq[Seq[Expression]] = clause match {
      case u: DeltaMergeIntoMatchedUpdateClause =>
        updateOutput(materializeNonDeterministic(u.resolvedActions.map(_.expr), routing),
          And(incrUpdatedCount, incrUpdatedMatchedCount))
      case _: DeltaMergeIntoMatchedDeleteClause =>
        deleteOutput(And(incrDeletedCount, incrDeletedMatchedCount))
      case i: DeltaMergeIntoNotMatchedInsertClause =>
        insertOutput(materializeNonDeterministic(i.resolvedActions.map(_.expr), routing),
          incrInsertedCount)
      case other =>
        throw new IllegalArgumentException(s"Unsupported low-shuffle merge clause: " +
            other.getClass.getName)
    }

    def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
      resolveOnJoinedPlan(Seq(clause.condition.getOrElse(TrueLiteral))).head
    }

    val targetRowHasNoMatch = resolveOnJoinedPlan(
      Seq(IsNull(UnresolvedAttribute(sourceRowPresentCol)))).head
    val sourceRowHasNoMatch = resolveOnJoinedPlan(
      Seq(IsNull(UnresolvedAttribute(targetRowPresentCol)))).head
    val matchedRow = And(Not(targetRowHasNoMatch), Not(sourceRowHasNoMatch))
    val matchedConditions = context.cmd.matchedClauses.map(clauseCondition)
    val matchedOutputs = context.cmd.matchedClauses.zipWithIndex.map { case (clause, index) =>
      clauseOutput(clause, clauseRouting(matchedRow, matchedConditions, index))
    }
    val notMatchedConditions = context.cmd.notMatchedClauses.map(clauseCondition)
    val notMatchedOutputs = context.cmd.notMatchedClauses.zipWithIndex.map {
      case (clause, index) =>
        clauseOutput(clause, clauseRouting(sourceRowHasNoMatch, notMatchedConditions, index))
    }
    val noopCopyOutput = resolveOnJoinedPlan(
      cdfTargetOutputCols :+ FalseLiteral :+ TrueLiteral :+ CDC_TYPE_NOT_CDC_LITERAL)
    val deleteRowOutput = resolveOnJoinedPlan(
      cdfTargetOutputCols :+ TrueLiteral :+ TrueLiteral :+ CDC_TYPE_NOT_CDC_LITERAL)
    val processorInputPlan = if (materializedValues.isEmpty) {
      joinedPlan
    } else {
      Project(joinedPlan.output ++ materializedValues, joinedPlan)
    }

    var outputDF = addMergeJoinProcessor(
      processorInputPlan,
      outputRowSchema,
      targetRowHasNoMatch,
      sourceRowHasNoMatch,
      matchedConditions,
      matchedOutputs,
      notMatchedConditions,
      notMatchedOutputs,
      Seq.empty,
      Seq.empty,
      noopCopyOutput,
      deleteRowOutput,
      rowDroppedColumnIndex)

    if (isDeleteWithDuplicateMatches) {
      val columnsToDedupeBy = if (context.cmd.notMatchedClauses.nonEmpty) {
        Seq(targetRowIdCol, sourceRowIdCol, CDC_TYPE_COLUMN_NAME)
      } else {
        Seq(targetRowIdCol)
      }
      outputDF = outputDF.dropDuplicates(columnsToDedupeBy)
    }

    val outputAttributes = outputDF.queryExecution.analyzed.output
    outputDF = Seq(ROW_DROPPED_COL, INCR_ROW_COUNT_COL)
        .flatMap(name => outputAttributes.reverse.find(_.name == name))
        .foldLeft(outputDF)((df, attr) => df.drop(DFUDFShims.exprToColumn(attr)))
    if (isDeleteWithDuplicateMatches) {
      outputDF = outputDF.drop(targetRowIdCol, sourceRowIdCol)
    }
    repartitionIfNeeded(outputDF)
  }

  /**
   * Generate a plan by calculating modified rows. It's computed by joining source and target
   * tables, where target table has been filtered by (`__metadata_file_name`,
   * `__metadata_row_idx`) pairs collected in first step.
   *
   * Schema of `modifiedDF`:
   *
   * targetSchema + ROW_DROPPED_COL + TARGET_ROW_PRESENT_COL +
   * SOURCE_ROW_PRESENT_COL + INCR_METRICS_COL
   * INCR_METRICS_COL
   *
   * It consists of several parts:
   *
   * 1. Unmatched source rows which are inserted
   * 2. Unmatched source rows which are deleted
   * 3. Target rows which are updated
   * 4. Target rows which are deleted
   */
  private def getModifiedDF(touchedFiles: Map[String, (Roaring64Bitmap, AddFile)]): DataFrame = {
    if (DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(context.deltaTxn.metadata)) {
      return getModifiedDFWithCdf(touchedFiles)
    }

    val sourceDF = this.sourceDF
      .withColumn(SOURCE_ROW_PRESENT_COL, DFUDFShims.exprToColumn(incrSourceRowCountExpr))

    // The join itself selects touched target rows, so this pass can scan the touched files without
    // applying the temporary deletion vectors used by the unmodified-row pass.
    val targetDF = buildTargetDFWithFiles(touchedFiles.values.map(_._2).toSeq)
      .withColumn(TARGET_ROW_PRESENT_COL, lit(true))

    val joinedDF = {
      val joinType = if (hasNoInserts &&
        context.spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
        "inner"
      } else {
        "leftOuter"
      }
      sourceDF.join(targetDF, DFUDFShims.exprToColumn(context.cmd.condition), joinType)
    }

    val modifiedRowsSchema = context.deltaTxn.metadata.schema
      .add(ROW_DROPPED_FIELD)
      .add(TARGET_ROW_PRESENT_FIELD.copy(nullable = true))
      .add(SOURCE_ROW_PRESENT_FIELD.copy(nullable = true))
      .add(INCR_METRICS_FIELD)

    // Here we generate a case when statement to handle all cases:
    // CASE
    // WHEN <source matched>
    //      CASE WHEN <matched condition 1>
    //            <matched expression 1>
    //           WHEN <matched condition 2>
    //            <matched expression 2>
    //           ELSE
    //            <matched else expression>
    // WHEN <source not matched>
    //      CASE WHEN <source not matched condition 1>
    //            <not matched expression 1>
    //           WHEN <matched condition 2>
    //            <not matched expression 2>
    //           ELSE
    //            <not matched else expression>
    // END

    val notMatchedConditions = context.cmd.notMatchedClauses.map(clauseCondition)
    val notMatchedExpr = {
      val deletedNotMatchedRow = {
        targetOutputCols :+
          Literal.TrueLiteral :+
          Literal.FalseLiteral :+
          Literal(null) :+
          Literal.TrueLiteral
      }
      if (context.cmd.notMatchedClauses.isEmpty) {
        // If there no `WHEN NOT MATCHED` clause, we should just delete not matched row
        deletedNotMatchedRow
      } else {
        val notMatchedOutputs = context.cmd.notMatchedClauses.map(clauseOutput)
        modifiedRowsSchema.zipWithIndex.map {
          case (_, idx) =>
            CaseWhen(notMatchedConditions.zip(notMatchedOutputs.map(_(idx))),
              deletedNotMatchedRow(idx))
        }
      }
    }

    val matchedConditions = context.cmd.matchedClauses.map(clauseCondition)
    val matchedOutputs = context.cmd.matchedClauses.map(clauseOutput)
    val matchedExprs = {
      val notMatchedRow = {
        targetOutputCols :+
          Literal.FalseLiteral :+
          Literal.TrueLiteral :+
          Literal(null) :+
          Literal.TrueLiteral
      }
      if (context.cmd.matchedClauses.isEmpty) {
        // If there is not matched clause, this is insert only, we should delete this row.
        notMatchedRow
      } else {
        modifiedRowsSchema.zipWithIndex.map {
          case (_, idx) =>
            CaseWhen(matchedConditions.zip(matchedOutputs.map(_(idx))),
              notMatchedRow(idx))
        }
      }
    }

    val sourceRowHasNoMatch = IsNull(UnresolvedAttribute(TARGET_ROW_PRESENT_COL))

    val modifiedCols = modifiedRowsSchema.zipWithIndex.map { case (col, idx) =>
      val caseWhen = CaseWhen(
        Seq(sourceRowHasNoMatch -> notMatchedExpr(idx)),
        matchedExprs(idx))
      DFUDFShims.exprToColumn(Alias(caseWhen, col.name)())
    }

    val modifiedDF = {

      // Make this a udf to avoid catalyst to be too aggressive to even remove the join!
      val noopRowDroppedCol = udf(new GpuDeltaNoopUDF()).apply(!col(ROW_DROPPED_COL))

      val modifiedDF = joinedDF.select(modifiedCols: _*)
        // This will not filter anything since they always return true, but we need to avoid
        // catalyst from optimizing these udf
        .filter(noopRowDroppedCol && col(INCR_METRICS_COL))
        .drop(ROW_DROPPED_COL, INCR_METRICS_COL, TARGET_ROW_PRESENT_COL, SOURCE_ROW_PRESENT_COL)

      repartitionIfNeeded(modifiedDF)
    }

    modifiedDF
  }

  private def getUnmodifiedDF(touchedFiles: Map[String, (Roaring64Bitmap, AddFile)]): DataFrame = {
    val hadoopConf = context.deltaTxn.deltaLog.newDeltaHadoopConf()
    val tablePath = context.deltaTxn.deltaLog.dataPath.toString
    val filesWithTemporaryDVs = touchedFiles.values.map { case (bitmap, addFile) =>
      addFile.copy(deletionVector = MergeExecutor.toDeletionVector(
        bitmap,
        Option(addFile.deletionVector),
        hadoopConf,
        tablePath))
    }.toSeq
    val unmodifiedDF = buildTargetDFWithFiles(filesWithTemporaryDVs)
    if (DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(context.deltaTxn.metadata)) {
      unmodifiedDF.withColumn(
        CDC_TYPE_COLUMN_NAME, DFUDFShims.exprToColumn(CDC_TYPE_NOT_CDC_LITERAL))
    } else {
      unmodifiedDF
    }
  }
}


object MergeExecutor {

  /**
   * Spark UI will track all normal accumulators along with Spark tasks to show them on Web UI.
   * However, the accumulator used by `MergeIntoCommand` can store a very large value since it
   * tracks all files that need to be rewritten. We should ask Spark UI to not remember it,
   * otherwise, the UI data may consume lots of memory. Hence, we use the prefix `internal.metrics.`
   * to make this accumulator become an internal accumulator, so that it will not be tracked by
   * Spark UI.
   */
  val TOUCHED_FILES_ACCUM_NAME = "internal.metrics.MergeIntoDelta.touchedFiles"

  val ROW_ID_COL = "_row_id_"
  val FILE_PATH_COL: String = GpuDeltaParquetFileFormatUtils.FILE_PATH_COL
  val SOURCE_ROW_PRESENT_COL: String = "_source_row_present_"
  val SOURCE_ROW_PRESENT_FIELD: StructField = StructField(SOURCE_ROW_PRESENT_COL, BooleanType,
    nullable = false)
  val TARGET_ROW_PRESENT_COL: String = "_target_row_present_"
  val TARGET_ROW_PRESENT_FIELD: StructField = StructField(TARGET_ROW_PRESENT_COL, BooleanType,
    nullable = false)
  val ROW_DROPPED_COL: String = GpuDeltaMergeConstants.ROW_DROPPED_COL
  val ROW_DROPPED_FIELD: StructField = StructField(ROW_DROPPED_COL, BooleanType, nullable = false)
  val INCR_METRICS_COL: String = "_incr_metrics_"
  val INCR_METRICS_FIELD: StructField = StructField(INCR_METRICS_COL, BooleanType, nullable = false)
  val INCR_ROW_COUNT_COL: String = "_incr_row_count_"

  // Some Delta versions use Literal(null) which translates to a literal of NullType instead
  // of the Literal(null, StringType) which is needed, so using a fixed version here
  // rather than the version from Delta Lake.
  val CDC_TYPE_NOT_CDC_LITERAL: Literal = Literal(null, StringType)

  private[rapids] def toDeletionVector(
      bitmap: Roaring64Bitmap,
      existing: Option[DeletionVectorDescriptor],
      hadoopConf: Configuration,
      tablePath: String): DeletionVectorDescriptor = {
    val combined = existing.map { descriptor =>
      RapidsDeletionVectors.loadScalaBitmap(
        hadoopConf,
        Some(descriptor.serializeToBase64()),
        Some(RowIndexFilterType.IF_CONTAINED),
        None,
        tablePath)
    }.getOrElse(new RoaringBitmapArray())
    val touchedIndexes = bitmap.getLongIterator
    while (touchedIndexes.hasNext) {
      combined.add(touchedIndexes.next())
    }
    combined.runOptimize()
    DeletionVectorDescriptor.inlineInLog(
      combined.serializeAsByteArray(RoaringBitmapArrayFormat.Portable), combined.cardinality)
  }

  /** Count the number of distinct partition values among the AddFiles in the given set. */
  def totalBytesAndDistinctPartitionValues(files: Seq[FileAction]): (Long, Int) = {
    val distinctValues = new mutable.HashSet[Map[String, String]]()
    var bytes = 0L
    val iter = files.collect { case a: AddFile => a }.iterator
    while (iter.hasNext) {
      val file = iter.next()
      distinctValues += file.partitionValues
      bytes += file.size
    }
    // If the only distinct value map is an empty map, then it must be an unpartitioned table.
    // Return 0 in that case.
    val numDistinctValues =
      if (distinctValues.size == 1 && distinctValues.head.isEmpty) 0 else distinctValues.size
    (bytes, numDistinctValues)
  }
}
