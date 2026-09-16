/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids.delta24x

import java.net.URI
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuOverrides, RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.RapidsConf.DELTA_LOW_SHUFFLE_MERGE_DEL_VECTOR_BROADCAST_THRESHOLD
import com.nvidia.spark.rapids.delta._
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils._
import com.nvidia.spark.rapids.shims.FileSourceScanExecMeta
import org.roaringbitmap.longlong.Roaring64Bitmap

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CaseWhen, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeAction, DeltaMergeIntoClause, DeltaMergeIntoMatchedClause, DeltaMergeIntoMatchedDeleteClause, DeltaMergeIntoMatchedUpdateClause, DeltaMergeIntoNotMatchedBySourceClause, DeltaMergeIntoNotMatchedBySourceDeleteClause, DeltaMergeIntoNotMatchedBySourceUpdateClause, DeltaMergeIntoNotMatchedClause, DeltaMergeIntoNotMatchedInsertClause, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaParquetFileFormat, DeltaTableUtils, DeltaUDF, NoMapping, OptimisticTransaction, RowIndexFilterType}
import org.apache.spark.sql.delta.DeltaOperations.MergePredicate
import org.apache.spark.sql.delta.DeltaParquetFileFormat.DeletionVectorDescriptorWithFilterType
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, DeletionVectorDescriptor, FileAction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuOptimisticTransactionBase}
import org.apache.spark.sql.delta.rapids.delta24x.MergeExecutor.{toDeletionVector, totalBytesAndDistinctPartitionValues, INCR_METRICS_COL, INCR_METRICS_FIELD, ROW_DROPPED_COL, ROW_DROPPED_FIELD, SOURCE_ROW_PRESENT_COL, SOURCE_ROW_PRESENT_FIELD, TARGET_ROW_PRESENT_COL, TARGET_ROW_PRESENT_FIELD}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{AnalysisHelper, DeltaFileOperations}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
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
    @transient gpuDeltaLog: GpuDeltaLog,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
    notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
    migratedSchema: Option[StructType])(
    @transient val rapidsConf: RapidsConf)
  extends LeafRunnableCommand
    with DeltaCommand with PredicateHelper with AnalysisHelper with ImplicitMetadataOperation {

  import SQLMetrics._

  override val otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

  override val canMergeSchema: Boolean = conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private[delta] lazy val targetDeltaLog: DeltaLog = gpuDeltaLog.deltaLog

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

  override def run(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.lowshufflemerge") {
      val startTime = System.nanoTime()
      val result = gpuDeltaLog.withNewTransaction { deltaTxn =>
        if (target.schema.size != deltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = deltaTxn.metadata.schema)
        }

        if (canMergeSchema) {
          updateMetadata(
            spark, deltaTxn, migratedSchema.getOrElse(target.schema),
            deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }


        val (executor, fallback) = {
          val context = MergeExecutorContext(this, spark, deltaTxn, rapidsConf)
          if (isSingleInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            (new InsertOnlyMergeExecutor(context), false)
          } else {
            val executor = new LowShuffleMergeExecutor(context)
            (executor, executor.shouldFallback())
          }
        }

        if (fallback) {
          None
        } else {
          Some(runLowShuffleMerge(spark, startTime, deltaTxn, executor))
        }
      }

      result match {
        case Some(row) => row
        case None =>
          // We should rollback to normal gpu
          new GpuMergeIntoCommand(source, target, gpuDeltaLog, condition, matchedClauses,
            notMatchedClauses, notMatchedBySourceClauses, migratedSchema)(rapidsConf)
            .run(spark)
      }
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

    deltaTxn.commit(
      deltaActions,
      DeltaOperations.Merge(
        Option(condition),
        matchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
        // We do not support notMatchedBySourcePredicates yet and fall back to CPU
        // See https://github.com/NVIDIA/spark-rapids/issues/8415
        notMatchedBySourcePredicates = Seq.empty[MergePredicate]
      ))

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
  private[delta] def recordMergeOperation[A](sqlMetricName: String)(thunk: => A): A = {
    val startTimeNs = System.nanoTime()
    val r = thunk
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
    if (sqlMetricName != null && timeTakenMs > 0) {
      metrics(sqlMetricName) += timeTakenMs
    }
    r
  }

  /** Expressions to increment SQL metrics */
  private[delta] def makeMetricUpdateUDF(name: String, deterministic: Boolean = false)
  : Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    var u = DeltaUDF.boolean(new GpuDeltaMetricUpdateUDF(metric))
    if (!deterministic) {
      u = u.asNondeterministic()
    }
    u.apply().expr
  }
}

/**
 * Context merge execution.
 */
case class MergeExecutorContext(cmd: GpuLowShuffleMergeCommand,
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    rapidsConf: RapidsConf)

trait MergeExecutor extends AnalysisHelper with PredicateHelper with Logging {

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

  protected def targetOutputCols: Seq[NamedExpression] = {
    context.deltaTxn.metadata.schema.map { col =>
      targetOutputAttributesMap
        .get(col.name)
        .map { a =>
          AttributeReference(col.name, col.dataType, col.nullable)(a.exprId)
        }
        .getOrElse(Alias(Literal(null), col.name)())
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
        case LogicalRelation(base, _, catalogTbl, isStreaming) =>
          LogicalRelation(
            base,
            // We can ignore the new columns which aren't yet AttributeReferences.
            targetOutputCols.collect { case a: AttributeReference => a },
            catalogTbl,
            isStreaming)
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
    val incrSourceRowCountExpr = context.cmd.makeMetricUpdateUDF("numSourceRows")
    Dataset.ofRows(context.spark, context.cmd.source)
      .filter(new Column(incrSourceRowCountExpr))
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
      val incrSourceRowCountExpr = context.cmd.makeMetricUpdateUDF("numSourceRows")
      val incrInsertedCountExpr = context.cmd.makeMetricUpdateUDF("numTargetRowsInserted")

      val outputColNames = targetOutputCols.map(_.name)
      // we use head here since we know there is only a single notMatchedClause
      val outputExprs = context.cmd.notMatchedClauses.head.resolvedActions.map(_.expr)
      val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
        new Column(Alias(expr, name)())
      }

      // source DataFrame
      val sourceDF = Dataset.ofRows(context.spark, context.cmd.source)
        .filter(new Column(incrSourceRowCountExpr))
        .filter(new Column(context.cmd.notMatchedClauses.head.condition
          .getOrElse(Literal.TrueLiteral)))

      // Skip data based on the merge condition
      val conjunctivePredicates = splitConjunctivePredicates(context.cmd.condition)
      val targetOnlyPredicates =
        conjunctivePredicates.filter(_.references.subsetOf(context.cmd.target.outputSet))
      val dataSkippedFiles = context.deltaTxn.filterFiles(targetOnlyPredicates)

      // target DataFrame
      val targetDF = buildTargetDFWithFiles(dataSkippedFiles)

      val insertDf = sourceDF.join(targetDF, new Column(context.cmd.condition), "leftanti")
        .select(outputCols: _*)
        .filter(new Column(incrInsertedCountExpr))

      val newFiles = context.deltaTxn
        .writeFiles(repartitionIfNeeded(insertDf,
        ))

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
  private val incrSourceRowCountExpr: Expression = context.cmd.
    makeMetricUpdateUDF("numSourceRowsInSecondScan")
  private val incrUpdatedCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsUpdated")
  private val incrUpdatedMatchedCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsMatchedUpdated")
  private val incrUpdatedNotMatchedBySourceCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceUpdated")
  private val incrInsertedCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsInserted")
  private val incrDeletedCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsDeleted")
  private val incrDeletedMatchedCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsMatchedDeleted")
  private val incrDeletedNotMatchedBySourceCountExpr: Expression = context.cmd
    .makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceDeleted")

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
   * 1. Low shuffle merge algorithm requires generating metadata columns such as
   * [[METADATA_ROW_IDX_COL]], [[METADATA_ROW_DEL_COL]], which only implemented on
   * [[org.apache.spark.sql.rapids.GpuFileSourceScanExec]]. That means we need to fallback to
   * this normal executor when [[org.apache.spark.sql.rapids.GpuFileSourceScanExec]] is disabled
   * for some reason.
   * 2. Low shuffle merge algorithm currently needs to broadcast deletion vector, which may
   * introduce extra overhead. It maybe better to fallback to this algorithm when the changeset
   * it too large.
   */
  private[delta] def shouldFallback(): Boolean = {
    // Trying to detect if we can execute finding touched files.
    val touchFilePlanOverrideSucceed = verifyGpuPlan(planForFindingTouchedFiles()) { planMeta =>
      def check(meta: SparkPlanMeta[SparkPlan]): Boolean = {
        meta match {
          case scan if scan.isInstanceOf[FileSourceScanExecMeta] => scan
            .asInstanceOf[FileSourceScanExecMeta]
            .wrapped
            .schema
            .fieldNames
            .contains(METADATA_ROW_IDX_COL) && scan.canThisBeReplaced
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
      var overrideCount = 0
      def count(meta: SparkPlanMeta[SparkPlan]): Unit = {
        meta match {
          case scan if scan.isInstanceOf[FileSourceScanExecMeta] =>
            if (scan.asInstanceOf[FileSourceScanExecMeta]
              .wrapped.schema.fieldNames.contains(METADATA_ROW_DEL_COL) && scan.canThisBeReplaced) {
              overrideCount += 1
            }
          case m => m.childPlans.foreach(count)
        }
      }

      count(planMeta)
      overrideCount == 2
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
    addRowIndexMetaColumn(buildTargetDFWithFiles(dataSkippedFiles))
  }

  private lazy val touchedFiles: Map[String, (Roaring64Bitmap, AddFile)] = this.findTouchedFiles()

  private def planForFindingTouchedFiles(): DataFrame = {

    // Apply inner join to between source and target using the merge condition to find matches
    // In addition, we attach two columns
    // - METADATA_ROW_IDX column to identify target row in file
    // - FILE_PATH_COL the target file name the row is from to later identify the files touched
    // by matched rows
    val targetDF = dataSkippedTargetDF.withColumn(FILE_PATH_COL, input_file_name())

    sourceDF.join(targetDF, new Column(context.cmd.condition), "inner")
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
   * Modify original data frame to insert
   * [[GpuDeltaParquetFileFormatUtils.METADATA_ROW_IDX_COL]].
   */
  private def addRowIndexMetaColumn(baseDF: DataFrame): DataFrame = {
    val rowIdxAttr = AttributeReference(
      METADATA_ROW_IDX_COL,
      METADATA_ROW_IDX_FIELD.dataType,
      METADATA_ROW_IDX_FIELD.nullable)()

    val newPlan = baseDF.queryExecution.analyzed.transformUp {
      case r@LogicalRelation(fs: HadoopFsRelation, _, _, _) =>
        val newSchema = StructType(fs.dataSchema.fields).add(METADATA_ROW_IDX_FIELD)

        // This is required to ensure that row index is correctly calculated.
        val newFileFormat = fs.fileFormat.asInstanceOf[DeltaParquetFileFormat]
          .copy(isSplittable = false, disablePushDowns = true)

        val newFs = fs.copy(dataSchema = newSchema, fileFormat = newFileFormat)(context.spark)

        val newOutput = r.output :+ rowIdxAttr
        r.copy(relation = newFs, output = newOutput)
      case p@Project(projectList, _) =>
        val newProjectList = projectList :+ rowIdxAttr
        p.copy(projectList = newProjectList)
    }

    Dataset.ofRows(context.spark, newPlan)
  }

  /**
   * The result is scanning target table with touched files, and added an extra
   * [[METADATA_ROW_DEL_COL]] to indicate whether filtered by joining with source table in first
   * step.
   */
  private def getTouchedTargetDF(touchedFiles: Map[String, (Roaring64Bitmap, AddFile)])
  : DataFrame = {
    // Generate a new target dataframe that has same output attributes exprIds as the target plan.
    // This allows us to apply the existing resolved update/insert expressions.
    val baseTargetDF = buildTargetDFWithFiles(touchedFiles.values.map(_._2).toSeq)

    val newPlan = {
      val rowDelAttr = AttributeReference(
        METADATA_ROW_DEL_COL,
        METADATA_ROW_DEL_FIELD.dataType,
        METADATA_ROW_DEL_FIELD.nullable)()

      baseTargetDF.queryExecution.analyzed.transformUp {
        case r@LogicalRelation(fs: HadoopFsRelation, _, _, _) =>
          val newSchema = StructType(fs.dataSchema.fields).add(METADATA_ROW_DEL_FIELD)

          // This is required to ensure that row index is correctly calculated.
          val newFileFormat = {
            val oldFormat = fs.fileFormat.asInstanceOf[DeltaParquetFileFormat]
            val dvs = touchedFiles.map(kv => (new URI(kv._1),
              DeletionVectorDescriptorWithFilterType(toDeletionVector(kv._2._1),
                RowIndexFilterType.UNKNOWN)))
            val broadcastDVs = context.spark.sparkContext.broadcast(dvs)

            oldFormat.copy(isSplittable = false,
              broadcastDvMap = Some(broadcastDVs),
              disablePushDowns = true)
          }

          val newFs = fs.copy(dataSchema = newSchema, fileFormat = newFileFormat)(context.spark)

          val newOutput = r.output :+ rowDelAttr
          r.copy(relation = newFs, output = newOutput)
        case p@Project(projectList, _) =>
          val newProjectList = projectList :+ rowDelAttr
          p.copy(projectList = newProjectList)
      }
    }

    val df = Dataset.ofRows(context.spark, newPlan)
      .withColumn(TARGET_ROW_PRESENT_COL, lit(true))

    df
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
    val sourceDF = this.sourceDF
      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))

    val targetDF = getTouchedTargetDF(touchedFiles)

    val joinedDF = {
      val joinType = if (hasNoInserts &&
        context.spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
        "inner"
      } else {
        "leftOuter"
      }
      val matchedTargetDF = targetDF.filter(METADATA_ROW_DEL_COL)
        .drop(METADATA_ROW_DEL_COL)

      sourceDF.join(matchedTargetDF, new Column(context.cmd.condition), joinType)
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

    val sourceRowHasNoMatch = col(TARGET_ROW_PRESENT_COL).isNull.expr

    val modifiedCols = modifiedRowsSchema.zipWithIndex.map { case (col, idx) =>
      val caseWhen = CaseWhen(
        Seq(sourceRowHasNoMatch -> notMatchedExpr(idx)),
        matchedExprs(idx))
      Column(Alias(caseWhen, col.name)())
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
    getTouchedTargetDF(touchedFiles)
      .filter(!col(METADATA_ROW_DEL_COL))
      .drop(TARGET_ROW_PRESENT_COL, METADATA_ROW_DEL_COL)
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

  private[delta] def toDeletionVector(bitmap: Roaring64Bitmap): DeletionVectorDescriptor = {
    DeletionVectorDescriptor.inlineInLog(RoaringBitmapWrapper(bitmap).serializeToBytes(),
      bitmap.getLongCardinality)
  }

  /** Count the number of distinct partition values among the AddFiles in the given set. */
  private[delta] def totalBytesAndDistinctPartitionValues(files: Seq[FileAction]): (Long, Int) = {
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