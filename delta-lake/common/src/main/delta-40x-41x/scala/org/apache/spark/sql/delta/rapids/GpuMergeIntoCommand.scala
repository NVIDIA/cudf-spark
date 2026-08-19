/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta._

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase
import org.apache.spark.sql.delta.commands.merge._
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuOptimisticTransactionBase}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.SetAccumulator
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.types.{LongType, StructType}


case class GpuMergeDataSizes(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    rows: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    files: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    bytes: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    partitions: Option[Long] = None)

/**
 * Represents the state of a single merge clause:
 * - merge clause's (optional) predicate
 * - action type (insert, update, delete)
 * - action's expressions
 */
case class GpuMergeClauseStats(
    condition: Option[String],
    actionType: String,
    actionExpr: Seq[String])

object GpuMergeClauseStats {
  def apply(mergeClause: DeltaMergeIntoClause): GpuMergeClauseStats = {
    GpuMergeClauseStats(
      condition = mergeClause.condition.map(_.sql),
      mergeClause.clauseType.toLowerCase(),
      actionExpr = mergeClause.actions.map(_.sql))
  }
}

/** State for a GPU merge operation */
case class GpuMergeStats(
    // Merge condition expression
    conditionExpr: String,

    // Expressions used in old MERGE stats, now always Null
    updateConditionExpr: String,
    updateExprs: Seq[String],
    insertConditionExpr: String,
    insertExprs: Seq[String],
    deleteConditionExpr: String,

    // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE
    matchedStats: Seq[GpuMergeClauseStats],
    notMatchedStats: Seq[GpuMergeClauseStats],
    notMatchedBySourceStats: Seq[GpuMergeClauseStats],

    // Timings
    executionTimeMs: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long,

    // Data sizes of source and target at different stages of processing
    source: GpuMergeDataSizes,
    targetBeforeSkipping: GpuMergeDataSizes,
    targetAfterSkipping: GpuMergeDataSizes,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    sourceRowsInSecondScan: Option[Long],

    // Data change sizes
    targetFilesRemoved: Long,
    targetFilesAdded: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetChangeFilesAdded: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetChangeFileBytes: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetBytesRemoved: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetBytesAdded: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetPartitionsRemovedFrom: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetPartitionsAddedTo: Option[Long],
    targetRowsCopied: Long,
    targetRowsUpdated: Long,
    targetRowsMatchedUpdated: Long,
    targetRowsNotMatchedBySourceUpdated: Long,
    targetRowsInserted: Long,
    targetRowsDeleted: Long,
    targetRowsMatchedDeleted: Long,
    targetRowsNotMatchedBySourceDeleted: Long,
    numTargetDeletionVectorsAdded: Long,
    numTargetDeletionVectorsRemoved: Long,
    numTargetDeletionVectorsUpdated: Long,

    // MergeMaterializeSource stats
    materializeSourceReason: Option[String] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    materializeSourceAttempts: Option[Long] = None,

    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numLogicalRecordsAdded: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numLogicalRecordsRemoved: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    commitVersion: Option[Long] = None
)

object GpuMergeStats {

  def fromMergeSQLMetrics(
      metrics: Map[String, SQLMetric],
      condition: Expression,
      matchedClauses: Seq[DeltaMergeIntoMatchedClause],
      notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
      notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
      isPartitioned: Boolean,
      performedSecondSourceScan: Boolean,
      commitVersion: Option[Long],
      numRecordsStats: NumRecordsStats): GpuMergeStats = {

    def metricValueIfPartitioned(metricName: String): Option[Long] = {
      if (isPartitioned) Some(metrics(metricName).value) else None
    }

    GpuMergeStats(
      // Merge condition expression
      conditionExpr = condition.sql,

      // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/
      // NOT MATCHED BY SOURCE
      matchedStats = matchedClauses.map(GpuMergeClauseStats(_)),
      notMatchedStats = notMatchedClauses.map(GpuMergeClauseStats(_)),
      notMatchedBySourceStats = notMatchedBySourceClauses.map(GpuMergeClauseStats(_)),

      // Timings
      executionTimeMs = metrics("executionTimeMs").value,
      scanTimeMs = metrics("scanTimeMs").value,
      rewriteTimeMs = metrics("rewriteTimeMs").value,

      // Data sizes of source and target at different stages of processing
      source = GpuMergeDataSizes(rows = Some(metrics("numSourceRows").value)),
      targetBeforeSkipping =
        GpuMergeDataSizes(
          files = Some(metrics("numTargetFilesBeforeSkipping").value),
          bytes = Some(metrics("numTargetBytesBeforeSkipping").value)),
      targetAfterSkipping =
        GpuMergeDataSizes(
          files = Some(metrics("numTargetFilesAfterSkipping").value),
          bytes = Some(metrics("numTargetBytesAfterSkipping").value),
          partitions = metricValueIfPartitioned("numTargetPartitionsAfterSkipping")),
      sourceRowsInSecondScan = if (performedSecondSourceScan) {
        Some(metrics("numSourceRowsInSecondScan").value)
      } else {
        None
      },

      // Data change sizes
      targetFilesAdded = metrics("numTargetFilesAdded").value,
      targetChangeFilesAdded = metrics.get("numTargetChangeFilesAdded").map(_.value),
      targetChangeFileBytes = metrics.get("numTargetChangeFileBytes").map(_.value),
      targetFilesRemoved = metrics("numTargetFilesRemoved").value,
      targetBytesAdded = Some(metrics("numTargetBytesAdded").value),
      targetBytesRemoved = Some(metrics("numTargetBytesRemoved").value),
      targetPartitionsRemovedFrom = metricValueIfPartitioned("numTargetPartitionsRemovedFrom"),
      targetPartitionsAddedTo = metricValueIfPartitioned("numTargetPartitionsAddedTo"),
      targetRowsCopied = metrics("numTargetRowsCopied").value,
      targetRowsUpdated = metrics("numTargetRowsUpdated").value,
      targetRowsMatchedUpdated = metrics("numTargetRowsMatchedUpdated").value,
      targetRowsNotMatchedBySourceUpdated = metrics("numTargetRowsNotMatchedBySourceUpdated").value,
      targetRowsInserted = metrics("numTargetRowsInserted").value,
      targetRowsDeleted = metrics("numTargetRowsDeleted").value,
      targetRowsMatchedDeleted = metrics("numTargetRowsMatchedDeleted").value,
      targetRowsNotMatchedBySourceDeleted = metrics("numTargetRowsNotMatchedBySourceDeleted").value,

      // Deletion Vector metrics.
      numTargetDeletionVectorsAdded = metrics("numTargetDeletionVectorsAdded").value,
      numTargetDeletionVectorsRemoved = metrics("numTargetDeletionVectorsRemoved").value,
      numTargetDeletionVectorsUpdated = metrics("numTargetDeletionVectorsUpdated").value,

      commitVersion = commitVersion,
      numLogicalRecordsAdded = numRecordsStats.numLogicalRecordsAdded,
      numLogicalRecordsRemoved = numRecordsStats.numLogicalRecordsRemoved,

      // Deprecated fields
      updateConditionExpr = null,
      updateExprs = null,
      insertConditionExpr = null,
      insertExprs = null,
      deleteConditionExpr = null)
  }
}

/**
 * GPU version of Delta Lake's MergeIntoCommand.
 *
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 *    for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source                     Source data to merge from
 * @param target                     Target table to merge into
 * @param gpuDeltaLog                Delta log to use
 * @param condition                  Condition for a source row to match with a target row
 * @param matchedClauses             All info related to matched clauses.
 * @param notMatchedClauses          All info related to not matched clauses.
 * @param notMatchedBySourceClauses  All info related to not matched by source clauses.
 * @param migratedSchema             The final schema of the target - may be changed by schema
 *                                   evolution.
 */
case class GpuMergeIntoCommand(
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
    schemaEvolutionEnabled: Boolean = false)(@transient val rapidsConf: RapidsConf)
  extends MergeIntoCommandBase
    with InsertOnlyMergeExecutor
    with ClassicMergeExecutor {

  override val otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)
  @transient override lazy val targetDeltaLog: DeltaLog = gpuDeltaLog.deltaLog

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  @transient override protected lazy val sc: SparkContext = SparkContext.getOrCreate()

  // No override: base metrics are extended at commit time for 4.0-specific keys

  protected def runMerge(spark: SqlSparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
      val startTime = System.nanoTime()
      gpuDeltaLog.withNewTransaction(catalogTable) { gpuDeltaTxn =>
        if (hasBeenExecuted(gpuDeltaTxn, spark)) {
          sendDriverMetrics(ClassicSparkSession.active, metrics)
          return Seq.empty
        }
        if (target.schema.size != gpuDeltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = gpuDeltaTxn.metadata.schema)
        }

        // Check that type widening wasn't enabled/disabled between analysis and the start of the
        // transaction.
        TypeWidening.ensureFeatureConsistentlyEnabled(
          protocol = targetFileIndex.protocol,
          metadata = targetFileIndex.metadata,
          otherProtocol = gpuDeltaTxn.protocol,
          otherMetadata = gpuDeltaTxn.metadata
        )

        if (canMergeSchema) {
          updateMetadata(
            spark, gpuDeltaTxn, migratedSchema.getOrElse(target.schema),
            gpuDeltaTxn.metadata.partitionColumns, gpuDeltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }

        checkIdentityColumnHighWaterMarks(gpuDeltaTxn)
        gpuDeltaTxn.setTrackHighWaterMarks(trackHighWaterMarks)

        // Materialize the source if needed.
        prepareMergeSource(
          spark,
          source,
          condition,
          matchedClauses,
          notMatchedClauses,
          isInsertOnly)

        // Ensure source row metric is populated early; some 4.0 plans may optimize away
        // the injected UDF used to increment the metric.
        if (metrics("numSourceRows").value == 0) {
          val numSourceRowsEarly = getMergeSource.df.count()
          metrics("numSourceRows").set(numSourceRowsEarly)
        }

        val mergeActions = {
          if (isInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            // This is a single-job execution so there is no WriteChanges.
            performedSecondSourceScan = false
            val srcMetricName = if (metrics("numSourceRows").value > 0) {
              // Avoid double counting when we already set the metric earlier
              "numSourceRowsInSecondScan"
            } else {
              "numSourceRows"
            }
            writeOnlyInserts(
              spark, gpuDeltaTxn, filterMatchedRows = true, numSourceRowsMetric = srcMetricName)
          } else {
            val (filesToRewrite, deduplicateCDFDeletes) =
              findTouchedFiles(spark, gpuDeltaTxn)
            if (filesToRewrite.nonEmpty) {
              val shouldWriteDeletionVectors =
                shouldWritePersistentDeletionVectors(spark, gpuDeltaTxn)
              if (shouldWriteDeletionVectors) {
                // We should never come here because we should have tagged the Exec to fallback
                throw new IllegalStateException("Deletion Vectors are not supported on the GPU")
              } else {
                val newWrittenFiles = withStatusCode("DELTA", "Writing modified data") {
                  writeAllChanges(
                    spark,
                    gpuDeltaTxn,
                    filesToRewrite,
                    deduplicateCDFDeletes,
                    writeUnmodifiedRows = true)
                }
                newWrittenFiles ++ filesToRewrite.map(_.remove)
              }
            } else {
              // Run an insert-only job instead of WriteChanges
              writeOnlyInserts(
                spark,
                gpuDeltaTxn,
                filterMatchedRows = false,
                numSourceRowsMetric = "numSourceRowsInSecondScan")
            }
          }
        }
        commitAndRecordStats(
          ClassicSparkSession.active,
          gpuDeltaTxn,
          mergeActions,
          startTime,
          getMergeSource.materializeReason)
      }
      val classicSession = ClassicSparkSession.active
      classicSession.sharedState.cacheManager.recacheByPlan(classicSession, target)
    }
    sendDriverMetrics(ClassicSparkSession.active, metrics)
    val num_affected_rows =
      metrics("numTargetRowsUpdated").value +
        metrics("numTargetRowsDeleted").value +
        metrics("numTargetRowsInserted").value
    Seq(Row(
      num_affected_rows,
      metrics("numTargetRowsUpdated").value,
      metrics("numTargetRowsDeleted").value,
      metrics("numTargetRowsInserted").value))
  }

  /**
   * Finalizes the merge operation before committing it to the delta log and records merge metrics:
   *   - Checks that the source table didn't change during the merge operation.
   *   - Register SQL metrics to be updated during commit.
   *   - Commit the operations.
   *   - Collects final merge stats and record them with a Delta event.
   */
  private def commitAndRecordStats(
      spark: SqlSparkSession,
      gpuDeltaTxn: GpuOptimisticTransactionBase,
      mergeActions: Seq[FileAction],
      startTime: Long,
      materializeSourceReason: MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason
      ): Unit = {
    checkNonDeterministicSource(spark)

    // Metrics should be recorded before commit (where they are written to delta logs).
    metrics("executionTimeMs").set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
    // No additional counting here; metric should be populated once (UDF or early count)
    gpuDeltaTxn.registerSQLMetrics(ClassicSparkSession.active, metrics)

    val finalActions = createSetTransaction(spark, targetDeltaLog).toSeq ++ mergeActions
    val numRecordsStats = NumRecordsStats.fromActions(finalActions)
    val commitVersion = gpuDeltaTxn.commitIfNeeded(
      actions = finalActions,
      op = DeltaOperations.Merge(
        predicate = Option(condition),
        matchedPredicates = matchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedPredicates = notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedBySourcePredicates =
          notMatchedBySourceClauses.map(DeltaOperations.MergePredicate(_))),
      tags = RowTracking.addPreservedRowTrackingTagIfNotSet(gpuDeltaTxn.snapshot))
    val stats = collectGpuMergeStats(gpuDeltaTxn, materializeSourceReason, commitVersion,
      numRecordsStats)
    recordDeltaEvent(targetDeltaLog, "delta.dml.merge.stats", data = stats)
  }

  /**
   * Collects the merge operation stats and metrics into a [[MergeStats]] object that can be
   * recorded with `recordDeltaEvent`. Merge stats should be collected after committing all new
   * actions as metrics may still be updated during commit.
   */
  private def collectGpuMergeStats(
      gpuDeltaTxn: GpuOptimisticTransactionBase,
      materializeSourceReason: MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason,
      commitVersion: Option[Long],
      numRecordsStats: NumRecordsStats): GpuMergeStats = {
    val stats = GpuMergeStats.fromMergeSQLMetrics(
      metrics,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      isPartitioned = gpuDeltaTxn.metadata.partitionColumns.nonEmpty,
      performedSecondSourceScan = performedSecondSourceScan,
      commitVersion = commitVersion,
      numRecordsStats = numRecordsStats
    )
    stats.copy(
      materializeSourceReason = Some(materializeSourceReason.toString),
      materializeSourceAttempts = Some(attempt))
  }

  /** Expressions to increment SQL metrics */
  private def makeMetricUpdateUDF(
      name: String,
      deterministic: Boolean = false): org.apache.spark.sql.Column = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    var u = DeltaUDF.boolean(new GpuDeltaMetricUpdateUDF(metric))
    if (!deterministic) {
      u = u.asNondeterministic()
    }
    u()
  }

  /**
   * We had to override this method from ClassicMergeExecutor to give it the UDF to accumulate the
   * files modified
   */
  private def findTouchedFiles(
      spark: SqlSparkSession,
      gpuDeltaTxn: GpuOptimisticTransactionBase
      ): (Seq[AddFile], DeduplicateCDFDeletes) = recordMergeOperation(
    extraOpType = "findTouchedFiles",
    status = "MERGE operation - scanning files for matches",
    sqlMetricName = "scanTimeMs") {

    val columnComparator = spark.sessionState.analyzer.resolver

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()

    import org.apache.spark.sql.delta.commands.MergeIntoCommandBase._

    spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

    // Prune non-matching files if we don't need to collect them for NOT MATCHED BY SOURCE clauses.
    val dataSkippedFiles =
      if (notMatchedBySourceClauses.isEmpty) {
        gpuDeltaTxn.filterFiles(getTargetOnlyPredicates(spark), keepNumRecords = true)
      } else {
        gpuDeltaTxn.filterFiles(filters = Seq(Literal.TrueLiteral), keepNumRecords = true)
      }

    // Join the source and target table using the merge condition to find touched files. An inner
    // join collects all candidate files for MATCHED clauses, a right outer join also includes
    // candidates for NOT MATCHED BY SOURCE clauses.
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val joinType = if (notMatchedBySourceClauses.isEmpty) "inner" else "right_outer"

    // When they are only MATCHED clauses, after the join we prune files that have no rows that
    // satisfy any of the clause conditions.
    val matchedPredicate =
      if (isMatchedOnly) {
        matchedClauses
          // An undefined condition (None) is implicitly true
          .map(_.condition.getOrElse(Literal.TrueLiteral))
          .reduce((a, b) => Or(a, b))
      } else Literal.TrueLiteral

    // Compute the columns needed for the inner join.
    val targetColsNeeded = {
      condition.references.map(_.name) ++ gpuDeltaTxn.snapshot.metadata.partitionColumns ++
        matchedPredicate.references.map(_.name)
    }

    val columnsToDrop = gpuDeltaTxn.snapshot.metadata.schema.map(_.name)
      .filterNot { field =>
        targetColsNeeded.exists { name => columnComparator(name, field) }
      }
    val incrSourceRowCountCol = makeMetricUpdateUDF("numSourceRows")
    // Only attach the incrementing UDF column if we haven't already populated the metric.
    // This avoids double-counting when we set the metric from an explicit count earlier.
    val addIncrMetricCol = metrics("numSourceRows").value == 0
    val baseSourceDF = getMergeSource.df
    val sourceDF = if (addIncrMetricCol) {
      // We can't use filter() directly on the expression because that will prevent
      // column pruning. We don't need the SOURCE_ROW_PRESENT_COL so we immediately drop it.
      baseSourceDF
        .withColumn(SOURCE_ROW_PRESENT_COL, incrSourceRowCountCol)
        .filter(SOURCE_ROW_PRESENT_COL)
        .drop(SOURCE_ROW_PRESENT_COL)
    } else {
      baseSourceDF
    }
    val targetPlan =
      buildTargetPlanWithFiles(
        spark,
        gpuDeltaTxn,
        dataSkippedFiles,
        columnsToDrop)
     val targetDF = TrampolineConnectShims.createDataFrame(
      TrampolineConnectShims.getActiveSession, targetPlan)
      .withColumn(ROW_ID_COL, monotonically_increasing_id())
      .withColumn(FILE_NAME_COL, input_file_name())

    val joinToFindTouchedFiles =
      sourceDF.join(targetDF, DFUDFShims.exprToColumn(condition), joinType)

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName =
      DeltaUDF.intFromStringBoolean(
        new GpuDeltaRecordTouchedFilesStringBoolUDF(touchedFilesAccum)).asNondeterministic()

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL),
        recordTouchedFileName(col(FILE_NAME_COL), DFUDFShims.exprToColumn(
          matchedPredicate)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))

    // Get multiple matches and simultaneously collect (using touchedFilesAccum) the file names
    val mmRow = matchedRowCounts
      .filter(col("count") > lit(1))
      .select(
        coalesce(count(lit(1)), lit(0)).as("cnt"),
        coalesce(sum("count"), lit(0)).as("sum"))
      .collect()
      .head
    val multipleMatchCount = mmRow.getLong(0)
    val multipleMatchSum = mmRow.getLong(1)

    val hasMultipleMatches = multipleMatchCount > 0
    throwErrorOnMultipleMatches(hasMultipleMatches, spark)
    if (hasMultipleMatches) {
      // This is only allowed for delete-only queries.
      // This query will count the duplicates for numTargetRowsDeleted in Job 2,
      // because we count matches after the join and not just the target rows.
      // We have to compensate for this by subtracting the duplicates later,
      // so we need to record them here.
      val duplicateCount = multipleMatchSum - multipleMatchCount
      multipleMatchDeleteOnlyOvercount = Some(duplicateCount)
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.map(
      getTouchedFile(targetDeltaLog.dataPath, _, nameToAddFileMap))

    // Do NOT re-count here if the metric has already been populated from prepareMergeSource.

    metrics("numTargetFilesBeforeSkipping") += gpuDeltaTxn.snapshot.numOfFiles
    metrics("numTargetBytesBeforeSkipping") += gpuDeltaTxn.snapshot.sizeInBytes
    val (afterSkippingBytes, afterSkippingPartitions) =
      totalBytesAndDistinctPartitionValues(dataSkippedFiles)
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
    metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
    val (removedBytes, removedPartitions) = totalBytesAndDistinctPartitionValues(touchedAddFiles)
    metrics("numTargetFilesRemoved") += touchedAddFiles.size
    metrics("numTargetBytesRemoved") += removedBytes
    metrics("numTargetPartitionsRemovedFrom") += removedPartitions
    val dedupe = DeduplicateCDFDeletes(
      hasMultipleMatches && isCdcEnabled(gpuDeltaTxn),
      includesInserts)
    (touchedAddFiles, dedupe)
  }
}
