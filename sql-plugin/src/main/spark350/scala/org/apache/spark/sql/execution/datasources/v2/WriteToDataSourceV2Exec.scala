/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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


/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import scala.util.control.NonFatal

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar => CudfScalar}
import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuColumnVector, GpuDeltaWrite, GpuExec, GpuMetric, GpuWrite}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.DeltaInsertFilter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{GpuProjectingColumnarBatch, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{DELETE_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, DeltaWriter, PhysicalWriteInfoImpl, Write, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution, SparkPlan, SparkPlanInfo}
import org.apache.spark.sql.execution.{SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.GpuDelteWritingSparkTask.filterByOperation
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{LongAccumulator, Utils}

trait GpuV2ExistingTableWriteExec extends GpuV2TableWriteExec {
  def refreshCache: () => Unit
  def write: Write

  override lazy val additionalMetrics: Map[String, GpuMetric] =
    write.supportedCustomMetrics().map { customMetric =>
      customMetric.name() -> GpuMetric.wrap(SQLMetrics.createV2CustomMetric(sparkContext,
        customMetric))
    }.toMap

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(write.toBatch)
    refreshCache()
    writtenRows
  }
}


/**
 * A trait for GPU implementations of V2 table write commands.
 * <br/>
 *
 * This class is derived from
 * [[org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec]].
 */
trait GpuV2TableWriteExec extends V2CommandExec with UnaryExecNode with GpuExec {
  def query: SparkPlan

  def writingTask: GpuWritingSparkTask[_] = GpuDataWritingSparkTask

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Seq.empty

  // V2 writes consume columnar batches even though the write command itself is a row-based command
  // node. Execute a columnar AQE copy, but keep that copy's SQL UI updates tied to the copied
  // columnar plan rather than the original row-wrapper plan.
  private def columnarWriteQuery(plan: SparkPlan): SparkPlan = plan match {
    case aqe: AdaptiveSparkPlanExec =>
      // Keep context.qe non-null because some Spark shims dereference it during AQE UI updates.
      lazy val copiedAqe: AdaptiveSparkPlanExec = aqe.copy(
        context = aqe.context.copy(qe = uiQueryExecution),
        supportsColumnar = true)
      lazy val uiQueryExecution: QueryExecution = new QueryExecution(
        aqe.context.session,
        aqe.context.qe.logical,
        aqe.context.qe.tracker,
        aqe.context.qe.mode) {
        override lazy val executedPlan: SparkPlan = copiedAqe
        override def explainString(mode: ExplainMode): String =
          Utils.redact(aqe.conf.stringRedactionPattern, copiedAqe.toString)
      }
      copiedAqe
    case GpuColumnarToRowExec(inner, _) => columnarWriteQuery(inner)
    case p => p
  }

  private lazy val finalQuery: SparkPlan = columnarWriteQuery(query)

  /**
   * Re-post the final, post-AQE plan to the SQL UI so the GPU nodes show up.
   *
   * An Iceberg (V2) write can wrap its query in an `AdaptiveSparkPlanExec`. The columnar copy
   * executed by this node posts metric-only AQE updates before each stage starts, so the SQL UI
   * can track task accumulator updates for the actual GPU plan. Once the write has executed (the
   * AQE `executedPlan` is settled) we re-post a `SparkListenerSQLAdaptiveExecutionUpdate` for this
   * execution with every `AdaptiveSparkPlanExec` replaced by its `executedPlan`, ensuring the final
   * graph under the write node is visible for the SQL UI / History Server.
   *
   * Uses `transformDown` (not a shallow match on the root) so nested
   * `AdaptiveSparkPlanExec` - e.g. wrapped under a `ProjectExec` - are also unwrapped,
   * keeping the unwrap symmetric with the deep AQE that produced the plan.
   */
  protected def postFinalPlanUpdateToSqlUi(): Unit = {
    val executionIdStr = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdStr != null) {
      // This is a purely observational UI refresh and is called inside the write's
      // try/catch that aborts the write on any Throwable. A failure here (e.g. building
      // SparkPlanInfo for an unusual plan node, or posting while the SparkContext is
      // stopping) must never abort a write whose tasks have already committed, so log
      // and swallow non-fatal errors instead of letting them propagate.
      try {
        val finalChildPlan = finalQuery.transformDown {
          case aqe: AdaptiveSparkPlanExec => aqe.executedPlan
        }
        val planForUi = this.withNewChildren(Seq(finalChildPlan))
        // Redact the plan description with the configured pattern, mirroring Spark's
        // explain / AQE update path, so sensitive strings (e.g. credentials in options)
        // are not written to the SQL UI / History Server event log.
        val planDescription = Utils.redact(conf.stringRedactionPattern, planForUi.toString)
        TrampolineUtil.postEvent(sparkContext,
          SparkListenerSQLAdaptiveExecutionUpdate(
            executionIdStr.toLong,
            planDescription,
            SparkPlanInfo.fromSparkPlan(planForUi)))
      } catch {
        case NonFatal(e) =>
          logWarning("Failed to post the final GPU plan to the SQL UI for this V2 " +
            "write; the write itself is unaffected.", e)
      }
    }
  }

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = finalQuery.executeColumnar()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTask
    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = metrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      // The write has executed and the AQE plan is settled; refresh the SQL UI so the
      // GPU plan is visible for this V2 write. Done before commit so the UI reflects the
      // executed plan even if the commit later fails.
      postFinalPlanUpdateToSqlUi()

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }

    Nil
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class GpuAppendDataExec(
  inner: SparkPlan,
  refreshCache: () => Unit,
  write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuAppendDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuAppendDataExec = {
    copy(inner = newChild)
  }
}

/**
 * Physical plan node for dynamic partition overwrite into a v2 table.
 *
 * Overwrites data in a table based on partitions present in the write data.
 * Only partitions represented in the write data will be overwritten; other partitions
 * remain intact.
 */
case class GpuOverwritePartitionsDynamicExec(
  inner: SparkPlan,
  refreshCache: () => Unit,
  write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuOverwritePartitionsDynamicExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan):
  GpuOverwritePartitionsDynamicExec = {
    copy(inner = newChild)
  }
}

/**
 * Physical plan node for overwrite into a v2 table.
 *
 * Overwrites data in a table matched by filters. Rows matching all of the filters will be
 * deleted and rows in the output data set are appended.
 */
case class GpuOverwriteByExpressionExec(
                                         inner: SparkPlan,
                                         refreshCache: () => Unit,
                                         write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuOverwriteByExpressionExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuOverwriteByExpressionExec = {
    copy(inner = newChild)
  }
}

/**
 * Physical plan node for writing delta (position deletes) in a v2 table.
 *
 * Used by merge-on-read operations like DELETE to write position delete files.
 * Position deletes track which rows should be logically deleted without rewriting data files.
 */
case class GpuWriteDeltaExec(
                               inner: SparkPlan,
                               refreshCache: () => Unit,
                               projections: WriteDeltaProjections,
                               write: GpuDeltaWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = inner

  override lazy val writingTask: GpuWritingSparkTask[_] = {
    // Match the CPU implementation: use DeltaWithMetadataWritingSparkTask if metadata
    // projection is defined, otherwise use DeltaWritingSparkTask
    if (projections.metadataProjection.isDefined) {
      GpuDeltaWithMetadataWritingSparkTask(projections)
    } else {
      GpuDeltaWritingSparkTask(projections)
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuWriteDeltaExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuWriteDeltaExec = {
    copy(inner = newChild)
  }
}

/**
 * This class is derived from [[org.apache.spark.sql.execution.datasources.v2.WritingSparkTask]].
 */
trait GpuWritingSparkTask[W <: DataWriter[ColumnarBatch]] extends Logging with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def run(
    writerFactory: DataWriterFactory,
    context: TaskContext,
    iter: Iterator[ColumnarBatch],
    useCommitCoordinator: Boolean,
    customMetrics: Map[String, SQLMetric]): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        // Count is here.
        val batch = iter.next()
        val numRows = batch.numRows
        write(dataWriter, batch)

        count += numRows
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
      }

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(commitDeniedException.getMessage)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

object GpuDataWritingSparkTask extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {
  override protected def write(writer: DataWriter[ColumnarBatch], row: ColumnarBatch): Unit = {
    writer.write(row)
  }
}

/**
 * GPU version of DeltaWritingSparkTask for writing delta (position delete) operations.
 * Applies projections to extract row data and metadata before writing.
 */
case class GpuDeltaWritingSparkTask(
    projs: WriteDeltaProjections) extends GpuWritingSparkTask[DeltaWriter[ColumnarBatch]] {

  private lazy val rowProjection = projs.rowProjection
    .map(GpuProjectingColumnarBatch(_))
    .orNull
  private lazy val rowDataTypes = projs.rowProjection
    .map(_.schema.fields.map(f => f.dataType))
    .orNull

  private lazy val rowIdProjection = GpuProjectingColumnarBatch(projs.rowIdProjection)
  private lazy val rowIdDataTypes = rowIdProjection.schema.fields.map(_.dataType)

  override protected def write(writer: DeltaWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
    withRetryNoSplit(batch) { _ =>
      val deleteFilter = filterByOperation(batch, DELETE_OPERATION)
      withResource(deleteFilter) { _ =>
        withResource(rowIdProjection.project(batch)) { rowIds =>
          val rowIdBatch = GpuColumnVector.filter(rowIds, rowIdDataTypes, deleteFilter)
          if (rowIdBatch.numRows() > 0) {
            writer.delete(null, rowIdBatch)
          } else {
            rowIdBatch.close()
          }
        }
      }

      if (rowProjection != null) {
        val updateFilter = filterByOperation(batch, UPDATE_OPERATION)
        withResource(updateFilter) { _ =>
          val rowIds = withResource(rowIdProjection.project(batch)) { rowIds =>
            GpuColumnVector.filter(rowIds, rowIdDataTypes, updateFilter)
          }

          closeOnExcept(rowIds) { _ =>
            if (rowIds.numRows() > 0) {
              val rows = withResource(rowProjection.project(batch)) { rows =>
                GpuColumnVector.filter(rows, rowDataTypes, updateFilter)
              }

              writer.update(null, rowIds, rows)
            } else {
              rowIds.close()
            }
          }
        }

        val insertFilter = DeltaInsertFilter.filterInsertRows(batch)
        withResource(insertFilter) { _ =>
          withResource(rowProjection.project(batch)) { rows =>
            val filteredRows = GpuColumnVector.filter(rows, rowDataTypes, insertFilter)
            if (filteredRows.numRows() > 0) {
              writer.insert(filteredRows)
            } else {
              filteredRows.close()
            }
          }
        }
      }
    }
  }
}

/**
 * GPU version of DeltaWithMetadataWritingSparkTask for writing delta operations with metadata.
 * Applies both row and metadata projections before writing.
 */
case class GpuDeltaWithMetadataWritingSparkTask(
    projs: WriteDeltaProjections) extends GpuWritingSparkTask[DeltaWriter[ColumnarBatch]] {

  private lazy val rowProjection = projs.rowProjection
    .map(GpuProjectingColumnarBatch(_))
    .orNull
  private lazy val rowDataTypes = projs.rowProjection
    .map(_.schema.fields.map(f => f.dataType))
    .orNull

  private lazy val rowIdProjection = GpuProjectingColumnarBatch(projs.rowIdProjection)
  private lazy val rowIdDataTypes = rowIdProjection.schema.fields.map(_.dataType)

  private lazy val metadataProjection = projs.metadataProjection
    .map(GpuProjectingColumnarBatch(_))
    .orNull
  private lazy val metadataDataTypes = projs.metadataProjection
    .map(_.schema.fields.map(f => f.dataType))
    .orNull

  override protected def write(writer: DeltaWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
    withRetryNoSplit(batch) { _ =>
      if (metadataProjection != null) {
        val deleteFilter = filterByOperation(batch, DELETE_OPERATION)
        withResource(deleteFilter) { _ =>
          val rowIds = withResource(rowIdProjection.project(batch)) { rowIds =>
            GpuColumnVector.filter(rowIds, rowIdDataTypes, deleteFilter)
          }

          closeOnExcept(rowIds) { _ =>
            if (rowIds.numRows() > 0) {
              val metadataBatch = withResource(metadataProjection.project(batch)) { metaBatch =>
                GpuColumnVector.filter(metaBatch, metadataDataTypes, deleteFilter)
              }

              writer.delete(metadataBatch, rowIds)
            } else {
              rowIds.close()
            }
          }
        }
      }

      if (rowProjection != null && metadataProjection != null) {
        val updateFilter = filterByOperation(batch, UPDATE_OPERATION)
        withResource(updateFilter) { _ =>
          val rowIds = withResource(rowIdProjection.project(batch)) { rowIds =>
            GpuColumnVector.filter(rowIds, rowIdDataTypes, updateFilter)
          }

          closeOnExcept(rowIds) { _ =>
            if (rowIds.numRows() > 0) {
              val rows = withResource(rowProjection.project(batch)) { rows =>
                GpuColumnVector.filter(rows, rowDataTypes, updateFilter)
              }

              closeOnExcept(rows) { _ =>
                val metadataBatch = withResource(metadataProjection.project(batch)) { metadata =>
                  GpuColumnVector.filter(metadata, metadataDataTypes, updateFilter)
                }

                writer.update(metadataBatch, rowIds, rows)
              }
            } else {
              rowIds.close()
            }
          }
        }
      }

      if (rowProjection != null) {
        val insertFilter = DeltaInsertFilter.filterInsertRows(batch)
        withResource(insertFilter) { _ =>
          withResource(rowProjection.project(batch)) { rows =>
            val filterRows = GpuColumnVector.filter(rows, rowDataTypes, insertFilter)
            if (filterRows.numRows() > 0) {
              writer.insert(filterRows)
            } else {
              filterRows.close()
            }
          }
        }
      }
    }
  }
}

object GpuDelteWritingSparkTask {
  private[v2] def filterByOperation(batch: ColumnarBatch, op: Int): CudfColumnVector = {
    withResource(CudfScalar.fromInt(op)) { cudfOp =>
      batch.column(0).asInstanceOf[GpuColumnVector].getBase.equalTo(cudfOp)
    }
  }
}
