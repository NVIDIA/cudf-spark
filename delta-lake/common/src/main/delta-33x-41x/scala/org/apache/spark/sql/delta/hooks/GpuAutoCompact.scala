/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * This file was derived from OptimisticTransaction.scala and TransactionalWrite.scala
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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeltaOptimizeContext
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.DeltaMdcShims.mdc
import org.apache.spark.sql.delta.rapids.GpuOptimisticTransactionBase
import org.apache.spark.sql.delta.rapids.commands.GpuOptimizeExecutor
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats

trait GpuAutoCompactBase extends AutoCompactBase {
  /**
   * Execute a prepared auto-compaction request. Version-specific shims are responsible for
   * constructing the request with the correct Delta API.
   */
  protected final def executeAutoCompactRequest(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      autoCompactRequest: AutoCompactRequest,
      opType: String,
      maxDeletedRowsRatio: Option[Double]
  ): Seq[OptimizeMetrics] = {
    val tableId = deltaLog.tableId
    if (autoCompactRequest.shouldCompact) {
      try {
        val metrics = GpuAutoCompact
          .compact(
            spark,
            deltaLog,
            catalogTable,
            autoCompactRequest.targetPartitionsPredicate,
            opType,
            maxDeletedRowsRatio
          )
        val partitionsStats = AutoCompactPartitionStats.instance(spark)
        // Mark partitions as compacted before releasing them.
        // Otherwise an already compacted partition might get picked up by a concurrent thread.
        // But only marks it as compacted, if no exception was thrown by auto compaction so that the
        // partitions stay eligible for subsequent auto compactions.
        partitionsStats.markPartitionsAsCompacted(
          tableId,
          autoCompactRequest.allowedPartitions
        )
        metrics
      } catch {
        case e: Throwable =>
          logError(log"Auto Compaction failed with: ${mdc(DeltaLogKeys.ERROR, e.getMessage)}")
          recordDeltaEvent(
            deltaLog,
            opType = "delta.autoCompaction.error",
            data = getErrorData(e))
          throw e
      } finally {
        if (AutoCompactUtils.reservePartitionEnabled(spark)) {
          AutoCompactPartitionReserve.releasePartitions(
            tableId,
            autoCompactRequest.allowedPartitions
          )
        }
      }
    } else {
      Seq.empty[OptimizeMetrics]
    }
  }

  /**
   * Launch Auto Compaction jobs if there is sufficient capacity.
   * @param spark The spark session of the parent transaction that triggers this Auto Compaction.
   * @param deltaLog The delta log of the parent transaction.
   * @return the optimize metrics of this compaction job.
   */
  private[delta] override def compact(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      partitionPredicates: Seq[Expression] = Nil,
      opType: String = OP_TYPE,
      maxDeletedRowsRatio: Option[Double] = None)
  : Seq[OptimizeMetrics] = recordDeltaOperation(deltaLog, opType) {
    val maxFileSize = spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE)
    val minFileSizeOpt = Some(spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_FILE_SIZE)
      .getOrElse(maxFileSize / 2))
    val maxFileSizeOpt = Some(maxFileSize)
    recordDeltaOperation(deltaLog, s"$opType.execute") {
      val optimizeContext = DeltaOptimizeContext(
        reorg = None,
        minFileSizeOpt,
        maxFileSizeOpt,
        maxDeletedRowsRatio = maxDeletedRowsRatio
      )
      val rows = new GpuOptimizeExecutor(
        spark,
        deltaLog.update(catalogTableOpt = catalogTable),
        catalogTable,
        partitionPredicates,
        zOrderByColumns = Seq(),
        isAutoCompact = true,
        optimizeContext
      ).optimize()
      val metrics = rows.map(_.getAs[OptimizeMetrics](1))
      metrics.headOption.foreach { metric =>
        recordDeltaEvent(deltaLog, s"$opType.execute.metrics", data = metric)
      }
      metrics
    }
  }
}

trait GpuTransactionalAutoCompactBase extends GpuAutoCompactBase {
  /**
   * Run the auto-compaction hook against the live GPU transaction API used by Delta 3.3/4.0.
   */
  def run(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit
}
