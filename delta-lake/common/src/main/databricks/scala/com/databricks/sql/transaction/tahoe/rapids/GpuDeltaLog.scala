/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.{DeltaLog, OptimisticTransaction, Snapshot}
import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.Clock

class GpuDeltaLog(val deltaLog: DeltaLog, rapidsConf: RapidsConf) {
  private lazy implicit val _clock: Clock = deltaLog.clock

  /* ------------------ *
   |  Delta Management  |
   * ------------------ */

  /**
   * Returns a new OptimisticTransaction that can be used to read the current state of the
   * log and then commit updates. The reads and updates will be checked for logical conflicts
   * with any concurrent writes to the log.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the DeltaLog otherwise they will not be checked for conflicts.
   */
  def startTransaction(): GpuOptimisticTransaction = {
    new GpuOptimisticTransaction(deltaLog, rapidsConf)
  }

  /**
   * Returns a new [[GpuOptimisticTransaction]] that can be used to read the current state of the
   * log and then commit updates. The reads and updates will be checked for logical conflicts with
   * any concurrent writes, and post-commit hooks can notify the table's catalog about schema
   * changes.
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   *                        None asserts this is a path-based table with no catalog entry.
   * @param snapshotOpt     The [[Snapshot]] this transaction should use, if not latest.
   */
  def startTransaction(
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None): GpuOptimisticTransaction = {
    new GpuOptimisticTransaction(deltaLog, catalogTableOpt, snapshotOpt, rapidsConf)
  }

  /**
   * Execute a piece of code within a new GpuOptimisticTransaction. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](thunk: GpuOptimisticTransaction => T): T = {
    try {
      val txn = startTransaction()
      OptimisticTransaction.setActive(txn)
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }

  /**
   * Execute a piece of code within a new [[GpuOptimisticTransaction]].
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   *                        None asserts this is a path-based table with no catalog entry.
   * @param snapshotOpt     The [[Snapshot]] this transaction should use, if not latest.
   */
  def withNewTransaction[T](
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None)(
      thunk: GpuOptimisticTransaction => T): T = {
    try {
      val txn = startTransaction(catalogTableOpt, snapshotOpt)
      OptimisticTransaction.setActive(txn)
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }
}

object GpuDeltaLog {
  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(
      spark: SparkSession,
      dataPath: String,
      options: Map[String, String],
      rapidsConf: RapidsConf): GpuDeltaLog = {
    val deltaLog = DeltaLog.forTable(spark, new Path(dataPath), options)
    new GpuDeltaLog(deltaLog, rapidsConf)
  }

  def forTable(
      spark: SparkSession,
      tableLocation: Path,
      rapidsConf: RapidsConf): GpuDeltaLog = {
    val deltaLog = DeltaLog.forTable(spark, tableLocation)
    new GpuDeltaLog(deltaLog, rapidsConf)
  }
}
