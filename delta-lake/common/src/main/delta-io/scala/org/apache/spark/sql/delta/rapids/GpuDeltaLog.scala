/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.util.Clock

class GpuDeltaLog(val deltaLog: DeltaLog, val rapidsConf: RapidsConf) {
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
  def startTransaction(): GpuOptimisticTransactionBase = {
    DeltaRuntimeShim.startTransaction(StartTransactionArg(deltaLog, rapidsConf, _clock, None,
      None))
  }

  /**
   * Returns a new [[GpuOptimisticTransactionBase]] that can be used to read the current state of
   * the log
   * and then commit updates. The reads and updates will be checked for logical conflicts with any
   * concurrent writes to the log, and post-commit hooks can be used to notify the table's catalog
   * of schema changes, etc.
   *
   * Note that all reads in a transaction must go through the returned transaction object, and not
   * directly to the [[DeltaLog]] otherwise they will not be checked for conflicts.
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   * None asserts this is a path-based table with no catalog entry.
   *
   * @param snapshotOpt THe [[Snapshot]] this transaction should use, if not latest.
   */
  def startTransaction(
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None): GpuOptimisticTransactionBase = {
    DeltaRuntimeShim.startTransaction(StartTransactionArg(deltaLog, rapidsConf, _clock,
      catalogTableOpt, snapshotOpt))
  }

  /**
   * Execute a piece of code within a new GpuOptimisticTransaction. Reads/write sets will
   * be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](thunk: GpuOptimisticTransactionBase => T): T = {
    try {
      val txn = startTransaction()
      OptimisticTransaction.setActive(txn)
      thunk(txn)
    } finally {
      OptimisticTransaction.clearActive()
    }
  }

  /**
   * Execute a piece of code within a new [[GpuOptimisticTransactionBase]].
   * Reads/write sets will be recorded for this table, and all other tables will be read
   * at a snapshot that is pinned on the first access.
   *
   * @param catalogTableOpt The [[CatalogTable]] for the table this transaction updates. Passing
   * None asserts this is a path-based table with no catalog entry.
   *
   * @param snapshotOpt THe [[Snapshot]] this transaction should use, if not latest.
   * @note This uses thread-local variable to make the active transaction visible. So do not use
   *       multi-threaded code in the provided thunk.
   */
  def withNewTransaction[T](
      catalogTableOpt: Option[CatalogTable],
      snapshotOpt: Option[Snapshot] = None)(
      thunk: GpuOptimisticTransactionBase => T): T = {
    val txn = startTransaction(catalogTableOpt, snapshotOpt)
    OptimisticTransaction.setActive(txn)
    try {
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
      options: Map[String, String],
      rapidsConf: RapidsConf): GpuDeltaLog = {
    val deltaLog = DeltaLog.forTable(spark, tableLocation, options)
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
