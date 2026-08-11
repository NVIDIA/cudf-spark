/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
 * This file was derived from DoAutoCompaction.scala
 * from https://github.com/delta-io/delta/pull/1156
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

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.actions.Action
import com.databricks.sql.transaction.tahoe.hooks.PostCommitHook
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging

import org.apache.spark.sql.SparkSession

object GpuDoAutoCompaction extends PostCommitHook
    with DeltaLogging
    with Serializable {
  override val name: String = "Triggers compaction if necessary"

  override def run(spark: SparkSession,
                   txn: OptimisticTransactionImpl,
                   committedVersion: Long,
                   postCommitSnapshot: Snapshot,
                   committedActions: Seq[Action]): Unit = {
    val gpuTxn = txn.asInstanceOf[GpuOptimisticTransaction]
    val newTxn = new GpuDeltaLog(gpuTxn.deltaLog, gpuTxn.rapidsConf).startTransaction()
    // Note: The Databricks AutoCompact PostCommitHook cannot be used here
    // (with a GpuOptimisticTransaction). It appears that AutoCompact creates a new transaction,
    // thereby circumventing GpuOptimisticTransaction (which intercepts Parquet writes
    // to go through the GPU).
    new GpuOptimizeExecutor(spark, newTxn, Seq.empty, Seq.empty, committedActions).optimize()
  }

  override def handleError(error: Throwable, version: Long): Unit =
    throw DeltaErrors.postCommitHookFailedException(this, version, name, error)
}