/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.{DeltaOperations, OptimisticTransaction}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDelta

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/** GPU version of Delta Lake's WriteIntoDelta. */
case class GpuWriteIntoDelta(
    gpuDeltaLog: GpuDeltaLog,
    cpuWrite: WriteIntoDelta)
    extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    gpuDeltaLog.withNewTransaction { txn =>
      // If this batch has already been executed within this query, then return.
      val skipExecution = hasBeenExecuted(txn)
      if (skipExecution) {
        return Seq.empty
      }

      val actions = cpuWrite.write(txn, sparkSession)
      val operation = DeltaOperations.Write(
        cpuWrite.mode,
        Option(cpuWrite.partitionColumns),
        cpuWrite.options.replaceWhere,
        cpuWrite.options.userMetadata)
      txn.commit(actions, operation)
    }
    Seq.empty
  }

  /**
   * Returns true if there is information in the spark session that indicates that this write, which
   * is part of a streaming query and a batch, has already been successfully written.
   */
  private def hasBeenExecuted(txn: OptimisticTransaction): Boolean = {
    val txnVersion = cpuWrite.options.txnVersion
    val txnAppId = cpuWrite.options.txnAppId
    for (v <- txnVersion; a <- txnAppId) {
      val currentVersion = txn.txnVersion(a)
      if (currentVersion >= v) {
        logInfo(s"Transaction write of version $v for application id $a " +
            s"has already been committed in Delta table id ${txn.deltaLog.tableId}. " +
            s"Skipping this write.")
        return true
      }
    }
    false
  }
}
