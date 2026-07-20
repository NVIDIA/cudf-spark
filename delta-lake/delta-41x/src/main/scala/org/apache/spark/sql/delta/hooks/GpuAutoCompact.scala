/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
import org.apache.spark.sql.delta._
/**
 * Delta 4.1 version-specific implementation of GpuAutoCompact.
 * Delta 4.1 drives post-commit hooks via CommittedTransaction instead of the older live
 * transaction hook signature used by Delta 4.0.
 */
case object GpuAutoCompact extends GpuAutoCompactBase {

  override def run(
      spark: SparkSession,
      txn: CommittedTransaction): Unit = {
    val conf = spark.sessionState.conf
    val autoCompactTypeOpt = getAutoCompactType(conf, txn.postCommitSnapshot.metadata)
    if (shouldSkipAutoCompact(autoCompactTypeOpt, spark, txn)) return
    compactIfNecessary(spark, txn)
  }

  private def compactIfNecessary(
      spark: SparkSession,
      txn: CommittedTransaction): Unit = {
    val autoCompactRequest = AutoCompactUtils.prepareAutoCompactRequest(
      spark,
      txn,
      OP_TYPE,
      maxDeletedRowsRatio = None)
    executeAutoCompactRequest(
      spark,
      txn.deltaLog,
      txn.catalogTable,
      autoCompactRequest,
      OP_TYPE,
      maxDeletedRowsRatio = None)
  }
}
