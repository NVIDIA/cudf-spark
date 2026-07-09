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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.{DeltaLog, Snapshot}
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.constraints.{Constraint, DeltaInvariantCheckerExec}
import com.databricks.sql.transaction.tahoe.files.{TahoeBatchFileIndex, TransactionalWriteOptions}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.util.Clock

abstract class GpuOptimisticTransactionBase(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)(implicit clock: Clock)
  extends GpuOptimisticTransactionBaseCommon(deltaLog, catalogTable, snapshot, rapidsConf)(clock) {

  override protected def getCpuInvariantCheckerExec(
      cpuPlan: SparkPlan,
      constraints: Seq[Constraint]): SparkPlan = {
    DeltaInvariantCheckerExec(cpuPlan.session, cpuPlan, constraints)
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: TransactionalWriteOptions,
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      isCDCWritePhase: Boolean,
      context: Option[String]): Seq[FileAction] = {
    if (isLiquidClustering || isCDCWritePhase) {
      super.writeFiles(inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, isCDCWritePhase, context)
    } else {
      writeFiles(inputData, writeOptions.deltaOptions, additionalConstraints)
    }
  }

  override protected def isOptimizeCommand(plan: LogicalPlan): Boolean = {
    val leaves = plan.collectLeaves()
    leaves.size == 1 && leaves.head.collect {
      case LogicalRelation(HadoopFsRelation(
      index: TahoeBatchFileIndex, _, _, _, _, _), _, _, _, _, _, _) =>
        index.actionType.equals("Optimize")
    }.headOption.getOrElse(false)
  }
}
private[rapids] object GpuDeltaCpuFallback {
  def withRapidsDisabled[T](spark: SparkSession)(f: => T): T = {
    val rapidsEnabled = RapidsConf.SQL_ENABLED.key
    val original = spark.conf.getOption(rapidsEnabled)
    spark.conf.set(rapidsEnabled, "false")
    try {
      f
    } finally {
      original match {
        case Some(value) => spark.conf.set(rapidsEnabled, value)
        case None => spark.conf.unset(rapidsEnabled)
      }
    }
  }
}
