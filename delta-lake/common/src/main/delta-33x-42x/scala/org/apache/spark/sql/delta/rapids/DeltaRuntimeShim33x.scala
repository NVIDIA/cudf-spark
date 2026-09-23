/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, NumRecordsStats, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.{DeltaReorgOperation, WriteIntoDelta}
import org.apache.spark.sql.types.StructType

/** Runtime APIs shared by Delta 3.3 and later. */
trait DeltaRuntimeShim33x extends DeltaRuntimeShim {
  def runDeltaOperation[A](deltaLog: DeltaLog, opType: String)(thunk: => A): A

  def emitDeltaEvent(deltaLog: DeltaLog, opType: String, data: AnyRef): Unit

  def assertRemovable(snapshot: Snapshot): Unit

  def filterFilesToReorg(
      operation: DeltaReorgOperation,
      spark: SparkSession,
      snapshot: Snapshot,
      candidates: Seq[AddFile]): Seq[AddFile]

  def preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns: DataFrame,
      snapshot: Snapshot,
      targetOutput: Seq[Attribute],
      updateExpressions: Seq[Expression]): (DataFrame, Seq[Attribute], Seq[Expression])

  override def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    WriteIntoDelta(
      deltaLog,
      mode,
      options,
      partitionColumns,
      configuration,
      data,
      catalogTableOpt,
      schemaInCatalog)
  }

  def groupOptimizeFilesByPartition(
      spark: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile]): Seq[(Map[String, String], Seq[AddFile])] = {
    files.groupBy(_.partitionValues).toSeq
  }

  def reportSomeZeroMetrics(
      spark: SparkSession,
      numCopiedRows: Option[Long],
      numDeletedRows: Option[Long]): (Option[Long], Option[Long]) = {
    (numCopiedRows, numDeletedRows)
  }

  def validateDeleteNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit = {}

  def validateUpdateNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit = {}
}

object DeltaRuntimeShim33x {
  private def shimInstance: DeltaRuntimeShim33x =
    DeltaRuntimeShim.getShimInstance.asInstanceOf[DeltaRuntimeShim33x]

  def runDeltaOperation[A](deltaLog: DeltaLog, opType: String)(thunk: => A): A =
    shimInstance.runDeltaOperation(deltaLog, opType)(thunk)

  def emitDeltaEvent(deltaLog: DeltaLog, opType: String, data: AnyRef): Unit =
    shimInstance.emitDeltaEvent(deltaLog, opType, data)

  def assertRemovable(snapshot: Snapshot): Unit = shimInstance.assertRemovable(snapshot)

  def filterFilesToReorg(
      operation: DeltaReorgOperation,
      spark: SparkSession,
      snapshot: Snapshot,
      candidates: Seq[AddFile]): Seq[AddFile] =
    shimInstance.filterFilesToReorg(operation, spark, snapshot, candidates)

  def preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns: DataFrame,
      snapshot: Snapshot,
      targetOutput: Seq[Attribute],
      updateExpressions: Seq[Expression]): (DataFrame, Seq[Attribute], Seq[Expression]) =
    shimInstance.preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns, snapshot, targetOutput, updateExpressions)

  def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    shimInstance.createCpuWrite(
      deltaLog, mode, options, partitionColumns, configuration, data,
      catalogTableOpt, schemaInCatalog)
  }

  def groupOptimizeFilesByPartition(
      spark: SparkSession,
      snapshot: Snapshot,
      files: Seq[AddFile]): Seq[(Map[String, String], Seq[AddFile])] =
    shimInstance.groupOptimizeFilesByPartition(spark, snapshot, files)

  def reportSomeZeroMetrics(
      spark: SparkSession,
      numCopiedRows: Option[Long],
      numDeletedRows: Option[Long]): (Option[Long], Option[Long]) = {
    shimInstance.reportSomeZeroMetrics(spark, numCopiedRows, numDeletedRows)
  }

  def validateDeleteNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit =
    shimInstance.validateDeleteNumRecords(spark, deltaLog, numRecordsStats)

  def validateUpdateNumRecords(
      spark: SparkSession,
      deltaLog: DeltaLog,
      numRecordsStats: NumRecordsStats): Unit =
    shimInstance.validateUpdateNumRecords(spark, deltaLog, numRecordsStats)
}
