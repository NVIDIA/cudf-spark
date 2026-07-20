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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.{
  AppendDataExecV1Meta,
  AtomicCreateTableAsSelectExecMeta,
  AtomicReplaceTableAsSelectExecMeta,
  CreatableRelationProviderRule,
  ExecRule,
  ExprRule,
  GpuExec,
  OverwriteByExpressionExecV1Meta,
  RapidsConf,
  RunnableCommandRule,
  ShimLoaderTemp,
  SparkPlanMeta
}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.{StagingTableCatalog, SupportsWrite}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.sources.CreatableRelationProvider

/** Probe interface to determine which Delta Lake provider to use. */
trait DeltaProbe {
  def getDeltaProvider: DeltaProvider
}

/** Interfaces to avoid accessing the optional Delta Lake jars directly in common code. */
trait DeltaProvider {
  def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]]

  def getExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]

  def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]]

  def getStrategyRules: Seq[SparkStrategy]

  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Map.empty

  def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean

  def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit

  def getReadFileFormat(relation: HadoopFsRelation, rapidsConf: RapidsConf): FileFormat

  def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean

  def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit

  def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec

  def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit

  def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec

  def tagForGpu(cpuExec: AppendDataExecV1,meta: AppendDataExecV1Meta): Unit

  def convertToGpu(cpuExec: AppendDataExecV1, meta: AppendDataExecV1Meta): GpuExec

  def tagForGpu(cpuExec: OverwriteByExpressionExecV1, meta: OverwriteByExpressionExecV1Meta): Unit

  def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec

  /**
   * Returns true if deletion vector predicate pushdown is enabled via configuration.
   */
  def isPushDVPredicateDownEnabled(conf: RapidsConf): Boolean = false

  /**
   * Tries to push down deletion vector predicates to the scan.
   */
  def tryPushDVPredicateDownToScan(plan: SparkPlan): SparkPlan = plan

  def pruneFileMetadata(plan: SparkPlan): SparkPlan = plan

  def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = false
}

object DeltaProvider {
  private lazy val provider = {
    ShimLoaderTemp.newDeltaProbe().getDeltaProvider
  }

  def apply(): DeltaProvider = provider
}

object NoDeltaProvider extends DeltaProvider {
  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = Map.empty

  override def getExecRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Map.empty

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = Map.empty

  override def getStrategyRules: Seq[SparkStrategy] = Nil

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = false

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = false

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit =
    throw new IllegalStateException("unsupported format")

  override def getReadFileFormat(relation: HadoopFsRelation, rapidsConf: RapidsConf): FileFormat =
    throw new IllegalStateException("unsupported format")

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = false

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    throw new IllegalStateException("catalog not supported, should not be called")
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    throw new IllegalStateException("catalog not supported, should not be called")
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    throw new IllegalStateException("catalog not supported, should not be called")
  }

  override def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    throw new IllegalStateException("catalog not supported, should not be called")
  }

  override def tagForGpu(cpuExec: AppendDataExecV1, meta: AppendDataExecV1Meta): Unit = {
    throw new IllegalStateException("write not supported, should not be called")
  }

  override def convertToGpu(cpuExec: AppendDataExecV1, meta: AppendDataExecV1Meta): GpuExec = {
    throw new IllegalStateException("write not supported, should not be called")
  }

  override def tagForGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    throw new IllegalStateException("write not supported, should not be called")
  }

  override def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec = {
    throw new IllegalStateException("write not supported, should not be called")
  }
}
