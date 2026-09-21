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

package com.nvidia.spark.rapids.delta.delta42x

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.GpuDeltaCatalogBase
import com.nvidia.spark.rapids.delta.common.{GpuDelta4xParquetFileFormat, GpuDeltaParquetFileFormat2}
import com.nvidia.spark.rapids.delta.common.DeltaDynamicPartitionOverwriteCommandMeta
import com.nvidia.spark.rapids.delta.common.DeltaProviderBase
import com.nvidia.spark.rapids.shims.InvalidateCacheShims

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, SupportsWrite}
import org.apache.spark.sql.delta.{DeltaDynamicPartitionOverwriteCommand, DeltaLog, DeltaOptions,
  DeltaParquetFileFormat}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, OptimizeTableCommand,
  UpdateCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlannedTable
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec,
  AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec,
  GpuAtomicReplaceTableAsSelectExec}

object Delta42xProvider extends DeltaProviderBase with Logging {

  override protected def getCDFRelationStrategy = Delta42xCDFRelationStrategy

  override protected def getDeltaLogForV1Write(
      spark: SparkSession,
      deltaTable: DeltaTableV2): DeltaLog = deltaTable.deltaLog

  override protected def getCatalogTableForV1Write(
      deltaTable: DeltaTableV2): Option[CatalogTable] = deltaTable.catalogTable

  override protected def getRecachePlanForV1Write(
      deltaLog: DeltaLog,
      deltaTable: DeltaTableV2): LogicalPlan = deltaTable.toLogicalRelation

  override protected def getRefreshTableNameForV1Write(
      deltaTable: DeltaTableV2): Option[String] =
    deltaTable.catalogTable.map(_.identifier.quotedString)

  override protected def createWriteIntoDelta(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      configuration: Map[String, String],
      data: DataFrame,
      catalogTable: Option[CatalogTable]): WriteIntoDelta = {
    WriteIntoDelta(deltaLog, mode, options, Nil, configuration, data, catalogTable)
  }

  private def tagIfUnityCatalogUnsupported(
      meta: RapidsMeta[_, _, _],
      catalog: StagingTableCatalog): Unit = {
    if (GpuDeltaCatalog.isUnityCatalog(catalog)) {
      GpuDeltaCatalog.unsupportedUnityCatalogReason(catalog).foreach(reason =>
        meta.willNotWorkOnGpu(reason))
    }
  }

  private def tagIfTargetTableUnsupported(
      meta: RapidsMeta[_, _, _],
      cpuExec: AtomicReplaceTableAsSelectExec): Unit = {
    if (cpuExec.catalog.tableExists(cpuExec.ident)) {
      cpuExec.catalog.loadTable(cpuExec.ident) match {
        case _: ServerSidePlannedTable =>
          meta.willNotWorkOnGpu(
            "Delta 4.2 server-side planned table replacement is not supported on GPU")
        case _ =>
      }
    }
  }

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    write == classOf[DeltaTableV2] || write == classOf[GpuDeltaCatalogBase#GpuStagedDeltaTableV2]
  }

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = {
    super.isSupportedCatalog(catalogClass) ||
      catalogClass.getCanonicalName == "io.unitycatalog.spark.UCSingleCatalog"
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
    super.isSupportedFormat(format) || format == classOf[GpuDelta4xParquetFileFormat]

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    tagIfUnityCatalogUnsupported(meta, cpuExec.catalog)
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    tagIfUnityCatalogUnsupported(meta, cpuExec.catalog)
    tagIfTargetTableUnsupported(meta, cpuExec)
  }

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    cpuExec.table match {
      case _: DeltaTableV2 => super.tagForGpu(cpuExec, meta)
      case _: GpuDeltaCatalogBase#GpuStagedDeltaTableV2 =>
      case _ => meta.willNotWorkOnGpu(s"${cpuExec.table} table class not supported on GPU")
    }
  }

  override def tagForGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    cpuExec.table match {
      case _: DeltaTableV2 => super.tagForGpu(cpuExec, meta)
      case _: GpuDeltaCatalogBase#GpuStagedDeltaTableV2 =>
      case _ => meta.willNotWorkOnGpu(s"${cpuExec.table} table class not supported on GPU")
    }
  }

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
          "Delete rows from a Delta Lake table",
          (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[UpdateCommand](
          "Update rows from a Delta Lake table",
          (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommand](
          "Merge of a source query/table into a Delta Lake table",
          (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[OptimizeTableCommand](
          "Optimize a Delta Lake table",
          (a, conf, p, r) => new OptimizeTableCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[DeltaDynamicPartitionOverwriteCommand](
        "Dynamic partition overwrite to a Delta Lake table",
        (a, conf, p, r) => new DeltaDynamicPartitionOverwriteCommandMeta(a, conf, p, r)),
      DeltaReorgTableCommandMeta.rule
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override protected def toGpuParquetFileFormat(conf: RapidsConf, fmt: DeltaParquetFileFormat)
  : FileFormat = {
    if (isPushDVPredicateDownEnabled(conf)) {
      GpuDeltaParquetFileFormat2(
        protocol = fmt.protocol,
        metadata = fmt.metadata,
        nullableRowTrackingFields = false,
        optimizationsEnabled = fmt.optimizationsEnabled,
        tablePath = fmt.tablePath,
        isCDCRead = fmt.isCDCRead)
    } else {
      val optimizationsEnabled = if (fmt.hasTablePath) {
        logWarning("Input Delta table has deletion vectors. Optimizations such as file splitting " +
          "and predicate pushdown are currently not supported for this table " +
          "(https://github.com/NVIDIA/spark-rapids/issues/13999). If you see performance issues, " +
          "consider disabling deletion vectors and running the optimize command on the table. " +
          "See https://docs.delta.io/delta-deletion-vectors/#apply-changes-to-parquet-data-files " +
          "for more details about how to apply delete changes to physical files.")
        false
      } else {
        fmt.optimizationsEnabled
      }
      GpuDelta4xParquetFileFormat(
        protocol = fmt.protocol,
        metadata = fmt.metadata,
        nullableRowTrackingFields = false,
        optimizationsEnabled = optimizationsEnabled,
        tablePath = fmt.tablePath,
        isCDCRead = fmt.isCDCRead)
    }
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      toGpuCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    GpuAtomicReplaceTableAsSelectExec(
      toGpuCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      InvalidateCacheShims.getInvalidateCache(
        cpuExec.invalidateCache,
        cpuExec.catalog,
        Some(qualifiedTableName(cpuExec.catalog.name(), cpuExec.ident))))
  }

  private def qualifiedTableName(catalogName: String, ident: Identifier): String = {
    def quote(part: String): String = s"`${part.replace("`", "``")}`"
    (Seq(catalogName) ++ ident.namespace() :+ ident.name()).map(quote).mkString(".")
  }

  private def toGpuCatalog(
      catalog: StagingTableCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = catalog match {
    case delta: DeltaCatalog => DeltaRuntimeShim.getGpuDeltaCatalog(delta, rapidsConf)
    case uc if GpuDeltaCatalog.isUnityCatalog(uc) =>
      GpuDeltaCatalog.wrapUnityCatalog(uc, rapidsConf)
    case other => throw new IllegalStateException(s"Unsupported Delta catalog: ${other.getClass}")
  }

  override def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    cpuExec.table match {
      case _: DeltaTableV2 =>
        super.convertToGpu(cpuExec, meta)
      case _: GpuDeltaCatalogBase#GpuStagedDeltaTableV2 =>
        GpuAppendDataExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
      case unknown =>
        throw new IllegalStateException(
          s"Unsupported table type for GPU conversion: $unknown. " +
            "Expected DeltaTableV2 or GpuStagedDeltaTableV2")
    }
  }

  override def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec = {
    cpuExec.table match {
      case _: DeltaTableV2 =>
        super.convertToGpu(cpuExec, meta)
      case _: GpuDeltaCatalogBase#GpuStagedDeltaTableV2 =>
        GpuOverwriteByExpressionExecV1(
          cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
      case unknown =>
        throw new IllegalStateException(
          s"Unsupported table type for GPU conversion: $unknown. " +
            "Expected DeltaTableV2 or GpuStagedDeltaTableV2")
    }
  }
}
