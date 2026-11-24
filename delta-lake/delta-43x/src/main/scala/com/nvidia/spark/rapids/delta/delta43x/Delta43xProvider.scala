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

package com.nvidia.spark.rapids.delta.delta43x

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.{DeltaWriteUtils, GpuDeltaCatalogBase}
import com.nvidia.spark.rapids.delta.common.DeleteCommandMeta
import com.nvidia.spark.rapids.delta.common.{GpuDelta4xParquetFileFormat, GpuDeltaParquetFileFormat2}
import com.nvidia.spark.rapids.delta.common.DeltaProviderBase
import com.nvidia.spark.rapids.delta.delta42x.Delta42xProvider
import com.nvidia.spark.rapids.shims.InvalidateCacheShims

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, SupportsWrite,
  TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaConfigs,
  DeltaDynamicPartitionOverwriteCommand, DeltaLog, DeltaOptions, DeltaParquetFileFormat,
  IcebergCompat, MaterializePartitionColumnsTableFeature}
import org.apache.spark.sql.delta.actions.{Metadata, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaCatalogRestApiShim, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, OptimizeTableCommand,
  UpdateCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTableUtils
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShim, GpuUnityCatalogWrapper}
import org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlannedTable
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec,
  AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec,
  GpuAtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.internal.SQLConf

object Delta43xProvider extends DeltaProviderBase with Logging {

  private val REST_API_FALLBACK_REASON =
    "Delta 4.3 Delta REST API operations that Delta does not route through its REST client " +
      "must run on CPU"
  private val NO_REST_API_FALLBACK_REASON =
    "Delta 4.3 Unity Catalog writes without the Delta REST API are not supported on GPU"
  private val CATALOG_MANAGED_FALLBACK_REASON =
    "Delta 4.3 catalog-managed table writes are not supported on GPU"
  private val NON_DELTA_FALLBACK_REASON =
    "Delta 4.3 non-Delta catalog writes must run on CPU"

  override protected def getCDFRelationStrategy = Delta43xCDFRelationStrategy

  // Delta 4.3 uses the same server-side scan API as 4.2.
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Delta42xProvider.getScans

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
    // Resolve the constructor and its default arguments against Delta 4.3.
    WriteIntoDelta(deltaLog, mode, options, Nil, configuration, data, catalogTable)
  }

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = {
    super.isSupportedCatalog(catalogClass) ||
      catalogClass.getCanonicalName == GpuUnityCatalogWrapper.UnityCatalogClassName
  }

  private def isCatalogManagedByProperty(
      properties: Map[String, String],
      spark: SparkSession): Boolean = {
    val tableFeatures =
      TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(properties)
    tableFeatures.contains(CatalogOwnedTableFeature) ||
      CatalogOwnedTableUtils.defaultCatalogOwnedEnabled(spark)
  }

  private def isDeltaProvider(
      properties: Map[String, String],
      spark: SparkSession): Boolean = {
    // Match DeltaCatalog.getProvider: the exact key and the catalog's captured session.
    val provider = properties.getOrElse(TableCatalog.PROP_PROVIDER,
      spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
    org.apache.spark.sql.delta.sources.DeltaSourceUtils.isDeltaDataSourceName(provider)
  }

  /**
   * Tags a CREATE, CTAS, REPLACE, or RTAS against its catalog.
   *
   * Requests that Delta routes through its Delta REST client are accelerated: the GPU catalog
   * delegates staging to the CPU catalog and its client and only the data plane runs on the GPU.
   * Anything else against a REST catalog, or against Unity Catalog without the REST client, stays
   * on the CPU.
   *
   * @return the CPU Delta catalog and whether Delta routes the request through its REST client,
   *         or `None` when the request was tagged as unsupported
   */
  private def tagCatalog(
      meta: RapidsMeta[_, _, _],
      catalog: StagingTableCatalog,
      ident: Identifier,
      properties: Map[String, String],
      isReplace: Boolean): Option[(DeltaCatalog, Boolean)] = {
    val isUnity = GpuDeltaCatalog.isUnityCatalog(catalog)
    val deltaCatalog = if (isUnity) {
      GpuDeltaCatalog.unsupportedUnityCatalogReason(catalog) match {
        case Some(reason) =>
          meta.willNotWorkOnGpu(reason)
          None
        case None => Some(GpuUnityCatalogWrapper.deltaCatalog(catalog))
      }
    } else {
      Some(catalog.asInstanceOf[DeltaCatalog])
    }
    deltaCatalog.flatMap { delta =>
      val javaProperties = properties.asJava
      val routedThroughRest = if (isReplace) {
        DeltaCatalogRestApiShim.shouldRouteReplace(delta, ident, javaProperties)
      } else {
        DeltaCatalogRestApiShim.shouldRouteCreate(delta, ident, javaProperties)
      }
      if (!isDeltaProvider(properties, delta.spark)) {
        meta.willNotWorkOnGpu(NON_DELTA_FALLBACK_REASON)
        None
      } else if (DeltaCatalogRestApiShim.isRestApiEnabled(delta) && !routedThroughRest) {
        meta.willNotWorkOnGpu(REST_API_FALLBACK_REASON)
        None
      } else if (isUnity && !routedThroughRest) {
        meta.willNotWorkOnGpu(NO_REST_API_FALLBACK_REASON)
        None
      } else if (!routedThroughRest && isCatalogManagedByProperty(properties, delta.spark)) {
        meta.willNotWorkOnGpu(CATALOG_MANAGED_FALLBACK_REASON)
        None
      } else {
        Some((delta, routedThroughRest))
      }
    }
  }

  private def tagIfTargetTableUnsupported(
      meta: RapidsMeta[_, _, _],
      cpuExec: AtomicReplaceTableAsSelectExec,
      routedThroughRest: Boolean): Unit = {
    if (cpuExec.catalog.tableExists(cpuExec.ident)) {
      cpuExec.catalog.loadTable(cpuExec.ident) match {
        case table: DeltaTableV2 =>
          val snapshot = table.deltaLog.unsafeVolatileSnapshot
          // A REST-routed replace commits through the catalog-managed commit path that Delta
          // itself uses, so only unrouted catalog-managed tables must stay on the CPU.
          if (snapshot.isCatalogOwned && !routedThroughRest) {
            meta.willNotWorkOnGpu(
              "Delta 4.3 catalog-managed table writes are not supported on GPU")
          }
          if (cpuExec.partitioning.nonEmpty &&
              snapshot.protocol.isFeatureSupported(MaterializePartitionColumnsTableFeature)) {
            meta.willNotWorkOnGpu(
              "Delta 4.3 materialized partition column writes are not supported on GPU")
          }
        case _: ServerSidePlannedTable =>
          meta.willNotWorkOnGpu(
            "Delta 4.3 server-side planned table replacement is not supported on GPU")
        case _ =>
      }
    }
  }

  private def tagIfUnsupportedWriterFeatures(
      meta: RapidsMeta[_, _, _],
      properties: Map[String, String],
      hasPartitionColumns: Boolean,
      spark: SparkSession): Unit = {
    val effectiveProperties = DeltaConfigs.mergeGlobalConfigs(
      spark.sessionState.conf, properties)
    if (DeltaConfigs.ENABLE_VARIANT_SHREDDING.fromMap(effectiveProperties)) {
      meta.willNotWorkOnGpu("Delta 4.3 variant shredding writes are not supported on GPU")
    }
    val supportedFeatures =
      TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(effectiveProperties)
    val materializesPartitionColumns =
      IcebergCompat.isAnyEnabled(effectiveProperties) ||
        DeltaConfigs.ENABLE_MATERIALIZE_PARTITION_COLUMNS_FEATURE
          .fromMap(effectiveProperties).contains(true) ||
        supportedFeatures.contains(MaterializePartitionColumnsTableFeature)
    if (hasPartitionColumns && materializesPartitionColumns) {
      meta.willNotWorkOnGpu(
        "Delta 4.3 materialized partition column writes are not supported on GPU")
    }
  }

  /**
   * Keeps partitioned Unity Catalog CTAS and RTAS on the CPU unless optimized write is on.
   *
   * Without optimized write, each task writes a file for every partition it holds. For many
   * partitions that is many small files, and the GPU Parquet writer's per-file cost made such
   * writes slower than the CPU fallback this support replaces (about 0.5x measured on a
   * 64-partition CTAS). Optimized write consolidates files per partition, which removes that cost.
   *
   * Only Unity Catalog is affected: other Delta tables already ran on the GPU, and this support
   * must not regress what used to be a CPU write. Clustering is also reported through the
   * partitioning but does not write a file per value, so only identity partitioning counts. The
   * DataFrameWriter options are not visible here, so a write enabling optimized write only
   * through them stays on the CPU.
   */
  private def tagIfSmallFilePartitionedWrite(
      meta: RapidsMeta[_, _, _],
      catalog: StagingTableCatalog,
      properties: Map[String, String],
      partitioning: Seq[Transform],
      spark: SparkSession): Unit = {
    if (GpuDeltaCatalog.isUnityCatalog(catalog) && partitioning.exists(_.name() == "identity")) {
      val effectiveProperties = DeltaConfigs.mergeGlobalConfigs(spark.sessionState.conf, properties)
      val optimizeWrite = DeltaWriteUtils.shouldOptimizeWrite(
        None, spark.sessionState.conf, Metadata(configuration = effectiveProperties))
      SmallFilePartitionedWrite.tagUnlessOptimizeWrite(
        meta, "partitioned Unity Catalog CTAS and RTAS", optimizeWrite)
    }
  }

  /**
   * Appends and overwrites of an existing partitioned catalog-managed table without optimized
   * write stay on the CPU for the same reason as the partitioned CTAS and RTAS above.
   */
  private def tagIfSmallFilePartitionedTableWrite(
      meta: RapidsMeta[_, _, _],
      table: DeltaTableV2): Unit = {
    val snapshot = table.deltaLog.unsafeVolatileSnapshot
    if (snapshot.isCatalogOwned && snapshot.metadata.partitionColumns.nonEmpty) {
      val optimizeWrite = DeltaWriteUtils.shouldOptimizeWrite(
        None, SparkSession.active.sessionState.conf, snapshot.metadata)
      SmallFilePartitionedWrite.tagUnlessOptimizeWrite(
        meta, "writes to a partitioned catalog-managed table", optimizeWrite)
    }
  }

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    // The REST-delegating staged table subclasses the GPU staged table to forward its abort.
    write == classOf[DeltaTableV2] ||
      classOf[GpuDeltaCatalogBase#GpuStagedDeltaTableV2].isAssignableFrom(write)
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
    super.isSupportedFormat(format) || format == classOf[GpuDelta4xParquetFileFormat]

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    tagCatalog(meta, cpuExec.catalog, cpuExec.ident, cpuExec.properties, isReplace = false)
    tagIfUnsupportedWriterFeatures(
      meta, cpuExec.properties, cpuExec.partitioning.nonEmpty, cpuExec.session)
    tagIfSmallFilePartitionedWrite(
      meta, cpuExec.catalog, cpuExec.properties, cpuExec.partitioning, cpuExec.session)
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    tagCatalog(meta, cpuExec.catalog, cpuExec.ident, cpuExec.properties, isReplace = true)
      .foreach { case (_, routedThroughRest) =>
        tagIfTargetTableUnsupported(meta, cpuExec, routedThroughRest)
      }
    tagIfUnsupportedWriterFeatures(
      meta, cpuExec.properties, cpuExec.partitioning.nonEmpty, cpuExec.session)
    tagIfSmallFilePartitionedWrite(
      meta, cpuExec.catalog, cpuExec.properties, cpuExec.partitioning, cpuExec.session)
  }

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    cpuExec.table match {
      case table: DeltaTableV2 =>
        super.tagForGpu(cpuExec, meta)
        tagIfSmallFilePartitionedTableWrite(meta, table)
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
      case table: DeltaTableV2 =>
        super.tagForGpu(cpuExec, meta)
        tagIfSmallFilePartitionedTableWrite(meta, table)
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
          (a, conf, p, r) => new CatalogManagedUpdateCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommand](
          "Merge of a source query/table into a Delta Lake table",
          (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[OptimizeTableCommand](
          "Optimize a Delta Lake table",
          (a, conf, p, r) => new OptimizeTableCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[DeltaDynamicPartitionOverwriteCommand](
        "Dynamic partition overwrite to a Delta Lake table",
        (a, conf, p, r) => new CatalogManagedDynamicPartitionOverwriteCommandMeta(a, conf, p, r)),
      DeltaReorgTableCommandMeta.rule
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override protected def toGpuParquetFileFormat(conf: RapidsConf, fmt: DeltaParquetFileFormat)
  : FileFormat = {
    val needsGeneratedRowIndex = !fmt.optimizationsEnabled && !fmt.hasTablePath
    if (isPushDVPredicateDownEnabled(conf) && !needsGeneratedRowIndex) {
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

  private def toGpuCatalog(
      catalog: StagingTableCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = catalog match {
    case delta: DeltaCatalog => DeltaRuntimeShim.getGpuDeltaCatalog(delta, rapidsConf)
    case uc if GpuDeltaCatalog.isUnityCatalog(uc) =>
      GpuDeltaCatalog.wrapUnityCatalog(uc, rapidsConf)
    case other => throw new IllegalStateException(s"Unsupported Delta catalog: ${other.getClass}")
  }

  private def qualifiedTableName(catalogName: String, ident: Identifier): String = {
    def quote(part: String): String = s"`${part.replace("`", "``")}`"
    (Seq(catalogName) ++ ident.namespace() :+ ident.name()).map(quote).mkString(".")
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
    // A Unity Catalog table is cached under its fully qualified name, which only the catalog
    // that owns the identifier can resolve.
    val qualifiedName = Some(cpuExec.catalog).filter(GpuDeltaCatalog.isUnityCatalog)
      .map(catalog => qualifiedTableName(catalog.name(), cpuExec.ident))
    GpuAtomicReplaceTableAsSelectExec(
      toGpuCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      InvalidateCacheShims.getInvalidateCache(
        cpuExec.invalidateCache, cpuExec.catalog, qualifiedName))
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
