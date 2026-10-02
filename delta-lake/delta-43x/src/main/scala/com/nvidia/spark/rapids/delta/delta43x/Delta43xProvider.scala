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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.GpuDeltaCatalogBase
import com.nvidia.spark.rapids.delta.common.{DeleteCommandMeta,
  DeltaDynamicPartitionOverwriteCommandMeta, UpdateCommandMeta}
import com.nvidia.spark.rapids.delta.common.{GpuDelta4xParquetFileFormat, GpuDeltaParquetFileFormat2}
import com.nvidia.spark.rapids.delta.common.DeltaProviderBase

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, SupportsWrite,
  TableCatalog}
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaConfigs,
  DeltaDynamicPartitionOverwriteCommand, DeltaParquetFileFormat, IcebergCompat,
  MaterializePartitionColumnsTableFeature}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaCatalogRestApiShim, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, OptimizeTableCommand,
  UpdateCommand}
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTableUtils
import org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlannedTable
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec,
  AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.internal.SQLConf

object Delta43xProvider extends DeltaProviderBase with Logging {

  private val UNITY_CATALOG_CLASS_NAME = "io.unitycatalog.spark.UCSingleCatalog"
  private val REST_API_FALLBACK_REASON =
    "Delta 4.3 Unity Catalog Delta REST API operations must run on CPU"

  override protected def getCDFRelationStrategy = Delta43xCDFRelationStrategy

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = {
    super.isSupportedCatalog(catalogClass) ||
      catalogClass.getCanonicalName == UNITY_CATALOG_CLASS_NAME
  }

  private def tagIfUnityCatalog(
      meta: RapidsMeta[_, _, _],
      catalog: StagingTableCatalog): Boolean = {
    if (catalog.getClass.getCanonicalName != UNITY_CATALOG_CLASS_NAME) {
      false
    } else {
      val reason = try {
        val delegateField = catalog.getClass.getDeclaredField("delegate")
        delegateField.setAccessible(true)
        delegateField.get(catalog) match {
          case delta: DeltaCatalog if DeltaCatalogRestApiShim.isRestApiEnabled(delta) =>
            REST_API_FALLBACK_REASON
          case _: DeltaCatalog =>
            "Delta 4.3 Unity Catalog writes without the Delta REST API are not supported on GPU"
          case other =>
            s"$UNITY_CATALOG_CLASS_NAME delegate ${other.getClass.getName} is not a Delta catalog"
        }
      } catch {
        case NonFatal(e) =>
          s"$UNITY_CATALOG_CLASS_NAME internals are not recognized for safe GPU staging: $e"
      }
      meta.willNotWorkOnGpu(reason)
      true
    }
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
    val provider = properties.collectFirst {
      case (key, value) if key.toLowerCase(Locale.ROOT) == TableCatalog.PROP_PROVIDER => value
    }.getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
    org.apache.spark.sql.delta.sources.DeltaSourceUtils.isDeltaDataSourceName(provider)
  }

  private def tagIfCatalogManagedCreate(
      meta: RapidsMeta[_, _, _],
      catalog: DeltaCatalog,
      ident: Identifier,
      properties: Map[String, String],
      spark: SparkSession): Unit = {
    if (isDeltaProvider(properties, spark) &&
        (DeltaCatalogRestApiShim.shouldRouteCreate(catalog, ident, properties.asJava, spark) ||
          isCatalogManagedByProperty(properties, spark))) {
      meta.willNotWorkOnGpu(REST_API_FALLBACK_REASON)
    }
  }

  private def tagIfCatalogManagedReplace(
      meta: RapidsMeta[_, _, _],
      catalog: DeltaCatalog,
      ident: Identifier,
      properties: Map[String, String],
      spark: SparkSession): Unit = {
    if (isDeltaProvider(properties, spark) &&
        (DeltaCatalogRestApiShim.shouldRouteOrValidateReplace(
          catalog, ident, properties.asJava, spark) ||
          isCatalogManagedByProperty(properties, spark))) {
      meta.willNotWorkOnGpu(REST_API_FALLBACK_REASON)
    }
  }

  private def tagIfTargetTableUnsupported(
      meta: RapidsMeta[_, _, _],
      cpuExec: AtomicReplaceTableAsSelectExec): Unit = {
    if (cpuExec.catalog.tableExists(cpuExec.ident)) {
      cpuExec.catalog.loadTable(cpuExec.ident) match {
        case table: DeltaTableV2 =>
          val snapshot = table.deltaLog.unsafeVolatileSnapshot
          if (snapshot.isCatalogOwned) {
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

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    write == classOf[DeltaTableV2] || write == classOf[GpuDeltaCatalogBase#GpuStagedDeltaTableV2]
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
    super.isSupportedFormat(format) || format == classOf[GpuDelta4xParquetFileFormat]

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    if (!tagIfUnityCatalog(meta, cpuExec.catalog)) {
      tagIfCatalogManagedCreate(
        meta,
        cpuExec.catalog.asInstanceOf[DeltaCatalog],
        cpuExec.ident,
        cpuExec.properties,
        cpuExec.session)
    }
    tagIfUnsupportedWriterFeatures(
      meta, cpuExec.properties, cpuExec.partitioning.nonEmpty, cpuExec.session)
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)
    if (!tagIfUnityCatalog(meta, cpuExec.catalog)) {
      tagIfCatalogManagedReplace(
        meta,
        cpuExec.catalog.asInstanceOf[DeltaCatalog],
        cpuExec.ident,
        cpuExec.properties,
        cpuExec.session)
      tagIfTargetTableUnsupported(meta, cpuExec)
    }
    tagIfUnsupportedWriterFeatures(
      meta, cpuExec.properties, cpuExec.partitioning.nonEmpty, cpuExec.session)
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
