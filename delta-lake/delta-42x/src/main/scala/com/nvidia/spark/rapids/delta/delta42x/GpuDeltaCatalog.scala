/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from DeltaDataSource.scala in the
 * Delta Lake project at https://github.com/delta-io/delta.
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

package com.nvidia.spark.rapids.delta.delta42x

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Modifier, Proxy}

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsConf
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier,
  StagingTableCatalog, TableCatalog}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.rapids.{
  GpuCreateDeltaTableCommand40x42xBase,
  GpuDeltaCatalog4x,
  GpuWriteIntoDeltaLike}
import org.apache.spark.sql.delta.rapids.delta42x.GpuCreateDeltaTableCommand
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

class GpuDeltaCatalog(
    cpuCatalog: DeltaCatalog,
    rapidsConf: RapidsConf)
  extends GpuDeltaCatalog4x(cpuCatalog, rapidsConf) {

  override def name(): String = cpuCatalog.name()

  override protected lazy val isUnityCatalog: Boolean = {
    val delegateField = classOf[DelegatingCatalogExtension].getDeclaredField("delegate")
    delegateField.setAccessible(true)
    delegateField.get(cpuCatalog).getClass.getCanonicalName.startsWith("io.unitycatalog.")
  }

  override protected def getTableIdentifier(ident: Identifier): TableIdentifier = {
    val table = super.getTableIdentifier(ident)
    if (isUnityCatalog) {
      table.copy(catalog = Some(cpuCatalog.name()))
    } else {
      table
    }
  }

  override protected def getExistingTableIfExists(
      table: TableIdentifier,
      ident: Identifier,
      operation: TableCreationModes.CreationMode): Option[CatalogTable] = {
    cpuCatalog.getExistingTableIfExists(table, Some(ident), operation)
  }

  override protected def useCatalogCreateTable(sourceQuery: Option[DataFrame]): Boolean = {
    isUnityCatalog
  }

  override protected def getDeltaLogForWrite(
      existingTableOpt: Option[CatalogTable],
      tablePath: Path,
      fileSystemOptions: Map[String, String]): DeltaLog = {
    DeltaUtils.getDeltaLogFromTableOrPath(
      spark, existingTableOpt, tablePath, fileSystemOptions)
  }

  override protected def normalizeStagedTableProperties(
      properties: java.util.Map[String, String]): Unit = {
    if (isUnityCatalog) {
      Option(properties.remove(UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD)).foreach {
        oldTableId =>
          properties.putIfAbsent(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, oldTableId)
      }
    }
  }

  override protected def createTableInCatalog(
      ident: Identifier,
      table: CatalogTable): Unit = {
    val delegateField = classOf[DelegatingCatalogExtension].getDeclaredField("delegate")
    delegateField.setAccessible(true)
    val delegate = delegateField.get(cpuCatalog).asInstanceOf[TableCatalog]
    val v1Table = org.apache.spark.sql.delta.rapids.DeltaTrampoline.getV1Table(table)
    // Spark 4.1 deprecates this overload in favor of TableInfo, which Spark 4.0 does not expose.
    (delegate.createTable(
      ident, v1Table.columns(), v1Table.partitioning, v1Table.properties):
      @scala.annotation.nowarn("cat=deprecation"))
  }

  override protected def buildGpuCreateDeltaTableCommand(
      withDb: CatalogTable,
      existingTableOpt: Option[CatalogTable],
      mode: SaveMode,
      writer: Option[GpuWriteIntoDeltaLike],
      operation: TableCreationModes.CreationMode,
      isByPath: Boolean,
      tableCreateFunc: Option[CatalogTable => Unit]): GpuCreateDeltaTableCommand40x42xBase = {
    GpuCreateDeltaTableCommand(
      withDb,
      existingTableOpt,
      operation.mode,
      writer,
      operation,
      tableByPath = isByPath,
      allowCatalogManaged = isUnityCatalog && withDb.tableType == CatalogTableType.MANAGED,
      createTableFunc = tableCreateFunc)(rapidsConf)
  }
}

object GpuDeltaCatalog {
  private val UnityCatalogClassName = "io.unitycatalog.spark.UCSingleCatalog"

  def isUnityCatalog(catalog: StagingTableCatalog): Boolean = {
    catalog.getClass.getCanonicalName == UnityCatalogClassName
  }

  /** Instance fields this class inherits, qualified by the class that declares them. */
  private def inheritedInstanceFields(clazz: Class[_]): Seq[String] = {
    Option(clazz.getSuperclass).toSeq.flatMap { superClass =>
      superClass.getDeclaredFields
        .filterNot(field => Modifier.isStatic(field.getModifiers))
        .map(field => s"${superClass.getName}.${field.getName}") ++
        inheritedInstanceFields(superClass)
    }
  }

  /**
   * Checks whether [[wrapUnityCatalog]] understands the internals of this Unity Catalog build.
   *
   * The wrapper depends on private state that is not part of any public API. Unity Catalog
   * versions other than the one this integration was written against are still accepted, so these
   * checks run while tagging: an unrecognized build then falls back to the CPU with a reason
   * instead of failing the query part-way through plan conversion.
   *
   * @return the reason the catalog cannot be wrapped, or `None` when it can be
   */
  def unsupportedUnityCatalogReason(catalog: StagingTableCatalog): Option[String] = {
    val catalogClass = catalog.getClass
    try {
      // A staging-only copy is built by cloning every declared field, so state declared by a
      // superclass would be silently dropped rather than reported.
      val inherited = inheritedInstanceFields(catalogClass)
      val delegateField = catalogClass.getDeclaredField("delegate")
      delegateField.setAccessible(true)
      catalogClass.getDeclaredConstructor()
      if (inherited.nonEmpty) {
        Some(s"$UnityCatalogClassName declares inherited state ${inherited.mkString(", ")} that " +
          "the GPU Delta catalog wrapper does not know how to copy")
      } else if (!delegateField.getType.isAssignableFrom(classOf[GpuDeltaCatalog])) {
        Some(s"$UnityCatalogClassName delegate field of type ${delegateField.getType.getName} " +
          "cannot hold the GPU Delta catalog")
      } else if (!delegateField.get(catalog).isInstanceOf[DeltaCatalog]) {
        Some(s"$UnityCatalogClassName delegate " +
          s"${delegateField.get(catalog).getClass.getName} is not a Delta catalog")
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        Some(s"$UnityCatalogClassName internals are not recognized by the GPU Delta catalog " +
          s"wrapper: $e")
    }
  }

  /**
   * Wraps OSS Unity Catalog while preserving its staging protocol.
   *
   * UC owns staging-table allocation, managed locations, credentials, and catalog commit. Its
   * private delegate is Delta's catalog. A staging-only copy routes that delegate through the GPU
   * Delta catalog so CPU sessions and non-stage catalog operations retain the original behavior.
   */
  def wrapUnityCatalog(
      catalog: StagingTableCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = {
    require(isUnityCatalog(catalog), s"Expected OSS Unity Catalog, found ${catalog.getClass}")
    // Tagging rejects these catalogs, so reaching here means the check above was skipped.
    unsupportedUnityCatalogReason(catalog).foreach(reason =>
      throw new IllegalStateException(reason))

    val delegateField = catalog.getClass.getDeclaredField("delegate")
    delegateField.setAccessible(true)
    val deltaCatalog = delegateField.get(catalog) match {
      case delta: DeltaCatalog => delta
      case other => throw new IllegalStateException(
        s"OSS Unity Catalog delegate ${other.getClass} is not DeltaCatalog")
    }
    val gpuDeltaCatalog = new GpuDeltaCatalog(deltaCatalog, rapidsConf)
    val stagingCatalog = catalog.getClass.getDeclaredConstructor().newInstance()
      .asInstanceOf[StagingTableCatalog]
    catalog.getClass.getDeclaredFields
      .filterNot(field => Modifier.isStatic(field.getModifiers))
      .foreach { field =>
        field.setAccessible(true)
        field.set(stagingCatalog, field.get(catalog))
      }
    delegateField.set(stagingCatalog, gpuDeltaCatalog)

    val handler = new InvocationHandler {
      override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
        def invokeCatalog(target: Object): Object = {
          try {
            method.invoke(target, Option(args).getOrElse(Array.empty[Object]): _*)
          } catch {
            case error: InvocationTargetException => throw error.getCause
          }
        }

        if (method.getName.startsWith("stage")) {
          invokeCatalog(stagingCatalog)
        } else {
          invokeCatalog(catalog)
        }
      }
    }

    Proxy.newProxyInstance(
      catalog.getClass.getClassLoader,
      Array(classOf[StagingTableCatalog]),
      handler).asInstanceOf[StagingTableCatalog]
  }
}
