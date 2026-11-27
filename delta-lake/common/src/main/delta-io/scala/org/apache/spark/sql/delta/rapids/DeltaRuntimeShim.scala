/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import scala.util.Try

import com.nvidia.spark.rapids.{RapidsConf, ShimReflectionUtils, VersionUtils}
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions, DeltaUDF, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Clock

case class StartTransactionArg(log: DeltaLog, conf: RapidsConf, clock: Clock,
    catalogTable: Option[CatalogTable] = None, snapshot: Option[Snapshot] = None)

/** The V1 entry points that prepare an upstream selective replacement command. */
sealed trait DeltaWriteOrigin

object DeltaWriteOrigin {
  case object V1Save extends DeltaWriteOrigin
  case object V1SaveAsTable extends DeltaWriteOrigin
}

trait DeltaRuntimeShim {
  def getDeltaConfigChecker: DeltaConfigChecker
  def getDeltaProvider: DeltaProvider

  /** Unknown runtimes must not assume that a VARIANT table contains only unshredded files. */
  def mayContainShreddedVariant(protocol: Protocol): Boolean = true
  def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock)
  : GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }
  def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta,
      origin: DeltaWriteOrigin): GpuWriteIntoDeltaLike = createGpuWrite(gpuDeltaLog, cpuWrite)

  def createCpuWrite(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      catalogTableOpt: Option[CatalogTable],
      schemaInCatalog: Option[StructType]): WriteIntoDelta = {
    require(catalogTableOpt.isEmpty, "Catalog tables require Delta 3.3 or later")
    WriteIntoDelta(
      deltaLog,
      mode,
      options,
      partitionColumns,
      configuration,
      data,
      schemaInCatalog = schemaInCatalog)
  }

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Write operation metadata is not implemented")
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    throw new UnsupportedOperationException("Replace table metadata is not implemented")
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog

  /**
   * Detect a DataFrameWriter V1 mode("overwrite").saveAsTable operation so it retains the
   * existing table metadata. Delta versions before 4.1 require stack-trace inspection.
   */
  def isV1WriterSaveAsTableOverwrite(options: DeltaOptions, mode: SaveMode): Boolean = {
    mode == SaveMode.Overwrite && Thread.currentThread().getStackTrace.exists(_.toString.contains(
      classOf[DataFrameWriter[_]].getCanonicalName + "."))
  }
}

object DeltaRuntimeShim {
  private val SparkVersion = """^(\d+)\.(\d+)\.(\d+).*""".r
  private val DELTA_MAJOR_MINOR = """^(\d+)\.(\d+)\..*""".r

  private def isDelta42Plus(deltaVersion: String): Boolean = deltaVersion match {
    case DELTA_MAJOR_MINOR(major, minor) =>
      val maj = BigInt(major)
      maj > 4 || (maj == 4 && BigInt(minor) >= 2)
    case _ => false
  }

  private[rapids] def getDelta42PlusShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    (deltaVersion, sparkVersion) match {
      case ("4.2.0", SparkVersion("4", "0", "1") | SparkVersion("4", "1", "1")) =>
        Some("org.apache.spark.sql.delta.rapids.delta42x.Delta42xRuntimeShim")
      case ("4.3.0", SparkVersion("4", "0", "1") | SparkVersion("4", "1", "1")) =>
        Some("org.apache.spark.sql.delta.rapids.delta43x.Delta43xRuntimeShim")
      case _ => None
    }
  }

  /**
   * Spark 3.5.3+ builds bundle only the Delta 3.3 shim, which links against classes that older
   * Delta releases lack (e.g. LoggingShims), so the Delta version on the classpath must be
   * checked before the shim is loaded.
   */
  private[rapids] def getDelta33ShimClassName(deltaVersion: String): Option[String] = {
    if (deltaVersion.startsWith("3.3.")) {
      Some("org.apache.spark.sql.delta.rapids.delta33x.Delta33xRuntimeShim")
    } else {
      None
    }
  }

  private[rapids] def getPreDelta42ShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    (deltaVersion, sparkVersion) match {
      case (version, SparkVersion("4", "0", _)) if version.startsWith("4.0.") =>
        Some("org.apache.spark.sql.delta.rapids.delta40x.Delta40xRuntimeShim")
      case ("4.1.0", SparkVersion("4", "1", "0" | "1")) =>
        Some("org.apache.spark.sql.delta.rapids.delta41x.Delta41xRuntimeShim")
      case (_, SparkVersion(major, _, _)) if BigInt(major) >= 4 =>
        None
      case _ =>
        getPreSpark4ShimClassName(deltaVersion)
    }
  }

  private def getPreSpark4ShimClassName(deltaVersion: String): Option[String] = {
    if (VersionUtils.cmpSparkVersion(3, 2, 0) < 0) {
      None
    } else if (VersionUtils.cmpSparkVersion(3, 3, 0) < 0) {
      Some("org.apache.spark.sql.delta.rapids.delta20x.Delta20xRuntimeShim")
    } else if (VersionUtils.cmpSparkVersion(3, 4, 0) < 0) {
      // Could not find a Delta Lake API to determine what version is being run,
      // so this resorts to "fingerprinting" via reflection probing.
      Try {
        DeltaUDF.getClass.getMethod("stringStringUdf", classOf[String => String])
      }.map(_ => "org.apache.spark.sql.delta.rapids.delta21x.Delta21xRuntimeShim")
        .orElse {
          Try {
            classOf[DeltaLog].getMethod("assertRemovable")
          }.map(_ => "org.apache.spark.sql.delta.rapids.delta22x.Delta22xRuntimeShim")
        }.toOption.orElse(Some("org.apache.spark.sql.delta.rapids.delta23x.Delta23xRuntimeShim"))
    } else if (VersionUtils.cmpSparkVersion(3, 5, 0) < 0) {
      Some("org.apache.spark.sql.delta.rapids.delta24x.Delta24xRuntimeShim")
    } else if (VersionUtils.cmpSparkVersion(3, 5, 2) > 0 &&
               VersionUtils.cmpSparkVersion(4, 0, 0) < 0) {
      getDelta33ShimClassName(deltaVersion)
    } else {
      None
    }
  }

  private[rapids] def getShimClassName(
      deltaVersion: String = io.delta.VERSION): Option[String] = {
    if (isDelta42Plus(deltaVersion)) {
      getDelta42PlusShimClassName(deltaVersion, SPARK_VERSION)
    } else {
      getPreDelta42ShimClassName(deltaVersion, SPARK_VERSION)
    }
  }

  private lazy val optionalShim = getShimClassName().map { name =>
    val cls = ShimReflectionUtils.loadClass(name)
    cls.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  private[rapids] lazy val shimInstance: DeltaRuntimeShim =
    optionalShim.getOrElse(throw new IllegalStateException("No supported Delta runtime shim"))

  private[rapids] def getShimInstance: DeltaRuntimeShim = shimInstance

  def getDeltaProvider: Option[DeltaProvider] = optionalShim.map(_.getDeltaProvider)

  def mayContainShreddedVariant(protocol: Protocol): Boolean =
    shimInstance.mayContainShreddedVariant(protocol)

  def getDeltaConfigChecker: DeltaConfigChecker = {
    shimInstance.getDeltaConfigChecker
  }

  def startTransaction(txArg: StartTransactionArg): GpuOptimisticTransactionBase = {
    shimInstance.startTransaction(txArg)
  }

  def stringFromStringUdf(f: String => String): UserDefinedFunction =
    shimInstance.stringFromStringUdf(f)

  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    shimInstance.unsafeVolatileSnapshotFromLog(deltaLog)

  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    shimInstance.fileFormatFromLog(deltaLog)

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike = {
    shimInstance.createGpuWrite(gpuDeltaLog, cpuWrite)
  }

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta,
      origin: DeltaWriteOrigin): GpuWriteIntoDeltaLike = {
    shimInstance.createGpuWrite(gpuDeltaLog, cpuWrite, origin)
  }

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

  def buildWriteOperation(
      mode: SaveMode,
      partitionColumns: Seq[String],
      options: DeltaOptions): DeltaOperations.Operation = {
    shimInstance.buildWriteOperation(mode, partitionColumns, options)
  }

  def buildReplaceTableOperation(
      metadata: Metadata,
      isManaged: Boolean,
      orCreate: Boolean,
      asSelect: Boolean,
      options: Option[DeltaOptions],
      clusterBy: Option[Seq[String]],
      isV1SaveAsTableOverwrite: Option[Boolean]): DeltaOperations.Operation = {
    shimInstance.buildReplaceTableOperation(
      metadata, isManaged, orCreate, asSelect, options, clusterBy, isV1SaveAsTableOverwrite)
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean =
    shimInstance.getTightBoundColumnOnFileInitDisabled(spark)

  def isV1WriterSaveAsTableOverwrite(options: DeltaOptions, mode: SaveMode): Boolean =
    shimInstance.isV1WriterSaveAsTableOverwrite(options, mode)

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog = {
    shimInstance.getGpuDeltaCatalog(cpuCatalog, rapidsConf)
  }
}
