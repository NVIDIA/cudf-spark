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

import com.nvidia.spark.rapids.{RapidsConf, ShimLoader, ShimReflectionUtils, VersionUtils}
import com.nvidia.spark.rapids.delta.{DeltaConfigChecker, DeltaProvider}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions, DeltaUDF, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeltaReorgOperation, WriteIntoDelta}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Clock

case class StartTransactionArg(log: DeltaLog, conf: RapidsConf, clock: Clock,
    catalogTable: Option[CatalogTable] = None, snapshot: Option[Snapshot] = None)

trait DeltaRuntimeShim {
  def getDeltaConfigChecker: DeltaConfigChecker
  def getDeltaProvider: DeltaProvider
  def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock)
  : GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }
  def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
  def fileFormatFromLog(deltaLog: DeltaLog): FileFormat

  def runDeltaOperation[A](
      deltaLog: DeltaLog,
      opType: String)(thunk: => A): A

  def emitDeltaEvent(
      deltaLog: DeltaLog,
      opType: String,
      data: AnyRef): Unit

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

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike

  def createCpuWrite(
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

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame): GpuWriteIntoDeltaLike = {
    createGpuWrite(
      gpuDeltaLog,
      createCpuWrite(
        gpuDeltaLog.deltaLog,
        mode,
        options,
        partitionColumns,
        configuration,
        data,
        None,
        None))
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
}

object DeltaRuntimeShim {
  private val SparkVersion = """^(\d+)\.(\d+)\.(\d+).*""".r

  private def parseSparkVersion(sparkVersion: String): (Int, Int, Int) = sparkVersion match {
    case SparkVersion(major, minor, patch) => (major.toInt, minor.toInt, patch.toInt)
    case _ => throw new IllegalStateException(s"Unable to parse Spark version $sparkVersion")
  }

  private[rapids] def getDelta42ShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    if (deltaVersion.startsWith("4.2.")) {
      val parsedSparkVersion = parseSparkVersion(sparkVersion)
      (deltaVersion, parsedSparkVersion) match {
        case ("4.2.0", (4, 0, 1) | (4, 1, 1)) =>
          Some("org.apache.spark.sql.delta.rapids.delta42x.Delta42xRuntimeShim")
        case _ =>
          throw new IllegalStateException(
            s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
      }
    } else {
      None
    }
  }

  private[rapids] def getDelta43ShimClassName(
      deltaVersion: String,
      sparkVersion: String): Option[String] = {
    if (deltaVersion.startsWith("4.3.")) {
      val parsedSparkVersion = parseSparkVersion(sparkVersion)
      (deltaVersion, parsedSparkVersion) match {
        case ("4.3.0", (4, 0, 1) | (4, 1, 1)) =>
          Some("org.apache.spark.sql.delta.rapids.delta43x.Delta43xRuntimeShim")
        case _ =>
          throw new IllegalStateException(
            s"Unsupported Delta Lake $deltaVersion and Spark $sparkVersion combination")
      }
    } else {
      None
    }
  }

  private def getPreDelta42ShimClassName: String = {
    if (VersionUtils.cmpSparkVersion(3, 2, 0) < 0) {
      throw new IllegalStateException("Delta Lake is not supported on Spark < 3.2.x")
    } else if (VersionUtils.cmpSparkVersion(3, 3, 0) < 0) {
      "org.apache.spark.sql.delta.rapids.delta20x.Delta20xRuntimeShim"
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
        }.getOrElse("org.apache.spark.sql.delta.rapids.delta23x.Delta23xRuntimeShim")
    } else if (VersionUtils.cmpSparkVersion(3, 5, 0) < 0) {
      "org.apache.spark.sql.delta.rapids.delta24x.Delta24xRuntimeShim"
    } else if (VersionUtils.cmpSparkVersion(3, 5, 2) > 0 &&
               VersionUtils.cmpSparkVersion(4, 0, 0) < 0) {
      "org.apache.spark.sql.delta.rapids.delta33x.Delta33xRuntimeShim"
    } else if (VersionUtils.cmpSparkVersion(4, 0, 0) >= 0 &&
               VersionUtils.cmpSparkVersion(4, 1, 0) < 0) {
      "org.apache.spark.sql.delta.rapids.delta40x.Delta40xRuntimeShim"
    } else if (VersionUtils.cmpSparkVersion(4, 1, 0) >= 0) {
      "org.apache.spark.sql.delta.rapids.delta41x.Delta41xRuntimeShim"
    } else {
      val sparkVer = ShimLoader.getShimVersion
      throw new IllegalStateException(
        s"${sparkVer}: No Delta Lake support for this build of Spark"
      )
    }
  }

  private def getShimClassName: String = {
    getDelta43ShimClassName(io.delta.VERSION, SPARK_VERSION)
      .orElse(getDelta42ShimClassName(io.delta.VERSION, SPARK_VERSION))
      .getOrElse(getPreDelta42ShimClassName)
  }

  private lazy val shimInstance = {
    val shimClassName = getShimClassName
    val shimClass = ShimReflectionUtils.loadClass(shimClassName)
    shimClass.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  def getDeltaProvider: DeltaProvider = shimInstance.getDeltaProvider

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

  def runDeltaOperation[A](
      deltaLog: DeltaLog,
      opType: String)(thunk: => A): A = {
    shimInstance.runDeltaOperation(deltaLog, opType)(thunk)
  }

  def emitDeltaEvent(
      deltaLog: DeltaLog,
      opType: String,
      data: AnyRef): Unit = {
    shimInstance.emitDeltaEvent(deltaLog, opType, data)
  }

  def assertRemovable(snapshot: Snapshot): Unit = shimInstance.assertRemovable(snapshot)

  def filterFilesToReorg(
      operation: DeltaReorgOperation,
      spark: SparkSession,
      snapshot: Snapshot,
      candidates: Seq[AddFile]): Seq[AddFile] = {
    shimInstance.filterFilesToReorg(operation, spark, snapshot, candidates)
  }

  def preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns: DataFrame,
      snapshot: Snapshot,
      targetOutput: Seq[Attribute],
      updateExpressions: Seq[Expression]): (DataFrame, Seq[Attribute], Seq[Expression]) = {
    shimInstance.preserveRowTrackingColumns(
      targetDfWithoutRowTrackingColumns, snapshot, targetOutput, updateExpressions)
  }

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      cpuWrite: WriteIntoDelta): GpuWriteIntoDeltaLike = {
    shimInstance.createGpuWrite(gpuDeltaLog, cpuWrite)
  }

  def createGpuWrite(
      gpuDeltaLog: GpuDeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame): GpuWriteIntoDeltaLike = {
    shimInstance.createGpuWrite(
      gpuDeltaLog, mode, options, partitionColumns, configuration, data)
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

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog = {
    shimInstance.getGpuDeltaCatalog(cpuCatalog, rapidsConf)
  }
}
