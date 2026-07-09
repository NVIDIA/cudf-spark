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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
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

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog
}

object DeltaRuntimeShim {
  private def getShimClassName: String = {
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

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean =
    shimInstance.getTightBoundColumnOnFileInitDisabled(spark)

  def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog, rapidsConf: RapidsConf): StagingTableCatalog = {
    shimInstance.getGpuDeltaCatalog(cpuCatalog, rapidsConf)
  }
}
