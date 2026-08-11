/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids.delta33x

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{AcceptAllConfigChecker, DeltaConfigChecker, DeltaProvider}
import com.nvidia.spark.rapids.delta.delta33x.{Delta33xProvider, GpuDeltaCatalog}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot, TransactionExecutionObserver}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShim, GpuOptimisticTransactionBase, StartTransactionArg}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * Delta runtime shim for Delta 3.3.x on Spark 3.5.x.
 *
 * @note This class is instantiated via reflection from DeltaProbeImpl
 */
class Delta33xRuntimeShim extends DeltaRuntimeShim {

  override def getDeltaConfigChecker: DeltaConfigChecker = AcceptAllConfigChecker

  override def getDeltaProvider: DeltaProvider = Delta33xProvider

  override def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot = {
    deltaLog.unsafeVolatileSnapshot
  }

  override def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    deltaLog.fileFormat(deltaLog.unsafeVolatileSnapshot.protocol,
      deltaLog.unsafeVolatileSnapshot.metadata)

  override def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean = false

  override def getGpuDeltaCatalog(
     cpuCatalog: DeltaCatalog,
     rapidsConf: RapidsConf): StagingTableCatalog = {
    new GpuDeltaCatalog(cpuCatalog, rapidsConf)
  }

  def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase = {
    TransactionExecutionObserver.getObserver.startingTransaction {
      new GpuOptimisticTransaction(arg.log, arg.catalogTable, arg.snapshot, arg.conf)
    }.asInstanceOf[GpuOptimisticTransactionBase]
  }

  override def stringFromStringUdf(f: String => String): UserDefinedFunction = {
    DeltaUDF.stringFromString(f)
  }
}
