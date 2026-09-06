/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{AcceptAllConfigChecker, DeltaConfigChecker, DeltaProvider}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot, TransactionExecutionObserver}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.Clock

/**
 * Shared base for 3.3.x, 4.0.x, and 4.1.x runtime shims.
 * Version-specific shims override provider, catalog, and transaction construction.
 */
abstract class DeltaRuntimeShimBase extends DeltaRuntimeShim {
  override def getDeltaConfigChecker: DeltaConfigChecker = AcceptAllConfigChecker

  // Provider is version-specific
  override def getDeltaProvider: DeltaProvider

  // Default behavior shared across versions
  override def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    deltaLog.unsafeVolatileSnapshot

  override def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    deltaLog.fileFormat(deltaLog.unsafeVolatileSnapshot.protocol,
      deltaLog.unsafeVolatileSnapshot.metadata)

  override def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean = false

  // Catalog wiring remains runtime-specific because Delta 3.3 and Delta 4.x use different
  // create-table command implementations, even though Delta 4.0/4.1 now share the same base.
  override def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog

  // Transaction construction is version-specific
  protected def constructOptimisticTransaction(arg: StartTransactionArg):
      GpuOptimisticTransactionBase

  override def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock):
      GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }

  override def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase = {
    TransactionExecutionObserver.getObserver.startingTransaction {
      constructOptimisticTransaction(arg)
    }.asInstanceOf[GpuOptimisticTransactionBase]
  }

  override def stringFromStringUdf(f: String => String): UserDefinedFunction =
    DeltaUDF.stringFromString(f)
}
