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

package org.apache.spark.sql.delta.rapids.delta41x

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.DeltaProvider
import com.nvidia.spark.rapids.delta.delta41x.Delta41xProvider
import com.nvidia.spark.rapids.delta.delta41x.GpuDeltaCatalog

import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.rapids.{
  DeltaRuntimeShimBase,
  GpuOptimisticTransaction,
  GpuOptimisticTransactionBase,
  StartTransactionArg
}

class Delta41xRuntimeShim extends DeltaRuntimeShimBase {

  override def getDeltaProvider: DeltaProvider = Delta41xProvider

  override def getGpuDeltaCatalog(
      cpuCatalog: DeltaCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog = {
    new GpuDeltaCatalog(cpuCatalog, rapidsConf)
  }

  override protected def constructOptimisticTransaction(
      arg: StartTransactionArg): GpuOptimisticTransactionBase =
    new GpuOptimisticTransaction(arg.log, arg.catalogTable, arg.snapshot, arg.conf)
}
