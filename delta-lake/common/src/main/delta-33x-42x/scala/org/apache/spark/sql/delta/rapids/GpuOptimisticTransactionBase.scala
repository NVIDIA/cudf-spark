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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec, RapidsConf, TargetSize}
import com.nvidia.spark.rapids.delta.DeltaWriteUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.perf.DeltaOptimizedWriterExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.delta.{DeltaShufflePartitionsUtil, GpuOptimizeWriteExchangeExec}
import org.apache.spark.sql.types.StructType

abstract class GpuOptimisticTransactionBase(
    deltaLog: DeltaLog,
    catalog: Option[CatalogTable],
    snapshot: Option[Snapshot],
    rapidsConf: RapidsConf)
  extends AbstractGpuOptimisticTransactionBase(deltaLog, catalog, snapshot,
    rapidsConf)(deltaLog.clock) {

  protected def applyOptimizeWriteIfNeeded(
      spark: SparkSession,
      physicalPlan: SparkPlan,
      partitionSchema: StructType,
      isOptimize: Boolean,
      writeOptions: Option[DeltaOptions]): SparkPlan = {
    // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
    // evenly-balanced data files already.
    val optimizeWriteEnabled = !isOptimize &&
      DeltaWriteUtils.shouldOptimizeWrite(writeOptions, spark.sessionState.conf, metadata)
    if (optimizeWriteEnabled) {
      val planWithoutTopRepartition =
        DeltaShufflePartitionsUtil.removeTopRepartition(physicalPlan)
      val partitioning = DeltaShufflePartitionsUtil.partitioningForRebalance(
        physicalPlan.output, partitionSchema, spark.sessionState.conf.numShufflePartitions)
      planWithoutTopRepartition match {
        case p: GpuExec =>
          val partMeta = GpuOverrides.wrapPart(partitioning, rapidsConf, None)
          partMeta.tagForGpu()
          if (partMeta.canThisBeReplaced) {
            val plan = GpuOptimizeWriteExchangeExec(partMeta.convertToGpu(), p, deltaLog)
            if (GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
              GpuCoalesceBatches(plan, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
            } else {
              GpuShuffleCoalesceExec(plan, rapidsConf.gpuTargetBatchSizeBytes)
            }
          } else {
            GpuColumnarToRowExec(DeltaOptimizedWriterExec(p, metadata.partitionColumns, deltaLog))
          }
        case p =>
          DeltaOptimizedWriterExec(p, metadata.partitionColumns, deltaLog)
      }
    } else {
      physicalPlan
    }
  }
}
