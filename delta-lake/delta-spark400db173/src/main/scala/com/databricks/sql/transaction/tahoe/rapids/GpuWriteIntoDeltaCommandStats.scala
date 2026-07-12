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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.DeltaConfigs
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, WriteIntoDeltaCommand}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.databricks.sql.transaction.tahoe.stats.DeltaJobStatisticsTracker
import com.nvidia.spark.rapids.delta.{GpuDeltaJobStatisticsTracker, GpuStatisticsCollection}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.rapids.{ColumnarWriteJobStatsTracker, ColumnarWriteTaskStatsTracker}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

private object GpuWriteIntoDeltaCommandStats {
  def apply(
      cpuCmd: WriteIntoDeltaCommand,
      nativeTracker: DeltaJobStatisticsTracker,
      sparkSession: SparkSession): ColumnarWriteJobStatsTracker = {
    val useTableSchema = sparkSession.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)
    val statsCollection = new GpuStatisticsCollection {
      override val spark = sparkSession
      override val deletionVectorsSupported: Boolean =
        DeletionVectorUtils.deletionVectorsWritable(
          cpuCmd.deltaLog.unsafeVolatileSnapshot, Some(cpuCmd.protocol), Some(cpuCmd.metadata))
      override val tableDataSchema: StructType = if (useTableSchema) {
        cpuCmd.metadata.schema
      } else {
        nativeTracker.dataCols.toStructType
      }
      override val dataSchema: StructType = nativeTracker.dataCols.toStructType
      override val numIndexedCols: Int =
        DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(cpuCmd.metadata)
      override val stringPrefixLength: Int =
        spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)
    }
    val statsSchema = statsCollection.statCollectionSchema
    val explodedDataSchema = statsCollection.explodedDataSchema
    val batchStatsToRow = (batch: ColumnarBatch, row: InternalRow) => {
      GpuStatisticsCollection.batchStatsToRow(statsSchema, explodedDataSchema, batch, row)
    }
    val gpuTracker = new GpuDeltaJobStatisticsTracker(
      nativeTracker.dataCols, nativeTracker.statsColExpr, batchStatsToRow)

    new ColumnarWriteJobStatsTracker {
      override def newTaskInstance(): ColumnarWriteTaskStatsTracker =
        gpuTracker.newTaskInstance()

      override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
        gpuTracker.processStats(stats, jobCommitTime)
        nativeTracker.recordedStats = gpuTracker.recordedStats
      }
    }
  }
}
