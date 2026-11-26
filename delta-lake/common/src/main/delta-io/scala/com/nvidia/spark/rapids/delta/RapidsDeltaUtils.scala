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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.{DeltaFormatType, FileFormatChecks, GpuParquetFileFormat, RapidsMeta, WriteFileOp}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.types.StructType

object RapidsDeltaUtils {
  def tagForDeltaWrite(
      meta: RapidsMeta[_, _, _],
      schema: StructType,
      deltaLog: Option[DeltaLog],
      options: Map[String, String],
      spark: SparkSession): Unit = {
    FileFormatChecks.tag(meta, schema, DeltaFormatType, WriteFileOp)
    // Reading a table's snapshot can fail validation. That happens for the snapshot of a table
    // that does not exist yet when the session's table property defaults are invalid, and Delta
    // can still hold such a snapshot for a table that has since been created. The CPU write
    // decides what to do about it, so these fall back instead of failing planning.
    try {
      val format = deltaLog.map(log => DeltaRuntimeShim.fileFormatFromLog(log).getClass)
        .getOrElse(classOf[DeltaParquetFileFormat])
      if (format == classOf[DeltaParquetFileFormat]) {
        GpuParquetFileFormat.tagGpuSupport(meta, spark, options, schema)
      } else {
        meta.willNotWorkOnGpu(s"file format $format is not supported")
      }
      val configChecker = DeltaRuntimeShim.getDeltaConfigChecker
      configChecker.checkIncompatibleConfs(meta, deltaLog, spark.sessionState.conf, options)
      deltaLog.foreach { log =>
        configChecker.checkTableProperties(meta, spark, options,
          DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(log))
      }
    } catch {
      case e @ (_: IllegalArgumentException | _: AnalysisException) =>
        meta.willNotWorkOnGpu(s"the Delta table's snapshot could not be read: ${e.getMessage}")
    }
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean = {
    DeltaRuntimeShim.getTightBoundColumnOnFileInitDisabled(spark)
  }
}
