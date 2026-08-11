/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.SparkSession
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
    val format = deltaLog.map(log => DeltaRuntimeShim.fileFormatFromLog(log).getClass)
      .getOrElse(classOf[DeltaParquetFileFormat])
    if (format == classOf[DeltaParquetFileFormat]) {
      GpuParquetFileFormat.tagGpuSupport(meta, spark, options, schema)
    } else {
      meta.willNotWorkOnGpu(s"file format $format is not supported")
    }
    DeltaRuntimeShim.getDeltaConfigChecker
      .checkIncompatibleConfs(meta, deltaLog, spark.sessionState.conf, options)
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean = {
    DeltaRuntimeShim.getTightBoundColumnOnFileInitDisabled(spark)
  }
}
