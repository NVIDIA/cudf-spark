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

package com.nvidia.spark.rapids.delta.delta41x

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.common.{DeleteCommandMeta, DeltaDynamicPartitionOverwriteCommandMeta, MergeIntoCommandMeta, OptimizeTableCommandMeta, UpdateCommandMeta}
import com.nvidia.spark.rapids.delta.common.{GpuDelta4xParquetFileFormat, GpuDeltaParquetFileFormat2}
import com.nvidia.spark.rapids.delta.common.DeltaProviderBase

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.delta.{DeltaDynamicPartitionOverwriteCommand, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, OptimizeTableCommand, UpdateCommand}
import org.apache.spark.sql.delta.rapids.GpuDeltaCatalog4x
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.AppendDataExecV1

object Delta41xProvider extends DeltaProviderBase with Logging {

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    write == classOf[DeltaTableV2] || write == classOf[GpuDeltaCatalog4x#GpuStagedDeltaTableV2]
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
    super.isSupportedFormat(format) || format == classOf[GpuDelta4xParquetFileFormat]

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    cpuExec.table match {
      case _: DeltaTableV2 => super.tagForGpu(cpuExec, meta)
      case _: GpuDeltaCatalog4x#GpuStagedDeltaTableV2 =>
      case _ => meta.willNotWorkOnGpu(s"${cpuExec.table} table class not supported on GPU")
    }
  }

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
          "Delete rows from a Delta Lake table",
          (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[UpdateCommand](
          "Update rows from a Delta Lake table",
          (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommand](
          "Merge of a source query/table into a Delta Lake table",
          (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[OptimizeTableCommand](
          "Optimize a Delta Lake table",
          (a, conf, p, r) => new OptimizeTableCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[DeltaDynamicPartitionOverwriteCommand](
        "Dynamic partition overwrite to a Delta Lake table",
        (a, conf, p, r) => new DeltaDynamicPartitionOverwriteCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override protected def toGpuParquetFileFormat(conf: RapidsConf, fmt: DeltaParquetFileFormat)
  : FileFormat = {
    if (isPushDVPredicateDownEnabled(conf)) {
      GpuDeltaParquetFileFormat2(
        protocol = fmt.protocol,
        metadata = fmt.metadata,
        nullableRowTrackingFields = false,
        optimizationsEnabled = fmt.optimizationsEnabled,
        tablePath = fmt.tablePath,
        isCDCRead = fmt.isCDCRead)
    } else {
      val optimizationsEnabled = if (fmt.hasTablePath) {
        logWarning("Input Delta table has deletion vectors. Optimizations such as file splitting " +
          "and predicate pushdown are currently not supported for this table " +
          "(https://github.com/NVIDIA/spark-rapids/issues/13999). If you see performance issues, " +
          "consider disabling deletion vectors and running the optimize command on the table. " +
          "See https://docs.delta.io/delta-deletion-vectors/#apply-changes-to-parquet-data-files " +
          "for more details about how to apply delete changes to physical files.")
        false
      } else {
        fmt.optimizationsEnabled
      }
      GpuDelta4xParquetFileFormat(
        protocol = fmt.protocol,
        metadata = fmt.metadata,
        nullableRowTrackingFields = false,
        optimizationsEnabled = optimizationsEnabled,
        tablePath = fmt.tablePath,
        isCDCRead = fmt.isCDCRead)
    }
  }

  override def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    cpuExec.table match {
      case _: DeltaTableV2 =>
        super.convertToGpu(cpuExec, meta)
      case _: GpuDeltaCatalog4x#GpuStagedDeltaTableV2 =>
        GpuAppendDataExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
      case unknown => throw new IllegalStateException(s"$unknown doesn't match any of the known ")
    }
  }
}
