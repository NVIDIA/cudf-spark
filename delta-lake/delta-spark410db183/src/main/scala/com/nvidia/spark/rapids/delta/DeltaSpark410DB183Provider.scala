/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Portions of this class have been taken from DeltaTableV2 class
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.nvidia.spark.rapids.{AppendDataExecV1Meta, GpuAppendDataExecV1, GpuExec,
  GpuOverwriteByExpressionExecV1, OverwriteByExpressionExecV1Meta, RapidsConf}

import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1,
  OverwriteByExpressionExecV1}

object DeltaSpark410DB183Provider extends DatabricksDeltaProvider173183Base {
  private val NATIVE_STAGED_DELTA_TABLE_CLASS_NAME =
    "com.databricks.sql.transaction.tahoe.catalog.AbstractDeltaCatalog$StagedDeltaTableV2"

  private def isNativeStagedDeltaTable(tableClass: Class[_]): Boolean =
    tableClass.getName == NATIVE_STAGED_DELTA_TABLE_CLASS_NAME

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean =
    super.isSupportedWrite(write) || isNativeStagedDeltaTable(write)

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (isNativeStagedDeltaTable(cpuExec.table.getClass)) {
      // The native staged-table write captures the query for the atomic CTAS/RTAS commit.
      // It has no DeltaTableV2 write builder for the shared tagging path to inspect.
      if (!meta.conf.isDeltaWriteEnabled) {
        meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
      }
    } else {
      super.tagForGpu(cpuExec, meta)
    }
  }

  override def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    if (isNativeStagedDeltaTable(cpuExec.table.getClass)) {
      GpuAppendDataExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
    } else {
      super.convertToGpu(cpuExec, meta)
    }
  }

  override def tagForGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    if (isNativeStagedDeltaTable(cpuExec.table.getClass)) {
      // The native writer captures the replacement query; the staged table owns atomic commit.
      if (!meta.conf.isDeltaWriteEnabled) {
        meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
      }
    } else {
      super.tagForGpu(cpuExec, meta)
    }
  }

  override def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec = {
    if (isNativeStagedDeltaTable(cpuExec.table.getClass)) {
      GpuOverwriteByExpressionExecV1(
        cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
    } else {
      super.convertToGpu(cpuExec, meta)
    }
  }
}
