/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.DeltaProvider

import org.apache.spark.rapids.hybrid.HybridExecutionUtils
import org.apache.spark.sql.catalyst.expressions.{FileSourceMetadataAttribute}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

object ScanExecShims {
  def tagGpuFileSourceScanExecSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (meta.wrapped.expressions.exists {
      case FileSourceMetadataAttribute(_) => true
      case _ => false
    }) {
      if (!DeltaProvider().isDVScan(meta)) {
        meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")
      }
    }
    GpuFileSourceScanExec.tagSupport(meta)
  }

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[BatchScanExec](
      "The backend for most file input",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
          TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
    GpuOverrides.exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      (fsse, conf, p, r) => {
        // TODO: HybridScan supports DataSourceV2
        if (HybridExecutionUtils.useHybridScan(conf, fsse)) {
          // Check if runtimes are satisfied: Spark is not Databricks or CDH; Java version is 1.8;
          // Scala version is 2.12; Hybrid jar is in the classpath; parquet v1 datasource
          val sqlConf = fsse.relation.sparkSession.sessionState.conf
          val v1SourceList = sqlConf.getConfString("spark.sql.sources.useV1SourceList", "")
          HybridExecutionUtils.checkRuntimes(v1SourceList)
          new HybridFileSourceScanExecMeta(fsse, conf, p, r)
        } else {
          new FileSourceScanExecMeta(fsse, conf, p, r)
        }
      })

  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}
