/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import org.apache.spark.rapids.hybrid.HybridFileSourceScanExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

class HybridFileSourceScanExecMeta(plan: FileSourceScanExec,
                                   conf: RapidsConf,
                                   parent: Option[RapidsMeta[_, _, _]],
                                   rule: DataFromReplacementRule)
  extends SparkPlanMeta[FileSourceScanExec](plan, conf, parent, rule) {

  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with native counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the native replacement of
  // FileSourceScan is independent from the replacement of the partitionFilters.
  private lazy val partitionFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.partitionFilters.map { filter =>
      filter.transformDown {
        case dpe@DynamicPruningShims(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              DynamicPruningShims(inSub.copy(plan = convertBroadcast(bc)))
            case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              DynamicPruningShims(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  // partition filters and data filters are not run on the GPU
  override val childExprs: Seq[ExprMeta[_]] = Seq.empty

  override def tagPlanForGpu(): Unit = {
    val cls = wrapped.relation.fileFormat.getClass
    if (cls != classOf[ParquetFileFormat]) {
      willNotWorkOnGpu(s"unsupported file format: ${cls.getCanonicalName}")
    }
  }

  override def convertToGpu(): GpuExec = {
    // Modifies the original plan to support DPP
    val fixedExec = wrapped.copy(partitionFilters = partitionFilters)
    HybridFileSourceScanExec(fixedExec)(conf)
  }
}
