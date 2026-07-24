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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

abstract class BatchScanExecMetaBase(p: BatchScanExec,
   conf: RapidsConf,
   parent: Option[RapidsMeta[_, _, _]],
   rule: DataFromReplacementRule)
  extends SparkPlanMeta[BatchScanExec](p, conf, parent, rule) {
  // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
  // if possible. Instead regarding filters as childExprs of current Meta, we create
  // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
  // BatchScanExec is independent from the replacement of the runtime filters. It is
  // possible that the BatchScanExec is on the CPU, while the dynamic runtime filters
  // are on the GPU. And vice versa.
  protected lazy val runtimeFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.runtimeFilters.map { filter =>
      filter.transformDown {
        case dpe @ DynamicPruningShims(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              DynamicPruningShims(inSub.copy(plan = convertBroadcast(bc)))
            case reuse @ ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              DynamicPruningShims(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
            case _ =>
              dpe
          }
      }
    }
  }

  override val childExprs: Seq[BaseExprMeta[_]] = {
    // We want to leave the runtime filters as CPU expressions
    p.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override val childScans: scala.Seq[ScanMeta[_]] =
    Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    if (!p.runtimeFilters.isEmpty && !childScans.head.supportsRuntimeFilters) {
      willNotWorkOnGpu("runtime filtering (DPP) is not supported for this scan")
    }
  }

  override def convertToCpu(): SparkPlan = {
    val cpu = wrapped.copy(runtimeFilters = runtimeFilters)
    cpu.copyTagsFrom(wrapped)
    cpu
  }
}
