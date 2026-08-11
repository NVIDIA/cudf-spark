/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class BatchScanExecMeta(p: BatchScanExec,
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
  private lazy val runtimeFilters = {
    val convertBroadcast = (bc: SubqueryBroadcastExec) => {
      val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
      meta.tagForExplain()
      meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
    }
    wrapped.runtimeFilters.map { filter =>
      filter.transformDown {
        case dpe @ DynamicPruningExpression(inSub: InSubqueryExec) =>
          inSub.plan match {
            case bc: SubqueryBroadcastExec =>
              dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
            case reuse @ ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
              dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
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
    if(p.reusesFileListingResultsSourceNode != None) {
      willNotWorkOnGpu("reusesFileListingResultsSourceNode is not supported for this scan")
    }
  }

  override def convertToCpu(): SparkPlan = {
    val cpu = wrapped.copy(runtimeFilters = runtimeFilters)
    cpu.copyTagsFrom(wrapped)
    cpu
  }

  override def convertToGpu(): GpuExec =
    GpuBatchScanExec(p.output, childScans.head.convertToGpu(), runtimeFilters, 
    p.keyGroupedPartitioning, p.ordering, p.table, p.reusesFileListingResultsSourceNode)
}
