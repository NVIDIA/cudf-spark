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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, RapidsConf, RapidsMeta}

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{EXECUTOR_BROADCAST, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}


class GpuShuffleMeta(
    shuffle: ShuffleExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuShuffleMetaBase(shuffle, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    shuffle.shuffleOrigin match {
      // Since we are handling a broadcast (this on executor-side), the similar
      // rules for BroadcastExchange apply for whether this should be replaced or not
      case EXECUTOR_BROADCAST =>
        // Copied from GpuBroadcastMeta
        def isSupported(rm: RapidsMeta[_, _, _]): Boolean = rm.wrapped match {
          case _: BroadcastHashJoinExec => true
          case _: BroadcastNestedLoopJoinExec => true
          case _ => false
        }
        if (parent.isDefined) {
          if (!parent.exists(isSupported)) {
            willNotWorkOnGpu("executor broadcast only works on the GPU if being used " +
                "with a GPU version of BroadcastHashJoinExec")
          }
        }

      case _ =>
    }
  }

  override protected def convertShuffleToGpu(newChild: SparkPlan): GpuExec =
    GpuShuffleExchangeExec(
      childParts.head.convertToGpu(),
      newChild,
      shuffle.shuffleOrigin
    // DBR 17.3 keeps marker partitionings in the target contract while advertising their
    // physical partitioning as the exchange output. AQE rules use both views.
    )(shuffle.targetOutputPartitioning)
}
