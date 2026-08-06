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
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec

class GroupPartitionsExecMeta(
    groupPartitions: GroupPartitionsExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[GroupPartitionsExec](groupPartitions, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    willNotWorkOnGpu("GroupPartitionsExec is not supported on GPU")
  }

  override def convertToCpu(): SparkPlan = {
    // GroupPartitionsExec reads its child's KeyedPartitioning at execution time.
    // GPU conversions can replace it with UnknownPartitioning, so keep the original
    // CPU subtree until GroupPartitionsExec has a GPU implementation.
    groupPartitions
  }

  override def convertToGpu(): GpuExec = {
    throw new IllegalStateException("GroupPartitionsExec cannot be converted to GPU")
  }
}
