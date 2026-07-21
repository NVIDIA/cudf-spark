/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.execution.datasources.v2.{ReplaceDataExec, WriteDeltaExec}

class ReplaceDataExecMeta(
    wrapped: ReplaceDataExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ReplaceDataExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSourceShim.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSourceShim.convertToGpu(wrapped, this)
  }
}

class WriteDeltaExecMeta(
    wrapped: WriteDeltaExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[WriteDeltaExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSourceShim.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSourceShim.convertToGpu(wrapped, this)
  }
}
