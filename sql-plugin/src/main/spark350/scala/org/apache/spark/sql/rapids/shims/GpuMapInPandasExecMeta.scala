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
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Attribute}
import org.apache.spark.sql.execution.python.MapInPandasExec
import org.apache.spark.sql.rapids.execution.python.{GpuMapInPandasExec, GpuMapInPandasExecMetaBase}

class GpuMapInPandasExecMeta(
    mapPandas: MapInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuMapInPandasExecMetaBase(mapPandas, conf, parent, rule) {

  override def convertToGpu(): GpuExec =
    GpuMapInPandasExec(
      udf.convertToGpu(),
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      childPlans.head.convertIfNeeded(),
      isBarrier = mapPandas.isBarrier,
    )
}
