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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.python.MapInPandasExec
import org.apache.spark.sql.rapids.execution.python.GpuMapInPandasExecMetaBase

class GpuMapInPandasExecMeta(
    mapPandas: MapInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuMapInPandasExecMetaBase(mapPandas, conf, parent, rule) {

}
