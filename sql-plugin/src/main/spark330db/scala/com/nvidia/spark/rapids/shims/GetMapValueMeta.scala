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

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
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

import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuExpression, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, GetMapValue}
import org.apache.spark.sql.rapids.GpuGetMapValue

/**
 * We define this type in the shim layer because `GetMapValue` doesn't
 * have the field `failOnError` since Spark 3.4.0 and it always returns `null`
 * on invalid access to map column in ANSI mode.
 */
abstract class GetMapValueMeta (
    expr: GetMapValue,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {

    override def convertToGpu(map: Expression, key: Expression): GpuExpression =
        GpuGetMapValue(map, key, false)(expr.origin)
}
