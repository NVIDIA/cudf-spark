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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExpression, RapidsConf, RapidsMeta, TernaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{Conv, Expression}
import org.apache.spark.sql.rapids.GpuConv

class GpuConvMeta(
    expr: Conv,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule) extends TernaryExprMeta(expr, conf, parent, rule) {

  /**
   * For Spark version < 340, do not support ansiEnabled.
   */
  override def convertToGpu(
      numStr: Expression,
      fromBase: Expression,
      toBase: Expression): GpuExpression = GpuConv(numStr, fromBase, toBase, ansiEnabled = false)
}
