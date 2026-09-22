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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{IntegerType, VariantType}

class GpuVariantGetMetaSuite extends AnyFunSuite {
  test("Variant extraction is supported outside UTC") {
    val expression = VariantGet(
      BoundReference(0, VariantType, nullable = true),
      Literal("$.id"),
      IntegerType,
      failOnError = false,
      timeZoneId = Some("Asia/Shanghai"))
    val conf = new RapidsConf(Map(
      RapidsConf.ENABLE_CPU_BRIDGE.key -> "true",
      RapidsConf.INCOMPATIBLE_OPS.key -> "true"))

    val meta = GpuOverrides.wrapExpr(expression, conf, None)
    meta.tagForGpu()

    assert(meta.canThisBeReplaced)
    val gpuExpression = meta.convertToGpu().asInstanceOf[GpuVariantGet]
    assert(gpuExpression.cpuFallback.timeZoneId.contains("Asia/Shanghai"))
  }
}
