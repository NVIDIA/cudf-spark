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
{"spark": "410db183"}
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
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, TimestampNTZType,
  TimestampType, VariantType}

class GpuVariantGetMetaSuite extends AnyFunSuite {
  private def loadClass(name: String): Class[_] = Class.forName(name, false,
    Thread.currentThread().getContextClassLoader)

  test("status extraction capability rejects missing, legacy, and incomplete APIs") {
    assert(GpuVariantExtraction.hasRequiredApis(loadClass))
    assert(!GpuVariantExtraction.hasRequiredApis(_ => throw new ClassNotFoundException))
    assert(!GpuVariantExtraction.hasRequiredApis { name =>
      if (name == "ai.rapids.cudf.VariantUtils") classOf[String] else loadClass(name)
    })
    Seq("ai.rapids.cudf.VariantOperationResult", "ai.rapids.cudf.VariantOperationStatus")
      .foreach { missing =>
        assert(!GpuVariantExtraction.hasRequiredApis { name =>
          if (name == missing) throw new ClassNotFoundException(name) else loadClass(name)
        })
      }
    // Methods returning the real result must not satisfy a different/incomplete owner API.
    assert(!GpuVariantExtraction.hasRequiredApis { name =>
      if (name == "ai.rapids.cudf.VariantOperationResult") classOf[AutoCloseable]
      else loadClass(name)
    })
    // A class can exist while one of its binary dependencies cannot be linked.
    assert(!GpuVariantExtraction.hasRequiredApis(_ => throw new NoClassDefFoundError))
  }

  private def assertRejected(
      targetType: DataType = IntegerType,
      strict: Boolean = false,
      bridge: Boolean = true,
      incompatible: Boolean = true): Unit = {
    val expression = VariantGet(BoundReference(0, VariantType, nullable = true),
      Literal("$.id"), targetType, failOnError = strict, timeZoneId = Some("UTC"))
    val conf = new RapidsConf(Map(
      RapidsConf.ENABLE_CPU_BRIDGE.key -> bridge.toString,
      RapidsConf.INCOMPATIBLE_OPS.key -> incompatible.toString))
    val meta = GpuOverrides.wrapExpr(expression, conf, None)
    meta.tagForGpu()
    assert(!meta.canThisBeReplaced)
  }

  test("status APIs do not enable new target types, strict mode, or bypass existing gates") {
    Seq(DecimalType(10, 2), TimestampType, TimestampNTZType).foreach { target =>
      assertRejected(targetType = target)
    }
    assertRejected(strict = true)
    assertRejected(bridge = false)
    assertRejected(incompatible = false)
  }

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
