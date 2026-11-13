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
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.VariantApiTestFixtures._
import org.apache.commons.io.IOUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, DoubleType,
  FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType, TimestampType,
  VariantType}

class GpuVariantGetMetaSuite extends AnyFunSuite {
  private def loadClass(name: String): Class[_] = Class.forName(name, false,
    Thread.currentThread().getContextClassLoader)

  test("cached decimal status capability uses the provider loader rather than the context loader") {
    val moduleName = "com.nvidia.spark.rapids.GpuVariantExtraction$"
    val parent = classOf[GpuVariantGet].getClassLoader
    val resource = parent.getResourceAsStream(moduleName.replace('.', '/') + ".class")
    assert(resource != null)
    val definition = withResource(resource)(IOUtils.toByteArray)
    class FreshModuleLoader extends ClassLoader(parent) {
      def defineModule(): Class[_] = defineClass(moduleName, definition, 0, definition.length)
    }
    // A fresh companion avoids inheriting a cached true from a different suite in the same JVM.
    val moduleClass = new FreshModuleLoader().defineModule()
    val thread = Thread.currentThread()
    val original = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(new ClassLoader(null) {})
      val module = moduleClass.getField("MODULE$").get(null)
      val available = moduleClass.getMethod("isAvailable").invoke(module)
      assert(available == java.lang.Boolean.TRUE)
    } finally {
      thread.setContextClassLoader(original)
    }
  }

  private def fixtureApis(
      utils: Class[_] = classOf[Utils],
      result: Class[_] = classOf[Result],
      status: Class[_] = classOf[Status]): Boolean = {
    GpuVariantExtraction.hasRequiredApis {
      case "ai.rapids.cudf.VariantUtils" => utils
      case "ai.rapids.cudf.VariantOperationResult" => result
      case "ai.rapids.cudf.VariantOperationStatus" => status
      case name => throw new ClassNotFoundException(name)
    }
  }

  test("coordinated reflection fixtures satisfy every required API") {
    assert(fixtureApis())
  }

  test("legacy scalar capability does not require any status API") {
    val legacyOnly: String => Class[_] = {
      case "ai.rapids.cudf.VariantUtils" => classOf[LegacyUtils]
      case name => throw new ClassNotFoundException(name)
    }
    Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      StringType).foreach { target =>
      assert(GpuVariantGet.hasRequiredApisForTarget(target, legacyOnly), target.toString)
    }
    assert(!GpuVariantGet.hasRequiredApisForTarget(DecimalType(10, 2), legacyOnly))
  }

  test("decimal status capability does not require legacy value-only APIs") {
    val statusOnly: String => Class[_] = {
      case "ai.rapids.cudf.VariantUtils" => classOf[Utils]
      case "ai.rapids.cudf.VariantOperationResult" => classOf[Result]
      case "ai.rapids.cudf.VariantOperationStatus" => classOf[Status]
      case name => throw new ClassNotFoundException(name)
    }
    assert(GpuVariantGet.hasRequiredApisForTarget(DecimalType(10, 2), statusOnly))
    assert(!GpuVariantGet.hasRequiredApisForTarget(IntegerType, statusOnly))
  }

  test("decimal extraction and cast APIs must be static and return the expected result owner") {
    Seq(classOf[WrongExtractionReturnUtils], classOf[NonStaticExtractionUtils],
      classOf[NonStaticCastUtils]).foreach { utils =>
      withClue(utils.getSimpleName) { assert(!fixtureApis(utils = utils)) }
    }
  }

  test("status cast must accept the prior status and return the expected result owner") {
    Seq(classOf[ExtractionOnlyUtils], classOf[WrongCastParametersUtils],
      classOf[WrongCastReturnUtils]).foreach { utils =>
      withClue(utils.getSimpleName) { assert(!fixtureApis(utils = utils)) }
    }
  }

  test("result must implement AutoCloseable even if it declares close") {
    assert(!fixtureApis(utils = classOf[NonOwningUtils], result = classOf[NonOwningResult]))
    // An AutoCloseable already inherits close():void, so a missing/wrong close return cannot
    // be isolated with a legal Java fixture after the assignability check succeeds.
  }

  test("result value getter must exist and return ColumnVector") {
    Seq((classOf[MissingValueUtils], classOf[MissingValueResult]),
      (classOf[WrongValueUtils], classOf[WrongValueResult])).foreach { case (utils, result) =>
      withClue(result.getSimpleName) { assert(!fixtureApis(utils = utils, result = result)) }
    }
  }

  test("result status getter must exist and return ColumnVector") {
    Seq((classOf[MissingStatusUtils], classOf[MissingStatusResult]),
      (classOf[WrongStatusUtils], classOf[WrongStatusResult])).foreach { case (utils, result) =>
      withClue(result.getSimpleName) { assert(!fixtureApis(utils = utils, result = result)) }
    }
  }

  test("status native ID must exist and return primitive int") {
    Seq(classOf[MissingNativeIdStatus], classOf[WrongNativeIdStatus]).foreach { status =>
      withClue(status.getSimpleName) { assert(!fixtureApis(status = status)) }
    }
  }

  test("each required status field must exist and have the status type") {
    Seq(classOf[MissingSuccessStatus], classOf[WrongSuccessStatus],
      classOf[MissingRowNullStatus], classOf[WrongRowNullStatus]).foreach { status =>
      withClue(status.getSimpleName) { assert(!fixtureApis(status = status)) }
    }
  }

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
    // This checks the first return-type predicate; the coordinated fixtures above reach the
    // remaining predicates without short-circuiting at that first comparison.
    assert(!GpuVariantExtraction.hasRequiredApis { name =>
      if (name == "ai.rapids.cudf.VariantOperationResult") classOf[AutoCloseable]
      else loadClass(name)
    })
    // A class can exist while one of its binary dependencies cannot be linked.
    assert(!GpuVariantExtraction.hasRequiredApis(_ => throw new NoClassDefFoundError))
  }

  private def assertRejected(
      targetType: DataType,
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

  test("decimal support does not enable timestamps, strict mode, or bypass existing gates") {
    Seq(TimestampType, TimestampNTZType).foreach { target =>
      assertRejected(targetType = target)
    }
    Seq(IntegerType, DecimalType(10, 2), DecimalType(38, 38)).foreach { target =>
      assertRejected(targetType = target, strict = true)
      assertRejected(targetType = target, bridge = false)
      assertRejected(targetType = target, incompatible = false)
    }
  }

  test("decimal target registration preserves precision and scale through GPU conversion") {
    Seq(DecimalType(1, 0), DecimalType(9, 2), DecimalType(10, 1), DecimalType(18, 0),
      DecimalType(19, 18), DecimalType(38, 37), DecimalType(38, 38)).foreach { target =>
      val expression = VariantGet(BoundReference(0, VariantType, nullable = true),
        Literal("$.id"), target, failOnError = false, timeZoneId = Some("UTC"))
      val conf = new RapidsConf(Map(
        RapidsConf.ENABLE_CPU_BRIDGE.key -> "true",
        RapidsConf.INCOMPATIBLE_OPS.key -> "true"))
      val meta = GpuOverrides.wrapExpr(expression, conf, None)
      meta.tagForGpu()
      assert(meta.canThisBeReplaced, s"decimal registration rejected $target")
      val gpu = meta.convertToGpu().asInstanceOf[GpuVariantGet]
      assert(gpu.dataType == target)
      assert(gpu.cpuFallback.targetType == target)
    }
  }

  test("legacy negative decimal scales fall back during planning") {
    val sqlConf = new SQLConf
    sqlConf.setConfString("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    SQLConf.withExistingConf(sqlConf) {
      assertRejected(targetType = DecimalType(10, -2))
    }
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
