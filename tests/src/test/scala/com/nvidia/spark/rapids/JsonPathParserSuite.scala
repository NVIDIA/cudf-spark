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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.PathInstruction.{Key, Named}
import com.nvidia.spark.rapids.shims.GetJsonObjectShim
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class JsonPathParserSuite extends AnyFunSuite {
  private val questionMarkPath = List(Key, Named("?"))

  // Classic 2.1 and Serverless 1.2 use the legacy parser through Spark 3.3.2/3.5.1.
  // Classic 2.2/2.3 and Serverless 2.2/2.3 use patched Spark 3.5.3; Spark 4 is fixed.
  private val dataprocUsesFixedParser = {
    val sparkVersion = org.apache.spark.SPARK_VERSION
    sparkVersion.startsWith("3.5.3") || sparkVersion.split('.').head.toInt >= 4
  }

  test("supported Dataproc shims select measured quoted question mark semantics") {
    val dataprocConf = new SparkConf(false).set("spark.dataproc.engine", "default")
    val questionMarkCases = Seq(
      "$['?']" -> List(Key, Named("?")),
      "$['a?b']" -> List(Key, Named("a?b")),
      "$.outer['?']" -> List(Key, Named("outer"), Key, Named("?")))

    questionMarkCases.foreach { case (path, expected) =>
      val expectedResult = if (dataprocUsesFixedParser) Some(expected) else None
      assert(GetJsonObjectShim.parse(path, dataprocConf) === expectedResult)
    }
  }

  test("unquoted and malformed paths are independent of the configured platform") {
    val vanillaConf = new SparkConf(false)
    val dataprocConf = new SparkConf(false).set("spark.dataproc.engine", "default")

    Seq(vanillaConf, dataprocConf).foreach { conf =>
      assert(GetJsonObjectShim.parse("$.?", conf) ===
        Some(questionMarkPath))
      assert(GetJsonObjectShim.parse("$['ordinary']", conf) ===
        Some(List(Key, Named("ordinary"))))
      assert(GetJsonObjectShim.parse("$['']", conf).isEmpty)
      assert(GetJsonObjectShim.parse("$['unterminated]", conf).isEmpty)
    }
  }

  test("literal path parsing handles null") {
    assert(GpuGetJsonObjectMeta.parseLiteralPath(null).isEmpty)
  }

  test("vanilla shim parser matches the active Spark CPU expression") {
    val expectedValue = "QUESTION"
    val json = Literal.create(
      UTF8String.fromString(s"""{"?":"$expectedValue"}"""), StringType)
    val path = Literal.create(UTF8String.fromString("$['?']"), StringType)
    val cpuResult = Option(GetJsonObject(json, path).eval(null)).map(_.toString)
    val expectedInstructions = cpuResult match {
      case Some(`expectedValue`) => Some(questionMarkPath)
      case None => None
      case other => fail(s"Unexpected CPU get_json_object result: $other")
    }
    val vanillaConf = new SparkConf(false)

    assert(GetJsonObjectShim.parse("$['?']", vanillaConf) ===
      expectedInstructions)
    assert(JsonPathParser.parse("$['?']") === expectedInstructions)
  }
}
