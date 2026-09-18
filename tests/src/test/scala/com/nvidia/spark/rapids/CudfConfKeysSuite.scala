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

import java.util.LinkedHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.internal.config.CudfConfKeys
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.RapidsPrivateUtil

class CudfConfKeysSuite extends AnyFunSuite {
  private val canonicalSqlEnabled = "spark.cudf.sql.enabled"
  private val legacySqlEnabled = "spark.rapids.sql.enabled"

  test("prefix conversion only changes complete plugin namespace prefixes") {
    assert(CudfConfKeys.canonicalKey(legacySqlEnabled) === canonicalSqlEnabled)
    assert(CudfConfKeys.canonicalKey(canonicalSqlEnabled) === canonicalSqlEnabled)
    assert(CudfConfKeys.legacyKey(canonicalSqlEnabled) === legacySqlEnabled)
    assert(CudfConfKeys.canonicalKey("spark.rapidsx.sql.enabled") ===
      "spark.rapidsx.sql.enabled")
    assert(CudfConfKeys.canonicalKey("other.spark.rapids.sql.enabled") ===
      "other.spark.rapids.sql.enabled")
  }

  test("canonical values win while materializing both aliases") {
    val input = new LinkedHashMap[String, String]()
    input.put(legacySqlEnabled, "false")
    input.put(canonicalSqlEnabled, "true")
    input.put("spark.app.name", "alias-test")

    val resolved = CudfConfKeys.withAliases(input)
    assert(resolved.get(canonicalSqlEnabled) === "true")
    assert(resolved.get(legacySqlEnabled) === "true")
    assert(resolved.get("spark.app.name") === "alias-test")
  }

  test("registered entries expose canonical names and accept both aliases") {
    assert(RapidsConf.SQL_ENABLED.key === canonicalSqlEnabled)
    assert(RapidsConf.SQL_ENABLED.legacyKey === legacySqlEnabled)
    assert(!new RapidsConf(Map(legacySqlEnabled -> "false")).isSqlEnabled)
    assert(!new RapidsConf(Map(canonicalSqlEnabled -> "false")).isSqlEnabled)

    val both = new RapidsConf(Map(
      legacySqlEnabled -> "not-a-boolean",
      canonicalSqlEnabled -> "true"))
    assert(both.isSqlEnabled)
  }

  test("private entries expose canonical names and canonical configuration references") {
    val fileCacheEntry = RapidsPrivateUtil.getPrivateConfigs().find(
      _.key == "spark.cudf.filecache.enabled").get
    assert(!fileCacheEntry.doc.contains("spark.rapids.filecache.maxBytes"))
    assert(fileCacheEntry.doc.contains("spark.cudf.filecache.maxBytes"))
  }

  test("canonical empty and invalid values win instead of falling back to legacy values") {
    val optionalCanonical = RapidsConf.OUTPUT_DEBUG_DUMP_PREFIX.key
    val optionalLegacy = RapidsConf.OUTPUT_DEBUG_DUMP_PREFIX.legacyKey
    val emptyWins = new RapidsConf(Map(optionalLegacy -> "legacy", optionalCanonical -> ""))
    assert(emptyWins.outputDebugDumpPrefix.contains(""))

    val invalidWins = new RapidsConf(Map(
      legacySqlEnabled -> "true",
      canonicalSqlEnabled -> "not-a-boolean"))
    val error = intercept[IllegalArgumentException](invalidWins.isSqlEnabled)
    assert(error.getMessage.contains(canonicalSqlEnabled))
  }

  test("SQLConf entry lookup gives canonical values precedence") {
    val conf = new SQLConf()
    conf.setConfString(legacySqlEnabled, "false")
    assert(!RapidsConf.SQL_ENABLED.get(conf))

    conf.setConfString(canonicalSqlEnabled, "true")
    assert(RapidsConf.SQL_ENABLED.get(conf))
    assert(RapidsConf.contains(conf, legacySqlEnabled))
  }

  test("SparkConf lookup accepts legacy names and gives canonical values precedence") {
    val conf = new SparkConf(false).set(legacySqlEnabled, "false")
    assert(RapidsConf.getOption(conf, canonicalSqlEnabled).contains("false"))
    assert(RapidsConf.contains(conf, legacySqlEnabled))

    conf.set(canonicalSqlEnabled, "true")
    assert(RapidsConf.getOption(conf, legacySqlEnabled).contains("true"))
  }

  test("dynamic operator and optimizer cost keys accept legacy aliases") {
    val legacyOperator = "spark.rapids.sql.exec.ProjectExec"
    val canonicalOperator = "spark.cudf.sql.exec.ProjectExec"
    val legacyCost = "spark.rapids.sql.optimizer.gpu.exec.CustomExec"
    val canonicalCost = "spark.cudf.sql.optimizer.gpu.exec.CustomExec"
    val conf = new RapidsConf(Map(
      legacyOperator -> "true",
      canonicalOperator -> "false",
      legacyCost -> "8.5",
      canonicalCost -> "4.25"))

    assert(!conf.isOperatorEnabled(canonicalOperator, incompat = false,
      isDisabledByDefault = false))
    assert(conf.getGpuOperatorCost("CustomExec").contains(4.25))
    assert(conf.isConfExplicitlySet(legacyOperator))
    assert(conf.isConfExplicitlySet(canonicalOperator))
  }

  test("driver-to-executor config map contains resolved values under both aliases") {
    val conf = new RapidsConf(Map(legacySqlEnabled -> "false"))
    val forwarded = conf.rapidsConfMap.asScala
    assert(forwarded(canonicalSqlEnabled) === "false")
    assert(forwarded(legacySqlEnabled) === "false")
  }

  test("legacy configuration warning is aggregated once per application") {
    val sparkContext = mock[SparkContext]
    val messages = LogCaptureUtils.captureLogsFrom(Seq(
      "com.nvidia.spark.rapids.RapidsConf")) {
      RapidsConf.warnIfLegacyConfs(sparkContext,
        Seq(legacySqlEnabled, "spark.rapids.memory.gpu.allocFraction"))
      RapidsConf.warnIfLegacyConfs(sparkContext,
        Seq("spark.rapids.shuffle.mode"))
    }

    val warnings = messages.filter(_.contains("configuration namespace is deprecated"))
    assert(warnings.length === 1)
    assert(warnings.head.contains(s"$legacySqlEnabled -> $canonicalSqlEnabled"))
    assert(!warnings.head.contains("false"))
  }
}
