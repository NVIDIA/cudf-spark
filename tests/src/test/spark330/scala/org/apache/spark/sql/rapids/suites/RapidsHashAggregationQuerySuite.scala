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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.scalatest.DoNotDiscover

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.execution.HashAggregationQuerySuite
import org.apache.spark.sql.rapids.utils.RapidsTestsCommonTrait
import org.apache.spark.tags.SlowHiveTest

// Run through RapidsHiveAggregationSuite to preserve the shared TestHive context.
@DoNotDiscover
@SlowHiveTest
class RapidsHashAggregationQuerySuite extends {
  private val initializeHive = RapidsHiveTestSession.initialize()
} with HashAggregationQuerySuite
  with RapidsTestsCommonTrait {
  override def afterAll(): Unit = super.afterAll()

  // Configuration checks alone do not establish native GPU execution. Keep the original
  // TestHiveSingleton lifecycle and QueryTest assertions; retain the original assertions.
  override def beforeAll(): Unit = {
    SparkSession.setActiveSession(spark)
    super.beforeAll()
    val actualConf = spark.sparkContext.getConf
    val plugins = actualConf.get("spark.plugins", "")
    val enabled = spark.conf.get("spark.rapids.sql.enabled", "false")
    val listeners = spark.conf.get("spark.sql.queryExecutionListeners", "")
    require(plugins.split(",").map(_.trim).contains("com.nvidia.spark.SQLPlugin"),
      "The original TestHive SparkContext must load SQLPlugin before suite construction")
    require(enabled.equalsIgnoreCase("true"),
      "The original TestHive SparkSession must enable RAPIDS SQL")
    require(listeners.split(",").map(_.trim)
      .contains("org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback"),
      "The original TestHive SparkSession must configure the plan capture listener")
  }
}
