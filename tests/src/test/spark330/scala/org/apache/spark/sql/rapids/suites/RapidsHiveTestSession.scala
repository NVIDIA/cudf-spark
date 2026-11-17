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

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

// TestHiveSingleton initializes its context in the superclass constructor, before
// beforeAll. Scope Spark system properties to that initialization and restore them
// immediately; the three inherited Hive suites share the resulting TestHive context.
private[suites] object RapidsHiveTestSession {
  def initialize(): Unit = synchronized {
    // TestHive creates and manages its own warehouse directory.
    val conf = RapidsSQLTestsBaseTrait.nativeSparkConf(new SparkConf(false), "")
      .remove("spark.sql.warehouse.dir")
      .set("spark.rapids.memory.gpu.allocFraction",
        System.getProperty("rapids.test.gpu.allocFraction", "0.3"))
      .set("spark.rapids.memory.gpu.maxAllocFraction",
        System.getProperty("rapids.test.gpu.maxAllocFraction", "0.3"))
      .set("spark.rapids.memory.gpu.minAllocFraction",
        System.getProperty("rapids.test.gpu.minAllocFraction", "0"))
    val saved = conf.getAll.map { case (key, _) => key -> Option(System.getProperty(key)) }
    try {
      conf.getAll.foreach { case (key, value) => System.setProperty(key, value) }
      require(TestHive.sparkContext.getConf.get("spark.plugins", "")
        .split(",").contains("com.nvidia.spark.SQLPlugin"),
        "TestHive must initialize with the RAPIDS plugin")
    } finally {
      saved.foreach {
        case (key, Some(value)) => System.setProperty(key, value)
        case (key, None) => System.clearProperty(key)
      }
    }
  }
}
