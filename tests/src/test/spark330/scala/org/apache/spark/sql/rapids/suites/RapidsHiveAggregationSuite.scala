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

import org.scalatest.{Args, Status, SucceededStatus, Suite, Suites}

import org.apache.spark.tags.SlowHiveTest

// Construct the inherited suites only when this group executes. TestHiveSingleton
// retains a concrete SparkSession val, so constructing it during discovery lets
// intervening SharedSparkSession suites stop the context before Hive tests run.
// Keep the Hive suites consecutive and release their context afterwards.
@SlowHiveTest
class RapidsHiveAggregationSuite extends Suite {
  override def run(testName: Option[String], args: Args): Status = {
    if (args.filter.excludeNestedSuites ||
        args.filter.tagsToExclude.contains(classOf[SlowHiveTest].getName) ||
        args.filter.tagsToInclude.exists(tags =>
          !tags.contains(classOf[SlowHiveTest].getName))) {
      SucceededStatus
    } else {
      RapidsHiveTestSession.withContext {
        val suites = new Suites(
          new RapidsHashAggregationQuerySuite,
          new RapidsHashUDAQuerySuite)
        val status = suites.run(testName, args.copy(distributor = None))
        status.waitUntilCompleted()
        status
      }
    }
  }
}
