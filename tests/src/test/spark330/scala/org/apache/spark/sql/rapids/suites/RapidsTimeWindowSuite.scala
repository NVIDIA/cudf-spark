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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.catalyst.expressions.TimeWindowSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for time window expressions.
 *
 * This test suite validates time window expression execution on GPU.
 * It extends the original Spark TimeWindowSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    TimeWindowSuite.scala
 * Test count: 14 tests
 *
 * Migration notes:
 * - TimeWindowSuite extends SparkFunSuite with ExpressionEvalHelper with PrivateMethodTester,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Time window creation and validation
 *   - Window duration, slide duration, start time
 *   - Tumbling and sliding windows
 *   - Window boundary calculations
 *   - Error handling for invalid window specifications
 */
class RapidsTimeWindowSuite
  extends TimeWindowSuite
  with RapidsTestsTrait {
  // All 14 tests from TimeWindowSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific time window configuration is handled by RapidsTestsTrait
}
