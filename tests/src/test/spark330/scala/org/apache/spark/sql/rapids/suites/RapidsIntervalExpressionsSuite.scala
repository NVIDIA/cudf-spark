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

import org.apache.spark.sql.catalyst.expressions.IntervalExpressionsSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for interval expressions.
 *
 * This test suite validates interval expression execution on GPU.
 * It extends the original Spark IntervalExpressionsSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    IntervalExpressionsSuite.scala
 * Test count: 20 tests
 *
 * Migration notes:
 * - IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Extract interval components: years, months, days, hours, minutes, seconds
 *   - Year-month interval operations
 *   - Day-time interval operations
 *   - Interval multiplication and division
 *   - Make interval functions
 *   - Interval type conversions and coercion
 */
class RapidsIntervalExpressionsSuite
  extends IntervalExpressionsSuite
  with RapidsTestsTrait {
  // All 20 tests from IntervalExpressionsSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific interval configuration is handled by RapidsTestsTrait
}
