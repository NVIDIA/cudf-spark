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

import org.apache.spark.sql.catalyst.expressions.ConditionalExpressionSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for conditional expressions.
 *
 * This test suite validates conditional expression execution on GPU.
 * It extends the original Spark ConditionalExpressionSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    ConditionalExpressionSuite.scala
 * Test count: 8 tests
 *
 * Migration notes:
 * - ConditionalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - If expression (IF-THEN-ELSE)
 *   - CaseWhen expression (CASE WHEN ... THEN ... ELSE)
 *   - Type coercion in conditional expressions
 *   - Null handling in conditions
 *   - Short-circuit evaluation
 */
class RapidsConditionalExpressionSuite
  extends ConditionalExpressionSuite
  with RapidsTestsTrait {
  // All 8 tests from ConditionalExpressionSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific conditional expression configuration is handled by RapidsTestsTrait
}
