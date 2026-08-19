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

import org.apache.spark.sql.catalyst.expressions.ArithmeticExpressionSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for arithmetic expressions.
 *
 * This test suite validates arithmetic expression execution on GPU.
 * It extends the original Spark ArithmeticExpressionSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    ArithmeticExpressionSuite.scala
 * Test count: 56 tests
 *
 * Migration notes:
 * - ArithmeticExpressionSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Basic arithmetic: Add, Subtract, Multiply, Divide, Remainder
 *   - Unary operations: UnaryMinus, UnaryPositive, Abs
 *   - Decimal arithmetic with precision and scale handling
 *   - Interval arithmetic (year-month and day-time intervals)
 *   - Overflow handling in ANSI mode
 *   - Special value handling (null, infinity, NaN)
 *   - Pmod (positive modulo) operation
 */
class RapidsArithmeticExpressionSuite
  extends ArithmeticExpressionSuite
  with RapidsTestsTrait {
  // All 56 tests from ArithmeticExpressionSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific arithmetic configuration is handled by RapidsTestsTrait
}
