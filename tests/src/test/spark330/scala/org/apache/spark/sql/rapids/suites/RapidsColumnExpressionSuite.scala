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

import org.apache.spark.sql.ColumnExpressionSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for Column expressions.
 *
 * This test suite validates Column expression execution on GPU.
 * It extends the original Spark ColumnExpressionSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/ColumnExpressionSuite.scala
 * Test count: 145 tests
 *
 * Migration notes:
 * - ColumnExpressionSuite extends QueryTest with SharedSparkSession,
 *   so we use RapidsSQLTestsTrait
 * - This is a comprehensive Column expression test suite covering:
 *   - Column operations and transformations
 *   - Arithmetic and comparison operators
 *   - String operations
 *   - Date and timestamp functions
 *   - Array and map operations
 *   - Struct field access
 *   - Type casting and conversions
 */
class RapidsColumnExpressionSuite
  extends ColumnExpressionSuite
  with RapidsSQLTestsTrait {
  // All 145 tests from ColumnExpressionSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific Column expression configuration is handled by RapidsSQLTestsTrait
}
