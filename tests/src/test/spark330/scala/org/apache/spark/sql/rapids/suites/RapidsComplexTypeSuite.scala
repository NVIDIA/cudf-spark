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

import org.apache.spark.sql.catalyst.expressions.ComplexTypeSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for complex type expressions.
 *
 * This test suite validates complex type expression execution on GPU.
 * It extends the original Spark ComplexTypeSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    ComplexTypeSuite.scala
 * Test count: 19 tests
 *
 * Migration notes:
 * - ComplexTypeSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Array access: GetArrayItem
 *   - Map access: GetMapValue
 *   - Struct field extraction: GetStructField
 *   - Array/Map/Struct creation
 *   - Complex type operations with null handling
 *   - Nested complex types
 */
class RapidsComplexTypeSuite
  extends ComplexTypeSuite
  with RapidsTestsTrait {
  // All 19 tests from ComplexTypeSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific complex type configuration is handled by RapidsTestsTrait
}
