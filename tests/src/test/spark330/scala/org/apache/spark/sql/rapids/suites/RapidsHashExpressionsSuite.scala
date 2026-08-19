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

import org.apache.spark.sql.catalyst.expressions.HashExpressionsSuite
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

/**
 * RAPIDS GPU tests for hash expressions.
 *
 * This test suite validates hash expression execution on GPU.
 * It extends the original Spark HashExpressionsSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/
 *    HashExpressionsSuite.scala
 * Test count: 25 tests
 *
 * Migration notes:
 * - HashExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - This test suite covers:
 *   - Hash functions: md5, sha1, sha2, crc32
 *   - Murmur3 hash (32-bit and 128-bit)
 *   - xxHash64
 *   - Hive hash
 *   - Hash consistency across different data types
 *   - Hash with complex types (arrays, maps, structs)
 *   - Hash with null values
 */
class RapidsHashExpressionsSuite
  extends HashExpressionsSuite
  with RapidsTestsTrait {
  // All 25 tests from HashExpressionsSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU
  // GPU-specific hash configuration is handled by RapidsTestsTrait
}
