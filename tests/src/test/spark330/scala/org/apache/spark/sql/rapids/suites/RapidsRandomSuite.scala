/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.{Literal, Rand, Randn, RandomSuite}
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Test suite for random expressions on GPU.
 * This suite inherits from Spark's RandomSuite and runs the tests on GPU
 * through the overridden checkEvaluation method in RapidsTestsTrait.
 */
class RapidsRandomSuite extends RandomSuite with RapidsTestsTrait {
  // All tests from RandomSuite will be inherited and run on GPU
  // The checkEvaluation method is overridden in RapidsTestsTrait to execute on GPU

  // Override tests that check specific random values
  // Original test: RandomSuite.scala lines 25-31
  // Reason: DataFrame API execution has partitionIndex context, causing seed offset
  // Actual seed becomes: seed + partitionIndex (not just seed)
  testRapids("random") {
    // Note: In DataFrame execution, partitionIndex affects the actual seed
    // Original test expects: checkEvaluation(Rand(30), 0.2762195585886885)
    // But in DataFrame: seed becomes 30 + partitionIndex
    // So we change the seed to 29, and partitionIndex is 1 to make the seed 30
    checkEvaluation(Rand(29), 0.2762195585886885)
    checkEvaluation(Randn(29), -1.0451987154313813)

    // null seed = 0, with partitionIndex=1, becomes seed=1
    // Use seed=-1 to compensate: -1 + 1 = 0
    checkEvaluation(new Rand(Literal(-1L, LongType)), 0.7604953758285915)
    checkEvaluation(new Randn(Literal(-1, IntegerType)), 1.6034991609278433)
  }

  // Override test: SPARK-9127 codegen with long seed
  // Original test: RandomSuite.scala lines 33-36
  testRapids("SPARK-9127 codegen with long seed") {
    // Same issue: partitionIndex affects actual seed
    // Original: Rand(5419823303878592871L)
    // In DataFrame: seed becomes 5419823303878592871L + partitionIndex
    // Adjust seed to compensate: 5419823303878592871L - 1 = 5419823303878592870L
    checkEvaluation(Rand(5419823303878592870L), 0.7145363364564755)
    checkEvaluation(Randn(5419823303878592870L), 0.7816815274533012)
  }
}

