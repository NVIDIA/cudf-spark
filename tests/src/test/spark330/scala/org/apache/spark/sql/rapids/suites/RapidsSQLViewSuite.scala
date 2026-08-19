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

import org.apache.spark.sql.execution.SimpleSQLViewSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for SQL View operations.
 *
 * This test suite validates SQL View operation execution on GPU.
 * It extends the original Spark SimpleSQLViewSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/execution/SQLViewSuite.scala
 * Test count: 50 tests
 *
 * Migration notes:
 * - SimpleSQLViewSuite extends SQLViewSuite with SharedSparkSession
 * - SQLViewSuite extends QueryTest with SQLTestUtils, so we use RapidsSQLTestsTrait
 * - This test suite covers:
 *   - CREATE VIEW (temporary and permanent)
 *   - ALTER VIEW
 *   - DROP VIEW
 *   - View resolution and metadata
 *   - View with complex queries
 *   - View dependencies
 */
class RapidsSQLViewSuite
  extends SimpleSQLViewSuite
  with RapidsSQLTestsTrait {
  // All 50 tests from SQLViewSuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific SQL View configuration is handled by RapidsSQLTestsTrait
}
