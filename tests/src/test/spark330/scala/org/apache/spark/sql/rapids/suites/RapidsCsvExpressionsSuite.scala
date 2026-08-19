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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.CsvExpressionsSuite
import org.apache.spark.sql.catalyst.expressions.CsvToStructs
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types._

/**
 * RAPIDS GPU tests for CSV expressions (from_csv, to_csv, schema_of_csv).
 *
 * This test suite validates CSV expression functions on GPU at the expression level.
 * It extends the original Spark CsvExpressionsSuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/CsvExpressionsSuite.scala
 * Test count: 17 tests
 *
 * Migration notes:
 * - CsvExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper,
 *   so we use RapidsTestsTrait
 * - Tests CSV expression-level operations: CsvToStructs, StructsToCsv, SchemaOfCsv
 * - GPU CSV expression support requires: spark.rapids.sql.expression.CsvToStructs=true
 */
class RapidsCsvExpressionsSuite extends CsvExpressionsSuite with RapidsTestsTrait {
  // GPU-specific CSV expression configuration is handled by RapidsTestsTrait
  // The trait automatically enables GPU CSV expression support when running tests
  testRapids("unsupported mode") {
    val csvData = "---"
    val schema = StructType(StructField("a", DoubleType) :: Nil)
    val exception = intercept[SparkException] {
      checkEvaluation(
        CsvToStructs(schema, Map("mode" -> DropMalformedMode.name), Literal(csvData), UTC_OPT),
        InternalRow(null))
    }
    assert(exception.getMessage.contains("from_csv() doesn't support the DROPMALFORMED mode"))
  }
}
