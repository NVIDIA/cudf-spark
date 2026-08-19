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

import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS test suite for CSV schema evolution without header.
 * 
 * Tests CSV file reading with schema changes:
 * - Add columns (schema evolution)
 * - Hide columns (schema projection)
 * - Type upcast (Byte→Short→Int→Long, Float→Double→Decimal, etc.)
 */
class RapidsCSVReadSchemaSuite 
  extends CSVReadSchemaSuite 
  with RapidsSQLTestsTrait {
  // All tests from CSVReadSchemaSuite are inherited and will be run on GPU.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for CSV schema evolution with header enabled.
 * 
 * Same as RapidsCSVReadSchemaSuite but with header option enabled.
 */
class RapidsHeaderCSVReadSchemaSuite 
  extends HeaderCSVReadSchemaSuite 
  with RapidsSQLTestsTrait {
  // All tests from HeaderCSVReadSchemaSuite are inherited and will be run on GPU.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for JSON schema evolution.
 * 
 * Tests JSON file reading with schema changes:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * - Type upcast (Byte→Short→Int→Long, Float→Double→Decimal, etc.)
 */
class RapidsJsonReadSchemaSuite
  extends JsonReadSchemaSuite
  with RapidsSQLTestsTrait {
  // JSON supports the widest range of schema evolution operations.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for ORC schema evolution (non-vectorized reader).
 * 
 * Tests ORC file reading with schema changes:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * 
 * Note: This suite disables vectorized ORC reader.
 */
class RapidsOrcReadSchemaSuite
  extends OrcReadSchemaSuite
  with RapidsSQLTestsTrait {
  // ORC schema evolution with non-vectorized reader.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for ORC schema evolution (vectorized reader).
 * 
 * Tests ORC file reading with schema changes using vectorized reader:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * - Type upcast (Boolean, Byte→Short→Int→Long, Float→Double)
 * 
 * Note: This suite enables vectorized ORC reader.
 */
class RapidsVectorizedOrcReadSchemaSuite
  extends VectorizedOrcReadSchemaSuite
  with RapidsSQLTestsTrait {
  // ORC schema evolution with vectorized reader (widest coverage).
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for ORC schema evolution with schema merging enabled.
 * 
 * Tests ORC file reading with schema merging across multiple files:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * - Type upcast (Boolean, Byte→Short→Int→Long, Float→Double)
 * 
 * Note: This suite enables ORC schema merging.
 */
class RapidsMergedOrcReadSchemaSuite
  extends MergedOrcReadSchemaSuite
  with RapidsSQLTestsTrait {
  // ORC schema evolution with schema merging enabled.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for Parquet schema evolution (non-vectorized reader).
 * 
 * Tests Parquet file reading with schema changes:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * 
 * Note: This suite disables vectorized Parquet reader.
 */
class RapidsParquetReadSchemaSuite
  extends ParquetReadSchemaSuite
  with RapidsSQLTestsTrait {
  // Parquet schema evolution with non-vectorized reader.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for Parquet schema evolution (vectorized reader).
 * 
 * Tests Parquet file reading with schema changes using vectorized reader:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * 
 * Note: This suite enables vectorized Parquet reader.
 */
class RapidsVectorizedParquetReadSchemaSuite
  extends VectorizedParquetReadSchemaSuite
  with RapidsSQLTestsTrait {
  // Parquet schema evolution with vectorized reader.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

/**
 * RAPIDS test suite for Parquet schema evolution with schema merging enabled.
 * 
 * Tests Parquet file reading with schema merging across multiple files:
 * - Add/hide columns (at end or in the middle)
 * - Add/hide nested columns
 * - Change column position
 * 
 * Note: This suite enables Parquet schema merging.
 */
class RapidsMergedParquetReadSchemaSuite
  extends MergedParquetReadSchemaSuite
  with RapidsSQLTestsTrait {
  // Parquet schema evolution with schema merging enabled.
  // Exclusions for known issues will be added in RapidsTestSettings if needed.
}

