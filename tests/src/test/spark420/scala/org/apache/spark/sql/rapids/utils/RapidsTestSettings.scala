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
{"spark":["420"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import org.apache.spark.sql.rapids.suites._

class RapidsTestSettings extends BackendTestSettings {
  enableSuite[RapidsConditionalExpressionSuite]
  enableSuite[RapidsDataFrameTungstenSuite]
  enableSuite[RapidsDatasetUnpivotSuite]
  enableSuite[RapidsSubqueryHintPropagationSuite]
    .exclude("Correlated Exists containing join with hint",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/17163 " +
        "AQE materializes a row-based shuffle stage. Restore with GPU shuffle under AQE (P2)."))
  enableSuite[RapidsOptimizeExpandQuerySuite]
  enableSuite[RapidsPushDownJoinThroughUnionSuite]
  enableSuite[RapidsInlineTableParsingImprovementsSuite]
    .exclude("Insert Into Values optimization - Basic literals.",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/8324 " +
        "https://github.com/NVIDIA/cudf-spark/issues/11233 " +
        "https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "Restore with GPU column defaults, CHAR read padding and CHAR/VARCHAR write checks (P2)."))
    .exclude("Insert Into Values optimization - Basic literals & expressions.",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/8324 " +
        "https://github.com/NVIDIA/cudf-spark/issues/11233 " +
        "https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "Restore with GPU column defaults, CHAR read padding and CHAR/VARCHAR write checks (P2)."))
    .exclude("Insert Into Values with defaults.",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "https://github.com/NVIDIA/cudf-spark/issues/8324 " +
        "Restore with GPU Parquet column defaults and CHAR read padding (P2)."))
  enableSuite[RapidsLateralColumnAliasSuite]
  enableSuite[RapidsParametersSuite]
    .exclude("SPARK-44680: parameters in DEFAULT work",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "Restore with GPU Parquet column defaults (P2)."))
    .exclude("named parameters in DEFAULT expressions",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "Restore with GPU Parquet column defaults (P2)."))
    .exclude("positional parameters in DEFAULT expressions",
      KNOWN_ISSUE("https://github.com/NVIDIA/cudf-spark/issues/9766 " +
        "Restore with GPU Parquet column defaults (P2)."))
  enableSuite[RapidsSQLFunctionSuite]
  enableSuite[RapidsSimpleMetricViewSuite]
  enableSuite[RapidsStringLiteralCoalescingSuite]
}
