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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class BloomFilterAggregateQuerySuiteSpark411 extends BloomFilterAggregateQuerySuiteBase {

  // V2 literal: version=2, numHashes=5, seed=0, numLongs=3, followed by 3 longs of bit data
  testSparkResultsAreEqual(
    "might_contain with V2 literal bloom filter buffer",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(
          """SELECT might_contain(
            |X'0000000200000005000000000000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267',
            |cast(201 as long))""".stripMargin)
      }
  }
}
