/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.StatisticsCollectionSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsStatisticsCollectionSuite
    extends StatisticsCollectionSuite with RapidsSQLTestsTrait {

  // The upstream suite validates statistics rather than JSON scan or cache serialization.
  // Its zero-column table is not supported by the GPU JSON reader, and its view-size assertions
  // are defined in terms of Spark's default cache serializer. Keep those setup paths on CPU while
  // retaining GPU execution for the suite's query, aggregation, and statistics paths.
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.rapids.sql.format.json.read.enabled", "false")
      .set("spark.sql.cache.serializer",
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
  }
}
