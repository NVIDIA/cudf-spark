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

import org.apache.spark.SparkConf
import org.apache.spark.sql.InjectRuntimeFilterSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsInjectRuntimeFilterSuite
  extends InjectRuntimeFilterSuite with RapidsSQLTestsTrait {

  // The upstream tests' thresholds (RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD,
  // AUTO_BROADCASTJOIN_THRESHOLD, RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD) are hardcoded
  // against parquet file sizes produced by Spark CPU writes (e.g. application=3362,
  // creation=3409 bytes). GPU parquet writes use different row-group/compression settings,
  // producing different sizeInBytes that fail the optimizer rule's threshold checks. Force
  // CPU parquet writes during beforeAll's saveAsTable so the catalog stats match the
  // hardcoded thresholds; GPU execution still happens for read/aggregation/scan paths.
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.rapids.sql.format.parquet.write.enabled", "false")
  }
}
