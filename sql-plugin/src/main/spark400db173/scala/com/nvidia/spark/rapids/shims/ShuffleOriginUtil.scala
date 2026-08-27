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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.execution.exchange.{DELTA_OPTIMIZED_WRITE, ENSURE_REQUIREMENTS,
  EXECUTOR_BROADCAST, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
  REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleOrigin}

object ShuffleOriginUtil {
  private val knownOrigins: Set[ShuffleOrigin] = Set(
    ENSURE_REQUIREMENTS,
    REPARTITION_BY_COL,
    REPARTITION_BY_NUM,
    REBALANCE_PARTITIONS_BY_NONE,
    REBALANCE_PARTITIONS_BY_COL,
    EXECUTOR_BROADCAST,
    DELTA_OPTIMIZED_WRITE)

  def isSupported(origin: ShuffleOrigin): Boolean = knownOrigins.contains(origin)
}
