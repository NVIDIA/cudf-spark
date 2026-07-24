/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.internal.SQLConf

/**
 * Shim for MAX_BROADCAST_TABLE_BYTES which was removed in Spark 4.1.0.
 * The constant was 8GB (8L << 30) and is now configurable via conf.maxBroadcastTableSizeInBytes.
 */
object BroadcastExchangeShims {
  // 8GB - the original hardcoded value from Spark (kept for backwards compatibility)
  val MAX_BROADCAST_TABLE_BYTES: Long = 8L << 30

  /**
   * Get the maximum broadcast table size in bytes.
   * In Spark 4.1.0+, this reads from the configurable value.
   */
  def getMaxBroadcastTableBytes(conf: SQLConf): Long = {
    conf.maxBroadcastTableSizeInBytes
  }
}
