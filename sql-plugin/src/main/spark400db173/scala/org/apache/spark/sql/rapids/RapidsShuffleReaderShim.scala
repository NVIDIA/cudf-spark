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
package org.apache.spark.sql.rapids

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter}

trait RapidsShuffleReaderShim {
  self: RapidsShuffleInternalManagerBase =>

  // 8-parameter version for Databricks 17.3 (added prismMapStatusEnabled)
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      prismMapStatusEnabled: Boolean): ShuffleReader[K, C] = {
    // Ignore prismMapStatusEnabled and call common implementation
    getReaderImpl(handle, startMapIndex, endMapIndex,
      startPartition, endPartition, context, metrics)
  }
}
