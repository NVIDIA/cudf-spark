/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400","420"],"carryForward":"all"}
{"spark":["350db143","400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.ScanMeta

import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeV2Filtering}

object TagScanForRuntimeFiltering {
  def tagScanForRuntimeFiltering[T <: Scan](meta: ScanMeta[T], scan: T): Unit = {
    val scanClass = scan.getClass
    // SupportsRuntimeV2Filtering is actually the parent of SupportsRuntimeFiltering in Spark 3.4.0,
    // which means this check will cover both cases.
    if (scan.isInstanceOf[SupportsRuntimeV2Filtering]) {
      meta.willNotWorkOnGpu(s"$scanClass does not support Runtime filtering (DPP)" +
        " on datasource V2 yet.")
    }
  }
}
