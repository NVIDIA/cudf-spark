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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.parquet

import org.apache.hadoop.conf.Configuration

object ParquetLegacyNanoAsLongShims {
  def legacyParquetNanosAsLong(): Boolean = {
    // this should be true for 3.2.4+, 3.3.2+, 3.4.0+ if
    //   spark.sql.legacy.parquet.nanosAsLong = true
    false
  }

  def setupLegacyParquetNanosAsLongForPCBS(conf: Configuration): Unit = {
    // LEGACY_PARQUET_NANOS_AS_LONG is only considered in Spark 3.3.2 and later
  }
}
