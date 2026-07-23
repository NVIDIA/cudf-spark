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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

/**
 * Shim for Parquet variant-related configurations.
 * In Spark <= 4.0.x, PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE doesn't exist.
 * In Spark 4.1.0+, we need to set this configuration for ParquetWriteSupport.
 */
object ParquetVariantShims {
  def setupParquetVariantConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // No-op for Spark versions before 4.1.0
    // PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE doesn't exist
  }
}
