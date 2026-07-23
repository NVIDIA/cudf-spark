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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims.parquet

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf
object ParquetLegacyNanoAsLongShims {
  def legacyParquetNanosAsLong(): Boolean = {
    SQLConf.get.legacyParquetNanosAsLong
  }

  /**
   * This method should strictly be used by ParquetCachedBatchSerializer(PCBS) as it is hard coding
   * the value of LEGACY_PARQUET_NANOS_AS_LONG.
   *
   * As far as PCBS is concerned it really doesn't matter what we set it to as long as
   * ParquetSchemaConverter doesn't see a "null" value.
   *
   * @param conf Hadoop conf
   */
  def setupLegacyParquetNanosAsLongForPCBS(conf: Configuration): Unit = {
    conf.setBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, true)
  }
}
