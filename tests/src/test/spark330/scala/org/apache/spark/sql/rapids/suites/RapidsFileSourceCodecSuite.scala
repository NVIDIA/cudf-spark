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

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.execution.datasources.{OrcCodecSuite, ParquetCodecSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsParquetCodecSuite extends ParquetCodecSuite with RapidsSQLTestsTrait {

  testRapids("write and read - file source parquet - codec: lz4 - v2 small pages no crc") {
    val activeSpark = spark
    import activeSpark.implicits._

    val compressibleValue = "a" * 1000
    val data = Seq.tabulate(512) { i =>
      if (i % 3 == 0) None else Some(compressibleValue)
    }.toDF("value")
    withSQLConf(
      SQLConf.PARQUET_COMPRESSION.key -> "lz4",
      ParquetOutputFormat.PAGE_SIZE -> "256",
      ParquetOutputFormat.MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK -> "1",
      ParquetOutputFormat.MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK -> "1",
      ParquetOutputFormat.ENABLE_DICTIONARY -> "false",
      ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED -> "false",
      ParquetOutputFormat.WRITER_VERSION ->
        ParquetProperties.WriterVersion.PARQUET_2_0.toString) {
      withTempPath { dir =>
        data.coalesce(1).write.parquet(dir.getCanonicalPath)
        checkAnswer(activeSpark.read.parquet(dir.getCanonicalPath), data)
      }
    }
  }
}

class RapidsOrcCodecSuite extends OrcCodecSuite with RapidsSQLTestsTrait
