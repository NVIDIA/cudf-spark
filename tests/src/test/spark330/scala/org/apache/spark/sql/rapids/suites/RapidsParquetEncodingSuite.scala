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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetEncodingSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetEncodingSuite
  extends ParquetEncodingSuite
  with RapidsSQLTestsBaseTrait {

  // Original: ParquetEncodingSuite.scala (Spark v3.3.0) lines 145-202 / 204-238.
  // GPU's libcudf parquet writer chooses different but valid Parquet v2
  // page encodings (PLAIN / RLE / PLAIN_DICTIONARY) than the CPU writer's
  // (DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY / RLE). Per #13745 / #13746,
  // the encoding format is an internal optimization detail that does not
  // affect data correctness; the round-trip data is identical. The
  // testRapids versions drop the encoding-format assertions and keep the
  // data-correctness round-trip — what the test actually guarantees about
  // user-visible behavior.
  // https://github.com/NVIDIA/spark-rapids/issues/13745
  // https://github.com/NVIDIA/spark-rapids/issues/13746
  testRapids("parquet v2 pages - delta encoding") {
    val extraOptions = Map[String, String](
      ParquetOutputFormat.WRITER_VERSION ->
        ParquetProperties.WriterVersion.PARQUET_2_0.toString,
      ParquetOutputFormat.ENABLE_DICTIONARY -> "false"
    )
    Seq("true", "false").foreach { offHeapMode =>
      withSQLConf(
        SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapMode,
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL") {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/test.parquet"
          val data = (1 to 8193).map { i =>
            (i,
              i.toLong, i.toShort, Array[Byte](i.toByte),
              if (i % 2 == 1) s"test_$i" else null,
              DateTimeUtils.fromJavaDate(Date.valueOf(s"2021-11-0" + ((i % 9) + 1))),
              DateTimeUtils.fromJavaTimestamp(
                Timestamp.valueOf(s"2020-11-01 12:00:0" + (i % 10))),
              Period.of(1, (i % 11) + 1, 0),
              Duration.ofMillis(((i % 9) + 1) * 100),
              new BigDecimal(java.lang.Long.toUnsignedString(i * 100000))
            )
          }
          spark.createDataFrame(data)
            .write.options(extraOptions).mode("overwrite").parquet(path)

          val actual = spark.read.parquet(path).collect()
          assert(actual.sortBy(_.getInt(0)) === data.map(Row.fromTuple))
        }
      }
    }
  }

  testRapids("parquet v2 pages - rle encoding for boolean value columns") {
    val extraOptions = Map[String, String](
      ParquetOutputFormat.WRITER_VERSION ->
        ParquetProperties.WriterVersion.PARQUET_2_0.toString
    )
    withSQLConf(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/test.parquet"
        val size = 10000
        val data = (1 to size).map { i => (true, false, i % 2 == 1) }

        spark.createDataFrame(data)
          .write.options(extraOptions).mode("overwrite").parquet(path)

        val actual = spark.read.parquet(path).collect()
        assert(actual.length == size)
        assert(actual.map(_.getBoolean(0)).forall(_ == true))
        assert(actual.map(_.getBoolean(1)).forall(_ == false))
        val expected = (1 to size).map { i => i % 2 == 1 }
        assert(actual.map(_.getBoolean(2)).sameElements(expected))
      }
    }
  }
}
