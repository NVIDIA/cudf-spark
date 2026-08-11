/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import org.apache.spark.SparkException
import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.execution.datasources.parquet.ParquetRebaseDatetimeSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.EXCEPTION
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType.{INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS}
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetRebaseDatetimeSuite
  extends ParquetRebaseDatetimeSuite
    with RapidsSQLTestsBaseTrait {

  import testImplicits._

  test("SPARK-35427: datetime rebasing in the EXCEPTION mode Rapids") {
    def checkTsWrite(): Unit = {
      withTempPath { dir =>
        val df = Seq("1001-01-01 01:02:03.123")
          .toDF("str")
          .select($"str".cast("timestamp").as("dt"))
        val e = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }
        val errMsg = e.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException].getMessage
        assert(errMsg.contains("You may get a different result due to the upgrading"))
      }
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
        Seq(TIMESTAMP_MICROS, TIMESTAMP_MILLIS).foreach { tsType =>
          withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsType.toString) {
            checkTsWrite()
          }
        }
        withTempPath { dir =>
          val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
          val e = intercept[SparkException] {
            df.write.parquet(dir.getCanonicalPath)
          }
          val errMsg = e.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException].getMessage
          assert(errMsg.contains("You may get a different result due to the upgrading"))
        }
      }
      withSQLConf(
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString,
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> INT96.toString) {
        checkTsWrite()
      }
    }

    def checkRead(fileName: String): Unit = {
      val e = intercept[SparkException] {
        spark.read.parquet(testFile("test-data/" + fileName)).collect()
      }
      val errMsg = e.getCause.asInstanceOf[SparkUpgradeException].getMessage
      assert(errMsg.contains("You may get a different result due to the upgrading"))
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
        Seq(
          "before_1582_date_v2_4_5.snappy.parquet",
          "before_1582_timestamp_micros_v2_4_5.snappy.parquet",
          "before_1582_timestamp_millis_v2_4_5.snappy.parquet").foreach(checkRead)
      }
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
        checkRead("before_1582_timestamp_int96_dict_v2_4_5.snappy.parquet")
      }
    }
  }
}
