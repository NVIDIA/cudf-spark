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

import org.apache.spark.{SparkException, SparkUpgradeException}
import org.apache.spark.sql.errors.QueryExecutionErrorsSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.EXCEPTION
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsQueryExecutionErrorsSuite extends QueryExecutionErrorsSuite with RapidsSQLTestsTrait {
  import testImplicits._

  testRapids("INCONSISTENT_BEHAVIOR_CROSS_VERSION: " +
    "compatibility with Spark 2.4/3.2 in reading/writing dates") {

    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
      val fileName = "before_1582_date_v2_4_5.snappy.parquet"
      val error = intercept[SparkException] {
        spark.read.parquet(testFile("test-data/" + fileName)).collect()
      }.getCause.asInstanceOf[SparkUpgradeException]

      val format = "Parquet"
      val config = "\"" + SQLConf.PARQUET_REBASE_MODE_IN_READ.key + "\""
      val option = "\"datetimeRebaseMode\""
      assert(error.getErrorClass === "INCONSISTENT_BEHAVIOR_CROSS_VERSION")
      assert(error.getMessage ===
        "You may get a different result due to the upgrading to Spark >= 3.0: " +
        s"""
          |reading dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z
          |from $format files can be ambiguous, as the files may be written by
          |Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar
          |that is different from Spark 3.0+'s Proleptic Gregorian calendar.
          |See more details in SPARK-31404. You can set the SQL config $config or
          |the datasource option $option to "LEGACY" to rebase the datetime values
          |w.r.t. the calendar difference during reading. To read the datetime values
          |as it is, set the SQL config $config or the datasource option $option
          |to "CORRECTED".
          |""".stripMargin)
    }

    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
      withTempPath { dir =>
        val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
        val error = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException]

        val format = "Parquet"
        val config = "\"" + SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key + "\""
        assert(error.getErrorClass === "INCONSISTENT_BEHAVIOR_CROSS_VERSION")
        assert(error.getMessage ===
          "You may get a different result due to the upgrading to Spark >= 3.0: " +
          s"""
            |writing dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z
            |into $format files can be dangerous, as the files may be read by Spark 2.x
            |or legacy versions of Hive later, which uses a legacy hybrid calendar that
            |is different from Spark 3.0+'s Proleptic Gregorian calendar. See more
            |details in SPARK-31404. You can set $config to "LEGACY" to rebase the
            |datetime values w.r.t. the calendar difference during writing, to get maximum
            |interoperability. Or set $config to "CORRECTED" to write the datetime
            |values as it is, if you are 100% sure that the written files will only be read by
            |Spark 3.0+ or other systems that use Proleptic Gregorian calendar.
            |""".stripMargin)
      }
    }
  }
}
