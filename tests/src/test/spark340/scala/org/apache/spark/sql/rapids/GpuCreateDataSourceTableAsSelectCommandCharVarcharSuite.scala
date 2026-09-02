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
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, RapidsConf, SparkQueryCompareTestSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

class GpuCreateDataSourceTableAsSelectCommandCharVarcharSuite
  extends SparkQueryCompareTestSuite
  with FunSuiteWithTempDir {

  test("CTAS preserves CHAR/VARCHAR metadata and honors CHAR_AS_VARCHAR") {
    val sourceTable = "charVarcharSource"
    val preserveTarget = "charVarcharPreserveTarget"
    val convertTarget = "charVarcharConvertTarget"
    // Spark's CHAR read-side padding remains a CPU Project on these shims.
    val conf = new SparkConf(false)
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ProjectExec")
    withGpuSparkSession({ spark =>
      withTable(spark, sourceTable, preserveTarget, convertTarget) {
        spark.conf.set(SQLConf.CHAR_AS_VARCHAR.key, "false")
        spark.sql(s"CREATE TABLE $sourceTable(c CHAR(5), v VARCHAR(4)) USING PARQUET")

        Seq(
          (false, preserveTarget, Seq("char(5)", "varchar(4)")),
          (true, convertTarget, Seq("varchar(5)", "varchar(4)"))).foreach {
          case (charAsVarchar, targetTable, expectedTypes) =>
            spark.conf.set(SQLConf.CHAR_AS_VARCHAR.key, charAsVarchar.toString)
            spark.sql(s"CREATE TABLE $targetTable USING PARQUET AS SELECT * FROM $sourceTable")
            val actualTypes = spark.sql(s"DESC $targetTable")
              .selectExpr("data_type")
              .where("data_type like '%char%'")
              .collect()
              .map(_.getString(0))
              .toSeq
            assert(actualTypes == expectedTypes)
        }
      }
    }, conf)
  }

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }
}
