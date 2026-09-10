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

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.sources.{FileExistingTestFileSystem, InsertSuite}

class RapidsInsertSuite extends InsertSuite with RapidsSQLTestsTrait {

  import testImplicits._

  private def exceptionMessages(error: Throwable): String = {
    val messages = new StringBuilder
    var current = error
    while (current != null) {
      messages.append(current.toString)
      messages.append('\n')
      current = current.getCause
    }
    messages.toString()
  }

  testRapids("Throw exceptions on inserting out-of-range int value with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(b int) using parquet")
        Seq((Int.MaxValue + 1L).toString, (Int.MinValue - 1L).toString).foreach { value =>
          val error = intercept[SparkException] {
            sql(s"insert into t values($value)")
          }
          assert(exceptionMessages(error).contains("overflow occurred"))
        }
        checkAnswer(sql("select * from t"), Seq.empty)
      }
    }
  }

  testRapids("Throw exceptions on inserting out-of-range long value with ANSI casting policy") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(b long) using parquet")
        Seq(Math.nextUp(Long.MaxValue), Math.nextDown(Long.MinValue)).foreach { value =>
          val error = intercept[SparkException] {
            sql(s"insert into t values(${value}D)")
          }
          assert(exceptionMessages(error).contains("overflow occurred"))
        }
        checkAnswer(sql("select * from t"), Seq.empty)
      }
    }
  }

  testRapids("Stop task set if FileAlreadyExistsException was thrown") {
    val fastFailMessage = "can not write to output file:"
    Seq(true, false).foreach { fastFail =>
      withSQLConf(
        "fs.file.impl" -> classOf[FileExistingTestFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true",
        SQLConf.FASTFAIL_ON_FILEFORMAT_OUTPUT.key -> fastFail.toString) {
        withTable("t") {
          sql("CREATE TABLE t(i INT, part1 INT) USING PARQUET PARTITIONED BY (part1)")
          val error = intercept[SparkException] {
            Seq((1, 1)).toDF("i", "part1")
              .write.mode("overwrite").format("parquet").insertInto("t")
          }
          assert(exceptionMessages(error).contains("FileAlreadyExistsException"))
          assert(exceptionMessages(error).contains(fastFailMessage) === fastFail)
        }
      }
    }
  }
}
