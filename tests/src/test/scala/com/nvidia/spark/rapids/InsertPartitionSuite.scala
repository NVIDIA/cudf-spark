/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.scalatest.BeforeAndAfterEach

class InsertPartitionSuite extends SparkQueryCompareTestSuite with BeforeAndAfterEach {
  var tableNr = 0

  override def afterEach(): Unit = {
    List(1, 2).foreach { tnr =>
      SparkSessionHolder.sparkSession.sql(s"DROP TABLE IF EXISTS t$tnr")
    }
  }

  testSparkResultsAreEqual(
    testName ="Insert null-value partition ",
    spark => {
      tableNr += 1
      spark.sql(s"""CREATE TABLE t${tableNr}(i STRING, c STRING)
                   |USING PARQUET PARTITIONED BY (c)""".stripMargin)
      spark.sql(s"""INSERT OVERWRITE t${tableNr} PARTITION (c=null)
                   |VALUES ('1')""".stripMargin)})(
    _.sparkSession.sql(s"SELECT * FROM t$tableNr"))
}
