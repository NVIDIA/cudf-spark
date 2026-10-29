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
{"spark":["330","350"],"carryForward":"all"}
{"spark":["330db","332db","350db143","400","401","402","403","411"],"carryForward":"none"}
{"spark":["404"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.internal.SQLConf

@scala.annotation.nowarn(
  "msg=method readFooters in class ParquetFileReader is deprecated"
)
class ConcurrentWriterMetricsSuite extends SparkQueryCompareTestSuite {
  test("Concurrent writer update file metrics") {
    withGpuSparkSession(spark => {
      try {
        spark.sql("CREATE TABLE t(c int) USING parquet PARTITIONED BY (p String)")
        SQLConf.get.setConfString(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key, "3")
        SQLConf.get.setConfString(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, "1")

        def checkMetrics(sqlStr: String, numFiles: Int, numOutputRows: Long): Unit = {
          val df = spark.sql(sqlStr)
          val insert = SparkShimImpl.findOperators(df.queryExecution.executedPlan,
            _.isInstanceOf[GpuDataWritingCommandExec]).head
            .asInstanceOf[GpuDataWritingCommandExec]
          assert(insert.metrics.contains("numFiles"))
          assert(insert.metrics("numFiles").value == numFiles)
          assert(insert.metrics.contains("numOutputBytes"))
          assert(insert.metrics("numOutputBytes").value > 0)
          assert(insert.metrics.contains("numOutputRows"))
          assert(insert.metrics("numOutputRows").value == numOutputRows)
        }

        checkMetrics("""
                       |INSERT INTO TABLE t PARTITION(p) SELECT * 
                       |FROM VALUES(1, 'a'),(2, 'a'),(1, 'b')""".stripMargin, 2, 3)
        
        checkMetrics("""
                       |INSERT INTO TABLE t PARTITION(p) SELECT *
                       |FROM VALUES(1, 'a'),(2, 'b'),(1, 'c'),(2, 'd')""".stripMargin, 4, 4)

      } finally {
        spark.sql("DROP TABLE IF EXISTS t")
        spark.sql("DROP TABLE IF EXISTS tempmetricstable")
        SQLConf.get.unsetConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS)
        SQLConf.get.unsetConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM)
      }
    })
  }
}