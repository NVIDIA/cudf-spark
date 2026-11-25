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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{ReusedSubqueryExec, ReuseExchangeAndSubquerySuite}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsReuseExchangeAndSubquerySuite
  extends ReuseExchangeAndSubquerySuite with RapidsSQLTestsBaseTrait {

  // The inherited tests inspect plans without collecting the queries. Exercise both
  // original query shapes and require nonempty GPU reuse as well as the ID contract.
  Seq(false, true).foreach { withUnion =>
    testRapids(s"SPARK-32041: executed GPU exchange reuse, union=$withUnion") {
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        withTable("df1", "df2") {
          withSQLConf("spark.rapids.sql.enabled" -> "false") {
            spark.range(100).select(col("id"), col("id").as("k"))
              .write.partitionBy("k").format(tableFormat).mode("overwrite").saveAsTable("df1")
            spark.range(10).select(col("id"), col("id").as("k"))
              .write.format(tableFormat).mode("overwrite").saveAsTable("df2")
          }
          val cte = """
            |WITH t AS (
            |  SELECT df1.id, df2.k FROM df1 JOIN df2 ON df1.k = df2.k
            |  WHERE df2.id < 2
            |)
            |""".stripMargin
          val query = if (withUnion) {
            cte + ", t2 AS (SELECT * FROM t UNION SELECT * FROM t) " +
              "SELECT * FROM t2 AS a JOIN t2 AS b ON a.id = b.id"
          } else {
            cte + "SELECT * FROM t AS a JOIN t AS b ON a.id = b.id"
          }
          var cpuRows = Array.empty[Row]
          var cpuSchema = org.apache.spark.sql.types.StructType(Nil)
          withSQLConf("spark.rapids.sql.enabled" -> "false") {
            val df = spark.sql(query)
            cpuSchema = df.schema
            cpuRows = df.collect()
          }
          withSQLConf("spark.rapids.sql.enabled" -> "true") {
            val df = spark.sql(query)
            val gpuRows = df.collect()
            assert(df.schema == cpuSchema)
            // Neither query orders its output; retain duplicates when comparing rows.
            def bag(rows: Array[Row]): Map[Row, Int] =
              rows.groupBy(identity).map { case (row, copies) => row -> copies.length }
            assert(bag(gpuRows) == bag(cpuRows))
            val plan = df.queryExecution.executedPlan
            val exchanges = plan.collectWithSubqueries {
              case exchange: Exchange => exchange
            }
            val reused = plan.collectWithSubqueries {
              case exchange: ReusedExchangeExec => exchange
            }
            if (withUnion) {
              assert(plan.collectWithSubqueries {
                case subquery: ReusedSubqueryExec => subquery
              }.nonEmpty, s"Expected reused subquery in UNION plan:\n$plan")
            }
            val gpuExchanges = exchanges.filter(_.isInstanceOf[GpuExec])
            val gpuReused = reused.filter(_.child.isInstanceOf[GpuExec])
            assert(gpuExchanges.nonEmpty, plan.toString)
            assert(gpuReused.nonEmpty, plan.toString)
            assert(reused.forall(re => exchanges.exists(_.id == re.child.id)),
              "Reused exchanges must reference an existing exchange")
            assert(gpuReused.forall(re => gpuExchanges.exists(_.id == re.child.id)),
              "GPU reused exchanges must reference an existing GPU exchange")
          }
        }
      }
    }
  }

}
