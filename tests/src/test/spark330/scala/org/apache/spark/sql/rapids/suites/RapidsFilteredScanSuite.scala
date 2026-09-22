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

import com.nvidia.spark.rapids.{GpuProjectExec, GpuRowToColumnarExec}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.{RapidsQueryTestUtil, RapidsSQLTestsTrait}
import org.apache.spark.sql.sources.{ColumnsRequired, EqualTo, Filter, FilteredScanSuite,
  FiltersPushed, LessThan}

class RapidsFilteredScanSuite extends FilteredScanSuite with RapidsSQLTestsTrait {
  private case class PushDownObservation(
      count: Long,
      requiredColumns: Set[String],
      pushedFilters: Seq[Filter],
      unhandledFilters: Set[Filter],
      resultRows: Seq[Row])

  // Adapted from Spark 3.3 FilteredScanSuite lines 237-344. The inherited helper executes the
  // CPU RowDataSourceScanExec directly, bypassing every GPU operator in the query-level plan.
  // https://github.com/apache/spark/blob/v3.3.0/sql/core/src/test/scala/org/apache/spark/sql/
  // sources/FilteredScanSuite.scala#L237-L344
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE A = 1", 1,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a FROM oneToTenFiltered WHERE A = 1", 1, Set("a"))
  testRapidsPushDown("SELECT b FROM oneToTenFiltered WHERE A = 1", 1, Set("b"))
  testRapidsPushDown("SELECT a, b FROM oneToTenFiltered WHERE A = 1", 1, Set("a", "b"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a = 1", 1,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE 1 = a", 1,
    Set("a", "b", "c"))

  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a > 1", 9,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a >= 2", 9,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE 1 < a", 9,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE 2 <= a", 9,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE 1 > a", 0,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE 2 >= a", 2,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a < 1", 0,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a <= 2", 2,
    Set("a", "b", "c"))

  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a > 1 AND a < 10", 8,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a IN (1,3,5)", 3,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a = 20", 0,
    Set("a", "b", "c"))
  testRapidsPushDown(
    "SELECT * FROM oneToTenFiltered WHERE b = 1",
    10,
    Set("a", "b", "c"),
    Set(EqualTo("b", 1)))

  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a < 5 AND a > 1", 3,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE a < 3 OR a > 8", 4,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT * FROM oneToTenFiltered WHERE NOT (a < 6)", 5,
    Set("a", "b", "c"))

  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like 'c%'", 1,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like 'C%'", 0,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like '%D'", 1,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like '%d'", 0,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like '%eE%'", 1,
    Set("a", "b", "c"))
  testRapidsPushDown("SELECT a, b, c FROM oneToTenFiltered WHERE c like '%Ee%'", 0,
    Set("a", "b", "c"))

  testRapidsPushDown("SELECT c FROM oneToTenFiltered WHERE c = 'aaaaaAAAAA'", 1,
    Set("c"))
  testRapidsPushDown(
    "SELECT c FROM oneToTenFiltered WHERE c IN ('aaaaaAAAAA', 'foo')",
    1,
    Set("c"))

  // Filters referencing multiple columns are not convertible, so all referenced columns must be
  // required by the source scan.
  testRapidsPushDown("SELECT c FROM oneToTenFiltered WHERE A + b > 9", 10,
    Set("a", "b", "c"))

  // A query with an inconvertible filter, an unhandled filter, and a handled filter.
  testRapidsPushDown(
    """SELECT a
      |  FROM oneToTenFiltered
      | WHERE a + b > 9
      |   AND b < 16
      |   AND c IN ('bbbbbBBBBB', 'cccccCCCCC', 'dddddDDDDD', 'foo')
    """.stripMargin.split("\n").map(_.trim).mkString(" "),
    3,
    Set("a", "b"),
    Set(LessThan("b", 16)))

  private def testRapidsPushDown(
      sqlString: String,
      expectedCount: Int,
      requiredColumnNames: Set[String]): Unit = {
    testRapidsPushDown(sqlString, expectedCount, requiredColumnNames, Set.empty[Filter])
  }

  private def testRapidsPushDown(
      sqlString: String,
      expectedCount: Int,
      requiredColumnNames: Set[String],
      expectedUnhandledFilters: Set[Filter]): Unit = {
    testRapids(s"PushDown Returns $expectedCount: $sqlString") {
      spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, false)
      try {
        val (cpuObservation, _) = executeQueryAndObservePushDown(sqlString, rapidsEnabled = false)
        val (gpuObservation, gpuPlan) =
          executeQueryAndObservePushDown(sqlString, rapidsEnabled = true)

        assert(cpuObservation.count === expectedCount.toLong)
        assert(cpuObservation.requiredColumns === requiredColumnNames)
        assert(cpuObservation.unhandledFilters === expectedUnhandledFilters)
        assert(gpuObservation === cpuObservation,
          s"GPU and CPU pushdown observations differ for $sqlString")
        assertGpuQueryPlan(gpuPlan)
      } finally {
        spark.conf.set(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.defaultValue.get)
      }
    }
  }

  private def executeQueryAndObservePushDown(
      sqlString: String,
      rapidsEnabled: Boolean): (PushDownObservation, SparkPlan) = {
    val rapidsSqlEnabledKey = "spark.rapids.sql.enabled"
    val originalRapidsEnabled = spark.conf.get(rapidsSqlEnabledKey)
    spark.conf.set(rapidsSqlEnabledKey, rapidsEnabled)
    try {
      val query = sql(sqlString).selectExpr("*", "1 AS __rapids_probe")
      val queryExecution = query.queryExecution
      val plan = queryExecution.executedPlan
      val sourceScan = plan.collect {
        case scan: DataSourceScanExec => scan
      } match {
        case Seq(scan) => scan
        case _ => fail(s"Expected exactly one DataSourceScanExec\n$queryExecution")
      }

      // Use the scan metric after the complete query runs to retain Spark's original assertion
      // about source rows without directly executing the CPU data-source leaf.
      val resultRows = RapidsQueryTestUtil.prepareAnswer(query.collect().toSeq, isSorted = false)
      val relation = spark.table("oneToTenFiltered").queryExecution.analyzed.collectFirst {
        case logicalRelation: LogicalRelation => logicalRelation.relation
      }.get
      val observation = PushDownObservation(
        sourceScan.metrics("numOutputRows").value,
        ColumnsRequired.set,
        FiltersPushed.list,
        relation.unhandledFilters(FiltersPushed.list.toArray).toSet,
        resultRows)
      (observation, plan)
    } finally {
      spark.conf.set(rapidsSqlEnabledKey, originalRapidsEnabled)
    }
  }

  private def assertGpuQueryPlan(plan: SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[GpuRowToColumnarExec]).nonEmpty,
      s"Expected GpuRowToColumnarExec above the CPU data source scan:\n$plan")
    assert(plan.find(_.isInstanceOf[GpuProjectExec]).nonEmpty,
      s"Expected GpuProjectExec in the query-level plan:\n$plan")
  }
}
