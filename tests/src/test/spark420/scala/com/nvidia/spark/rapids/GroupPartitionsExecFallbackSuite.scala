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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Collections

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.IntegerType

class GroupPartitionsExecFallbackSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)
    .set(SQLConf.V2_BUCKETING_ENABLED.key, "true")
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  override protected def filterCapturedPlans(plans: Array[SparkPlan]): Array[SparkPlan] = {
    super.filterCapturedPlans(plans).filter(_.exists(_.isInstanceOf[GroupPartitionsExec]))
  }

  testGpuFallback(
    "GroupPartitionsExec fallback preserves storage-partitioned join results",
    "GroupPartitionsExec",
    createStoragePartitionedJoin,
    conf = conf,
    repart = 0,
    sort = true,
    execsAllowedNonGpu =
      Seq("BatchScanExec", "FilterExec", "GroupPartitionsExec", "ProjectExec")) {
    df => df
  }

  private def createStoragePartitionedJoin(spark: SparkSession) = {
    val catalog = spark.sessionState.catalogManager
      .catalog("testcat")
      .asInstanceOf[InMemoryCatalog]
    catalog.clearTables()

    createTable(catalog, "left_table")
    createTable(catalog, "right_table")
    val rapidsEnabled = spark.conf.get(RapidsConf.SQL_ENABLED.key)
    spark.conf.set(RapidsConf.SQL_ENABLED.key, "false")
    try {
      spark.sql(
        "INSERT INTO testcat.ns.left_table VALUES (1, 10), (1, 20), (2, 30)")
      spark.sql(
        "INSERT INTO testcat.ns.right_table VALUES (1, 100), (2, 200), (2, 300)")
    } finally {
      spark.conf.set(RapidsConf.SQL_ENABLED.key, rapidsEnabled)
    }

    spark.sql(
      """SELECT /*+ MERGE(l, r) */ l.id, l.value AS left_value, r.value AS right_value
        |FROM testcat.ns.left_table l
        |JOIN testcat.ns.right_table r ON l.id = r.id
        |""".stripMargin)
  }

  private def createTable(catalog: InMemoryCatalog, name: String): Unit = {
    catalog.createTable(
      Identifier.of(Array("ns"), name),
      Array(
        Column.create("id", IntegerType),
        Column.create("value", IntegerType)),
      Array(Expressions.identity("id")),
      Collections.emptyMap[String, String](),
      Distributions.unspecified(),
      Array.empty,
      None,
      None,
      numRowsPerSplit = 1)
  }
}
