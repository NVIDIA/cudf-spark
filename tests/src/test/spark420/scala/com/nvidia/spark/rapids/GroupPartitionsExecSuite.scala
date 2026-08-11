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

import com.nvidia.spark.rapids.shims.{GpuGroupPartitionsExec, GpuGroupPartitionsExecInfo,
  GroupPartitionsExecMeta}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.IntegerType

class GroupPartitionsExecSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)
    .set(SQLConf.V2_BUCKETING_ENABLED.key, "true")
    .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  override protected def filterCapturedPlans(plans: Array[SparkPlan]): Array[SparkPlan] = {
    super.filterCapturedPlans(plans).filter(_.exists {
      case _: GroupPartitionsExec | _: GpuGroupPartitionsExec => true
      case _ => false
    })
  }

  testSparkResultsAreEqualWithCapture(
    "GroupPartitionsExec coalesces and pads storage-partitioned joins on GPU",
    createStoragePartitionedJoin,
    conf = conf,
    repart = 0,
    sort = true,
    execsAllowedNonGpu = Seq("BatchScanExec", "FilterExec", "ProjectExec")) {
    df => df
  } { (_, gpuPlan) =>
    val gpuGroups = gpuPlan.collect { case g: GpuGroupPartitionsExec => g }
    assert(gpuGroups.nonEmpty, s"Expected GpuGroupPartitionsExec in plan:\n$gpuPlan")
    assert(gpuPlan.collect { case g: GroupPartitionsExec => g }.isEmpty,
      s"GroupPartitionsExec unexpectedly fell back to CPU:\n$gpuPlan")
    assert(gpuPlan.collect { case exchange: Exchange => exchange }.isEmpty,
      s"Storage-partitioned join unexpectedly contains an exchange:\n$gpuPlan")
    assert(gpuGroups.exists(_.groupedPartitions.exists(_._2.size > 1)),
      "Expected at least one key to coalesce multiple input partitions")
    assert(gpuGroups.exists(_.groupedPartitions.exists(_._2.isEmpty)),
      "Expected a missing key to produce an empty padded partition group")
    val groupWithExpectedKeys = gpuGroups.find(_.expectedPartitionKeys.nonEmpty).getOrElse {
      fail("Expected a GPU group with expected partition keys")
    }
    val summary = groupWithExpectedKeys.simpleString(maxFields = 1)
    assert(summary.contains(
      s"ExpectedPartitionKeys: ${groupWithExpectedKeys.expectedPartitionKeys.get.size}"))
    assert(summary.length < 256, s"GPU plan summary is unexpectedly long: $summary")
  }

  testSparkResultsAreEqualWithCapture(
    "GroupPartitionsExec preserves grouping across a child transition",
    createStoragePartitionedJoin,
    conf = conf.clone().set("spark.rapids.sql.exec.BatchScanExec", "false"),
    repart = 0,
    sort = true,
    execsAllowedNonGpu = Seq("BatchScanExec", "FilterExec", "ProjectExec")) {
    df => df
  } { (_, gpuPlan) =>
    val gpuGroups = gpuPlan.collect { case g: GpuGroupPartitionsExec => g }
    assert(gpuGroups.nonEmpty, s"Expected GpuGroupPartitionsExec in plan:\n$gpuPlan")
    assert(gpuPlan.exists(_.isInstanceOf[GpuRowToColumnarExec]),
      s"Expected GpuRowToColumnarExec in plan:\n$gpuPlan")
    assert(gpuGroups.exists(_.groupedPartitions.exists(_._2.size > 1)),
      "Expected grouping metadata from the original CPU child")
  }

  test("GpuGroupPartitionsExec returns an empty RDD for an empty grouping plan") {
    withGpuSparkSession { _ =>
      val groupPartitions = GpuGroupPartitionsExec(
        LocalTableScanExec(Nil, Nil, None),
        GpuGroupPartitionsExecInfo(
          UnknownPartitioning(0),
          Seq.empty,
          Seq.empty,
          isGrouped = true,
          joinKeyPositions = None,
          expectedPartitionKeys = None,
          reducers = None,
          distributePartitions = false))

      assert(groupPartitions.allMetrics.keySet == Set(GpuMetric.OP_TIME_NEW))
      assert(groupPartitions.getOpTimeNewMetric.nonEmpty)
      assert(groupPartitions.executeColumnar().partitions.isEmpty)
    }
  }

  test("sorted-merge GroupPartitionsExec keeps the original CPU subtree") {
    val groupPartitions = GroupPartitionsExec(
      LocalTableScanExec(Nil, Nil, None),
      enableSortedMerge = true)
    val meta = GpuOverrides.wrapAndTagPlan(
      groupPartitions,
      new RapidsConf(Map.empty[String, String]))

    assert(meta.isInstanceOf[GroupPartitionsExecMeta])
    assert(!meta.canThisBeReplaced)
    assert(meta.explain(false).contains(
      "Sorted-merge GroupPartitionsExec is not supported on GPU"))
    assert(meta.asInstanceOf[GroupPartitionsExecMeta].convertToCpu().eq(groupPartitions))
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
        "INSERT INTO testcat.ns.left_table VALUES (1, 10), (1, 20), (2, 30), (3, 40)")
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
