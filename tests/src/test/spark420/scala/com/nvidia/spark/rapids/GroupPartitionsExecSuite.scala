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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shims.{GpuGroupPartitionsExec, GpuGroupPartitionsExecInfo,
  GpuGroupPartitionsExecMeta}
import com.nvidia.spark.rapids.shims.ShimLeafExecNode
import org.mockito.Mockito.{doReturn, mock, spy, when}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference,
  Descending, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    assert(gpuGroups.exists(_.partitionGroups.exists(_.size > 1)),
      "Expected at least one key to coalesce multiple input partitions")
    assert(gpuGroups.exists(_.partitionGroups.exists(_.isEmpty)),
      "Expected a missing key to produce an empty padded partition group")
    val groupWithExpectedKeys = gpuGroups.find(_.expectedPartitionKeyCount.nonEmpty).getOrElse {
      fail("Expected a GPU group with expected partition keys")
    }
    val summary = groupWithExpectedKeys.simpleString(maxFields = 1)
    assert(summary.contains(
      s"ExpectedPartitionKeys: ${groupWithExpectedKeys.expectedPartitionKeyCount.get}"))
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
    assert(gpuGroups.exists(_.partitionGroups.exists(_.size > 1)),
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
          Seq.empty,
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = false))

      assert(groupPartitions.allMetrics.keySet == Set(GpuMetric.OP_TIME_NEW))
      assert(groupPartitions.getOpTimeNewMetric.nonEmpty)
      assert(groupPartitions.executeColumnar().partitions.isEmpty)
    }
  }

  test("GpuGroupPartitionsExec drops unsafe batching guarantees when coalescing") {
    val singleBatchChild = GpuRowToColumnarExec(
      LocalTableScanExec(Nil, Nil, None),
      RequireSingleBatch)
    val groupInfo = GpuGroupPartitionsExecInfo(
      UnknownPartitioning(1),
      Seq.empty,
      Seq.empty,
      Seq(Seq(0, 1)),
      joinKeyPositions = None,
      expectedPartitionKeyCount = None,
      reducerNames = None,
      distributePartitions = false,
      enableSortedMerge = false)

    assert(GpuGroupPartitionsExec(singleBatchChild, groupInfo).outputBatching == null)
    assert(GpuGroupPartitionsExec(
      singleBatchChild,
      groupInfo.copy(partitionGroups = Seq(Seq(0)))).outputBatching == RequireSingleBatch)

    val target = TargetSize(1024)
    val targetSizeChild = GpuRowToColumnarExec(
      LocalTableScanExec(Nil, Nil, None),
      target)
    assert(GpuGroupPartitionsExec(targetSizeChild, groupInfo).outputBatching == target)
  }

  test("GpuGroupPartitionsExec canonicalizes captured output expressions") {
    def newPlan(): GpuGroupPartitionsExec = {
      val attr = AttributeReference("id", IntegerType, nullable = false)()
      GpuGroupPartitionsExec(
        LocalTableScanExec(Seq(attr), Nil, None),
        GpuGroupPartitionsExecInfo(
          HashPartitioning(Seq(attr), 2),
          Seq(SortOrder(attr, Ascending)),
          Seq(SortOrder(attr, Ascending)),
          Seq(Seq(0)),
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = false))
    }

    val first = newPlan()
    val second = newPlan()
    assert(first != second)
    assert(first.canonicalized == second.canonicalized)
  }

  test("GpuGroupPartitionsExec summary includes all grouping metadata") {
    val groupPartitions = GpuGroupPartitionsExec(
      LocalTableScanExec(Nil, Nil, None),
      GpuGroupPartitionsExecInfo(
        UnknownPartitioning(1),
        Seq.empty,
        Seq.empty,
        Seq(Seq(0)),
        joinKeyPositions = Some(Seq(0, 2)),
        expectedPartitionKeyCount = Some(3),
        reducerNames = Some(Seq("bucket", "identity")),
        distributePartitions = true,
        enableSortedMerge = false))

    val summary = groupPartitions.simpleString(maxFields = 10)
    assert(summary.contains("JoinKeyPositions: [0, 2]"))
    assert(summary.contains("ExpectedPartitionKeys: 3"))
    assert(summary.contains("Reducers: [bucket, identity]"))
    assert(summary.contains("DistributePartitions: true"))
  }

  test("GpuGroupPartitionsExec configures sorted-merge execution") {
    val attr = AttributeReference("id", IntegerType, nullable = false)()
    val ordering = Seq(SortOrder(attr, Ascending))
    val groupPartitions = GpuGroupPartitionsExec(
      GpuRowToColumnarExec(
        LocalTableScanExec(Seq(attr), Nil, None),
        RequireSingleBatch),
      GpuGroupPartitionsExecInfo(
        HashPartitioning(Seq(attr), 1),
        ordering,
        ordering,
        Seq(Seq(0, 1)),
        joinKeyPositions = None,
        expectedPartitionKeyCount = None,
        reducerNames = None,
        distributePartitions = false,
        enableSortedMerge = true))

    assert(groupPartitions.enableSortedMerge)
    assert(groupPartitions.outputBatching == null)
    assert(!groupPartitions.coalesceAfter)
    assert(groupPartitions.allMetrics.keySet == Set(
      GpuMetric.NUM_OUTPUT_ROWS,
      GpuMetric.NUM_OUTPUT_BATCHES,
      GpuMetric.OP_TIME_LEGACY,
      GpuMetric.OP_TIME_NEW,
      GpuMetric.SORT_TIME))

    val noCoalescing = groupPartitions.copy(
      groupInfo = groupPartitions.groupInfo.copy(partitionGroups = Seq(Seq(0))))
    assert(noCoalescing.outputBatching == RequireSingleBatch)
    assert(noCoalescing.coalesceAfter)
    assert(noCoalescing.allMetrics.keySet == Set(GpuMetric.OP_TIME_NEW))
  }

  test("GpuGroupPartitionsExecMeta propagates child output type conversions") {
    val attr = AttributeReference("id", IntegerType, nullable = false)()
    val convertedAttr = AttributeReference(
      attr.name,
      LongType,
      attr.nullable,
      attr.metadata)(attr.exprId, attr.qualifier)
    val groupPartitions = GroupPartitionsExec(
      LocalTableScanExec(Seq(attr), Nil, None),
      enableSortedMerge = false)
    val originalMeta = new GpuGroupPartitionsExecMeta(
      groupPartitions,
      new RapidsConf(Map.empty[String, String]),
      None,
      new NoRuleDataFromReplacementRule)
    assert(originalMeta.availableRuntimeDataTransition ==
      originalMeta.childPlans.head.availableRuntimeDataTransition)

    val childMeta = mock(classOf[SparkPlanMeta[SparkPlan]])
    when(childMeta.availableRuntimeDataTransition).thenReturn(true)
    when(childMeta.outputAttributes).thenReturn(Seq(convertedAttr))
    val meta = spy(originalMeta)
    doReturn(Seq(childMeta)).when(meta).childPlans

    assert(meta.outputAttributes == Seq(convertedAttr))
  }

  test("GroupPartitionsExec fallback keeps the original CPU subtree") {
    val groupPartitions = GroupPartitionsExec(
      LocalTableScanExec(Nil, Nil, None),
      enableSortedMerge = false)
    val meta = GpuOverrides.wrapAndTagPlan(
      groupPartitions,
      new RapidsConf(Map.empty[String, String]))
      .asInstanceOf[GpuGroupPartitionsExecMeta]

    meta.willNotWorkOnGpu("test-only fallback")
    assert(!meta.canThisBeReplaced)
    assert(meta.convertToCpu().eq(groupPartitions))
  }

  Seq(
    (Ascending, (0L until 8L)),
    (Descending, (0L until 8L).reverse)
  ).foreach { case (direction, expected) =>
    test(s"GpuGroupPartitionsExec sorted merge orders rows $direction") {
      withGpuSparkSession { spark =>
        spark.conf.set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
        val attr = AttributeReference("id", LongType, nullable = false)()
        val ordering = Seq(SortOrder(attr, direction))
        val child = GpuRangeExec(
          start = 0,
          end = 8,
          step = 1,
          numSlices = 2,
          output = Seq(attr),
          targetSizeBytes = 1024)
        val groupPartitions = GpuGroupPartitionsExec(
          child,
          GpuGroupPartitionsExecInfo(
            UnknownPartitioning(1),
            ordering,
            ordering,
            // Reverse the two input partitions so the ascending case is not already ordered.
            Seq(Seq(1, 0)),
            joinKeyPositions = None,
            expectedPartitionKeyCount = None,
            reducerNames = None,
            distributePartitions = false,
            enableSortedMerge = true))

        val actual = GpuColumnarToRowExec(groupPartitions)
          .executeCollect()
          .map(_.getLong(0))
          .toSeq
        assert(actual == expected)
        assert(groupPartitions.metrics(GpuMetric.SORT_TIME).value > 0)
        assert(groupPartitions.allMetrics(GpuMetric.NUM_OUTPUT_ROWS).value == expected.size)
      }
    }
  }

  test("GpuGroupPartitionsExec sorted merge handles padded and single-input groups") {
    withGpuSparkSession { spark =>
      spark.conf.set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
      val attr = AttributeReference("value", IntegerType, nullable = false)()
      val ordering = Seq(SortOrder(attr, Ascending))
      val child = GroupPartitionsTestGpuLeaf(
        Seq(attr),
        Seq(
          Seq[Integer](1, 3),
          Seq[Integer](2, 4),
          Seq[Integer](5, 6)),
        ordering)
      val groupPartitions = GpuGroupPartitionsExec(
        child,
        GpuGroupPartitionsExecInfo(
          UnknownPartitioning(3),
          ordering,
          ordering,
          Seq(Seq.empty, Seq(0, 1), Seq(2)),
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = true))

      val actual = GpuColumnarToRowExec(groupPartitions)
        .execute()
        .mapPartitionsWithIndex { case (index, rows) =>
          Iterator.single(index -> rows.map(_.getInt(0)).toSeq)
        }
        .collect()
        .sortBy(_._1)
        .map(_._2)
        .toSeq
      assert(actual == Seq(Seq.empty, Seq(1, 2, 3, 4), Seq(5, 6)))
      assert(groupPartitions.allMetrics(GpuMetric.NUM_OUTPUT_ROWS).value == 6)
      assert(groupPartitions.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value == 2)
      assert(groupPartitions.allMetrics(GpuMetric.SORT_TIME).value > 0)
    }
  }

  test("GpuGroupPartitionsExec sorted merge emits target-sized batches") {
    withGpuSparkSession { spark =>
      spark.conf.set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, (16 * 1024).toString)
      spark.conf.set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
      val attr = AttributeReference("id", LongType, nullable = false)()
      val ordering = Seq(SortOrder(attr, Ascending))
      val child = GpuRangeExec(
        start = 0,
        end = 10000,
        step = 1,
        numSlices = 4,
        output = Seq(attr),
        targetSizeBytes = 16 * 1024)
      val groupPartitions = GpuGroupPartitionsExec(
        child,
        GpuGroupPartitionsExecInfo(
          UnknownPartitioning(1),
          ordering,
          ordering,
          Seq(Seq(3, 2, 1, 0)),
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = true))

      val actual = GpuColumnarToRowExec(groupPartitions)
        .executeCollect()
        .map(_.getLong(0))
        .toSeq
      assert(actual == (0L until 10000L))
      assert(groupPartitions.allMetrics(GpuMetric.NUM_OUTPUT_BATCHES).value > 1)
    }
  }

  test("GpuGroupPartitionsExec sorted merge honors non-default null ordering") {
    withGpuSparkSession { _ =>
      val attr = AttributeReference("value", IntegerType, nullable = true)()
      val ordering = Seq(SortOrder(attr, Ascending, NullsLast, Seq.empty))
      val child = GroupPartitionsTestGpuLeaf(
        Seq(attr),
        Seq(
          Seq[Integer](1, 3, null),
          Seq[Integer](2, 4, null)),
        ordering)
      val groupPartitions = GpuGroupPartitionsExec(
        child,
        GpuGroupPartitionsExecInfo(
          UnknownPartitioning(1),
          ordering,
          ordering,
          Seq(Seq(0, 1)),
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = true))

      val actual = GpuColumnarToRowExec(groupPartitions)
        .executeCollect()
        .map { row =>
          if (row.isNullAt(0)) None else Some(row.getInt(0))
        }
        .toSeq
      assert(actual == Seq(Some(1), Some(2), Some(3), Some(4), None, None))
    }
  }

  test("GpuGroupPartitionsExec sorted merge retries GPU OOM") {
    withGpuSparkSession { _ =>
      val attr = AttributeReference("value", IntegerType, nullable = false)()
      val ordering = Seq(SortOrder(attr, Ascending))
      val child = GroupPartitionsTestGpuLeaf(
        Seq(attr),
        Seq(
          Seq[Integer](1, 3),
          Seq[Integer](2, 4)),
        ordering,
        injectRetry = true)
      val groupPartitions = GpuGroupPartitionsExec(
        child,
        GpuGroupPartitionsExecInfo(
          UnknownPartitioning(1),
          ordering,
          ordering,
          Seq(Seq(0, 1)),
          joinKeyPositions = None,
          expectedPartitionKeyCount = None,
          reducerNames = None,
          distributePartitions = false,
          enableSortedMerge = true))

      val actual = GpuColumnarToRowExec(groupPartitions)
        .executeCollect()
        .map(_.getInt(0))
        .toSeq
      assert(actual == Seq(1, 2, 3, 4))
    }
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

private case class GroupPartitionsTestGpuLeaf(
    override val output: Seq[Attribute],
    partitionValues: Seq[Seq[Integer]],
    override val outputOrdering: Seq[SortOrder],
    injectRetry: Boolean = false)
    extends ShimLeafExecNode with GpuExec {
  override def outputPartitioning = UnknownPartitioning(partitionValues.size)

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Row execution is not supported")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    sparkContext.parallelize(partitionValues, partitionValues.size).mapPartitions { parts =>
      val values = parts.flatten.toArray
      val gpuColumn = GpuColumnVector.from(ColumnVector.fromBoxedInts(values: _*), IntegerType)
      val batch = closeOnExcept(gpuColumn) { _ =>
        if (injectRetry) {
          RmmSpark.forceRetryOOM(
            RmmSpark.getCurrentThreadId,
            1,
            RmmSpark.OomInjectionType.GPU.ordinal,
            0)
        }
        new ColumnarBatch(Array(gpuColumn), values.length)
      }
      Iterator.single(batch)
    }
  }
}
