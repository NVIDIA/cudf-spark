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

import com.nvidia.spark.rapids.{GpuExec, RapidsConf}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.execution.{
  CoalescedMapperPartitionSpec,
  CoalescedPartitionSpec,
  CoalescedShuffleRead,
  CoalesceShufflePartitionsSuite,
  SparkPlan,
  SQLExecution
}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec,
  ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.execution.{GpuCustomShuffleReaderExec, TrampolineUtil}
import org.apache.spark.sql.rapids.utils.{RapidsSQLTestsBaseTrait, RapidsTestsBaseTrait}
import org.apache.spark.sql.rapids.utils.RapidsTestConstants.RAPIDS_TEST

class RapidsCoalesceShufflePartitionsSuite
  extends CoalesceShufflePartitionsSuite with RapidsTestsBaseTrait {

  override protected def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    super.beforeAll()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }

  override def withSparkSession(
      f: SparkSession => Unit,
      targetPostShuffleInputSize: Int,
      minNumPostShufflePartitions: Option[Int],
      enableIOEncryption: Boolean = false): Unit = {
    withAqeSparkSession(
      targetPostShuffleInputSize, minNumPostShufflePartitions, enableIOEncryption) { spark =>
      ExecutionPlanCaptureCallback.startCapture()
      try {
        f(spark)
        val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        val adaptivePlans = plans.collect {
          case plan: AdaptiveSparkPlanExec if isFinalAdaptivePlan(plan) => plan.executedPlan
        }
        assert(adaptivePlans.exists(_.exists(_.isInstanceOf[GpuExec])),
          s"Did not capture an executed GPU adaptive plan:\n${plans.mkString("\n")}")
        // The enabled inherited cases expect no coalesced reader. Spark checks CPU readers.
        adaptivePlans.foreach { plan =>
          assert(!plan.exists {
            case reader: GpuCustomShuffleReaderExec => isCoalescedGpuRead(reader)
            case _ => false
          }, s"Unexpected coalesced GPU shuffle reader in:\n$plan")
        }
      } finally {
        ExecutionPlanCaptureCallback.endCapture()
      }
    }
  }

  private def withAqeSparkSession(
      targetPostShuffleInputSize: Int,
      minNumPostShufflePartitions: Option[Int] = None,
      enableIOEncryption: Boolean = false)(f: SparkSession => Unit): Unit = {
    // The upstream suite creates a fresh session for each set of AQE parameters.
    val sparkConf =
      RapidsSQLTestsBaseTrait.nativeSparkConf(new SparkConf(false), warehouse)
        .setMaster("local[*]")
        .setAppName("test")
        .set(UI_ENABLED, false)
        .set(IO_ENCRYPTION_ENABLED, enableIOEncryption)
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key,
          targetPostShuffleInputSize.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  private def isCoalescedGpuRead(reader: GpuCustomShuffleReaderExec): Boolean = {
    // Match Spark 3.3 CoalescedShuffleRead semantics, including the empty shuffle sentinel.
    val isLocalRead = reader.isLocalReader ||
      reader.partitionSpecs.exists(_.isInstanceOf[CoalescedMapperPartitionSpec])
    !isLocalRead && !reader.hasSkewedPartition && reader.partitionSpecs.exists {
      case CoalescedPartitionSpec(0, 0, _) => true
      case spec: CoalescedPartitionSpec => spec.endReducerIndex - spec.startReducerIndex > 1
      case _ => false
    }
  }

  private def testRapids(testName: String)(testFun: => Unit)(implicit pos: Position): Unit = {
    test(RAPIDS_TEST + testName)(testFun)
  }

  private def checkCpuAndGpu(spark: SparkSession, expectedRows: Seq[Row])(query: => DataFrame)(
      checkPlan: (SparkPlan, Boolean) => Unit): Unit = {
    val enabled = spark.conf.get(RapidsConf.SQL_ENABLED.key)
    try {
      spark.conf.set(RapidsConf.SQL_ENABLED.key, false)
      val cpuDf = query
      SQLExecution.withSQLConfPropagated(spark) {
        cpuDf.rdd.count()
      }
      val cpuRows = cpuDf.collect().toSeq
      QueryTest.sameRows(expectedRows, cpuRows).foreach(error => fail(error))
      val cpuPlan = finalAdaptivePlan(cpuDf)
      assert(!cpuPlan.exists(_.isInstanceOf[GpuExec]), s"Unexpected GPU operators in:\n$cpuPlan")
      checkPlan(cpuPlan, false)

      spark.conf.set(RapidsConf.SQL_ENABLED.key, true)
      val gpuDf = query
      assert(gpuDf.schema == cpuDf.schema)
      QueryTest.checkAnswer(gpuDf, cpuRows)
      checkPlan(finalAdaptivePlan(gpuDf), true)
    } finally {
      spark.conf.set(RapidsConf.SQL_ENABLED.key, enabled)
    }
  }

  private def finalAdaptivePlan(df: DataFrame): SparkPlan = {
    df.queryExecution.executedPlan match {
      case plan: AdaptiveSparkPlanExec if isFinalAdaptivePlan(plan) => plan.executedPlan
      case plan => fail(s"Expected a final adaptive plan:\n$plan")
    }
  }

  private def isFinalAdaptivePlan(plan: AdaptiveSparkPlanExec): Boolean = {
    // Spark 3.3 exposes its private isFinalPlan flag in the plan's first line.
    plan.simpleString(1).startsWith("AdaptiveSparkPlan isFinalPlan=true")
  }

  private def coalescedReaders(plan: SparkPlan): Seq[SparkPlan] = {
    plan.collect {
      case reader @ CoalescedShuffleRead() => reader
      case reader: GpuCustomShuffleReaderExec if isCoalescedGpuRead(reader) => reader
    }
  }

  private def checkReaders(
      plan: SparkPlan,
      expectedCount: Int,
      gpu: Boolean,
      expectedPartitions: Option[Int] = None): Unit = {
    val readers = coalescedReaders(plan)
    assert(readers.length == expectedCount,
      s"Expected $expectedCount coalesced shuffle readers, got ${readers.length}:\n$plan")
    if (gpu) {
      assert(readers.forall(_.isInstanceOf[GpuCustomShuffleReaderExec]),
        s"Expected GPU coalesced shuffle readers:\n$plan")
    }
    expectedPartitions.foreach { count =>
      readers.foreach { reader =>
        assert(reader.outputPartitioning.numPartitions == count,
          s"Expected $count shuffle output partitions:\n$reader")
      }
    }
  }

  private def keyedRange(spark: SparkSession, side: Int): DataFrame = {
    spark.range(0, 1000, 1, numInputPartitions)
      .selectExpr(s"id % 500 as key$side", s"id as value$side")
  }

  // Adapt Spark 3.3's coalescing cases to verify both CPU and GPU shuffle readers.
  testRapids("determining the number of reducers: join operator") {
    withAqeSparkSession(16384) { spark =>
      val expectedRows = (0L until 1000L).flatMap(id => Seq.fill(2)(Row(id % 500L, id)))
      checkCpuAndGpu(spark, expectedRows) {
        val left = keyedRange(spark, 1)
        val right = keyedRange(spark, 2)
        left.join(right, col("key1") === col("key2")).select(col("key1"), col("value2"))
      } { (plan, gpu) =>
        checkReaders(plan, 2, gpu, Some(2))
      }
    }
  }

  testRapids("determining the number of reducers: complex query 1") {
    withAqeSparkSession(16384) { spark =>
      val expectedRows = (0L until 500L).map(id => Row(id, 2L))
      checkCpuAndGpu(spark, expectedRows) {
        val left = keyedRange(spark, 1).groupBy("key1").count().toDF("key1", "cnt1")
        val right = keyedRange(spark, 2).groupBy("key2").count().toDF("key2", "cnt2")
        left.join(right, col("key1") === col("key2")).select(col("key1"), col("cnt2"))
      } { (plan, gpu) =>
        checkReaders(plan, 2, gpu, Some(2))
      }
    }
  }

  testRapids("determining the number of reducers: complex query 2") {
    withAqeSparkSession(12000) { spark =>
      val expectedRows = (0L until 1000L).map(id => Row(id % 500L, 2L, id))
      checkCpuAndGpu(spark, expectedRows) {
        val left = keyedRange(spark, 1).groupBy("key1").count().toDF("key1", "cnt1")
        val right = keyedRange(spark, 2)
        left.join(right, col("key1") === col("key2"))
          .select(col("key1"), col("cnt1"), col("value2"))
      } { (plan, gpu) =>
        checkReaders(plan, 2, gpu, Some(3))
      }
    }
  }

  private def checkStageReuse(stages: Seq[QueryStageExec], expectedReuses: Int): Unit = {
    val reused = stages.collect {
      case ShuffleQueryStageExec(_, exchange: ReusedExchangeExec, _) => exchange
    }
    val originals = stages.map(_.plan).filterNot(_.isInstanceOf[ReusedExchangeExec])
    assert(reused.length == expectedReuses)
    assert(originals.length == 1)
    assert(reused.forall(_.child.id == originals.head.id),
      "The reused stages should point to the same original exchange")
  }

  testRapids("SPARK-24705 adaptive query execution works correctly when exchange reuse enabled") {
    withAqeSparkSession(400) { spark =>
      spark.conf.set(SQLConf.EXCHANGE_REUSE_ENABLED.key, true)

      checkCpuAndGpu(spark, (0L to 5L).map(i => Row(i, i, i, i))) {
        val source = spark.range(0, 6, 1).selectExpr("id AS key", "id AS value")
        source.join(source, "key").join(source, "key")
      } { (plan, gpu) =>
        checkReaders(plan, 3, gpu)
        checkStageReuse(plan.collect { case stage: ShuffleQueryStageExec => stage }, 2)
      }

      val expectedRows = (0L to 5L).flatMap(i => Seq(Row(i + 1, i), Row(i + 2, i)))
      checkCpuAndGpu(spark, expectedRows) {
        val source = spark.range(0, 6, 1).selectExpr("id AS key", "id AS value")
        val grouped = source.groupBy("key").agg(max("value").as("value"))
        grouped.groupBy(col("key") + 1).max("value")
          .union(grouped.groupBy(col("key") + 2).max("value"))
      } { (plan, gpu) =>
        checkReaders(plan, 2, gpu)
        val parentStages = plan.collect { case stage: QueryStageExec => stage }
        assert(parentStages.length == 2)
        val leafStages = parentStages.flatMap { parent =>
          checkReaders(parent.plan, 1, gpu)
          val children = parent.plan.collect { case stage: QueryStageExec => stage }
          assert(children.length == 1)
          children
        }
        assert(leafStages.length == 2)
        checkStageReuse(leafStages, 1)
      }
    }
  }

  testRapids("Union two datasets with different pre-shuffle partition number") {
    withAqeSparkSession(100) { spark =>
      checkCpuAndGpu(spark, (0L to 3L).map(Row(_))) {
        val joined = spark.range(3).join(spark.range(3), "id").toDF()
        val summed = spark.range(3).groupBy().sum()
        joined.union(summed)
      } { (plan, gpu) =>
        checkReaders(plan, 2, gpu)
      }
    }
  }

  testRapids("SPARK-34790: enable IO encryption in AQE partition coalescing") {
    Seq(true, false).foreach { encryption =>
      withAqeSparkSession(Int.MaxValue, enableIOEncryption = encryption) { spark =>
        checkCpuAndGpu(spark, (0L until 100L).map(Row(_))) {
          val data = spark.range(0, 100, 1, numInputPartitions)
          data.repartition(data.col("id")).toDF()
        } { (plan, gpu) =>
          val readers = coalescedReaders(plan)
          assert(readers.nonEmpty, s"Expected a coalesced shuffle reader:\n$plan")
          if (gpu) {
            assert(readers.exists(_.isInstanceOf[GpuCustomShuffleReaderExec]),
              s"Expected a GPU coalesced shuffle reader:\n$plan")
          }
        }
      }
    }
  }
}
