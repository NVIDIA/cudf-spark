/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.io.File

import com.nvidia.spark.rapids.shims.{OperatorsUtilShims, SparkShimImpl}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, LocalTableScanExec,
  PartialReducerPartitionSpec, ReusedSubqueryExec, SortExec, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.{InSubqueryExec => SparkInSubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ENSURE_REQUIREMENTS,
  Exchange, REPARTITION_BY_NUM, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, GpuFileSourceScanExec}
import org.apache.spark.sql.rapids.execution.{GpuCustomShuffleReaderExec, GpuJoinExec, GpuShuffleExchangeExecBase}
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataTypes, DecimalType, IntegerType, StringType, StructField, StructType}

class AdaptiveQueryExecSuite
    extends SparkQueryCompareTestSuite
    with AdaptiveSparkPlanHelper
    with FunSuiteWithTempDir
    with Logging {

  private def runAdaptiveAndVerifyResult(
      spark: SparkSession, query: String): (SparkPlan, SparkPlan) = {

    val dfAdaptive = spark.sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))

    dfAdaptive.collect()
    val planAfter = dfAdaptive.queryExecution.executedPlan
    // isFinalPlan is a private field so we have to use toString to access it
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    // With AQE, the query is broken down into query stages based on exchange boundaries, so the
    // final query that is executed depends on the results from its child query stages. There
    // cannot be any exchange nodes left when the final query is executed because they will
    // have already been replaced with QueryStageExecs.
    val exchanges = adaptivePlan.collect {
      case e: Exchange => e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findTopLevelSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] = {
    collect(plan) {
      case j: SortMergeJoinExec => j
    }
  }

  private def findTopLevelGpuBroadcastHashJoin(plan: SparkPlan): Seq[GpuExec] = {
    collect(plan) {
      case j: GpuExec with BroadcastExchangeLike => j
    }
  }

  private def findTopLevelGpuShuffleHashJoin(plan: SparkPlan): Seq[GpuJoinExec] = {
    collect(plan) {
      case j: GpuShuffledHashJoinExec => j
      case j: GpuShuffledSymmetricHashJoinExec => j
      case j: GpuShuffledAsymmetricHashJoinExec => j
    }
  }

  private def findTopLevelSort(plan: SparkPlan): Seq[SortExec] = {
    collect(plan) {
      case s: SortExec => s
    }
  }

  private def findReusedExchange(plan: SparkPlan): Seq[ReusedExchangeExec] = {
    collectWithSubqueries(plan)(SparkShimImpl.reusedExchangeExecPfn)
  }

  private case class TestLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("TestLeafExec should not be executed")
  }

  test("GPU planning rules use their captured session when no session is active") {
    val conf = new SparkConf().set("spark.sql.adaptive.enabled", "true")
    withGpuSparkSession({ spark =>
      val scanPath = new File(TEST_FILES_ROOT, "captured-session-scan").getCanonicalPath
      spark.conf.set(RapidsConf.SQL_ENABLED.key, "false")
      spark.range(10).write.parquet(scanPath)
      spark.conf.set(RapidsConf.SQL_ENABLED.key, "true")

      val cpuPlan = spark.read.parquet(scanPath).repartition(2).queryExecution.sparkPlan
      val previousSession = org.apache.spark.sql.SparkSession.getActiveSession
      org.apache.spark.sql.SparkSession.clearActiveSession()
      try {
        assert(org.apache.spark.sql.SparkSession.getActiveSession.isEmpty)

        val expectedFailure = intercept[RuntimeException] {
          GpuOverrideUtil.withActiveSession(spark) {
            assert(org.apache.spark.sql.SparkSession.getActiveSession.contains(spark))
            throw new RuntimeException("expected test failure")
          }
        }
        assert(expectedFailure.getMessage == "expected test failure")
        assert(org.apache.spark.sql.SparkSession.getActiveSession.isEmpty)

        // Direct, unregistered conversion remains session-less, but scan construction must not
        // eagerly dereference that missing session.
        spark.conf.set(RapidsConf.EXPLAIN.key, "NONE")
        spark.conf.set(RapidsConf.TEST_CONF.key, "false")
        spark.conf.set(RapidsConf.TAG_LORE_ID_ENABLED.key, "false")
        val unscopedPlan = GpuOverrides().apply(cpuPlan)
        val unscopedScan = unscopedPlan.find(_.isInstanceOf[GpuFileSourceScanExec]).get
        assert(SparkSessionUtils.sessionFromPlan(unscopedScan) == null)

        spark.conf.set(RapidsConf.TEST_CONF.key, "true")
        spark.conf.set(RapidsConf.TAG_LORE_ID_ENABLED.key, "true")
        ShimLoader.newGpuQueryStagePrepOverrides(spark).apply(cpuPlan)
        assert(org.apache.spark.sql.SparkSession.getActiveSession.isEmpty)

        val columnarRules = ShimLoader.newColumnarOverrideRules(spark)
        val gpuPlan = columnarRules.preColumnarTransitions.apply(cpuPlan)
        val gpuScan = gpuPlan.find(_.isInstanceOf[GpuFileSourceScanExec]).get
        assert(SparkSessionUtils.sessionFromPlan(gpuScan) eq spark)
        assert(org.apache.spark.sql.SparkSession.getActiveSession.isEmpty)

        val transitionedPlan = columnarRules.postColumnarTransitions.apply(gpuPlan)
        val transitionedScan = transitionedPlan.find(_.isInstanceOf[GpuFileSourceScanExec]).get
        assert(SparkSessionUtils.sessionFromPlan(transitionedScan) eq spark)
        assert(org.apache.spark.sql.SparkSession.getActiveSession.isEmpty)
      } finally {
        previousSession.foreach { session =>
          org.apache.spark.sql.SparkSession.setActiveSession(session)
        }
      }
    }, conf)
  }

  test("AQE query stage exchange tagging descends into subquery plans") {
    val attr = AttributeReference("a", IntegerType)()
    val scan = TestLeafExec(Seq(attr))
    val mainExchange = ShuffleExchangeExec(SinglePartition, scan, ENSURE_REQUIREMENTS)
    val subqueryExchange = ShuffleExchangeExec(SinglePartition, scan, ENSURE_REQUIREMENTS)
    val subquery = SubqueryExec("merged runtime filters", subqueryExchange)
    val reusedSubquery = ReusedSubqueryExec(subquery)
    val inSubquery = SparkInSubqueryExec(attr, reusedSubquery, NamedExpression.newExprId)
    val plan = FilterExec(inSubquery, mainExchange)

    GpuTransitionOverrides.tagAqeQueryStageExchanges(plan)

    assert(mainExchange.getTagValue(GpuTransitionOverrides.aqeQueryStageExchange).contains(true))
    assert(subqueryExchange.getTagValue(GpuTransitionOverrides.aqeQueryStageExchange)
        .contains(true))
  }

  test("AQE can expose untagged parentless ENSURE_REQUIREMENTS shuffle") {
    val attr = AttributeReference("a", IntegerType)()
    val scan = TestLeafExec(Seq(attr))
    val ensureRequirementsExchange = ShuffleExchangeExec(SinglePartition, scan, ENSURE_REQUIREMENTS)
    val repartitionExchange = ShuffleExchangeExec(SinglePartition, scan, REPARTITION_BY_NUM)

    assert(ensureRequirementsExchange.getTagValue(
      GpuTransitionOverrides.aqeQueryStageExchange).isEmpty)
    assert(repartitionExchange.getTagValue(GpuTransitionOverrides.aqeQueryStageExchange).isEmpty)
    assert(GpuTransitionOverrides.canExposeExchangeForAqeStage(
      ensureRequirementsExchange, None))
    assert(!GpuTransitionOverrides.canExposeExchangeForAqeStage(
      repartitionExchange, None))
    assert(!GpuTransitionOverrides.canExposeExchangeForAqeStage(
      ensureRequirementsExchange, Some(scan)))
  }

  test("get row counts from executed shuffle query stages") {
    skewJoinTest { spark =>
      val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
      val shuffleExchanges =
          OperatorsUtilShims.findOperators(innerAdaptivePlan, _.isInstanceOf[ShuffleQueryStageExec])
              .map(_.asInstanceOf[ShuffleQueryStageExec])
      assert(shuffleExchanges.length === 2)
      val stats = shuffleExchanges.map(_.getRuntimeStatistics)
      assert(stats.forall(_.rowCount.contains(1000)))
    }
  }

  test("skewed inner join optimization") {
    skewJoinTest { spark =>
      val (_, innerAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 join skewData2 ON key1 = key2")
      val innerSmj = findTopLevelGpuShuffleHashJoin(innerAdaptivePlan)
      // Spark changed how skewed joins work and now the numbers are different
      // depending on the version being used
      if (cmpSparkVersion(3,1,1) >= 0) {
        checkSkewJoin(innerSmj, 2, 1)
      } else {
        checkSkewJoin(innerSmj, 1, 1)
      }
    }
  }

  test("skewed left outer join optimization") {
    skewJoinTest { spark =>
      val (_, leftAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 left outer join skewData2 ON key1 = key2")
      val leftSmj = findTopLevelGpuShuffleHashJoin(leftAdaptivePlan)
      // Spark changed how skewed joins work and now the numbers are different
      // depending on the version being used
      if (cmpSparkVersion(3,1,1) >= 0) {
        checkSkewJoin(leftSmj, 2, 0)
      } else {
        checkSkewJoin(leftSmj, 1, 0)
      }
    }
  }

  test("skewed right outer join optimization") {
    skewJoinTest { spark =>
      val (_, rightAdaptivePlan) = runAdaptiveAndVerifyResult(
        spark,
        "SELECT * FROM skewData1 right outer join skewData2 ON key1 = key2")
      val rightSmj = findTopLevelGpuShuffleHashJoin(rightAdaptivePlan)
      checkSkewJoin(rightSmj, 0, 1)
    }
  }

  test("Join partitioned tables DPP fallback") {
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1") // force shuffle exchange

    withGpuSparkSession(spark => {
      import spark.implicits._

      val path = new File(TEST_FILES_ROOT, "test.parquet").getAbsolutePath
      (0 until 100)
          .map(i => (i,i*5))
          .toDF("a", "b")
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("testData")

      spark.sql("DROP TABLE IF EXISTS t1").collect()
      spark.sql("DROP TABLE IF EXISTS t2").collect()

      spark.sql("CREATE TABLE t1 (a INT, b INT) USING parquet").collect()
      spark.sql("CREATE TABLE t2 (a INT, b INT) USING parquet PARTITIONED BY (a)").collect()

      spark.sql("INSERT INTO TABLE t1 SELECT a, b FROM testData").collect()
      spark.sql("INSERT INTO TABLE t2 SELECT a, b FROM testData").collect()

      // This test checks that inputs to the SHJ are coalesced. We need to check both sides
      // if we are not optimizing the build-side coalescing logic, and only the stream side
      // if the optimization is enabled (default).
      // See `RapidsConf.SHUFFLED_HASH_JOIN_OPTIMIZE_SHUFFLE` for more information.
      Seq(true, false).foreach { shouldOptimizeHashJoinShuffle =>
        spark.conf.set(
          RapidsConf.SHUFFLED_HASH_JOIN_OPTIMIZE_SHUFFLE.key,
          shouldOptimizeHashJoinShuffle.toString)
        val df = spark.sql(
          "SELECT t1.a, t2.b " +
            "FROM t1 " +
            "JOIN t2 " +
            "ON t1.a = t2.a " +
            "WHERE t2.a = 5" // filter on partition key to force dynamic partition pruning
        )
        df.collect()

        val isAdaptiveQuery = df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec]
        if (cmpSparkVersion(3, 2, 0) < 0) {
          // assert that DPP did cause this to run as a non-AQE plan prior to Spark 3.2.0
          assert(!isAdaptiveQuery)
        } else {
          // In 3.2.0 AQE works with DPP
          assert(isAdaptiveQuery)
        }

        val shj = TestUtils.findOperator(df.queryExecution.executedPlan,
          _.isInstanceOf[GpuJoinExec]).get
        assert(shj.children.length == 2)
        shj match {
          case j: GpuShuffledSymmetricHashJoinExec =>
            // assert that both children are NOT coalesced, as join will directly handle shuffle
            assert(j.children.forall {
              case GpuShuffleCoalesceExec(_, _) => false
              case GpuCoalesceBatches(GpuShuffleCoalesceExec(_, _), _) => false
              case _ => true
            })
          case j: GpuShuffledHashJoinExec =>
            val childrenToCheck = if (shouldOptimizeHashJoinShuffle) {
              // assert that the stream side of SHJ is coalesced
              j.buildSide match {
                case GpuBuildLeft => Seq(j.right)
                case GpuBuildRight => Seq(j.left)
              }
            } else {
              // assert that both the build and stream side of SHJ are coalesced
              // if we are not optimizing the build side shuffle
              j.children
            }
            assert(childrenToCheck.forall {
              case GpuShuffleCoalesceExec(_, _) => true
              case GpuCoalesceBatches(GpuShuffleCoalesceExec(_, _), _) => true
              case _ => false
            })
          case j => throw new IllegalStateException(s"Unexpected join: $j")
        }
      }

    }, conf)
  }

  test("Plugin should translate child plan of GPU DataWritingCommandExec to GPU") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")

    withGpuSparkSession(spark => {
      import spark.implicits._

      // read from a parquet file so we can test reading on GPU
      val path = new File(TEST_FILES_ROOT, "DataWritingCommandExecGPU.parquet").getAbsolutePath
      (0 until 100).toDF("a")
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("testData")

      spark.sql("CREATE TABLE IF NOT EXISTS DataWritingCommandExecGPU (a INT) USING parquet")
          .collect()

      val df = spark.sql("INSERT INTO TABLE DataWritingCommandExecGPU SELECT * FROM testData")
      df.collect()

      // write should be on GPU
      val writeCommand = TestUtils.findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuDataWritingCommandExec])
      assert(writeCommand.isDefined)

      if (cmpSparkVersion(3,4,0) < 0) {
        // the read should be an adaptive plan prior to Spark 3.4.0
        val adaptiveSparkPlanExec = TestUtils.findOperator(writeCommand.get,
          _.isInstanceOf[AdaptiveSparkPlanExec])
          .get.asInstanceOf[AdaptiveSparkPlanExec]

        // assert that at least part of the adaptive plan ran on GPU
        assert(TestUtils.findOperator(adaptiveSparkPlanExec, _.isInstanceOf[GpuExec]).isDefined)
      } else {
        assert(TestUtils.findOperator(writeCommand.get,
          _.isInstanceOf[GpuFileSourceScanExec]).isDefined)
      }
    }, conf)
  }

  test("Plugin should translate child plan of CPU DataWritingCommandExec to GPU") {

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
      // force DataWritingCommandExec onto CPU for this test because we want to verify that
      // the read will still happen on GPU with a CPU write
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "DataWritingCommandExec,WriteFilesExec")
      .set("spark.rapids.sql.exec.DataWritingCommandExec", "false")

      withGpuSparkSession(spark => {
        import spark.implicits._

        // read from a parquet file so we can test reading on GPU
        val path = new File(TEST_FILES_ROOT, "DataWritingCommandExecCPU.parquet").getAbsolutePath
        (0 until 100).toDF("a")
            .write
            .mode(SaveMode.Overwrite)
            .parquet(path)

        spark.read.parquet(path).createOrReplaceTempView("testData")

        spark.sql("CREATE TABLE IF NOT EXISTS DataWritingCommandExecCPU (a INT) USING parquet")
            .collect()

        val df = spark.sql("INSERT INTO TABLE DataWritingCommandExecCPU SELECT * FROM testData")
        df.collect()

        // write should be on CPU
        val writeCommand = TestUtils.findOperator(df.queryExecution.executedPlan,
          _.isInstanceOf[DataWritingCommandExec])
        assert(writeCommand.isDefined)

        if (cmpSparkVersion(3, 4, 0) < 0) {
          // the read should be an adaptive plan prior to Spark 3.4.0
          val adaptiveSparkPlanExec = TestUtils.findOperator(writeCommand.get,
            _.isInstanceOf[AdaptiveSparkPlanExec])
            .get.asInstanceOf[AdaptiveSparkPlanExec]

          // even though the write couldn't run on GPU, the read should have done
          assert(TestUtils.findOperator(adaptiveSparkPlanExec.executedPlan,
            _.isInstanceOf[GpuExec]).isDefined)
        } else {
          assert(TestUtils.findOperator(writeCommand.get,
            _.isInstanceOf[GpuFileSourceScanExec]).isDefined)
        }

    }, conf)
  }

  test("Avoid transitions to row when writing to Parquet") {
    logError("Avoid transitions to row when writing to Parquet")
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
        .set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      import spark.implicits._

      // read from a parquet file so we can test reading on GPU
      val path = new File(TEST_FILES_ROOT, "AvoidTransitionInput.parquet").getAbsolutePath
      (0 until 100).toDF("a")
          .repartition(2          )
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)

      val df = spark.read.parquet(path)

      ExecutionPlanCaptureCallback.startCapture()

      val outputPath = new File(TEST_FILES_ROOT, "AvoidTransitionOutput.parquet").getAbsolutePath
      df.write.mode(SaveMode.Overwrite).parquet(outputPath)

      val capturedPlans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
      assert(capturedPlans.length == 1,
        s"Expected to capture exactly one plan: ${capturedPlans.mkString("\n")}")
      val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(capturedPlans.head)

      // write should be on GPU
      val writeCommand = TestUtils.findOperator(executedPlan,
        _.isInstanceOf[GpuDataWritingCommandExec])
      assert(writeCommand.isDefined)

      if (cmpSparkVersion(3, 4, 0) < 0) {
        // the read should be an adaptive plan prior to Spark 3.4.0
        val adaptiveSparkPlanExec = TestUtils.findOperator(writeCommand.get,
          _.isInstanceOf[AdaptiveSparkPlanExec])
          .get.asInstanceOf[AdaptiveSparkPlanExec]

        if (SparkShimImpl.supportsColumnarAdaptivePlans) {
          // we avoid the transition entirely with Spark 3.2+ due to the changes in SPARK-35881 to
          // support columnar adaptive plans
          assert(adaptiveSparkPlanExec
            .executedPlan
            .isInstanceOf[GpuFileSourceScanExec])
        } else {
          val transition = adaptiveSparkPlanExec
            .executedPlan
            .asInstanceOf[GpuColumnarToRowExec]

          // although the plan contains a GpuColumnarToRowExec, we bypass it in
          // AvoidAdaptiveTransitionToRow so the metrics should reflect that
          assert(transition.metrics("numOutputRows").value === 0)
        }
      } else {
        // there is no transition since Spark 3.4.0
        val transition = TestUtils.findOperator(writeCommand.get,
          _.isInstanceOf[GpuColumnarToRowExec])
        assert(transition.isEmpty)
      }
    }, conf)
  }

  test("Keep transition to row when collecting results") {
    logError("Keep transition to row when collecting results")
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
        .set(RapidsConf.METRICS_LEVEL.key, "DEBUG")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      import spark.implicits._

      // read from a parquet file so we can test reading on GPU
      val path = new File(TEST_FILES_ROOT, "AvoidTransitionInput.parquet").getAbsolutePath
      (0 until 100).toDF("a")
          .repartition(2          )
          .write
          .mode(SaveMode.Overwrite)
          .parquet(path)

      val df = spark.read.parquet(path)

      ExecutionPlanCaptureCallback.startCapture()

      df.collect()

      val capturedPlans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
      assert(capturedPlans.length == 1,
        s"Expected to capture exactly one plan: ${capturedPlans.mkString("\n")}")
      val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(capturedPlans.head)

      // find the first occurrence (last node) of GpuColumnarToRowExec in the plan 
      // and assert the metrics
      val transition = OperatorsUtilShims.findOperators(
        executedPlan, _.isInstanceOf[GpuColumnarToRowExec]).head.asInstanceOf[GpuColumnarToRowExec]

      // because we are calling collect, AvoidAdaptiveTransitionToRow will not bypass
      // GpuColumnarToRowExec so we should see accurate metrics
      assert(transition.metrics("numOutputRows").value === 100)

    }, conf)
  }

  test("Keep transition to row for final AQE repartition exchange") {
    logError("Keep transition to row for final AQE repartition exchange")

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
        .set(SQLConf.SORT_BEFORE_REPARTITION.key, "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      import spark.implicits._

      val df = Seq("a", "b", "c")
          .toDF("s")
          .selectExpr("s", "1 AS x")
          .repartition(10)

      val rows = df.collect()
      assert(rows.map(_.getAs[String]("s")).toSet === Set("a", "b", "c"))

      val finalPlan = df.queryExecution.executedPlan match {
        case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
        case other => other
      }
      assert(!finalPlan.isInstanceOf[GpuShuffleExchangeExecBase],
        s"Final plan should not execute a bare GPU exchange as rows: $finalPlan")
    }, conf)
  }

  // repro case for https://github.com/NVIDIA/spark-rapids/issues/4351
  test("Write parquet from AQE shuffle with limit") {
    logError("Write parquet from AQE shuffle with limit")

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

    withGpuSparkSession(spark => {
      import spark.implicits._

      val path = new File(TEST_FILES_ROOT, "AvoidTransitionInput.parquet").getAbsolutePath
      (0 until 100).toDF("a")
        .write
        .mode(SaveMode.Overwrite)
        .parquet(path)

      val outputPath = new File(TEST_FILES_ROOT, "AvoidTransitionOutput.parquet").getAbsolutePath
      spark.read.parquet(path)
        .limit(100)
        .write.mode(SaveMode.Overwrite)
        .parquet(outputPath)
    }, conf)
  }


  test("Exchange reuse") {
    logError("Exchange reuse")

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,HashPartitioning")

    withGpuSparkSession(spark => {
      setupTestData(spark)

      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(spark,
        "SELECT value FROM testData join testData2 ON key = a " +
            "join (SELECT value v from testData join testData3 ON key = a) on value = v")

      // initial plan should have three SMJs
      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 3)

      // executed GPU plan replaces SMJ with SHJ
      val shj = findTopLevelGpuShuffleHashJoin(adaptivePlan)
      assert(shj.size == 3)

      // one of the GPU exchanges should have been re-used
      val ex = findReusedExchange(adaptivePlan)
      assert(ex.size == 1)
      assert(ex.head.child.isInstanceOf[ShuffleExchangeLike])
      assert(ex.head.child.isInstanceOf[GpuExec])

    }, conf)
  }

  test("Change merge join to broadcast join without local shuffle reader") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/11552")
    logError("Change merge join to broadcast join without local shuffle reader")

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "50")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "50")
      // disable DemoteBroadcastHashJoin rule from removing BHJ due to empty partitions
      .set(SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key, "0")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,HashPartitioning")

    withGpuSparkSession(spark => {
      setupTestData(spark)
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(spark,
        """
          |SELECT * FROM lowerCaseData t1 join testData2 t2
          |ON t1.n = t2.a join testData3 t3 on t2.a = t3.a
          |where t1.l = 1
        """.stripMargin)

      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 2)
      val bhj = findTopLevelGpuBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      // There is still a SMJ, and its two shuffles can't apply local reader.
      checkNumLocalShuffleReaders(adaptivePlan, 2)
    }, conf)
  }

  test("Verify the reader is LocalShuffleReaderExec") {
    logError("Verify the reader is LocalShuffleReaderExec")

    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "400")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "50")
      // disable DemoteBroadcastHashJoin rule from removing BHJ due to empty partitions
      .set(SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key, "0")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "DataWritingCommandExec,ShuffleExchangeExec,HashPartitioning")

    withGpuSparkSession(spark => {
      setupTestData(spark)

      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(spark, "SELECT * FROM testData join " +
        "testData2 ON key = a where value = '1'")

      val smj = findTopLevelSortMergeJoin(plan)
      assert(smj.size == 1)

      val bhj = findTopLevelGpuBroadcastHashJoin(adaptivePlan)
      assert(bhj.size == 1)
      val localReaders = collect(adaptivePlan) {
        case reader: GpuCustomShuffleReaderExec if reader.isLocalReader => reader
      }
      // Verify local readers length
      assert(localReaders.length == 2)

      // test with non-common data types
      uncommonTypeTestData(spark)
      val (plan2, adaptivePlan2) = runAdaptiveAndVerifyResult(spark,
        "SELECT * FROM testData join uncommonTypeTestData ON key = a where value = '1'")

      val smj2 = findTopLevelSortMergeJoin(plan2)
      assert(smj2.size == 1)

      val localReaders2 = collect(adaptivePlan2) {
        case reader: GpuCustomShuffleReaderExec if reader.isLocalReader => reader
      }
      // Verify local readers length
      assert(localReaders2.length == 2)
    }, conf)
  }

  test("Avoid splitting CustomShuffleReader and ShuffleQueryStageExec pairs") {
    logError("Avoid splitting CustomShuffleReader and ShuffleQueryStageExec pairs")

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "1")
        .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
        .set("spark.rapids.sql.exec.SortExec", "false")
        .set("spark.rapids.sql.exec.SortMergeJoinExec", "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "DataWritingCommandExec,ShuffleExchangeExec,HashPartitioning,SortExec,SortMergeJoinExec")

    withGpuSparkSession(spark => {
      val data = Seq(
        Row("Adam",Map("hair"->"black","eye"->"black"), Row("address" , Map("state"->"CA"))),
        Row("Bob",Map("hair"->"red","eye"->"red"),Row("address", Map("state"->"GA"))),
        Row("Cathy",Map("hair"->"blue","eye"->"blue"),Row("address",Map("state"->"NC")))
      )
      val mapType  = DataTypes.createMapType(StringType,StringType)
      val schema = new StructType().add("name",StringType).add("properties", mapType)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
      df.createOrReplaceTempView("df1")
      df.createOrReplaceTempView("df2")

      runAdaptiveAndVerifyResult(spark,
        "SELECT df1.properties from df1, df2 where df1.name=df2.name")
    }, conf)
  }

  test("SPARK-35585: Support propagate empty relation through project/filter") {
    logError("SPARK-35585: Support propagate empty relation through project/filter")
    assumeSpark320orLater
    // There is difference in Spark plans between 3.2.x-3.5.x and 4.0+
    // Spark 4.0+ has EmptyRelationExec, but 3.2.x-3.5.x has LocalTableScanExec as the
    // root node of the plan.
    // Issue to support EmptyRelationExec:https://github.com/NVIDIA/spark-rapids/issues/11100
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "DataWritingCommandExec,ShuffleExchangeExec,HashPartitioning,EmptyRelationExec")

    withGpuSparkSession(spark => {
      testData(spark)

      val (plan1, adaptivePlan1) = runAdaptiveAndVerifyResult(spark,
        "SELECT key FROM testData WHERE key = 0 ORDER BY key, value")
      assert(findTopLevelSort(plan1).size == 1)
      // Check for either LocalTableScanExec (pre-4.0) or EmptyRelationExec (4.0+)
      val strippedPlan1 = stripAQEPlan(adaptivePlan1)
      assert(strippedPlan1.isInstanceOf[LocalTableScanExec] ||
            strippedPlan1.getClass.getSimpleName.equals("EmptyRelationExec"),
            s"Expected empty relation plan, but got: ${strippedPlan1}")

      val (plan2, adaptivePlan2) = runAdaptiveAndVerifyResult(spark,
        "SELECT key FROM (SELECT * FROM testData WHERE value = 'no_match' ORDER BY key)" +
            " WHERE key > rand()")
      assert(findTopLevelSort(plan2).size == 1)
      // Check for either LocalTableScanExec (pre-4.0) or EmptyRelationExec (4.0+)
      val strippedPlan2 = stripAQEPlan(adaptivePlan2)
      assert(strippedPlan2.isInstanceOf[LocalTableScanExec] ||
            strippedPlan2.getClass.getSimpleName.equals("EmptyRelationExec"),
            s"Expected empty relation plan, but got: ${strippedPlan2}")
    }, conf)
  }

  private def checkNumLocalShuffleReaders(
    plan: SparkPlan,
    numShufflesWithoutLocalReader: Int): Int = {
    val numShuffles = collect(plan) {
      case s: ShuffleQueryStageExec => s
    }.length

    val numLocalReaders = collect(plan) {
      case reader: GpuCustomShuffleReaderExec if reader.isLocalReader => reader
    }
    numLocalReaders.foreach { r =>
      val rdd = r.executeColumnar()
      val parts = rdd.partitions
      assert(parts.forall(rdd.preferredLocations(_).nonEmpty))
    }
    assert(numShuffles == (numLocalReaders.length + numShufflesWithoutLocalReader))
    numLocalReaders.length
  }

  def skewJoinTest(fun: SparkSession => Unit): Unit = {
    val conf = new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "100")
      .set(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key, "800")
      .set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "800")

    withGpuSparkSession(spark => {

      spark
          .range(0, 1000, 1, 10)
          .select(
            when(col("id") < 250, 249)
                .when(col("id") >= 750, 1000)
                .otherwise(col("id")).as("key1"),
            col("id") as "value1")
          .createOrReplaceTempView("skewData1")

      // note that the skew amount here has been modified compared to the original Spark test to
      // compensate for the effects of compression when running on GPU which can change the
      // partition sizes substantially
      spark
          .range(0, 1000, 1, 10)
          .select(
            when(col("id") < 500, 249)
                .otherwise(col("id")).as("key2"),
            col("id") as "value2")
          .createOrReplaceTempView("skewData2")

      // invoke the test function
      fun(spark)

    }, conf)
  }

  def checkSkewJoin(
      joins: Seq[GpuJoinExec],
      leftSkewNum: Int,
      rightSkewNum: Int): Unit = {
    assert(joins.size == 1 && joins.head.isSkewJoin)

    val leftSkew = joins.head.left.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(leftSkew.length == leftSkewNum)

    val rightSkew = joins.head.right.collect {
      case r: GpuCustomShuffleReaderExec => r
    }.head.partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct
    assert(rightSkew.length == rightSkewNum)
  }

  private def setupTestData(spark: SparkSession): Unit = {
    testData(spark)
    testData2(spark)
    testData3(spark)
    lowerCaseData(spark)
  }

  /** Ported from org.apache.spark.sql.test.SQLTestData */
  private def testData(spark: SparkSession): Unit = {
    import spark.implicits._
    val data: Seq[(Int, String)] = (1 to 100).map(i => (i, i.toString))
    val df = data.toDF("key", "value")
        .repartition(col("key"))
    registerAsParquetTable(spark, df, "testData")  }

  /** Ported from org.apache.spark.sql.test.SQLTestData */
  private def testData2(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq[(Int, Int)]((1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2))
      .toDF("a", "b")
      .repartition(col("a"))
    registerAsParquetTable(spark, df, "testData2")
  }

  /** Ported from org.apache.spark.sql.test.SQLTestData */
  private def testData3(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq[(Int, Option[Int])]((1, None), (2, Some(2)))
      .toDF("a", "b")
        .repartition(col("a"))
    registerAsParquetTable(spark, df, "testData3")
  }

  /** Ported from org.apache.spark.sql.test.SQLTestData */
  private def uncommonTypeTestData(spark: SparkSession): Unit = {
    import scala.collection.JavaConverters._
    val df = spark.createDataFrame(
      List.tabulate(20)(i => Row(i % 3, BigDecimal(i), Array(i, i), Row(i))).asJava,
      StructType(Array(
        StructField("a", IntegerType),
        StructField("b", DecimalType(4, 0)),
        StructField("c", ArrayType(IntegerType)),
        StructField("d", StructType(Array(StructField("i", IntegerType))))
      ))
    ).repartition(col("a"))
    registerAsParquetTable(spark, df, "uncommonTypeTestData")
  }

  /** Ported from org.apache.spark.sql.test.SQLTestData */
  private def lowerCaseData(spark: SparkSession): Unit = {
    import spark.implicits._
    // note that this differs from the original Spark test by generating a larger data set so that
    // we can trigger larger stats in the logical mode, preventing BHJ, and then our queries filter
    // this down to a smaller data set so that SMJ can be replaced with BHJ at execution time when
    // AQE is enabled`
    val data: Seq[(Int, String)] = (0 to 10000).map(i => (i, if (i<5) i.toString else "z"))
    val df = data
      .toDF("n", "l")
      .repartition(col("n"))
    registerAsParquetTable(spark, df, "lowercaseData")
  }

  private def registerAsParquetTable(spark: SparkSession, df: Dataset[Row], name: String): Unit = {
    val path = new File(TEST_FILES_ROOT, s"$name.parquet").getAbsolutePath
    df.write
        .mode(SaveMode.Overwrite)
        .parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

}
