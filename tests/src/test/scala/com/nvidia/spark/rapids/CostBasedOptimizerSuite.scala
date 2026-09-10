/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.shims.OperatorsUtilShims
import org.scalactic.Tolerance
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.DataTypes

class CostBasedOptimizerSuite extends SparkQueryCompareTestSuite
  with Tolerance
  with BeforeAndAfter
  with Logging {

  // these tests currently rely on setting expensive transitions to force desired outcomes
  // and were written before we had the concept of memory access costs, so for now we use these
  // config keys to set an explicit cost for these transitions
  private val TRANSITION_TO_GPU_COST = "spark.rapids.sql.optimizer.gpu.exec.GpuRowToColumnarExec"
  private val TRANSITION_TO_CPU_COST = "spark.rapids.sql.optimizer.gpu.exec.GpuColumnarToRowExec"

  private def createDefaultConf() = new SparkConf()
    .set(RapidsConf.EXPLAIN.key, "ALL")
    .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
    .set(RapidsConf.OPTIMIZER_EXPLAIN.key, "ALL")

  before {
    GpuOverrides.removeAllListeners()
  }

  after {
    GpuOverrides.removeAllListeners()
  }

  test("Memory cost algorithm") {
    val GIGABYTE = 1024 * 1024 * 1024
    assert(2d === MemoryCostHelper.calculateCost(GIGABYTE, 0.5) +- 0.01)
    assert(1d === MemoryCostHelper.calculateCost(GIGABYTE, 1) +- 0.01)
    assert(0.5d === MemoryCostHelper.calculateCost(GIGABYTE, 2) +- 0.01)
  }

  test("Estimate data size") {
    assert(8 === MemoryCostHelper.estimateGpuMemory(
      Some(DataTypes.LongType), nullable = false, 1))
    assert(17179869176L === MemoryCostHelper.estimateGpuMemory(
      Some(DataTypes.LongType), nullable = false, Int.MaxValue))
    // we cap the estimate at Int.MaxValue rows to avoid integer overflow
    assert(17179869176L === MemoryCostHelper.estimateGpuMemory(
      Some(DataTypes.LongType), nullable = false, Long.MaxValue))
  }

  // see https://github.com/NVIDIA/spark-rapids/issues/3526
  ignore("Avoid transition to GPU for trivial projection after CPU SMJ") {
    logError("Avoid transition to GPU for trivial projection after CPU SMJ")

    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
          "Alias,Cast,LessThan,ShuffleExchangeExec,RangePartitioning,HashPartitioning," +
          "ShuffleExchangeExec")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
        .alias("df1")
        .orderBy("more_strings_1")

      df.collect()

      val avoided = getAvoidedTransitions(optimizations)
      assert(avoided.nonEmpty)
      assert(avoided.forall(_.toString.startsWith(
        "It is not worth moving to GPU for operator: Project [more_strings_2")))

      val cpuPlans = PlanUtils.findOperators(df.queryExecution.executedPlan,
        _.isInstanceOf[WholeStageCodegenExec])
        .map(_.asInstanceOf[WholeStageCodegenExec])

      assert(cpuPlans.size === 1)
      assert(cpuPlans.head
        .asInstanceOf[WholeStageCodegenExec]
        .child
        .asInstanceOf[ProjectExec]
        .child
        .isInstanceOf[SortMergeJoinExec])

      df
    }, conf)

  }

  // see https://github.com/NVIDIA/spark-rapids/issues/3526
  ignore("Force section of plan back onto CPU, AQE on") {
    logError("Force section of plan back onto CPU, AQE on")

    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
      .set(RapidsConf.OPTIMIZER_DEFAULT_CPU_OPERATOR_COST.key, "0")
      .set(RapidsConf.OPTIMIZER_DEFAULT_GPU_OPERATOR_COST.key, "0")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RangePartitioning,HashPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
      optimizations += costOptimizations
    })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark)
          .alias("df1")
          .orderBy("more_strings_1")
      val df2: DataFrame = createQuery(spark)
          .alias("df2")
          .orderBy("more_strings_2")
      val df = df1.join(df2, col("df1.more_strings_1").equalTo(col("df2.more_strings_2")))
          .orderBy("df2.more_strings_2")

      df.collect()

      // check that the expected optimization was applied
      val replacedSections = getReplacedSections(optimizations)
      val opt = replacedSections.last

      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

      // check that the final plan has a CPU sort and no GPU sort
      val cpuSort =
        PlanUtils.findOperators(df.queryExecution.executedPlan, _.isInstanceOf[SortExec])

      val gpuSort =
        PlanUtils.findOperators(df.queryExecution.executedPlan, _.isInstanceOf[GpuSortExec])

      assert(cpuSort.nonEmpty)
      assert(gpuSort.isEmpty)

      df
    }, conf)

  }

  test("Force section of plan back onto CPU, AQE off") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Force section of plan back onto CPU, AQE off")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      .set(TRANSITION_TO_CPU_COST, "0.15")
      .set(TRANSITION_TO_GPU_COST, "0.15")
      .set("spark.rapids.sql.optimizer.cpu.exec.LocalTableScanExec", "1.0")
      .set("spark.rapids.sql.optimizer.gpu.exec.LocalTableScanExec", "0.8")
      .set("spark.rapids.sql.optimizer.cpu.exec.SortExec", "1.0")
      .set("spark.rapids.sql.optimizer.gpu.exec.SortExec", "0.8")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RangePartitioning,HashPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark)
          .alias("df1")
          .orderBy("more_strings_1")
      val df2: DataFrame = createQuery(spark)
          .alias("df2")
          .orderBy("more_strings_2")
      val df = df1.join(df2, col("df1.more_strings_1").equalTo(col("df2.more_strings_2")))
          .orderBy("df2.more_strings_2")

      df.collect()

      // check that the expected optimization was applied
      assert(7 == optimizations.flatten
          .filter(_.isInstanceOf[ReplaceSection[_]])
          .map(_.asInstanceOf[ReplaceSection[_]])
          .count(_.plan.wrapped.isInstanceOf[SortExec]))

      // check that the final plan has a CPU sort and no GPU sort
      val cpuSort =
        PlanUtils.findOperators(df.queryExecution.executedPlan, _.isInstanceOf[SortExec])

      val gpuSort =
        PlanUtils.findOperators(df.queryExecution.executedPlan, _.isInstanceOf[GpuSortExec])

      assert(cpuSort.nonEmpty)
      assert(gpuSort.isEmpty)

      df
    }, conf)

  }

  // see https://github.com/NVIDIA/spark-rapids/issues/3526
  ignore("Force last section of plan back onto CPU, AQE on") {
    logError("Force last section of plan back onto CPU, AQE on")

    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(TRANSITION_TO_CPU_COST, "0.3")
      .set(TRANSITION_TO_GPU_COST, "0.3")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RangePartitioning,RoundRobinPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()

      // check that the expected optimization was applied
      val replacedSections = getReplacedSections(optimizations)
      val opt = replacedSections.last
      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

      //assert that the top-level sort stayed on the CPU
      val finalPlan = df.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
        .executedPlan
      finalPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[SortExec]

      df
    }, conf)

  }

  test("Force last section of plan back onto CPU, AQE off") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Force last section of plan back onto CPU, AQE off")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(TRANSITION_TO_CPU_COST, "0.15")
      .set(TRANSITION_TO_GPU_COST, "0.15")
      .set("spark.rapids.sql.optimizer.cpu.exec.LocalTableScanExec", "1.0")
      .set("spark.rapids.sql.optimizer.gpu.exec.LocalTableScanExec", "0.8")
      .set("spark.rapids.sql.optimizer.cpu.exec.SortExec", "1.0")
      .set("spark.rapids.sql.optimizer.gpu.exec.SortExec", "0.8")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RangePartitioning,RoundRobinPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()

      // check that the expected optimization was applied
      val replacedSections = getReplacedSections(optimizations)
      val opt = replacedSections.last
      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

      //assert that the top-level sort stayed on the CPU
      df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[SortExec]

      df
    }, conf)

  }

  test("Avoid move to GPU for trivial projection, AQE on") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Avoid move to GPU for trivial projection, AQE on")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RoundRobinPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      val cpuProjects = OperatorsUtilShims.findOperators(df.queryExecution.executedPlan,
        _.isInstanceOf[ProjectExec])
      val gpuProjects = OperatorsUtilShims.findOperators(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuProjectExec])
      // Assert that the top-level projection stayed on the CPU
      assert(cpuProjects.nonEmpty, "No CPU ProjectExec found in the plan")
      assert(gpuProjects.isEmpty, "Found GPU ProjectExec in the plan when it should be on CPU")

      df
    }, conf)

  }

  test("Avoid move to GPU for trivial projection, AQE off") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Avoid move to GPU for trivial projection, AQE off")
    val conf = createDefaultConf()
      .set(TRANSITION_TO_CPU_COST, "0.1")
      .set(TRANSITION_TO_GPU_COST, "0.1")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
        "Alias,Cast,LessThan,ShuffleExchangeExec,RoundRobinPartitioning")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      // check that the expected optimization was applied
      assert(3 == optimizations
          .flatten
          .filter(_.isInstanceOf[AvoidTransition[_]])
          .map(_.asInstanceOf[AvoidTransition[_]])
          .count(_.plan.wrapped.isInstanceOf[ProjectExec]))

      // check that the expected optimization was applied
      assert(3 == optimizations
          .flatten
          .filter(_.isInstanceOf[AvoidTransition[_]])
          .map(_.asInstanceOf[AvoidTransition[_]])
          .count(_.plan.wrapped.isInstanceOf[ProjectExec]))

      // assert that the top-level projection stayed on the CPU
      assert(df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.isInstanceOf[ProjectExec])

      df
    }, conf)
  }

  test("Avoid move to GPU for shuffle, AQE on") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Avoid move to GPU for shuffle, AQE on")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      val gpuExchanges = PlanUtils.findOperators(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuShuffleExchangeExecBase])
      assert(gpuExchanges.isEmpty)

      df
    }, conf)
  }

  test("Avoid move to GPU for shuffle, AQE off") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Avoid move to GPU for shuffle, AQE off")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
      .set(RapidsConf.OPTIMIZER_DEFAULT_CPU_OPERATOR_COST.key, "0")
      .set(RapidsConf.OPTIMIZER_DEFAULT_GPU_OPERATOR_COST.key, "0")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
            "Alias,Cast,LessThan,ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      val gpuExchanges = PlanUtils.findOperators(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuShuffleExchangeExecBase])
      assert(gpuExchanges.isEmpty)

      df
    }, conf)
  }


  test("keep CustomShuffleReaderExec on GPU") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("keep CustomShuffleReaderExec on GPU")
    // if we force a GPU CustomShuffleReaderExec back onto CPU due to cost then the query will
    // fail because the shuffle already happened on GPU and we end up with an invalid plan

    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "1")
      .set("spark.rapids.sql.optimizer.gpu.exec.GpuCustomShuffleReaderExec", "99999999")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan,ShuffleExchangeExec," +
            "RoundRobinPartitioning,HashPartitioning,EmptyRelationExec")

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()

      df
    }, conf)
  }

  test("Compute estimated row count nested joins no broadcast") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Compute estimated row count nested joins no broadcast")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan,ShuffleExchangeExec," +
            "HashPartitioning,EmptyRelationExec")

    val plans: ListBuffer[SparkPlanMeta[SparkPlan]] =
      new ListBuffer[SparkPlanMeta[SparkPlan]]()
    GpuOverrides.addListener(
      (plan: SparkPlanMeta[SparkPlan],
          _: SparkPlan,
          _: Seq[Optimization]) => {
        plans += plan
      })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()
    }, conf)

    val accum = new ListBuffer[SparkPlanMeta[_]]()
    plans.foreach(collectPlansWithRowCount(_, accum))

    val summary = accum
        .map(plan => plan.wrapped.getClass.getSimpleName -> plan.estimatedOutputRows.get)
        .distinct
        .sorted

    // due to the concurrent nature of adaptive execution, the results are not deterministic
    // so we just check that we do see row counts for shuffle exchanges and sort-merge joins

    val shuffleExchanges = summary
        .filter(_._1 == "ShuffleExchangeExec")
    assert(shuffleExchanges.nonEmpty)
    assert(shuffleExchanges.forall(_._2.toLong > 0))

    val sortMergeJoins = summary
        .filter(_._1 == "SortMergeJoinExec")
    assert(sortMergeJoins.nonEmpty)
    assert(sortMergeJoins.forall(_._2.toLong > 0))
  }

  test("Compute estimated row count nested joins with broadcast") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/12632")
    logError("Compute estimated row count nested joins with broadcast")
    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan,ShuffleExchangeExec," +
            "RoundRobinPartitioning,EmptyRelationExec")

    val plans: ListBuffer[SparkPlanMeta[SparkPlan]] =
      new ListBuffer[SparkPlanMeta[SparkPlan]]()
    GpuOverrides.addListener(
      (plan: SparkPlanMeta[SparkPlan],
          _: SparkPlan,
          _: Seq[Optimization]) => {
        plans += plan
      })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()
    }, conf)

    val accum = new ListBuffer[SparkPlanMeta[_]]()
    plans.foreach(collectPlansWithRowCount(_, accum))

    val summary = accum
        .map(plan => plan.wrapped.getClass.getSimpleName -> plan.estimatedOutputRows.get)
        .distinct
        .sorted

    // due to the concurrent nature of adaptive execution, the results are not deterministic
    // so we just check that we do see row counts for multiple broadcast exchanges

    val broadcastExchanges = summary
        .filter(_._1 == "BroadcastExchangeExec")

    assert(broadcastExchanges.nonEmpty)
    assert(broadcastExchanges.forall(_._2.toLong > 0))
  }

  // The purpose of this test is simply to make sure that we do not try and calculate the cost
  // of expressions that cannot be evaluated. This can cause exceptions when estimating data
  // sizes. For example, WindowFrame.dataType throws UnsupportedOperationException.
  test("Window expression (unevaluable)") {
    logError("Window expression (unevaluable)")

    val conf = createDefaultConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      // disable gpu cpu bridge, or else the cost optimizer will not run properly
      .set(RapidsConf.ENABLE_CPU_BRIDGE.key, "false")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "ProjectExec,WindowExec,ShuffleExchangeExec,WindowSpecDefinition," +
          "SpecifiedWindowFrame,WindowExpression,Alias,Rank,HashPartitioning," +
          "UnboundedPreceding$,CurrentRow$,SortExec")

    withGpuSparkSession(spark => {
      employeeDf(spark).createOrReplaceTempView("employees")
      val df: DataFrame = spark.sql("SELECT name, dept, RANK() " +
        "OVER (PARTITION BY dept ORDER BY salary) AS rank FROM employees")
      df.collect()
    }, conf)
  }

  private def employeeDf(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq(
      ("A", "IT", 1234),
      ("B", "IT", 4321),
      ("C", "Sales", 4321),
      ("D", "Sales", 1234)
    ).toDF("name", "dept", "salary")
  }

  private def collectPlansWithRowCount(
      plan: SparkPlanMeta[_],
      accum: ListBuffer[SparkPlanMeta[_]]): Unit = {
    if (plan.estimatedOutputRows.exists(_ > 0)) {
      accum += plan
    }
    plan.childPlans.foreach(collectPlansWithRowCount(_, accum))
  }

  private def createQuery(spark: SparkSession) = {
    val df1 = nullableStringsDf(spark)
        .repartition(2)
        .withColumnRenamed("more_strings", "more_strings_1")

    val df2 = nullableStringsDf(spark)
        .repartition(2)
        .withColumnRenamed("more_strings", "more_strings_2")

    val df = df1.join(df2, "strings")
        // filter on unsupported CAST to force operation onto CPU
        .filter(col("more_strings_2").cast(DataTypes.TimestampType)
            .lt(col("more_strings_1").cast(DataTypes.TimestampType)))
        // this projection just swaps the order of the attributes and we want CBO to keep
        // this on CPU
        .select("more_strings_2", "more_strings_1")
    df
  }

  private def getReplacedSections(optimizations: ListBuffer[Seq[Optimization]]) = {
    optimizations.flatten
      .filter(_.isInstanceOf[ReplaceSection[_]])
      .map(_.asInstanceOf[ReplaceSection[_]])
  }

  private def getAvoidedTransitions(optimizations: ListBuffer[Seq[Optimization]]) = {
    optimizations.flatten
      .filter(_.isInstanceOf[AvoidTransition[_]])
      .map(_.asInstanceOf[AvoidTransition[_]])
  }

}
