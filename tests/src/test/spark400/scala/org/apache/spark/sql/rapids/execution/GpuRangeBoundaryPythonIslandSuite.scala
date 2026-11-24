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
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution

import java.io.File

import com.nvidia.spark.rapids.{
  CoalesceReadOption, GpuColumnarToRowExec, GpuFilterExec, GpuIsNotNull, GpuProjectExec,
  GpuRowToColumnarExec, GpuShuffleCoalesceExec, GpuShuffledAsymmetricHashJoinExec, RapidsConf,
  SparkQueryCompareTestSuite, TargetSize}

import org.apache.spark.SparkConf
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, AttributeReference, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.execution.python.{GpuArrowEvalPythonExec, GpuPythonUDF}
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * Plan-shape tests for key-only range-boundary collection across Python row-boundary islands.
 *
 * The fixtures build the island nodes directly and the tests assert on the plan returned by
 * `GpuRangeBoundaryPlan.build`, so no Python runtime or UDF execution is involved.
 */
class GpuRangeBoundaryPythonIslandSuite extends SparkQueryCompareTestSuite {
  private def conf(pruneThroughOperators: Boolean = false): SparkConf = {
    val base = new SparkConf()
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.sources.useV1SourceList", "parquet")
    if (pruneThroughOperators) {
      base.set(RapidsConf.RANGE_PARTITIONING_PRUNE_THROUGH_OPERATORS.key, "true")
    } else {
      base
    }
  }

  private def islandFixture(spark: SparkSession, path: File): GpuFileSourceScanExec = {
    spark.range(200)
      .select(
        col("id").as("device_type_id"),
        (col("id") % 10).cast("string").as("country_iso_code"),
        (col("id") % 7).as("client_version"),
        lit("payload").as("payload"))
      .write.mode("overwrite").parquet(path.getCanonicalPath)
    val df = spark.read.parquet(path.getCanonicalPath)
    df.queryExecution.executedPlan.collectFirst {
      case s: GpuFileSourceScanExec => s
    }.getOrElse(fail(s"scan not found in:\n${df.queryExecution.executedPlan}"))
  }

  private def pythonUdfFixture(resultAttrName: String, deterministic: Boolean)
      : (PythonUDF, AttributeReference) = {
    val pythonFunc = new SimplePythonFunction(
      Array.emptyByteArray,
      new java.util.HashMap[String, String](),
      new java.util.ArrayList[String](),
      "python",
      "3",
      new java.util.ArrayList[Broadcast[PythonBroadcast]](),
      null)
    val attr = AttributeReference(resultAttrName, StringType, nullable = true)()
    val udf = PythonUDF(
      "noop_py_udf",
      pythonFunc,
      StringType,
      Seq.empty[Expression],
      PythonEvalType.SQL_BATCHED_UDF,
      deterministic,
      attr.exprId)
    (udf, attr)
  }

  private def islandPlan(
      scan: GpuFileSourceScanExec,
      udf: PythonUDF,
      resultAttr: Attribute): SparkPlan = {
    val payloadAttr = scan.output.last
    val udfExpr = PythonUDF(
      udf.name,
      udf.func,
      udf.dataType,
      Seq(payloadAttr),
      udf.evalType,
      udf.udfDeterministic,
      udf.resultId)
    val islandProject = scan.output.map { attr =>
      AttributeReference(attr.name, attr.dataType, attr.nullable)(exprId = attr.exprId)
    }.toList
    GpuRowToColumnarExec(
      BatchEvalPythonExec(Seq(udfExpr), Seq(resultAttr),
        GpuColumnarToRowExec(GpuProjectExec(islandProject, scan)(), exportColumnarRdd = false)),
      TargetSize(1073741824))
  }

  // The Arrow-batched fallback island: the same row boundaries as the batch island, so the
  // widened EvalPythonExec arm must prune it too.
  private def cpuArrowIslandPlan(
      scan: GpuFileSourceScanExec,
      udf: PythonUDF,
      resultAttr: Attribute): SparkPlan = {
    val payloadAttr = scan.output.last
    val udfExpr = PythonUDF(
      udf.name,
      udf.func,
      udf.dataType,
      Seq(payloadAttr),
      PythonEvalType.SQL_ARROW_BATCHED_UDF,
      udf.udfDeterministic,
      udf.resultId)
    val islandProject = scan.output.map { attr =>
      AttributeReference(attr.name, attr.dataType, attr.nullable)(exprId = attr.exprId)
    }.toList
    GpuRowToColumnarExec(
      ArrowEvalPythonExec(Seq(udfExpr), Seq(resultAttr),
        GpuColumnarToRowExec(GpuProjectExec(islandProject, scan)(), exportColumnarRdd = false),
        PythonEvalType.SQL_ARROW_BATCHED_UDF),
      TargetSize(1073741824))
  }

  private def gpuPythonUdfFixture(resultAttrName: String, deterministic: Boolean)
      : (GpuPythonUDF, AttributeReference) = {
    val (pythonUdf, attr) = pythonUdfFixture(resultAttrName, deterministic)
    // The island child projects the scan payload, so the udf consumes that column as a plain
    // attribute; pruning only inspects the island's result attributes.
    val payloadAttr = AttributeReference("payload", StringType, nullable = true)()
    val udf = GpuPythonUDF(
      pythonUdf.name,
      pythonUdf.func,
      StringType,
      Seq(payloadAttr),
      PythonEvalType.SQL_ARROW_BATCHED_UDF,
      pythonUdf.udfDeterministic,
      attr.exprId)
    (udf, attr)
  }

  private def arrowIslandPlan(
      scan: GpuFileSourceScanExec,
      udf: GpuPythonUDF,
      resultAttr: Attribute): SparkPlan = {
    val islandProject = scan.output.map { attr =>
      AttributeReference(attr.name, attr.dataType, attr.nullable)(exprId = attr.exprId)
    }.toList
    GpuArrowEvalPythonExec(Seq(udf), Seq(resultAttr), GpuProjectExec(islandProject, scan)(),
      PythonEvalType.SQL_ARROW_BATCHED_UDF)
  }

  private def ordering(keys: Seq[AttributeReference]): Seq[SortOrder] =
    keys.map(k => SortOrder(k, Ascending))

  private def bindKeysByName(
      output: Seq[Attribute],
      keys: (String, org.apache.spark.sql.types.DataType)*): Seq[AttributeReference] =
    keys.map { case (name, dt) =>
      val attr = output.find(_.name == name)
        .getOrElse(fail(s"key column $name not found in scan output ${output.map(_.name)}"))
      AttributeReference(name, dt, attr.nullable)(exprId = attr.exprId)
    }

  private def aqeConf(pruneThroughOperators: Boolean): SparkConf =
    conf(pruneThroughOperators).set("spark.sql.adaptive.enabled", "true")

  // AQE materializes a shuffle stage only when the query runs; taking the stage from an
  // executed plan sidesteps the Databricks stage constructor, which needs an adaptive context.
  private def gpuShuffleStageFixture(
      spark: SparkSession,
      path: File): (ShuffleQueryStageExec, Seq[AttributeReference]) = {
    spark.range(200)
      .select(
        col("id").as("device_type_id"),
        (col("id") % 10).cast("string").as("country_iso_code"),
        (col("id") % 7).as("client_version"),
        lit("payload").as("payload"))
      .write.mode("overwrite").parquet(path.getCanonicalPath)
    val df = spark.read.parquet(path.getCanonicalPath)
      .repartition(4, col("device_type_id"), col("country_iso_code"), col("client_version"))
    df.collect()
    val executed = df.queryExecution.executedPlan match {
      case adaptive: AdaptiveSparkPlanExec => adaptive.executedPlan
      case other => other
    }
    // Spark 4.0 wraps the final plan in a result stage; a shuffle stage can also be the root.
    val finalPlan = executed match {
      case stage: ShuffleQueryStageExec => stage
      case stage: QueryStageExec => stage.plan
      case other => other
    }
    val stage = finalPlan.collect { case s: ShuffleQueryStageExec => s }.headOption
      .getOrElse(fail(s"shuffle stage not found in:\n$finalPlan"))
    assert(stage.shuffle.isInstanceOf[GpuShuffleExchangeExecBase],
      s"expected a GPU shuffle stage, got ${stage.shuffle.getClass.getName}")
    (stage, bindKeysByName(stage.output,
      "device_type_id" -> LongType, "country_iso_code" -> StringType,
      "client_version" -> LongType))
  }

  // The island's bottom is a materialized stage, so the stage is the only valid prune target.
  private def stageIslandPlan(
      stage: ShuffleQueryStageExec,
      udf: PythonUDF,
      resultAttr: Attribute): SparkPlan = {
    val udfExpr = PythonUDF(
      udf.name,
      udf.func,
      udf.dataType,
      Seq(stage.output.last),
      udf.evalType,
      udf.udfDeterministic,
      udf.resultId)
    GpuRowToColumnarExec(
      BatchEvalPythonExec(Seq(udfExpr), Seq(resultAttr), GpuColumnarToRowExec(stage)),
      TargetSize(1073741824))
  }

  test("island plan prunes through a host-serialized shuffle coalesce") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = GpuShuffleCoalesceExec(islandPlan(scan, udf, resultAttr), 1L)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        val boundary = built.get
        assert(!boundary.exists(_.isInstanceOf[BatchEvalPythonExec]))
        // The coalesce reads host-serialized shuffle batches, so it cannot stay over device ones.
        assert(!boundary.exists(_.isInstanceOf[GpuShuffleCoalesceExec]))
      }, conf(pruneThroughOperators = true))
    }
  }

  test("island plan is not pruned by default") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = islandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isEmpty)
      }, conf())
    }
  }

  test("island plan is not pruned when pruning is explicitly disabled") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = islandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isEmpty)
      }, conf().set(RapidsConf.RANGE_PARTITIONING_PRUNE_THROUGH_OPERATORS.key, "false"))
    }
  }

  test("island plan prunes through the island to the scan") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = islandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        val boundary = built.get
        assert(!boundary.exists(_.isInstanceOf[GpuRowToColumnarExec]))
        assert(!boundary.exists(_.isInstanceOf[GpuColumnarToRowExec]))
        assert(!boundary.exists(_.isInstanceOf[BatchEvalPythonExec]))
      }, conf(pruneThroughOperators = true))
    }
  }

  test("cpu arrow island plan prunes through the island to the scan") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = cpuArrowIslandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        val boundary = built.get
        assert(!boundary.exists(_.isInstanceOf[GpuRowToColumnarExec]))
        assert(!boundary.exists(_.isInstanceOf[GpuColumnarToRowExec]))
        assert(!boundary.exists(_.isInstanceOf[ArrowEvalPythonExec]))
      }, conf(pruneThroughOperators = true))
    }
  }

  test("island plan is not pruned when a key depends on the udf output") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = islandPlan(scan, udf, resultAttr)

        val udfKeyAttr = AttributeReference("udf_key", StringType, nullable = true)(
          exprId = resultAttr.exprId)
        val built = GpuRangeBoundaryPlan.build(island, ordering(Seq(udfKeyAttr)))
        assert(built.isEmpty)
      }, conf(pruneThroughOperators = true))
    }
  }

  test("island plan prunes through a nondeterministic udf no key or filter reads") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = false)
        val island = islandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        val boundary = built.get
        assert(!boundary.exists(_.isInstanceOf[BatchEvalPythonExec]))
        assert(!boundary.exists(_.isInstanceOf[GpuRowToColumnarExec]))
      }, conf(pruneThroughOperators = true))
    }
  }

  test("island plan is not pruned when a filter reads the nondeterministic udf output") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = false)
        val island = islandPlan(scan, udf, resultAttr)
        val gpuResultAttr = AttributeReference(resultAttr.name, resultAttr.dataType,
          resultAttr.nullable)(exprId = resultAttr.exprId)
        val filtered = GpuFilterExec(GpuIsNotNull(gpuResultAttr), island)()

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(filtered, ordering(boundKeys))
        assert(built.isEmpty)
      }, conf(pruneThroughOperators = true))
    }
  }

  test("arrow island plan prunes through a nondeterministic udf no key or filter reads") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = gpuPythonUdfFixture("udf_result", deterministic = false)
        val island = arrowIslandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        val boundary = built.get
        assert(!boundary.exists(_.isInstanceOf[GpuArrowEvalPythonExec]))
      }, conf(pruneThroughOperators = true))
    }
  }

  test("arrow island plan is not pruned when a filter reads the nondeterministic udf output") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = gpuPythonUdfFixture("udf_result", deterministic = false)
        val island = arrowIslandPlan(scan, udf, resultAttr)
        val gpuResultAttr = AttributeReference(resultAttr.name, resultAttr.dataType,
          resultAttr.nullable)(exprId = resultAttr.exprId)
        val filtered = GpuFilterExec(GpuIsNotNull(gpuResultAttr), island)()

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(filtered, ordering(boundKeys))
        assert(built.isEmpty)
      }, conf(pruneThroughOperators = true))
    }
  }

  test("island plan prunes to a materialized shuffle stage") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val (stage, boundKeys) = gpuShuffleStageFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = true)
        val island = stageIslandPlan(stage, udf, resultAttr)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isDefined)
        assert(built.get.exists(_.isInstanceOf[ShuffleQueryStageExec]))

        // A filter that reads the udf output must block the Python pass-through.
        val gpuResultAttr = AttributeReference(resultAttr.name, resultAttr.dataType,
          resultAttr.nullable)(exprId = resultAttr.exprId)
        val filtered = GpuFilterExec(GpuIsNotNull(gpuResultAttr), island)()
        assert(GpuRangeBoundaryPlan.build(filtered, ordering(boundKeys)).isEmpty)
      }, aqeConf(pruneThroughOperators = true))
    }
  }

  test("plan prunes through a custom shuffle reader to its stage") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val (stage, boundKeys) = gpuShuffleStageFixture(spark, path)
        val reader = GpuCustomShuffleReaderExec(stage, Seq(CoalescedPartitionSpec(0, 4)))

        val built = GpuRangeBoundaryPlan.build(reader, ordering(boundKeys))
        assert(built.isDefined)
        // The reader stays as the terminal leaf of the boundary plan.
        assert(built.get.exists(_.isInstanceOf[GpuCustomShuffleReaderExec]))
      }, aqeConf(pruneThroughOperators = true))
    }
  }

  test("plan prunes a left outer sized join from its left leg") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val left = islandFixture(spark, path)
        val right = islandFixture(spark, path)
        val leftKeys = bindKeysByName(left.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)
        val rightKeys = bindKeysByName(right.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)
        val readOption = CoalesceReadOption(new RapidsConf(spark.sessionState.conf))

        def sizedJoin(joinType: JoinType): GpuShuffledAsymmetricHashJoinExec =
          GpuShuffledAsymmetricHashJoinExec(
            joinType,
            leftKeys,
            rightKeys,
            None,
            left,
            right,
            isGpuShuffle = false,
            gpuBatchSizeBytes = 1073741824L,
            partitionNumAmplification = 1.0,
            readOption = readOption,
            isSkewJoin = false)(
            leftKeys,
            rightKeys,
            Integer.valueOf(1000))

        assert(GpuRangeBoundaryPlan.build(sizedJoin(LeftOuter), ordering(leftKeys)).isDefined)
        // RightOuter also keeps unmatched right rows, so the left leg is not a key superset.
        assert(GpuRangeBoundaryPlan.build(sizedJoin(RightOuter), ordering(leftKeys)).isEmpty)
      }, conf(pruneThroughOperators = true))
    }
  }
}
