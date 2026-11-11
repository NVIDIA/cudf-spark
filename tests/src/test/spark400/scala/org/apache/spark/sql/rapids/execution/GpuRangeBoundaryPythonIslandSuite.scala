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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "410db183"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.execution

import java.io.File

import com.nvidia.spark.rapids.{
  GpuColumnarToRowExec, GpuProjectExec, GpuRowToColumnarExec, GpuShuffleCoalesceExec,
  RapidsConf, SparkQueryCompareTestSuite, TargetSize}

import org.apache.spark.SparkConf
import org.apache.spark.api.python.{PythonBroadcast, PythonEvalType, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Ascending, Attribute, AttributeReference, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.BatchEvalPythonExec
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
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
    GpuColumnarToRowExec(
      GpuRowToColumnarExec(
        BatchEvalPythonExec(Seq(udfExpr), Seq(resultAttr),
          GpuProjectExec(islandProject, scan)()),
        TargetSize(1073741824)),
      exportColumnarRdd = false)
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

  test("island plan is not pruned when the udf is nondeterministic") {
    withTempPath { path =>
      withGpuSparkSession({ spark =>
        val scan = islandFixture(spark, path)
        val (udf, resultAttr) = pythonUdfFixture("udf_result", deterministic = false)
        val island = islandPlan(scan, udf, resultAttr)

        val boundKeys = bindKeysByName(scan.output,
          "device_type_id" -> LongType, "country_iso_code" -> StringType,
          "client_version" -> LongType)

        val built = GpuRangeBoundaryPlan.build(island, ordering(boundKeys))
        assert(built.isEmpty)
      }, conf(pruneThroughOperators = true))
    }
  }
}
