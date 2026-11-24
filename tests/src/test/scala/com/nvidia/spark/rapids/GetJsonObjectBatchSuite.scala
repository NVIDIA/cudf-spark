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

package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.Files
import java.util.Locale
import java.util.TimeZone

import scala.util.{Failure, Success, Try}

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.GpuRetryOOM
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * CPU/GPU parity for sibling get_json_object projections plus POSITIVE
 * provenance via the `GpuBatchFusion` counter deltas ((firedGroups,
 * batchedMembers, droppedMembers), reset before / snapshot after each GPU run
 * so assertions are order-independent in a shared JVM).
 *
 * Counter deltas assume the tiny table arrives as ONE GPU batch per task (6
 * rows, single part file, default 1GiB batch size); the reduced batch-size
 * case checks members per fired group instead of an absolute delta.
 *
 * Self-contained session setup mirrors the tests-module
 * SparkQueryCompareTestSuite pattern (that module is not on this module's
 * test classpath). Case (e) (a batchInput that evaluates to a non-Gpu
 * ColumnVector) is unreachable from SQL because every GpuExpression.columnarEval
 * returns a GpuColumnVector, so it is exercised by driving `GpuBatchFusion.project`
 * directly with a stub member; there is no case (j) because the batch path is
 * unconditional (no eligibility gate to disable).
 */
class GetJsonObjectBatchSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var tableDir: File = _
  private var bigTableDir: File = _
  private var verifyDir: File = _

  // Session-level confs a test may set; cleared before/after every run so
  // tests cannot leak confs into each other (the Rapids toggle is reset too).
  private val perTestConfKeys = Seq(
    "spark.sql.ansi.enabled",
    "spark.sql.subexpressionElimination.enabled",
    "spark.rapids.sql.batchSizeBytes",
    "spark.sql.files.maxPartitionBytes",
    "spark.rapids.sql.batchFusion.enabled",
    "spark.rapids.sql.tiered.project.enabled",
    "spark.rapids.sql.expression.GetJsonObject.debugPath")

  private def clearPerTestConfs(): Unit = perTestConfKeys.foreach { key =>
    if (spark.conf.get(key, null) != null) {
      spark.conf.unset(key)
    }
  }

  private def withRapids[U](enabled: Boolean,
      confs: Map[String, String])(f: SparkSession => U): U = {
    clearPerTestConfs()
    confs.foreach { case (k, v) => spark.conf.set(k, v) }
    spark.conf.set("spark.rapids.sql.enabled", enabled.toString)
    try {
      f(spark)
    } finally {
      spark.conf.set("spark.rapids.sql.enabled", "false")
      clearPerTestConfs()
    }
  }

  private def withCpu[U](confs: Map[String, String] = Map.empty)(f: SparkSession => U): U =
    withRapids(enabled = false, confs)(f)

  private def withGpu[U](confs: Map[String, String] = Map.empty)(f: SparkSession => U): U =
    withRapids(enabled = true, confs)(f)

  private def rowsOf(rs: Array[Row]): Seq[String] = rs.map(_.mkString("|")).toSeq.sorted

  private def assertCpuGpuParity(sql: String, confs: Map[String, String] = Map.empty): Unit = {
    val cpu = withCpu(confs)(_.sql(sql).collect())
    val gpu = withGpu(confs)(_.sql(sql).collect())
    assert(rowsOf(cpu) == rowsOf(gpu),
      s"CPU/GPU mismatch for [$sql]\nCPU: ${rowsOf(cpu).mkString(", ")}" +
        s"\nGPU: ${rowsOf(gpu).mkString(", ")}")
  }

  /** Row parity OR identical exception class — malformed/invalid-path inputs. */
  private def assertCpuGpuOutcomeParity(sql: String,
      confs: Map[String, String] = Map.empty): Unit = {
    val cpuT = Try(withCpu(confs)(_.sql(sql).collect()))
    val gpuT = Try(withGpu(confs)(_.sql(sql).collect()))
    (cpuT, gpuT) match {
      case (Success(cpu), Success(gpu)) =>
        assert(rowsOf(cpu) == rowsOf(gpu),
          s"CPU/GPU mismatch for [$sql]\nCPU: ${rowsOf(cpu).mkString(", ")}" +
            s"\nGPU: ${rowsOf(gpu).mkString(", ")}")
      case (Failure(ce), Failure(ge)) =>
        assert(ce.getClass == ge.getClass,
          s"CPU/GPU exception class mismatch for [$sql]: " +
            s"${ce.getClass.getName} vs ${ge.getClass.getName}")
      case _ =>
        fail(s"CPU and GPU disagree on outcome for [$sql]: cpu=$cpuT gpu=$gpuT")
    }
  }

  /** Resets the counters, runs the GPU query, returns the counter deltas. */
  private def withGpuDelta(sql: String,
      confs: Map[String, String] = Map.empty): (Long, Long, Long) = {
    GpuBatchFusion.reset()
    withGpu(confs)(_.sql(sql).collect())
    GpuBatchFusion.snapshot()
  }

  private lazy val spark: SparkSession = {
    TrampolineUtil.cleanupAnyExistingSession()
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    Locale.setDefault(Locale.US)
    SparkSession.builder()
      .master("local[1]")
      .appName("GetJsonObjectBatchSuite")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.rapids.sql.enabled", "false")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("gjo-warehouse")
        .toFile.getAbsolutePath)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TrampolineUtil.cleanupAnyExistingSession()
  }

  private def jsonOf(fields: (String, String)*): String =
    fields.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")

  /** Walmart-shaped wide JSON: the 60 f-fields the 60-extract workload reads. */
  private def wideJson(a: String, b: String, c: String, prefix: String): String = {
    val fields = Seq(("a", a), ("b", b), ("c", c)) ++
      (0 until 60).map(i => (s"f$i", s"$prefix$i"))
    jsonOf(fields: _*)
  }

  private val j1 = wideJson("V1", "W1", "7", "X")
  private val j1b = wideJson("V1b", "W1b", "9", "Y")
  private val j2 = jsonOf("a" -> "V2", "b" -> "W2", "c" -> "8")
  private val j2b = jsonOf("a" -> "V2b", "b" -> "W2b", "c" -> "10")
  private val malformed1 = """{"a":1"""
  private val malformed2 = "not json"

  private val schema = StructType(Seq(
    StructField("jsonA", StringType),
    StructField("jsonB", StringType),
    StructField("numStr", StringType),
    StructField("arr", ArrayType(IntegerType)),
    StructField("jsonBad", StringType)))

  // 6 rows: covers null JSON input, absent paths, empty arrays, malformed JSON.
  private val tinyRows = Seq(
    Row(j1, j2, "42", Seq(1, 2, 3), malformed1),
    Row(j1b, null, "43", Seq(4, 5), malformed2),
    Row(null, j2b, "44", null, null),
    Row(j1, j2, "45", Seq(6), malformed1),
    Row(j1b, j2b, "46", Seq(1, 2), malformed1),
    Row(j1, j2, "47", Seq.empty[Int], malformed1))

  // Large enough for the 1024-byte batch target to produce multiple scan batches.
  private val bigRows = (0 until 256).map { i =>
    if (i % 2 == 0) {
      Row(j1, j2, "42", Seq(i % 7), malformed1)
    } else {
      Row(j1b, j2b, "43", Seq(i % 7), malformed2)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tableDir = Files.createTempDirectory("gjo-batch-t").toFile
    bigTableDir = Files.createTempDirectory("gjo-batch-tbig").toFile
    verifyDir = Files.createTempDirectory("gjo-batch-verify").toFile
    withCpu() { s =>
      s.createDataFrame(s.sparkContext.parallelize(tinyRows), schema)
        .repartition(1)
        .write.mode("overwrite").parquet(tableDir.getAbsolutePath)
      s.createDataFrame(s.sparkContext.parallelize(bigRows), schema)
        .repartition(1)
        .write.mode("overwrite").parquet(bigTableDir.getAbsolutePath)
      s.read.parquet(tableDir.getAbsolutePath).createOrReplaceTempView("t")
      s.read.parquet(bigTableDir.getAbsolutePath).createOrReplaceTempView("tbig")
    }
  }

  private def extracts(n: Int, col: String): String =
    (0 until n).map(i => s"get_json_object($col, '$$.f$i')").mkString(", ")

  // ---- (a) bare 60/5-sibling projections ----------------------------------

  test("(a) bare 60- and 5-sibling projections: row parity and one fired group per batch") {
    val sql60 = s"SELECT ${extracts(60, "jsonA")} FROM t"
    assertCpuGpuParity(sql60)
    assert(withGpuDelta(sql60) == ((1L, 60L, 0L)),
      s"60 shared-input siblings must fire one group per batch, got ${withGpuDelta(sql60)}")

    val sql5 = s"SELECT ${extracts(5, "jsonA")} FROM t"
    assertCpuGpuParity(sql5)
    assert(withGpuDelta(sql5) == ((1L, 5L, 0L)))
  }

  // ---- (b) single-arg COALESCE-wrapped projections -------------------------

  test("(b) COALESCE-wrapped siblings: wrapper descends, parity and batching") {
    val wrapped = (n: Int) => "SELECT " +
      (0 until n).map(i => s"coalesce(get_json_object(jsonA, '$$.f$i'))").mkString(", ") +
      " FROM t"
    assertCpuGpuParity(wrapped(60))
    assert(withGpuDelta(wrapped(60)) == ((1L, 60L, 0L)),
      s"single-arg COALESCE must unwrap to the batchable leaf, got ${withGpuDelta(wrapped(60))}")
    assertCpuGpuParity(wrapped(5))
    assert(withGpuDelta(wrapped(5)) == ((1L, 5L, 0L)))
  }

  // ---- (c) per-input grouping with a reorderable non-member in between ----

  test("(c) different inputs group separately and a reorderable cast does not split them") {
    val sql =
      """SELECT get_json_object(jsonA, '$.a') AS a1,
        |       CAST(get_json_object(jsonA, '$.c') AS INT) AS a3,
        |       get_json_object(jsonA, '$.b') AS a2,
        |       get_json_object(jsonB, '$.a') AS b1
        |FROM t""".stripMargin
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql) == ((2L, 3L, 0L)),
      s"expected one group for jsonA siblings (across the cast) and one for jsonB, " +
        s"got ${withGpuDelta(sql)}")
  }

  // ---- (d) a side-effecting sibling seals open groups ----------------------

  test("(d) side-effecting cast under ANSI seals open groups: two groups, not one") {
    val sql = "SELECT get_json_object(jsonA, '$.a') AS a1, CAST(numStr AS INT) AS n, " +
      "get_json_object(jsonA, '$.b') AS a2 FROM t"
    val confs = Map("spark.sql.ansi.enabled" -> "true")
    assertCpuGpuParity(sql, confs)
    assert(withGpuDelta(sql, confs) == ((2L, 2L, 0L)),
      s"the side-effecting cast must seal the first group, got ${withGpuDelta(sql, confs)}")
  }

  // ---- (e) dropout: batchInput evaluates to a non-GpuColumnVector ----------
  // Unreachable from SQL: every GpuExpression.columnarEval returns a
  // GpuColumnVector, so the hook's drop branch is driven directly.

  private class BatchableStub(inputExpr: Expression) extends GpuExpression
      with GpuBatchableExpression {
    override def dataType: DataType = StringType
    override def children: Seq[Expression] = Seq(inputExpr)
    override def nullable: Boolean = true
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      throw new UnsupportedOperationException("not evaluated in this suite")
    override def batchEligible: Boolean = true
    override def batchGroupKey: Any = inputExpr
    override def batchInput: Expression = inputExpr
    override def batchEval(siblings: Seq[Expression],
        inputs: Seq[GpuColumnVector]): Seq[ai.rapids.cudf.ColumnVector] =
      throw new IllegalStateException("group must not fire when its input drops out")
    override def canEqual(that: Any): Boolean = that.isInstanceOf[BatchableStub]
    override def productArity: Int = 1
    override def productElement(n: Int): Any =
      if (n == 0) inputExpr else throw new IndexOutOfBoundsException(n.toString)
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression =
      new BatchableStub(newChildren.head)
  }

  test("a fully dropped group evaluates each projection output only once") {
    val sentinel = GpuLiteral(null, StringType)
    val exprs = Seq(
      GpuAlias(new BatchableStub(sentinel), "o1")(),
      GpuAlias(new BatchableStub(sentinel), "o2")())
    val evaluatedOutputs = scala.collection.mutable.ArrayBuffer.empty[Expression]
    val cb = new ColumnarBatch(Array.empty[ColumnVector], 3)
    GpuBatchFusion.reset()
    def evalColumn(expr: Expression): ColumnVector = {
      if (expr ne sentinel) {
        evaluatedOutputs += expr
      }
      val column = new OnHeapColumnVector(3, StringType)
      column.putNulls(0, 3)
      column
    }
    val projected = GpuBatchFusion.project(cb, exprs, evalColumn).getOrElse {
      new ColumnarBatch(exprs.map(evalColumn).toArray, cb.numRows())
    }
    withResource(projected) { out =>
      assert(evaluatedOutputs.toSeq == exprs)
      assert(out.numCols() == 2)
      assert(out.column(0).numNulls() == 3)
      assert(out.column(1).numNulls() == 3)
    }
    assert(GpuBatchFusion.snapshot() == ((0L, 0L, 2L)),
      s"both members must count as dropped, got ${GpuBatchFusion.snapshot()}")
  }

  test("projection rollback preserves a retryable failure when closing an output throws") {
    val sentinel = GpuLiteral(null, StringType)
    val exprs = (0 until 3).map(i => GpuAlias(new BatchableStub(sentinel), s"o$i")())
    val retry = new GpuRetryOOM("third output failed")
    val closeFailure = new IllegalStateException("first output close failed")
    val outputs = Seq("first", "second").safeMap(value => CudfColumnVector.fromStrings(value))
    try {
      // The callback throws after cuDF releases the reference, so the test does not leak it.
      outputs.head.setEventHandler((_, _) => throw closeFailure)
      withResource(new ColumnarBatch(Array.empty[ColumnVector], 1)) { cb =>
        val thrown = intercept[GpuRetryOOM] {
          GpuBatchFusion.project(cb, exprs, expr => {
            if (expr eq sentinel) {
              new OnHeapColumnVector(1, StringType)
            } else if (expr eq exprs.last) {
              throw retry
            } else {
              val column = if (expr eq exprs.head) outputs.head else outputs(1)
              GpuColumnVector.from(column, StringType)
            }
          }).foreach(_.safeClose())
        }
        assert(thrown eq retry)
        assert(thrown.getSuppressed.toSeq == Seq(closeFailure))
        assert(outputs.forall(_.getRefCount == 0))
      }
    } finally {
      outputs.head.setEventHandler(null)
      outputs.filter(_.getRefCount > 0).safeClose()
    }
  }

  // ---- (f) size-1 group -----------------------------------------------------

  test("(f) size-1 group: row parity (cost-neutrality is measured by the benchmark harness)") {
    val sql = "SELECT get_json_object(jsonA, '$.a') FROM t"
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql) == ((1L, 1L, 0L)))
  }

  // ---- (g) GpuAlias unwrap --------------------------------------------------

  test("(g) GpuAlias-wrapped batchable members unwrap and fire") {
    val sql = "SELECT get_json_object(jsonA, '$.a') AS ga, " +
      "get_json_object(jsonA, '$.b') AS gb FROM t"
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql)._1 > 0,
      s"alias-wrapped siblings must still fire, got ${withGpuDelta(sql)}")
  }

  // ---- (h) None-path: null input / absent path ------------------------------

  test("(h) null input and absent path produce all-null columns matching CPU") {
    val sql = "SELECT get_json_object(jsonA, '$.missing') AS absent FROM t"
    assertCpuGpuParity(sql)
    val gpu = withGpu()(s => s.sql(sql).collect())
    assert(gpu.forall(_.getString(0) == null),
      "an absent path is an all-null column (including the null-input row)")
  }

  // ---- (i) verify-mode stays per-column -------------------------------------

  test("(i) verify-mode get_json_object runs per-column: parity, no batch counter") {
    val sql = "SELECT get_json_object(jsonA, '$.a') AS ga, " +
      "get_json_object(jsonA, '$.b') AS gb FROM t"
    val confs = Map("spark.rapids.sql.expression.GetJsonObject.debugPath" ->
      verifyDir.getAbsolutePath)
    assertCpuGpuParity(sql, confs)
    assert(withGpuDelta(sql, confs) == ((0L, 0L, 0L)),
      s"verify-mode members are not batch-eligible, got ${withGpuDelta(sql, confs)}")
  }

  // ---- (k) malformed JSON + invalid paths: outcome parity -------------------
  // Intra-call error order is unspecified by the plan; only the per-row
  // outcome (rows or exception class) must match the CPU.

  test("(k) malformed JSON and invalid paths: rows or exception class match CPU") {
    assertCpuGpuOutcomeParity("SELECT get_json_object(jsonBad, '$.a') AS m FROM t")
    assertCpuGpuOutcomeParity("SELECT get_json_object(jsonA, '$[') AS bad FROM t")
    assertCpuGpuOutcomeParity("SELECT get_json_object(jsonBad, '$.a') AS m1, " +
      "get_json_object(jsonBad, '$.b') AS m2 FROM t")
  }

  // ---- (l) reduced batch-size target ----------------------------------------

  test("(l) get_json_object preserves results and full groups at a 1024-byte batch target") {
    val sql = s"SELECT ${extracts(60, "jsonA")} FROM tbig"
    val confs = Map("spark.rapids.sql.batchSizeBytes" -> "1024")
    assertCpuGpuParity(sql, confs)
    // A lower batch-size target changes how rows are batched; it does not inject an OOM.
    val defaultBatch = withGpu()(s => rowsOf(s.sql(sql).collect()))
    val tinyBatch = withGpu(confs)(s => rowsOf(s.sql(sql).collect()))
    assert(tinyBatch == defaultBatch,
      s"batchEval results must not depend on the batch-size target\n" +
        s"default: ${defaultBatch.mkString(", ")}\n1024B: ${tinyBatch.mkString(", ")}")
    val delta = withGpuDelta(sql, confs)
    assert(delta._1 > 0, s"the batch hook must fire at least once, got $delta")
    assert(delta._2 == 60L * delta._1,
      s"every fired group must carry all 60 members, got $delta")
    assert(delta._3 == 0L, s"no member should drop out, got $delta")
  }

  // ---- (m) filter context: projection-only hook -----------------------------

  test("(m) filter-context siblings evaluate per-column: parity, no batch counter") {
    val sql = "SELECT jsonB FROM t WHERE get_json_object(jsonA, '$.a') = 'V1' " +
      "AND get_json_object(jsonA, '$.b') = 'W1'"
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql) == ((0L, 0L, 0L)),
      s"the hook is projection-only, filter conditions must not fire it, got ${withGpuDelta(sql)}")
  }

  // ---- (n) HOF-fusing projection skips batch fusion --------------------------

  test("HOF-fusing projection skips batch fusion") {
    // Two transform siblings over the same array argument form a fusable HOF
    // group (a lone HOF is never fused — groupByIntermediateCoverage needs
    // members.length > 1 — so batch fusion would legitimately fire instead).
    val sql = "SELECT transform(arr, x -> x + 1) AS t1, " +
      "transform(arr, x -> x * 2) AS t2 FROM t"
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql) == ((0L, 0L, 0L)),
      s"a fused HOF projection must skip batch fusion, got ${withGpuDelta(sql)}")
  }

  // ---- (o) mixed None-path members in one group -----------------------------

  test("multi-member none-path member yields all-null column") {
    // '$.*' is a separate wildcard (hasSeparateWildcard) so the batch path maps
    // it to None while '$.a'/'$.b' stay valid: one group, no member drops.
    val sql = "SELECT get_json_object(jsonA, '$.a') AS a1, " +
      "get_json_object(jsonA, '$.*') AS star, " +
      "get_json_object(jsonA, '$.b') AS a2 FROM t"
    assertCpuGpuParity(sql)
    val gpu = withGpu()(s => s.sql(sql).collect())
    assert(gpu.forall(_.getString(1) == null),
      "the separate-wildcard path member must be an all-null column")
    assert(withGpuDelta(sql) == ((1L, 3L, 0L)),
      s"the none-path member must stay in the fired group via nullIndexes, " +
        s"got ${withGpuDelta(sql)}")
  }

  // ---- (p) runtime kill switch: spark.rapids.sql.batchFusion.enabled -------

  test("kill switch default ON: batch fusion fires without an explicit conf") {
    val sql = s"SELECT ${extracts(60, "jsonA")} FROM t"
    assertCpuGpuParity(sql)
    assert(withGpuDelta(sql) == ((1L, 60L, 0L)),
      s"default-on must batch the 60 shared-input siblings, got ${withGpuDelta(sql)}")
  }

  test("kill switch OFF: per-column parity and zero batch-fusion activity") {
    val sql = s"SELECT ${extracts(60, "jsonA")} FROM t"
    val confs = Map("spark.rapids.sql.batchFusion.enabled" -> "false")
    // Per-column fallback: results stay CPU-parity and no group ever fires.
    assertCpuGpuParity(sql, confs)
    assert(withGpuDelta(sql, confs) == ((0L, 0L, 0L)),
      s"disabled conf must not fire any batch-fusion group, got ${withGpuDelta(sql, confs)}")
  }

  test("batch fusion preserves floating-point evaluation order in distinct JSON inputs") {
    val confs = Map("spark.rapids.sql.tiered.project.enabled" -> "false",
      "spark.sql.subexpressionElimination.enabled" -> "false",
      "spark.sql.ansi.enabled" -> "false")
    withCpu(confs) { s =>
      import s.implicits._
      Seq((1.0, 1e16, -1e16)).toDF("a", "b", "c")
        .write.mode("overwrite").parquet(bigTableDir.getAbsolutePath + "/rounding")
      s.read.parquet(bigTableDir.getAbsolutePath + "/rounding")
        .createOrReplaceTempView("rounding_input")
    }
    val sql =
      """SELECT get_json_object(IF((a + b) + c > 0.5D, '{"x":"p","y":"p"}',
        |                          '{"x":"q","y":"q"}'), '$.x'),
        |       get_json_object(IF(a + (b + c) > 0.5D, '{"x":"p","y":"p"}',
        |                          '{"x":"q","y":"q"}'), '$.y')
        |FROM rounding_input""".stripMargin
    assertCpuGpuParity(sql, confs)
    assert(withGpuDelta(sql, confs) == ((2L, 2L, 0L)))
  }
}
