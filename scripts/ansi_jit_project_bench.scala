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

/*
 * Benchmark ANSI integral project expressions with CPU, GPU project, and hot/cold GPU AST JIT
 * project runtime.
 *
 * Run with spark-shell, for example:
 *
 *   export CUDA129=/home/haoyangl/code/.conda/cudf-cuda12.9-envs/rapids
 *   export LD_LIBRARY_PATH=$CUDA129/lib:$CUDA129/targets/x86_64-linux/lib:$LD_LIBRARY_PATH
 *   export LIBCUDF_JIT_ENABLED=0
 *   export LIBCUDF_JIT_VERBOSE=1
 *   export BENCH_BASE_PATH=/tmp/ansi_jit_project_bench
 *   export LIBCUDF_KERNEL_CACHE_PATH=$BENCH_BASE_PATH/jit_cache
 *
 *   $SPARK_HOME/bin/spark-shell \
 *     --jars dist/target/rapids-4-spark_2.12-*-cuda12.jar \
 *     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
 *     --conf spark.executorEnv.LIBCUDF_JIT_ENABLED=0 \
 *     --conf spark.executorEnv.LIBCUDF_JIT_VERBOSE=$LIBCUDF_JIT_VERBOSE \
 *     --conf spark.executorEnv.LIBCUDF_KERNEL_CACHE_PATH=$LIBCUDF_KERNEL_CACHE_PATH \
 *     --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
 *     -i scripts/ansi_jit_project_bench.scala
 *
 * Useful knobs:
 *   BENCH_ROWS=100000000
 *   BENCH_PARTITIONS=16
 *   BENCH_EXPR_COUNTS=8
 *   BENCH_EXPR_DEPTHS=1,2,4,8,16,32
 *   BENCH_EXPR_SUITES=try_arithmetic
 *   BENCH_WARMUPS=1
 *   BENCH_ITERS=3
 *   BENCH_MODES=CPU,GPU_PROJECT,GPU_AST_JIT_COLD,GPU_AST_JIT_DISK_WARM,GPU_AST_JIT_PCH_WARM_KERNEL_COLD,GPU_AST_JIT_HOT
 *   BENCH_BASE_PATH=/tmp/ansi_jit_project_bench
 *   BENCH_DATA_PATH=/tmp/ansi_jit_project_bench/parquet_v2_rows_100000000_parts_16
 *   BENCH_RESULT_CSV_PATH=/tmp/ansi_jit_project_bench/results.csv
 *   BENCH_CLEAR_JIT_CACHE_FOR_COLD=true
 *   BENCH_REGENERATE_DATA=true
 *   BENCH_PRINT_PLAN=false
 *   BENCH_CONSUME_MODE=aggregate
 */

import java.io.PrintWriter
import java.nio.file.{Files, Path => JPath, Paths}
import java.util.Comparator
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AnsiJitProjectBench {
  val spark: SparkSession = SparkSession.active

  def env(name: String, defaultValue: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)

  def envBool(name: String, defaultValue: Boolean): Boolean =
    env(name, defaultValue.toString).toBoolean

  def parseInts(value: String): Seq[Int] =
    value.split(",").map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq

  def parseStrings(value: String): Seq[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  val rows: Long = env("BENCH_ROWS", "100000000").toLong
  val partitions: Int = env("BENCH_PARTITIONS",
    math.max(spark.sparkContext.defaultParallelism, 1).toString).toInt
  val exprCounts: Seq[Int] = parseInts(env("BENCH_EXPR_COUNTS", "8"))
  val exprDepths: Seq[Int] = sys.env.get("BENCH_EXPR_DEPTHS").filter(_.nonEmpty)
    .orElse(sys.env.get("BENCH_EXPR_DEPTH").filter(_.nonEmpty))
    .map(parseInts)
    .getOrElse(Seq(1, 2, 4, 8, 16, 32))
  val exprSuites: Seq[String] = parseStrings(env("BENCH_EXPR_SUITES", "mixed"))
      .map(_.toLowerCase(Locale.ROOT))
      .map {
        case "decimal_cast" => "casts"
        case "all" => "mixed"
        case other => other
      }
  val warmups: Int = env("BENCH_WARMUPS", "1").toInt
  val iters: Int = env("BENCH_ITERS", "3").toInt
  val basePath: String =
    env("BENCH_BASE_PATH", s"/tmp/ansi_jit_project_bench_${sys.props("user.name")}")
  val regenerateData: Boolean = envBool("BENCH_REGENERATE_DATA", false)
  val printPlan: Boolean = envBool("BENCH_PRINT_PLAN", false)
  val consumeMode: String = env("BENCH_CONSUME_MODE", "aggregate").toLowerCase(Locale.ROOT)
  val clearJitCacheForCold: Boolean = envBool("BENCH_CLEAR_JIT_CACHE_FOR_COLD", true)
  val freshAppMode: Boolean = envBool("BENCH_FRESH_APP_MODE", false)
  val requireAstForJit: Boolean = envBool("BENCH_REQUIRE_AST_FOR_JIT", true)
  val allowClearNonBenchJitCache: Boolean =
    envBool("BENCH_ALLOW_CLEAR_NON_BENCH_JIT_CACHE", false)
  val kernelCachePath: Option[String] = sys.env.get("LIBCUDF_KERNEL_CACHE_PATH").filter(_.nonEmpty)
  val dataPath: String = env(
    "BENCH_DATA_PATH", s"$basePath/parquet_v2_rows_${rows}_parts_${partitions}")
  val resultCsvPath: String = env("BENCH_RESULT_CSV_PATH",
    s"$basePath/results_${System.currentTimeMillis()}.csv")

  case class Mode(name: String, confs: Map[String, String], jitCacheState: String = "none")

  val astJitConfs: Map[String, String] = Map(
    "spark.rapids.sql.enabled" -> "true",
    "spark.rapids.sql.projectAstEnabled" -> "true",
    "spark.rapids.sql.projectAstRowIrEnabled" -> "true",
    "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "true")

  val allModes: Seq[Mode] = Seq(
    Mode("CPU", Map(
      "spark.rapids.sql.enabled" -> "false",
      "spark.rapids.sql.projectAstEnabled" -> "false",
      "spark.rapids.sql.projectAstRowIrEnabled" -> "false",
      "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "false")),
    Mode("GPU_PROJECT", Map(
      "spark.rapids.sql.enabled" -> "true",
      "spark.rapids.sql.projectAstEnabled" -> "false",
      "spark.rapids.sql.projectAstRowIrEnabled" -> "false",
      "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "false")),
    Mode("GPU_AST_JIT_COLD", astJitConfs, "cold"),
    Mode("GPU_AST_JIT_DISK_WARM", astJitConfs, "disk_warm"),
    Mode("GPU_AST_JIT_PCH_WARM_KERNEL_COLD", astJitConfs, "pch_warm_kernel_cold"),
    Mode("GPU_AST_JIT_HOT", astJitConfs, "hot")
  )

  val modeByName: Map[String, Mode] = allModes.map(m => m.name -> m).toMap
  val modeAliases: Map[String, String] = Map(
    "GPU_AST_JIT" -> "GPU_AST_JIT_HOT",
    "GPU_AST_JIT_PCH_COLD" -> "GPU_AST_JIT_PCH_WARM_KERNEL_COLD")
  val modes: Seq[Mode] = parseStrings(env("BENCH_MODES", allModes.map(_.name).mkString(",")))
    .map { name =>
      val upperName = name.toUpperCase(Locale.ROOT)
      modeAliases.getOrElse(upperName, upperName)
    }
    .distinct
    .map { name =>
      modeByName.getOrElse(name, throw new IllegalArgumentException(
        s"Unknown BENCH_MODES entry $name. Expected one of ${allModes.map(_.name).mkString(",")}"))
    }

  val commonConfs: Map[String, String] = Map(
    "spark.sql.ansi.enabled" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.rapids.sql.metrics.level" -> "DEBUG")

  case class RunResult(
      exprSuite: String,
      mode: String,
      jitCacheState: String,
      exprCount: Int,
      exprDepth: Int,
      iteration: Int,
      warmup: Boolean,
      wallMs: Double,
      projectOpMs: Double,
      compileAstsMs: Double,
      computeAstsMs: Double,
      projectNonCompileMs: Double,
      projectOtherMs: Double,
      allGpuOpMs: Double,
      rowsSeen: Long,
      checksum: Long,
      projectNodes: String,
      planHead: String)

  case class Summary(
      exprSuite: String,
      mode: String,
      jitCacheState: String,
      exprCount: Int,
      exprDepth: Int,
      avgWallMs: Double,
      minWallMs: Double,
      avgProjectOpMs: Double,
      avgCompileAstsMs: Double,
      avgComputeAstsMs: Double,
      avgProjectNonCompileMs: Double,
      avgProjectOtherMs: Double,
      avgAllGpuOpMs: Double,
      speedupVsCpu: Double,
      projectNodes: String)

  def getConf(key: String): Option[String] = Try(spark.conf.get(key)).toOption

  def withConfs[T](confs: Map[String, String])(body: => T): T = {
    val old = confs.keys.map(k => k -> getConf(k)).toMap
    confs.foreach { case (k, v) => spark.conf.set(k, v) }
    try {
      body
    } finally {
      old.foreach {
        case (k, Some(v)) => Try(spark.conf.set(k, v))
        case (k, None) => Try(spark.conf.unset(k))
      }
    }
  }

  def hadoopPathExists(path: String): Boolean = {
    val p = new Path(path)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
  }

  def deletePath(path: String): Unit = {
    val p = new Path(path)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(p, true)
  }

  def deleteLocalPathRecursive(path: JPath): Unit = {
    if (Files.exists(path)) {
      val paths = Files.walk(path)
      try {
        paths.sorted(Comparator.reverseOrder[JPath]()).forEach { (p: JPath) =>
          Files.deleteIfExists(p)
          ()
        }
      } finally {
        paths.close()
      }
    }
  }

  def isUnderBasePath(path: JPath): Boolean = {
    val normalizedBasePath = Paths.get(basePath).toAbsolutePath.normalize()
    path.toAbsolutePath.normalize().startsWith(normalizedBasePath)
  }

  def isFullCold(mode: Mode): Boolean = mode.jitCacheState == "cold"

  def isPchWarmKernelCold(mode: Mode): Boolean =
    mode.jitCacheState == "pch_warm_kernel_cold"

  def isProcessColdMeasured(mode: Mode): Boolean =
    isFullCold(mode) || mode.jitCacheState == "disk_warm" || isPchWarmKernelCold(mode)

  var warnedColdCacheNotCleared = false

  def warnColdCacheNotCleared(reason: String): Unit = {
    if (!warnedColdCacheNotCleared) {
      println(s"WARNING: GPU_AST_JIT_* cold cache was not cleared: $reason")
      println("         Cold results may include hot JIT cache effects.")
      warnedColdCacheNotCleared = true
    }
  }

  def clearFullJitCache(label: String): Unit = {
    if (!clearJitCacheForCold) {
      warnColdCacheNotCleared("BENCH_CLEAR_JIT_CACHE_FOR_COLD=false")
    } else {
      kernelCachePath match {
        case Some(path) =>
          val localPath = Paths.get(path).toAbsolutePath.normalize()
          if (!isUnderBasePath(localPath) && !allowClearNonBenchJitCache) {
            warnColdCacheNotCleared(
              s"LIBCUDF_KERNEL_CACHE_PATH=$localPath is outside BENCH_BASE_PATH=$basePath")
          } else {
            println(s"Clearing JIT cache before cold run[$label]: $localPath")
            deleteLocalPathRecursive(localPath)
            Files.createDirectories(localPath)
          }
        case None =>
          warnColdCacheNotCleared("LIBCUDF_KERNEL_CACHE_PATH is not set")
      }
    }
  }

  def maybeGenerateData(): Unit = {
    if (regenerateData && hadoopPathExists(dataPath)) {
      println(s"Deleting existing data: $dataPath")
      deletePath(dataPath)
    }
    if (hadoopPathExists(dataPath)) {
      println(s"Using existing parquet data: $dataPath")
      return
    }

    println(s"Generating parquet data: rows=$rows partitions=$partitions path=$dataPath")
    withConfs(commonConfs ++ Map("spark.rapids.sql.enabled" -> "false")) {
      val id = col("id")
      val i0 = (id % lit(64)).cast(IntegerType)
      val i1 = (((id * lit(3)) + lit(7)) % lit(64)).cast(IntegerType)
      val i2 = (((id * lit(5)) + lit(11)) % lit(64)).cast(IntegerType)
      val i3 = (((id * lit(7)) + lit(13)) % lit(64)).cast(IntegerType)
      val l0 = (id % lit(1024)).cast(LongType)
      val l1 = (((id * lit(3)) + lit(7)) % lit(1024)).cast(LongType)
      val l2 = (((id * lit(5)) + lit(11)) % lit(1024)).cast(LongType)
      val l3 = (((id * lit(7)) + lit(13)) % lit(1024)).cast(LongType)
      val iDen = (((id % lit(27)) - lit(13))).cast(IntegerType)
      val lDen = (((id % lit(27)) - lit(13))).cast(LongType)
      val shiftAmount = (((id % lit(63)) - lit(31))).cast(IntegerType)
      spark.range(0, rows, 1, partitions)
        .select(
          i0.as("i0"),
          i1.as("i1"),
          i2.as("i2"),
          i3.as("i3"),
          l0.as("l0"),
          l1.as("l1"),
          l2.as("l2"),
          l3.as("l3"),
          when((id % lit(5)) === lit(0), lit(null).cast(IntegerType))
            .otherwise(i0).as("ni0"),
          when((id % lit(7)) === lit(0), lit(null).cast(IntegerType))
            .otherwise(i1).as("ni1"),
          when((id % lit(11)) === lit(0), lit(null).cast(LongType))
            .otherwise(l0).as("nl0"),
          when(iDen === lit(0), lit(1).cast(IntegerType)).otherwise(iDen).as("i_den"),
          when(lDen === lit(0), lit(1).cast(LongType)).otherwise(lDen).as("l_den"),
          shiftAmount.as("shift_amt"),
          ((id % lit(3)) === lit(0)).as("b0"),
          expr("CAST(((id % 10000) - 5000) AS DECIMAL(7, 2))").as("d7_2"),
          expr("CAST(((id % 1000000) - 500000) AS DECIMAL(18, 2))").as("d18_2"))
        .write
        .mode("overwrite")
        .parquet(dataPath)
    }
  }

  def typedLit(value: Long, dt: DataType): Column =
    lit(value).cast(dt)

  def intOrLongNames(useLong: Boolean): Array[String] =
    if (useLong) Array("l0", "l1", "l2", "l3") else Array("i0", "i1", "i2", "i3")

  def sqlIntOrLongName(useLong: Boolean, idx: Int): String =
    intOrLongNames(useLong)(idx & 3)

  def makeArithmeticExpr(idx: Int, depth: Int): Column = {
    val useLong = (idx & 1) == 1
    val dt = if (useLong) LongType else IntegerType
    val names = intOrLongNames(useLong)

    var c: Column = col(names(0))
    var d = 0
    while (d < depth) {
      val k = ((idx + d) % 17) + 1
      c = d % 6 match {
        case 0 => c + col(names(1))
        case 1 => c - col(names(2))
        case 2 => abs(c - col(names(3)))
        case 3 => -c
        case 4 => c * typedLit(2, dt)
        case _ => c + typedLit(k, dt)
      }
      d += 1
    }
    c.as(s"p_$idx")
  }

  def makeDivModExpr(idx: Int, depth: Int): Column = {
    val useLong = (idx & 1) == 1
    val denom = if (useLong) "l_den" else "i_den"
    var sql = sqlIntOrLongName(useLong, idx)
    var d = 0
    while (d < depth) {
      sql = if (((idx + d) & 1) == 0) {
        s"($sql DIV $denom)"
      } else {
        s"($sql % $denom)"
      }
      d += 1
    }
    expr(sql).as(s"p_$idx")
  }

  def makeTryArithmeticExpr(idx: Int, depth: Int): Column = {
    val useLong = (idx & 1) == 1
    val names = intOrLongNames(useLong)
    val sqlType = if (useLong) "BIGINT" else "INT"
    var sql = names(idx & 3)
    var d = 0
    while (d < depth) {
      val rhs = names((idx + d + 1) & 3)
      sql = (idx + d) % 3 match {
        case 0 => s"try_add($sql, $rhs)"
        case 1 => s"try_subtract($sql, $rhs)"
        case _ => s"try_multiply($sql, cast(2 as $sqlType))"
      }
      d += 1
    }
    expr(sql).as(s"p_$idx")
  }

  def makeConditionalExpr(idx: Int, depth: Int): Column = {
    var sql = idx % 4 match {
      case 0 => "coalesce(ni0, ni1, cast(17 as INT))"
      case 1 => "if(isnull(ni0), i1, ni0)"
      case 2 => "if(isnotnull(ni1), ni1, i2)"
      case _ => "if(b0, cast(null as INT), i3)"
    }
    var d = 1
    while (d < depth) {
      sql = s"coalesce($sql, i2)"
      d += 1
    }
    expr(sql).as(s"p_$idx")
  }

  def makeCastExpr(idx: Int, depth: Int): Column = {
    if (idx % 6 >= 4) {
      val sql = idx % 2 match {
        case 0 => "CAST(i0 AS BIGINT)"
        case _ => "CAST(coalesce(ni0, i1) AS BIGINT)"
      }
      expr(sql).as(s"p_$idx")
    } else {
      val base = idx % 4 match {
        case 0 => "CAST(d7_2 AS DECIMAL(9, 4))"
        case 1 => "CAST(d7_2 AS DECIMAL(18, 4))"
        case 2 => "CAST(d18_2 AS DECIMAL(30, 4))"
        case _ => "CAST(d18_2 AS DECIMAL(30, 4))"
      }
      var sql = base
      var d = 1
      while (d < depth) {
        sql = s"CAST($sql AS DECIMAL(30, 4))"
        d += 1
      }
      expr(sql).as(s"p_$idx")
    }
  }

  def makeShiftExpr(idx: Int, depth: Int): Column = {
    val useLong = (idx & 1) == 1
    var sql = sqlIntOrLongName(useLong, idx)
    var d = 0
    while (d < depth) {
      sql = if (((idx + d) & 1) == 0) {
        s"shiftleft($sql, shift_amt)"
      } else {
        s"shiftright($sql, shift_amt)"
      }
      d += 1
    }
    expr(sql).as(s"p_$idx")
  }

  val passThroughNames: Array[String] = Array(
    "i0", "i1", "i2", "i3", "l0", "l1", "l2", "l3", "ni0", "ni1", "nl0")

  def makePassThroughExpr(idx: Int): Column =
    col(passThroughNames((idx * 5 + 1) % passThroughNames.length)).as(s"p_$idx")

  def makeMixedPassThroughExpr(idx: Int, depth: Int): Column = {
    if ((idx & 1) == 0) makePassThroughExpr(idx) else makeArithmeticExpr(idx, depth)
  }

  def makeDuplicateTryPassThroughExpr(idx: Int, depth: Int): Column = {
    if ((idx & 1) == 0) {
      makePassThroughExpr(idx)
    } else {
      var sql = "l0"
      var d = 0
      while (d < depth) {
        sql = s"try_add($sql, l1)"
        d += 1
      }
      expr(sql).as(s"p_$idx")
    }
  }

  def makeLiteralExpr(idx: Int): Column =
    typedLit(104729L + idx, LongType).as(s"p_$idx")

  def makeDuplicateLiteralExpr(idx: Int): Column =
    typedLit(104729L, LongType).as(s"p_$idx")

  def makeMixedLiteralExpr(idx: Int, depth: Int): Column = {
    if ((idx & 1) == 0) makeTryArithmeticExpr(idx, depth) else makeLiteralExpr(idx)
  }

  val mixedExprSuites: Seq[String] = Seq("arithmetic", "div_mod", "conditionals", "casts",
    "shifts")

  def makeMixedExpr(idx: Int, depth: Int): Column = {
    makeExpr(mixedExprSuites(idx % mixedExprSuites.size), idx, depth)
  }

  def makeExpr(exprSuite: String, idx: Int, depth: Int): Column = exprSuite match {
    case "mixed" | "all" => makeMixedExpr(idx, depth)
    case "arithmetic" => makeArithmeticExpr(idx, depth)
    case "try_arithmetic" => makeTryArithmeticExpr(idx, depth)
    case "div_mod" => makeDivModExpr(idx, depth)
    case "conditionals" => makeConditionalExpr(idx, depth)
    case "casts" | "decimal_cast" => makeCastExpr(idx, depth)
    case "shifts" => makeShiftExpr(idx, depth)
    case "pass_through" => makePassThroughExpr(idx)
    case "mixed_pass_through" => makeMixedPassThroughExpr(idx, depth)
    case "duplicate_try_pass_through" => makeDuplicateTryPassThroughExpr(idx, depth)
    case "literal" => makeLiteralExpr(idx)
    case "duplicate_literal" => makeDuplicateLiteralExpr(idx)
    case "mixed_literal" => makeMixedLiteralExpr(idx, depth)
    case other => throw new IllegalArgumentException(
      s"Unknown BENCH_EXPR_SUITES entry $other. Expected mixed, arithmetic, try_arithmetic, " +
        "div_mod, conditionals, casts, decimal_cast, shifts, pass_through, " +
        "mixed_pass_through, duplicate_try_pass_through, literal, duplicate_literal, " +
        "or mixed_literal.")
  }

  def makeQuery(exprSuite: String, exprCount: Int, exprDepth: Int): DataFrame = {
    val input = spark.read.parquet(dataPath)
    input.select((0 until exprCount).map(i => makeExpr(exprSuite, i, exprDepth)): _*)
  }

  def makePchProbeQuery(): DataFrame = {
    val input = spark.read.parquet(dataPath).limit(1)
    input.select(expr(
      "try_subtract(try_multiply(try_add(l3, cast(104729 as BIGINT)), " +
        "cast(37 as BIGINT)), l2)").as("__pch_probe"))
  }

  def flattenPlan(plan: SparkPlan): Seq[SparkPlan] =
    plan +: plan.children.flatMap(flattenPlan)

  def metricValueByKeyOrName(plan: SparkPlan, keyPart: String, namePart: String): Long = {
    flattenPlan(plan).flatMap { node =>
      node.metrics.collect {
        case (key, metric) if key.contains(keyPart) ||
            metric.name.exists(_.toLowerCase.contains(namePart.toLowerCase)) =>
          metric.value
      }
    }.sum
  }

  def isProjectNode(plan: SparkPlan): Boolean =
    plan.nodeName.contains("Project") || plan.getClass.getSimpleName.contains("Project")

  def projectOpTimeNs(plan: SparkPlan): Long = {
    projectMetricNs(plan, "opTimeLegacy")
  }

  def projectCompileAstsTimeNs(plan: SparkPlan): Long = {
    projectMetricNs(plan, "compileAstsTime")
  }

  def projectComputeAstsTimeNs(plan: SparkPlan): Long = {
    projectMetricNs(plan, "computeAstsTime")
  }

  def projectMetricNs(plan: SparkPlan, metricName: String): Long = {
    flattenPlan(plan)
      .filter(isProjectNode)
      .flatMap(_.metrics.get(metricName).map(_.value))
      .sum
  }

  def projectNodeNames(plan: SparkPlan): String = {
    val names = flattenPlan(plan)
      .filter(isProjectNode)
      .map(p => s"${p.nodeName}:${p.getClass.getSimpleName}")
      .distinct
    if (names.isEmpty) "none" else names.mkString("+")
  }

  def longValue(v: Any): Long = v match {
    case null => 0L
    case d: BigDecimal => d.longValue
    case n: java.lang.Number => n.longValue()
    case other => throw new IllegalStateException(s"Unexpected aggregate value: $other")
  }

  def consumeViaAggregate(df: DataFrame): (Long, Long, SparkPlan) = {
    val aggregateColumns = count(lit(1)).as("__row_count") +:
      df.columns.map(c => sum(col(c).cast(LongType)).as(s"sum_$c"))
    val agg = df.agg(aggregateColumns.head, aggregateColumns.tail: _*)
    val row = agg.collect()(0)
    val rowsSeen = row.getLong(0)
    var checksum = 0L
    var i = 1
    while (i < row.length) {
      checksum ^= longValue(row.get(i))
      i += 1
    }
    (rowsSeen, checksum, agg.queryExecution.executedPlan)
  }

  def columnarPlan(plan: SparkPlan): Option[SparkPlan] = {
    if (plan.supportsColumnar) {
      Some(plan)
    } else if (plan.nodeName.contains("ColumnarToRow") && plan.children.size == 1 &&
        plan.children.head.supportsColumnar) {
      Some(plan.children.head)
    } else {
      None
    }
  }

  def consumeViaPlan(df: DataFrame, label: String): (Long, Long, SparkPlan) = {
    val queryExecution = df.queryExecution
    val plan = queryExecution.executedPlan
    val rowsSeen = SQLExecution.withNewExecutionId(queryExecution, Some(label)) {
      columnarPlan(plan) match {
        case Some(columnar) =>
          columnar.executeColumnar().mapPartitions { batches =>
            var rows = 0L
            while (batches.hasNext) {
              val batch = batches.next()
              try {
                rows += batch.numRows().toLong
              } finally {
                batch.close()
              }
            }
            Iterator.single(rows)
          }.collect().sum
        case None =>
          plan.execute().mapPartitions { rowsIter =>
            var rows = 0L
            while (rowsIter.hasNext) {
              rowsIter.next()
              rows += 1
            }
            Iterator.single(rows)
          }.collect().sum
      }
    }
    (rowsSeen, rowsSeen, plan)
  }

  def consume(df: DataFrame, label: String): (Long, Long, SparkPlan) = consumeMode match {
    case "plan" => consumeViaPlan(df, label)
    case "aggregate" | "agg" => consumeViaAggregate(df)
    case other => throw new IllegalArgumentException(
      s"Unknown BENCH_CONSUME_MODE=$other. Expected plan or aggregate.")
  }

  def requireAstProject(mode: Mode, exprSuite: String, plan: SparkPlan): String = {
    val nodes = projectNodeNames(plan)
    val projectElided = Set("literal", "duplicate_literal").contains(exprSuite) && nodes == "none"
    if (requireAstForJit && mode.jitCacheState != "none" &&
        !projectElided && !nodes.contains("GpuProjectAstExec")) {
      val head = plan.treeString.split('\n').take(8).mkString(" | ")
      throw new IllegalStateException(
        s"${mode.name} expected GpuProjectAstExec but ran $nodes. " +
          "Use BENCH_REQUIRE_AST_FOR_JIT=false to allow fallback benchmarking. " +
          s"Plan head: $head")
    }
    nodes
  }

  def prewarmPch(mode: Mode, exprSuite: String, exprCount: Int, exprDepth: Int): Unit = {
    val label = s"${mode.name}_${exprSuite}_exprs_${exprCount}_depth_${exprDepth}_pch_probe"
    clearFullJitCache(label)
    withConfs(commonConfs ++ mode.confs) {
      spark.sparkContext.setJobDescription(label)
      spark.sparkContext.setLocalProperty("spark.job.description", label)

      val startNs = System.nanoTime()
      val (rowsSeen, _, plan) = consume(makePchProbeQuery(), label)
      val wallMs = (System.nanoTime() - startNs).toDouble / 1000000.0
      val nodes = requireAstProject(mode, exprSuite, plan)
      if (printPlan) {
        println(s"plan[$label]:")
        println(plan.treeString)
      }

      spark.sparkContext.setLocalProperty("spark.job.description", null)
      println(f"PCH probe wall=$wallMs%.1f ms rows=$rowsSeen nodes=$nodes")
    }
  }

  def runOne(
      exprSuite: String,
      mode: Mode,
      exprCount: Int,
      exprDepth: Int,
      iteration: Int,
      warmup: Boolean): RunResult = {
    val label = s"${mode.name}_${exprSuite}_exprs_${exprCount}_depth_${exprDepth}_" +
      s"${if (warmup) "warmup" else s"iter_$iteration"}"
    if (isFullCold(mode)) { clearFullJitCache(label) }
    withConfs(commonConfs ++ mode.confs) {
      spark.sparkContext.setJobDescription(label)
      spark.sparkContext.setLocalProperty("spark.job.description", label)

      val df = makeQuery(exprSuite, exprCount, exprDepth)
      val startNs = System.nanoTime()
      val (rowsSeen, checksum, plan) = consume(df, label)
      val wallMs = (System.nanoTime() - startNs).toDouble / 1000000.0
      val projectMs = projectOpTimeNs(plan).toDouble / 1000000.0
      val compileAstsMs = projectCompileAstsTimeNs(plan).toDouble / 1000000.0
      val computeAstsMs = projectComputeAstsTimeNs(plan).toDouble / 1000000.0
      val projectNonCompileMs = math.max(0.0, projectMs - compileAstsMs)
      val projectOtherMs = math.max(0.0, projectMs - compileAstsMs - computeAstsMs)
      val allGpuMs = metricValueByKeyOrName(plan, "opTime", "op time").toDouble / 1000000.0
      val nodes = requireAstProject(mode, exprSuite, plan)
      val head = plan.treeString.split('\n').take(8).mkString(" | ")
      if (printPlan) {
        println(s"plan[$label]:")
        println(plan.treeString)
      }

      spark.sparkContext.setLocalProperty("spark.job.description", null)
      RunResult(exprSuite, mode.name, mode.jitCacheState, exprCount, exprDepth, iteration, warmup,
        wallMs, projectMs, compileAstsMs, computeAstsMs, projectNonCompileMs, projectOtherMs,
        allGpuMs, rowsSeen, checksum, nodes, head)
    }
  }

  def summarize(results: Seq[RunResult]): Seq[Summary] = {
    val measured = results.filterNot(_.warmup)
    val byKey = measured.groupBy(r => (r.exprSuite, r.mode, r.exprCount, r.exprDepth))
    val cpuByExpr = byKey.collect {
      case ((exprSuite, "CPU", exprCount, exprDepth), rs) =>
        (exprSuite, exprCount, exprDepth) -> (rs.map(_.wallMs).sum / rs.size)
    }

    byKey.toSeq.map { case ((exprSuite, mode, exprCount, exprDepth), rs) =>
      val avgWall = rs.map(_.wallMs).sum / rs.size
      val minWall = rs.map(_.wallMs).min
      val avgProject = rs.map(_.projectOpMs).sum / rs.size
      val avgCompileAsts = rs.map(_.compileAstsMs).sum / rs.size
      val avgComputeAsts = rs.map(_.computeAstsMs).sum / rs.size
      val avgProjectNonCompile = rs.map(_.projectNonCompileMs).sum / rs.size
      val avgProjectOther = rs.map(_.projectOtherMs).sum / rs.size
      val avgAllGpu = rs.map(_.allGpuOpMs).sum / rs.size
      val speedup =
        cpuByExpr.get((exprSuite, exprCount, exprDepth)).map(_ / avgWall).getOrElse(Double.NaN)
      val nodes = rs.map(_.projectNodes).distinct.mkString("/")
      val jitCacheState = rs.map(_.jitCacheState).distinct.mkString("/")
      Summary(exprSuite, mode, jitCacheState, exprCount, exprDepth, avgWall, minWall, avgProject,
        avgCompileAsts, avgComputeAsts, avgProjectNonCompile, avgProjectOther, avgAllGpu, speedup,
        nodes)
    }.sortBy(s => (
      exprSuites.indexOf(s.exprSuite),
      s.exprCount,
      s.exprDepth,
      modes.indexWhere(_.name == s.mode)))
  }

  def printTable(summaries: Seq[Summary]): Unit = {
    println()
    println("=== ANSI JIT Project Benchmark Summary ===")
    println(f"${"suite"}%-14s  ${"exprs"}%8s  ${"depth"}%6s  ${"mode"}%-16s  " +
      f"${"cache"}%-5s  ${"avg_wall_ms"}%12s  ${"min_wall_ms"}%12s  ${"speedup"}%8s  " +
      f"${"project_op_ms"}%14s  ${"compile_asts_ms"}%15s  " +
      f"${"compute_asts_ms"}%15s  ${"project_other_ms"}%16s  " +
      f"${"all_gpu_op_ms"}%14s  ${"project_nodes"}")
    summaries.foreach { s =>
      val speed = if (s.speedupVsCpu.isNaN) "n/a" else f"${s.speedupVsCpu}%.2f"
      println(f"${s.exprSuite}%-14s  ${s.exprCount}%8d  ${s.exprDepth}%6d  " +
        f"${s.mode}%-16s  " +
        f"${s.jitCacheState}%-5s  ${s.avgWallMs}%12.1f  ${s.minWallMs}%12.1f  " +
        f"${speed}%8s  ${s.avgProjectOpMs}%14.1f  ${s.avgCompileAstsMs}%15.1f  " +
        f"${s.avgComputeAstsMs}%15.1f  ${s.avgProjectOtherMs}%16.1f  " +
        f"${s.avgAllGpuOpMs}%14.1f  ${s.projectNodes}")
    }
    println()
    println("Notes:")
    println("  project_op_ms is read from Spark SQL metrics on Project/GpuProject nodes.")
    println("  compile_asts_ms is read from GpuProjectAstExec compileAstsTime.")
    println("  compute_asts_ms is read from GpuProjectAstExec computeAstsTime.")
    println("  project_other_ms is project_op_ms minus compile_asts_ms and compute_asts_ms.")
    println("  These are the same SQL metric accumulators written to the Spark event log.")
    println("  GPU_AST_JIT_HOT uses warmups to prime the JIT cache before measured runs.")
    println("  GPU_AST_JIT_COLD clears LIBCUDF_KERNEL_CACHE_PATH, skips warmups, and runs one " +
      "measured iteration per expression shape.")
    println("  GPU_AST_JIT_DISK_WARM reuses the matching GPU_AST_JIT_COLD disk cache in a fresh " +
      "process, skips warmups, and runs one measured iteration per expression shape.")
    println("  GPU_AST_JIT_PCH_WARM_KERNEL_COLD runs a distinct one-row AST probe in the same " +
      "process, then measures target expressions with warm automatic PCH and cold kernel keys.")
    println("  With BENCH_CONSUME_MODE=aggregate, CPU and GPU run the same SQL query end to end.")
    println("  With BENCH_CONSUME_MODE=plan, wall time is a lower-level Project " +
      "materialization probe.")
  }

  def writeCsv(results: Seq[RunResult], summaries: Seq[Summary]): Unit = {
    val parent = Paths.get(resultCsvPath).getParent.toFile
    parent.mkdirs()

    val out = new PrintWriter(resultCsvPath)
    try {
      out.println("kind,expr_suite,mode,jit_cache_state,expr_count,expr_depth,iteration,warmup," +
        "wall_ms,min_wall_ms,project_op_ms,compile_asts_ms,compute_asts_ms," +
        "project_no_compile_ms,project_other_ms,all_gpu_op_ms,rows_seen,checksum,project_nodes")
      results.foreach { r =>
        out.println(Seq("run", r.exprSuite, r.mode, r.jitCacheState, r.exprCount, r.exprDepth,
          r.iteration, r.warmup, r.wallMs, "", r.projectOpMs, r.compileAstsMs, r.computeAstsMs,
          r.projectNonCompileMs, r.projectOtherMs, r.allGpuOpMs, r.rowsSeen, r.checksum,
          r.projectNodes.replace(',', ';')).mkString(","))
      }
      summaries.foreach { s =>
        out.println(Seq("summary", s.exprSuite, s.mode, s.jitCacheState, s.exprCount,
          s.exprDepth, "", "", s.avgWallMs, s.minWallMs, s.avgProjectOpMs, s.avgCompileAstsMs,
          s.avgComputeAstsMs, s.avgProjectNonCompileMs, s.avgProjectOtherMs, s.avgAllGpuOpMs, "",
          "",
          s.projectNodes.replace(',', ';')).mkString(","))
      }
    } finally {
      out.close()
    }
    println(s"CSV written to: $resultCsvPath")
  }

  def printEnvironment(): Unit = {
    println("=== ANSI JIT Project Benchmark Config ===")
    println(s"rows=$rows")
    println(s"partitions=$partitions")
    println(s"exprCounts=${exprCounts.mkString(",")}")
    println(s"exprDepths=${exprDepths.mkString(",")}")
    println(s"exprSuites=${exprSuites.mkString(",")}")
    println(s"modes=${modes.map(_.name).mkString(",")}")
    println(s"warmups=$warmups")
    println(s"iters=$iters")
    println(s"dataPath=$dataPath")
    println(s"appId=${spark.sparkContext.applicationId}")
    println(s"spark.master=${getConf("spark.master").getOrElse("(unset)")}")
    println(s"eventLog.enabled=${getConf("spark.eventLog.enabled").getOrElse("false")}")
    println(s"eventLog.dir=${getConf("spark.eventLog.dir").getOrElse("(unset)")}")
    println(s"printPlan=$printPlan")
    println(s"consumeMode=$consumeMode")
    println(s"freshAppMode=$freshAppMode")
    println(s"requireAstForJit=$requireAstForJit")
    println(s"LIBCUDF_JIT_ENABLED=${sys.env.getOrElse("LIBCUDF_JIT_ENABLED", "(unset)")}")
    println(s"LIBCUDF_JIT_VERBOSE=${sys.env.getOrElse("LIBCUDF_JIT_VERBOSE", "(unset)")}")
    println("LIBCUDF_JIT_DUMP_CODEGEN=" +
      sys.env.getOrElse("LIBCUDF_JIT_DUMP_CODEGEN", "(unset)"))
    println(s"LIBCUDF_KERNEL_CACHE_PATH=${kernelCachePath.getOrElse("(unset)")}")
    println(s"clearJitCacheForCold=$clearJitCacheForCold")
    println(s"allowClearNonBenchJitCache=$allowClearNonBenchJitCache")
    println(s"LD_LIBRARY_PATH=${sys.env.getOrElse("LD_LIBRARY_PATH", "(unset)")}")
    if (modes.exists(isProcessColdMeasured) && kernelCachePath.isEmpty) {
      println("WARNING: GPU_AST_JIT_* cold modes cannot force a cold disk cache without " +
        "LIBCUDF_KERNEL_CACHE_PATH.")
    }
    if (modes.exists(isProcessColdMeasured) &&
        !getConf("spark.master").exists(_.startsWith("local"))) {
      println("WARNING: GPU_AST_JIT_* cold cache clearing only deletes the driver-local path.")
    }
    val hotIndex = modes.indexWhere(_.jitCacheState == "hot")
    val coldIndex = modes.indexWhere(isProcessColdMeasured)
    if (hotIndex >= 0 && coldIndex >= 0 && hotIndex < coldIndex) {
      println("WARNING: GPU_AST_JIT_HOT appears before GPU_AST_JIT_* cold modes; executor process " +
        "cache may already be warm for cold runs.")
    }
    if (modes.exists(isProcessColdMeasured) && !freshAppMode) {
      println("WARNING: GPU_AST_JIT_* cold modes cannot clear libcudf's in-process JIT cache from " +
        "Scala. Use a fresh Spark app for repeated process-cold samples.")
    }
    if (modes.exists(isPchWarmKernelCold) && !freshAppMode) {
      println("WARNING: GPU_AST_JIT_PCH_WARM_KERNEL_COLD requires a fresh Spark app so the " +
        "in-process kernel cache contains only its distinct PCH probe before measurement.")
    }
  }

  def run(): Unit = {
    printEnvironment()
    maybeGenerateData()

    val results = ArrayBuffer.empty[RunResult]
    exprSuites.foreach { exprSuite =>
      exprCounts.foreach { exprCount =>
        exprDepths.foreach { exprDepth =>
          modes.foreach { mode =>
            println()
            println(s"--- suite=$exprSuite mode=${mode.name} exprCount=$exprCount " +
              s"exprDepth=$exprDepth ---")
            if (isPchWarmKernelCold(mode)) {
              prewarmPch(mode, exprSuite, exprCount, exprDepth)
            }
            val modeWarmups = mode.jitCacheState match {
              case "cold" | "disk_warm" | "pch_warm_kernel_cold" => 0
              case "hot" if warmups == 0 => 1
              case _ => warmups
            }
            val modeIters = if (isProcessColdMeasured(mode)) 1 else iters
            if (isProcessColdMeasured(mode) && warmups != 0) {
              println(s"Skipping warmups for ${mode.name}.")
            }
            if (isProcessColdMeasured(mode) && iters != 1) {
              println(s"Using one measured iteration for ${mode.name}. " +
                "Rerun in a fresh Spark app for repeated cold samples.")
            }
            if (mode.jitCacheState == "hot" && warmups == 0) {
              println("Forcing one warmup for GPU_AST_JIT_HOT so measured runs use a hot cache.")
            }
            (0 until modeWarmups).foreach { w =>
              val r = runOne(exprSuite, mode, exprCount, exprDepth, w, warmup = true)
              results += r
              println(f"warmup[$w] wall=${r.wallMs}%.1f ms projectOp=${r.projectOpMs}%.1f ms " +
                f"compileAsts=${r.compileAstsMs}%.1f ms " +
                f"computeAsts=${r.computeAstsMs}%.1f ms " +
                s"rows=${r.rowsSeen} nodes=${r.projectNodes}")
            }
            (0 until modeIters).foreach { i =>
              val r = runOne(exprSuite, mode, exprCount, exprDepth, i, warmup = false)
              results += r
              println(f"iter[$i] wall=${r.wallMs}%.1f ms projectOp=${r.projectOpMs}%.1f ms " +
                f"compileAsts=${r.compileAstsMs}%.1f ms " +
                f"computeAsts=${r.computeAstsMs}%.1f ms " +
                f"projectNoCompile=${r.projectNonCompileMs}%.1f ms " +
                f"projectOther=${r.projectOtherMs}%.1f ms " +
                f"allGpuOp=${r.allGpuOpMs}%.1f ms rows=${r.rowsSeen} checksum=${r.checksum} " +
                s"nodes=${r.projectNodes}")
            }
          }
        }
      }
    }

    val summaries = summarize(results.toSeq)
    printTable(summaries)
    writeCsv(results.toSeq, summaries)
  }
}

AnsiJitProjectBench.run()
