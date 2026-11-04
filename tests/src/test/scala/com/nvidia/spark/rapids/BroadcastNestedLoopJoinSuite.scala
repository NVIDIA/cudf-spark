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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.OperatorsUtilShims

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike,
  JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuGreaterThan
import org.apache.spark.sql.rapids.execution.{GpuBroadcastNestedLoopJoinExec,
  GpuBroadcastNestedLoopJoinExecBase}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class BroadcastNestedLoopJoinSuite extends SparkQueryCompareTestSuite {

  test("BroadcastNestedLoopJoinExec AQE off") {
    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark).repartition(2)
      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df3 = df1.crossJoin(broadcast(df2))
      df3.collect()
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        OperatorsUtilShims.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])
      assert(nljCount.size === 1)
    }, conf)
  }

  test("BroadcastNestedLoopJoinExec AQE on") {
    val conf = new SparkConf()
        .set("spark.rapids.sql.exec.BroadcastNestedLoopJoinExec", "true")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        // In some cases AQE can make the children not look like they are on the GPU
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ShuffleExchangeExec,RoundRobinPartitioning")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark).repartition(2)
      val df2 = nonZeroLongsDf(spark).repartition(2)
      val df3 = df1.crossJoin(broadcast(df2))
      df3.collect()
      val plan = df3.queryExecution.executedPlan

      val nljCount =
        OperatorsUtilShims.findOperators(plan, _.isInstanceOf[GpuBroadcastNestedLoopJoinExec])

      assert(nljCount.size === 1)
    }, conf)
  }

  private val streamAttr = AttributeReference("a", IntegerType, nullable = true)()
  private val existsAttr = AttributeReference("exists", BooleanType, nullable = false)()

  private def column(ordinal: Int, name: String): GpuExpression =
    GpuBoundReference(ordinal, IntegerType, nullable = true)(ExprId(ordinal), name)

  // With columns on one side only, the condition reads that side's column at ordinal 0.
  private val condition = GpuGreaterThan(column(0, "a"), GpuLiteral(5, IntegerType))

  private def matches(v: Integer): Boolean = v != null && v > 5

  // Fails the test if the join evaluates it.
  private case class FailIfEvaluated() extends GpuLeafExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      fail(s"the join evaluated its condition over ${batch.numRows()} rows")
  }

  // Records the rows of each batch the join evaluates it on, then evaluates the condition.
  private case class RecordEvaluations(rows: ArrayBuffer[Int]) extends GpuLeafExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = true
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
      rows += batch.numRows()
      condition.columnarEval(batch)
    }
  }

  // The join types and build sides that tagPlanForGpu runs on the GPU with a condition
  private val joinShapes = Seq[(String, JoinType, GpuBuildSide)](
    ("Inner", Inner, GpuBuildLeft),
    ("Inner", Inner, GpuBuildRight),
    ("Cross", Cross, GpuBuildLeft),
    ("Cross", Cross, GpuBuildRight),
    ("LeftOuter", LeftOuter, GpuBuildRight),
    ("RightOuter", RightOuter, GpuBuildLeft),
    ("LeftSemi", LeftSemi, GpuBuildRight),
    ("LeftAnti", LeftAnti, GpuBuildRight),
    ("ExistenceJoin", ExistenceJoin(existsAttr), GpuBuildRight))

  // The join types and build sides that tagPlanForGpu sends to the CPU. ExistenceJoin building
  // left, and Inner and Cross with no columns on either side, have tests of their own.
  private val unsupportedShapes = Seq[(String, JoinType, GpuBuildSide)](
    ("LeftOuter", LeftOuter, GpuBuildLeft),
    ("RightOuter", RightOuter, GpuBuildRight),
    ("FullOuter", FullOuter, GpuBuildLeft),
    ("FullOuter", FullOuter, GpuBuildRight),
    ("LeftSemi", LeftSemi, GpuBuildLeft),
    ("LeftAnti", LeftAnti, GpuBuildLeft))

  // Which sides have columns, as (stream, build)
  private val columnPresences = Seq(
    "a rows-only build" -> ((true, false)),
    "a rows-only stream" -> ((false, true)),
    "no columns on either side" -> ((false, false)),
    "columns on both sides" -> ((true, true)))

  private val streams = Seq(
    "no stream batch" -> Seq.empty[Seq[Integer]],
    "only zero-row stream batches" -> Seq(Seq.empty[Integer], Seq.empty[Integer]),
    "mixed stream batches" ->
      Seq(Seq[Integer](null, 0, 5, 6, 9), Seq.empty[Integer], Seq[Integer](6, 9)))

  // An empty build, one with no row above 5, and one with two
  private val builds = Seq(Seq.empty[Integer], Seq[Integer](null, 0, 5),
    Seq[Integer](null, 6, 0, 9))

  private def intBatch(values: Seq[Integer]): ColumnarBatch =
    new ColumnarBatch(Array(GpuColumnVector.from(
      ColumnVector.fromBoxedInts(values: _*), IntegerType)), values.length)

  // A side without columns keeps only the number of values.
  private def batch(values: Seq[Integer], hasColumns: Boolean): ColumnarBatch =
    if (hasColumns) intBatch(values) else new ColumnarBatch(Array.empty, values.length)

  private def rowsOf(cb: ColumnarBatch): Seq[Seq[Any]] = {
    val columns = (0 until cb.numCols()).map { i =>
      val column = cb.column(i).asInstanceOf[GpuColumnVector]
      withResource(column.copyToHost()) { host =>
        (0 until cb.numRows()).map { r =>
          if (host.isNullAt(r)) {
            null
          } else if (column.dataType() == BooleanType) {
            host.getBoolean(r)
          } else {
            host.getInt(r)
          }
        }
      }
    }
    (0 until cb.numRows()).map(r => columns.map(_(r)))
  }

  // The condition binds to the first table's columns, then the second's. The first table is the
  // right one for RightOuter and the left one otherwise.
  private def firstIsStream(joinType: JoinType, buildSide: GpuBuildSide): Boolean =
    (joinType == RightOuter) == (buildSide == GpuBuildLeft)

  // The conditions to join with for each mix of columns, and which stream and build values each
  // matches. With no columns on either side, the condition reads nothing.
  private def conditions(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      streamHasColumns: Boolean,
      buildHasColumns: Boolean): Seq[(String, GpuExpression, (Integer, Integer) => Boolean)] =
    (streamHasColumns, buildHasColumns) match {
      case (true, false) => Seq(("stream > 5", condition, (s, _) => matches(s)))
      case (false, true) => Seq(("build > 5", condition, (_, b) => matches(b)))
      case (false, false) => Seq[Any](true, false, null).map { value =>
        val matched = (_: Integer, _: Integer) => value == true
        (s"literal $value", GpuLiteral(value, BooleanType), matched)
      }
      case (true, true) =>
        val (s, b) = if (firstIsStream(joinType, buildSide)) (0, 1) else (1, 0)
        Seq(("stream > build", GpuGreaterThan(column(s, "a"), column(b, "b")),
          (sv, bv) => sv != null && bv != null && sv > bv))
    }

  // Spark's result: a stream row and a build row join when `matched` holds for them. The output
  // takes the left side's columns, then the right side's, and a side without columns adds none.
  private def expected(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      stream: Seq[Integer],
      streamHasColumns: Boolean,
      build: Seq[Integer],
      buildHasColumns: Boolean,
      matched: (Integer, Integer) => Boolean): Seq[Seq[Any]] = {
    def side(v: Integer, hasColumns: Boolean): Seq[Any] = if (hasColumns) Seq(v) else Seq.empty
    stream.flatMap { s =>
      val buildMatches = build.filter(b => matched(s, b))
      def joined(b: Integer): Seq[Any] = if (buildSide == GpuBuildLeft) {
        side(b, buildHasColumns) ++ side(s, streamHasColumns)
      } else {
        side(s, streamHasColumns) ++ side(b, buildHasColumns)
      }
      joinType match {
        case _: InnerLike => buildMatches.map(joined)
        case LeftOuter | RightOuter =>
          if (buildMatches.isEmpty) Seq(joined(null)) else buildMatches.map(joined)
        case LeftSemi => if (buildMatches.nonEmpty) Seq(side(s, streamHasColumns)) else Seq.empty
        case LeftAnti => if (buildMatches.isEmpty) Seq(side(s, streamHasColumns)) else Seq.empty
        case _ => Seq(side(s, streamHasColumns) :+ buildMatches.nonEmpty)
      }
    }
  }

  private def nestedLoopJoin(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      build: => ColumnarBatch,
      stream: Seq[() => ColumnarBatch],
      streamHasColumns: Boolean,
      boundCondition: GpuExpression): Seq[Seq[Any]] = {
    val streamAttrs = if (streamHasColumns) Seq(streamAttr) else Seq.empty
    withGpuSparkSession { _ =>
      withResource(build) { buildCb =>
        val numFirstTableColumns =
          if (firstIsStream(joinType, buildSide)) streamAttrs.size else buildCb.numCols()
        withResource(LazySpillableColumnarBatch(buildCb, "build")) { builtBatch =>
          val streamBatches = stream.iterator.map { make =>
            withResource(make())(LazySpillableColumnarBatch(_, "stream"))
          }
          // A small target size splits the output into several batches. 0.75 is the default
          // size estimate threshold of the join gatherer.
          GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(joinType, buildSide,
            numFirstTableColumns, builtBatch, streamBatches, streamAttrs, 16, 0.75,
            Some(boundCondition), NoopMetric, NoopMetric, NoopMetric, NoopMetric)
            .flatMap(cb => withResource(cb)(rowsOf)).toList
        }
      }
    }
  }

  private def sorted(rows: Seq[Seq[Any]]): Seq[Seq[Any]] = rows.sortBy(_.toString)

  // tagPlanForGpu sends these joins to the CPU. If one reaches nestedLoopJoin, it fails naming
  // the join type and build side, before it reads either side.
  private def assertRejected(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      streamHasColumns: Boolean,
      buildHasColumns: Boolean,
      boundCondition: GpuExpression): Unit = {
    val e = intercept[IllegalArgumentException] {
      nestedLoopJoin(joinType, buildSide, batch(Seq[Integer](0, 6), buildHasColumns),
        Seq(() => batch(Seq[Integer](0, 6), streamHasColumns)), streamHasColumns,
        boundCondition)
    }
    assert(e.getMessage.contains(s"$joinType") && e.getMessage.contains(s"$buildSide"),
      e.getMessage)
  }

  for ((name, joinType, buildSide) <- joinShapes;
       (presence, (streamHasColumns, buildHasColumns)) <- columnPresences
       if streamHasColumns || buildHasColumns || !joinType.isInstanceOf[InnerLike]) {
    test(s"$name $buildSide nested loop join with $presence") {
      for ((streamName, stream) <- streams; build <- builds;
           (conditionName, cond, matched) <-
             conditions(joinType, buildSide, streamHasColumns, buildHasColumns)) {
        withClue(s"$streamName, build $build, $conditionName: ") {
          val want = expected(joinType, buildSide, stream.flatten, streamHasColumns, build,
            buildHasColumns, matched)
          val actual = nestedLoopJoin(joinType, buildSide, batch(build, buildHasColumns),
            stream.map(values => () => batch(values, streamHasColumns)), streamHasColumns, cond)
          assertResult(sorted(want))(sorted(actual))
        }
      }
    }
  }

  for ((name, joinType, buildSide) <- joinShapes) {
    test(s"$name $buildSide nested loop join skips the condition over an empty rows-only build") {
      // Spark tests the condition only against a build row.
      val stream = Seq[Integer](null, 0, 6)
      val expected = joinType match {
        case _: InnerLike | LeftSemi => Seq.empty
        case LeftOuter | RightOuter | LeftAnti => stream.map(Seq(_))
        case _ => stream.map(v => Seq(v, false))
      }
      val actual = nestedLoopJoin(joinType, buildSide, new ColumnarBatch(Array.empty, 0),
        Seq(() => intBatch(stream)), streamHasColumns = true, boundCondition = FailIfEvaluated())
      assertResult(sorted(expected))(sorted(actual))
    }

    test(s"$name $buildSide nested loop join with a rows-only build skips the condition " +
        "when the stream has no batch") {
      // Spark tests the condition only against a stream row.
      val actual = nestedLoopJoin(joinType, buildSide, new ColumnarBatch(Array.empty, 3),
        Seq.empty, streamHasColumns = true, boundCondition = FailIfEvaluated())
      assertResult(Seq.empty)(actual)
    }

    // The outer joins' rows-only stream path filters the build before it reads the stream.
    if (joinType != LeftOuter && joinType != RightOuter) {
      test(s"$name $buildSide nested loop join with a rows-only stream skips the condition " +
          "when the stream has no batch") {
        val actual = nestedLoopJoin(joinType, buildSide, intBatch(Seq[Integer](null, 0, 6)),
          Seq.empty, streamHasColumns = false, boundCondition = FailIfEvaluated())
        assertResult(Seq.empty)(actual)
      }
    }

    test(s"$name $buildSide nested loop join filters the build once for a rows-only stream") {
      // A zero-row batch is still a batch, so a stream of them filters the build once too.
      val build = Seq[Integer](null, 6, 0, 9)
      for (streamRows <- Seq(Seq(5, 0, 2), Seq(0, 0))) {
        withClue(s"stream batches of $streamRows rows: ") {
          val evaluated = ArrayBuffer[Int]()
          nestedLoopJoin(joinType, buildSide, intBatch(build),
            streamRows.map(rows => () => new ColumnarBatch(Array.empty, rows)),
            streamHasColumns = false, boundCondition = RecordEvaluations(evaluated))
          assertResult(Seq(build.length))(evaluated)
        }
      }
    }
  }

  for ((name, joinType, buildSide) <- unsupportedShapes;
       (presence, (streamHasColumns, buildHasColumns)) <- columnPresences
       if streamHasColumns != buildHasColumns) {
    test(s"$name $buildSide nested loop join with $presence is rejected") {
      assertRejected(joinType, buildSide, streamHasColumns, buildHasColumns, condition)
    }
  }

  for ((name, joinType, buildSide) <- joinShapes if joinType.isInstanceOf[InnerLike]) {
    test(s"$name $buildSide nested loop join with no columns on either side is rejected") {
      assertRejected(joinType, buildSide, streamHasColumns = false, buildHasColumns = false,
        GpuLiteral(false, BooleanType))
    }
  }

  // The existence join's iterators take the stream as the outer side, which only building right
  // gives.
  for ((presence, (streamHasColumns, buildHasColumns)) <- columnPresences) {
    test(s"ExistenceJoin GpuBuildLeft nested loop join with $presence is rejected") {
      assertRejected(ExistenceJoin(existsAttr), GpuBuildLeft, streamHasColumns, buildHasColumns,
        FailIfEvaluated())
    }
  }
}
