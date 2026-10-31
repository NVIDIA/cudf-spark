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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.OperatorsUtilShims

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, Inner, InnerLike, JoinType,
  LeftAnti, LeftOuter, LeftSemi, RightOuter}
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
  private val condition = GpuGreaterThan(
    GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "a"),
    GpuLiteral(5, IntegerType))
  private val existsAttr = AttributeReference("exists", BooleanType, nullable = false)()

  // Fails the test if the join evaluates it.
  private case class FailIfEvaluated() extends GpuLeafExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      fail(s"the join evaluated its condition over ${batch.numRows()} rows")
  }

  // The join types and build sides that nestedLoopJoin handles with a condition
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

  private def intBatch(values: Seq[Integer]): ColumnarBatch =
    new ColumnarBatch(Array(GpuColumnVector.from(
      ColumnVector.fromBoxedInts(values: _*), IntegerType)), values.length)

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

  private def nestedLoopJoin(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      build: => ColumnarBatch,
      stream: Seq[() => ColumnarBatch],
      streamHasColumns: Boolean,
      boundCondition: GpuExpression = condition): Seq[Seq[Any]] = {
    // The condition binds to the first table's columns, then the second's. The side with no
    // columns adds none, so the condition reads ordinal 0 of the side that has the column.
    val firstIsStream = (joinType == RightOuter) == (buildSide == GpuBuildLeft)
    val numFirstTableColumns = if (firstIsStream == streamHasColumns) 1 else 0
    val streamAttrs = if (streamHasColumns) Seq(streamAttr) else Seq.empty
    withGpuSparkSession { _ =>
      withResource(build) { buildCb =>
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

  private def matches(v: Integer): Boolean = v != null && v > 5

  private def sorted(rows: Seq[Seq[Any]]): Seq[Seq[Any]] = rows.sortBy(_.toString)

  for ((name, joinType, buildSide) <- joinShapes) {
    test(s"$name $buildSide nested loop join accepts a rows-only build") {
      val streamValues = Seq(Seq[Integer](null, 0, 5, 6, 9), Seq.empty[Integer], Seq[Integer](6, 9))
      val stream = streamValues.flatten
      for (buildRows <- Seq(0, 1, 3)) {
        withClue(s"build rows $buildRows: ") {
          val matched = (v: Integer) => buildRows > 0 && matches(v)
          val expected = joinType match {
            case _: InnerLike => stream.filter(matched).flatMap(v => Seq.fill(buildRows)(Seq(v)))
            case LeftOuter | RightOuter =>
              stream.flatMap(v => Seq.fill(if (matched(v)) buildRows else 1)(Seq(v)))
            case LeftSemi => stream.filter(matched).map(Seq(_))
            case LeftAnti => stream.filterNot(matched).map(Seq(_))
            case _ => stream.map(v => Seq(v, matched(v)))
          }
          val actual = nestedLoopJoin(joinType, buildSide,
            new ColumnarBatch(Array.empty, buildRows),
            streamValues.map(values => () => intBatch(values)), streamHasColumns = true)
          assertResult(sorted(expected))(sorted(actual))
        }
      }
    }

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

    test(s"$name $buildSide nested loop join accepts a rows-only stream") {
      val streamRows = Seq(5, 0, 2)
      for (build <- Seq(Seq.empty[Integer], Seq[Integer](null, 0, 5),
          Seq[Integer](null, 6, 0, 9))) {
        withClue(s"build $build: ") {
          val matched = build.filter(matches)
          val expected = joinType match {
            case _: InnerLike => matched.flatMap(v => Seq.fill(streamRows.sum)(Seq(v)))
            case LeftOuter | RightOuter =>
              (if (matched.isEmpty) Seq[Integer](null) else matched)
                .flatMap(v => Seq.fill(streamRows.sum)(Seq(v)))
            case LeftSemi if matched.nonEmpty => Seq.fill(streamRows.sum)(Seq.empty)
            case LeftAnti if matched.isEmpty => Seq.fill(streamRows.sum)(Seq.empty)
            case ExistenceJoin(_) => Seq.fill(streamRows.sum)(Seq(matched.nonEmpty))
            case _ => Seq.empty
          }
          val actual = nestedLoopJoin(joinType, buildSide, intBatch(build),
            streamRows.map(rows => () => new ColumnarBatch(Array.empty, rows)),
            streamHasColumns = false)
          assertResult(sorted(expected))(sorted(actual))
        }
      }
    }
  }

  // A NOT EXISTS subquery whose predicate folds to false plans a LEFT ANTI join with that condition
  // over two sides with no columns.
  for ((name, joinType) <- Seq(("LeftSemi", LeftSemi), ("LeftAnti", LeftAnti))) {
    test(s"$name GpuBuildRight nested loop join accepts rows-only build and stream") {
      val streamRows = Seq(5, 0, 2)
      for (buildRows <- Seq(0, 3)) {
        withClue(s"build rows $buildRows: ") {
          val expected =
            if (joinType == LeftAnti) Seq.fill(streamRows.sum)(Seq.empty) else Seq.empty
          val actual = nestedLoopJoin(joinType, GpuBuildRight,
            new ColumnarBatch(Array.empty, buildRows),
            streamRows.map(rows => () => new ColumnarBatch(Array.empty, rows)),
            streamHasColumns = false, boundCondition = GpuLiteral(false, BooleanType))
          assertResult(expected)(actual)
        }
      }
    }
  }
}
