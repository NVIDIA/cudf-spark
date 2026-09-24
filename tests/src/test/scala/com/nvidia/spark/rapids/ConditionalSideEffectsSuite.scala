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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.rapids.{GpuAnd, GpuOr}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

/**
 * Lazy evaluation of the side-effecting branches of GpuIf, GpuCaseWhen, GpuAnd and GpuOr
 * filters the input batch, and column pruning can leave that batch with rows and no columns.
 *
 * The side-effecting stand-ins below report the row count of the batch they are handed rather
 * than a constant, because the gather that reassembles the result nullifies rows the mask did
 * not select. A constant would make an over-selecting filter indistinguishable from a correct
 * one, which is the property these tests exist to pin.
 */
class ConditionalSideEffectsSuite extends RmmSparkRetrySuiteBase {

  // Odd and not a multiple of the null stride, so the true, false and null row counts all differ.
  private val numRows = 509
  private val elseValue = -1

  /** Column pruning can empty a batch while its row count still matters. */
  private def rowsOnly(rows: Int): ColumnarBatch =
    new ColumnarBatch(Array.empty[SparkColumnVector], rows)

  /**
   * A predicate that is neither all true nor all false, so none of the short circuits in
   * GpuIf, GpuCaseWhen, GpuAnd or GpuOr fires and the filtering path is reached.
   *
   * @param nullEvery when positive, every nth row is null instead of a boolean. Table.filter and
   *                  the zero-column row count treat a null entry as not selected, and this is
   *                  what checks that they agree.
   */
  private case class MixedPredicate(nullEvery: Int = 0) extends GpuLeafExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = nullEvery > 0

    def valueAt(row: Int): Option[Boolean] =
      if (nullEvery > 0 && row % nullEvery == 0) None else Some(row % 2 == 0)

    /** How many rows this predicate selects, which is what the true branch must be handed. */
    def selected: Int = (0 until numRows).count(valueAt(_).contains(true))

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
      val values = (0 until batch.numRows()).map { row =>
        valueAt(row).map(java.lang.Boolean.valueOf).orNull
      }
      closeOnExcept(ColumnVector.fromBoxedBooleans(values: _*)) { cv =>
        GpuColumnVector.from(cv, BooleanType)
      }
    }
  }

  /** All null, which makes the filter mask sum over a column with nothing to sum. */
  private case class NullPredicate() extends GpuLeafExpression {
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = true

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
      val values = Array.fill[java.lang.Boolean](batch.numRows())(null)
      closeOnExcept(ColumnVector.fromBoxedBooleans(values: _*)) { cv =>
        GpuColumnVector.from(cv, BooleanType)
      }
    }
  }

  /**
   * Stands in for an expression the plugin will not evaluate eagerly, such as GpuCeil. Only
   * hasSideEffects selects the lazy path; reporting the row count is what lets a test see how
   * many rows the filter actually selected.
   */
  private case class FilteredRowCount() extends GpuLeafExpression {
    override def hasSideEffects: Boolean = true
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      withResource(GpuScalar.from(batch.numRows(), IntegerType)) { scalar =>
        GpuColumnVector.from(scalar, batch.numRows(), IntegerType)
      }
  }

  /** The same, for the boolean right-hand side of GpuAnd and GpuOr, which cannot carry a count. */
  private case class FilteredRowCountIs(expected: Int) extends GpuLeafExpression {
    override def hasSideEffects: Boolean = true
    override def dataType: DataType = BooleanType
    override def nullable: Boolean = false

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      withResource(GpuScalar.from(batch.numRows() == expected, BooleanType)) { scalar =>
        GpuColumnVector.from(scalar, batch.numRows(), BooleanType)
      }
  }

  private def evalOnRowsOnly(expr: GpuExpression, rows: Int = numRows)(
      check: GpuColumnVector => Unit): Unit = {
    withResource(rowsOnly(rows)) { batch =>
      withResource(expr.columnarEval(batch)) { ret =>
        assertResult(rows)(ret.getRowCount.toInt)
        check(ret)
      }
    }
  }

  private def assertInts(ret: GpuColumnVector, expected: Int => Int): Unit = {
    withResource(ret.copyToHost()) { host =>
      (0 until numRows).foreach { row =>
        assert(!host.isNullAt(row), s"row $row is null")
        assertResult(expected(row), s"row $row")(host.getInt(row))
      }
    }
  }

  private def assertBools(ret: GpuColumnVector, expected: Int => Boolean): Unit = {
    withResource(ret.copyToHost()) { host =>
      (0 until numRows).foreach { row =>
        assert(!host.isNullAt(row), s"row $row is null")
        assertResult(expected(row), s"row $row")(host.getBoolean(row))
      }
    }
  }

  test("GpuIf hands each branch only its own rows of a rows-only batch") {
    val pred = MixedPredicate()
    val taken = pred.selected
    val expr = GpuIf(pred, FilteredRowCount(), FilteredRowCount())
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret, row => if (pred.valueAt(row).get) taken else numRows - taken)
    }
  }

  test("GpuIf routes null predicate rows on a rows-only batch to the false branch") {
    val pred = MixedPredicate(nullEvery = 3)
    val taken = pred.selected
    val expr = GpuIf(pred, FilteredRowCount(), FilteredRowCount())
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret,
        row => if (pred.valueAt(row).contains(true)) taken else numRows - taken)
    }
  }

  test("GpuIf evaluates a rows-only batch against a scalar false branch") {
    val pred = MixedPredicate()
    val taken = pred.selected
    val expr = GpuIf(pred, FilteredRowCount(), GpuLiteral(elseValue))
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret, row => if (pred.valueAt(row).get) taken else elseValue)
    }
  }

  test("GpuIf evaluates a rows-only batch whose predicate is entirely null") {
    // The true branch keeps no rows at all, so the filter counts an all-null mask. Only the
    // false branch has side effects, or GpuIf would take its all-false short circuit instead.
    val expr = GpuIf(NullPredicate(), GpuLiteral(elseValue), FilteredRowCount())
    evalOnRowsOnly(expr)(assertInts(_, _ => numRows))
  }

  test("GpuCaseWhen hands the THEN and ELSE branches only their own rows") {
    val when = MixedPredicate()
    val taken = when.selected
    val expr = GpuCaseWhen(Seq((when, FilteredRowCount())), Some(FilteredRowCount()))
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret, row => if (when.valueAt(row).get) taken else numRows - taken)
    }
  }

  test("GpuCaseWhen treats a null WHEN on a rows-only batch as not matched") {
    val when = MixedPredicate(nullEvery = 3)
    val taken = when.selected
    val expr = GpuCaseWhen(Seq((when, FilteredRowCount())), Some(FilteredRowCount()))
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret,
        row => if (when.valueAt(row).contains(true)) taken else numRows - taken)
    }
  }

  test("GpuAnd evaluates its right side only where the left side is true") {
    val lhs = MixedPredicate()
    val expr = GpuAnd(lhs, FilteredRowCountIs(lhs.selected))
    evalOnRowsOnly(expr)(assertBools(_, row => lhs.valueAt(row).get))
  }

  test("GpuOr evaluates its right side only where the left side is false") {
    val lhs = MixedPredicate()
    val expr = GpuOr(lhs, FilteredRowCountIs(numRows - lhs.selected))
    evalOnRowsOnly(expr)(assertBools(_, _ => true))
  }

  // The tests below cover the other half of the bug: each of these paths built the input view
  // before deciding whether to filter, so a rows-only batch aborted on a path that was about to
  // do little or no work. Three of them return without filtering at all, so they cannot observe
  // a selection count. Zero-row GpuAnd does filter, but cannot distinguish a correct count from
  // an over-selecting one because both are zero. Only the cumulative GpuCaseWhen below filters
  // enough to pin a count.

  test("GpuAnd short-circuits a rows-only batch when its left side is all false") {
    val expr = GpuAnd(GpuLiteral(false, BooleanType), FilteredRowCountIs(numRows))
    evalOnRowsOnly(expr)(assertBools(_, _ => false))
  }

  test("GpuOr short-circuits a rows-only batch when its left side is all true") {
    val expr = GpuOr(GpuLiteral(true, BooleanType), FilteredRowCountIs(numRows))
    evalOnRowsOnly(expr)(assertBools(_, _ => true))
  }

  test("GpuAnd evaluates a rows-only batch of no rows") {
    // An empty mask sums to an invalid scalar, and the row count still has to come out zero.
    val expr = GpuAnd(GpuLiteral(true, BooleanType), FilteredRowCountIs(0))
    evalOnRowsOnly(expr, rows = 0)(_ => ())
  }

  test("GpuCaseWhen returns early from a rows-only batch when the first WHEN is all true") {
    val expr = GpuCaseWhen(Seq((GpuLiteral(true, BooleanType), FilteredRowCount())),
      Some(GpuLiteral(elseValue)))
    evalOnRowsOnly(expr)(assertInts(_, _ => numRows))
  }

  // Lives here rather than beside GpuColumnVector's other tests because it needs RMM, and
  // because GpuIf reaching an all-null predicate is what makes this branch load-bearing.
  test("GpuColumnVector.filter counts a rows-only batch whose mask sum is invalid") {
    val noTypes = Array.empty[DataType]
    val allNull = Array.fill[java.lang.Boolean](numRows)(null)
    withResource(rowsOnly(numRows)) { batch =>
      withResource(ColumnVector.fromBoxedBooleans(allNull: _*)) { mask =>
        withResource(GpuColumnVector.filter(batch, noTypes, mask)) { filtered =>
          assertResult(0)(filtered.numRows())
          assertResult(0)(filtered.numCols())
        }
      }
    }
    withResource(rowsOnly(0)) { batch =>
      withResource(ColumnVector.fromBoxedBooleans(Array.empty[java.lang.Boolean]: _*)) { mask =>
        withResource(GpuColumnVector.filter(batch, noTypes, mask)) { filtered =>
          assertResult(0)(filtered.numRows())
        }
      }
    }
  }

  test("GpuCaseWhen returns early from a rows-only batch once every row has matched") {
    // The second WHEN covers whatever the first left, so the cumulative predicate becomes all
    // true and the ELSE is never reached.
    val when = MixedPredicate()
    val taken = when.selected
    val expr = GpuCaseWhen(
      Seq((when, FilteredRowCount()), (GpuLiteral(true, BooleanType), FilteredRowCount())),
      Some(GpuLiteral(elseValue)))
    evalOnRowsOnly(expr) { ret =>
      assertInts(ret, row => if (when.valueAt(row).get) taken else numRows - taken)
    }
  }
}
