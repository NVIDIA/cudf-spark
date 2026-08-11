/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf._
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BooleanType, DataType, DoubleType, FloatType}
import org.apache.spark.sql.vectorized.ColumnarBatch


case class GpuNot(child: Expression) extends CudfUnaryExpression
    with Predicate with ImplicitCastInputTypes with NullIntolerantShim {
  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def sql: String = s"(NOT ${child.sql})"

  override def unaryOp: UnaryOp = UnaryOp.NOT

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    child match {
      case c: GpuEqualTo =>
        // optimize the AST expression since Spark doesn't have a NotEqual
        new ast.BinaryOperation(ast.BinaryOperator.NOT_EQUAL,
          c.left.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns),
          c.right.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns))
      case _ => super.convertToAst(numFirstTableColumns)
    }
  }
}

abstract class CudfBinaryPredicateWithSideEffect extends CudfBinaryOperator with Predicate {

  override def inputType: AbstractDataType = BooleanType

  def shortCircuitVal: Boolean
  def shouldShortCircuit(col: GpuColumnVector): Boolean
  def processLHS(col: ColumnVector): ColumnVector

  import GpuExpressionWithSideEffectUtils._

  def applyShortCircuit(col: GpuColumnVector): GpuColumnVector =
    shortCircuitWithBool(col, shortCircuitVal)

  /**
   * When computing logical expressions on the CPU, the true and false
   * expressions are evaluated lazily, meaning that the RHS expression
   * of logical-AND is not evaluated when LHS is False. For logical-OR,
   * RHS is not evaluated when LHS is True.
   * This is important in the case where the expressions can have
   * side-effects, such as throwing exceptions for invalid inputs.
   *
   * This method performs lazy evaluation on the GPU by first filtering
   * the input batch where the LHS predicate is True.
   * The RHS predicate is evaluated against these batches and then the
   * results are combined back into a single batch using the gather
   * algorithm.
   */
  def columnarEvalWithSideEffects(batch: ColumnarBatch): GpuColumnVector = {
    val leftExpr = left.asInstanceOf[GpuExpression]
    withResource(GpuColumnVector.from(batch)) { tbl =>
      withResource(leftExpr.columnarEval(batch)) { lhsBool =>
        if (shouldShortCircuit(lhsBool)) {
          applyShortCircuit(lhsBool)
        } else {
          val rightExpr = right.asInstanceOf[GpuExpression]
          val colTypes = GpuColumnVector.extractTypes(batch)
          // Process the LHS. It may imply replacing null values (if any) with true.
          withResource(processLHS(lhsBool.getBase)) { lhsNoNulls =>
            withResource(filterBatch(tbl, lhsNoNulls, colTypes)) { leftTrueBatch =>
              withResource(rightExpr.columnarEval(leftTrueBatch)) {
                rEval =>
                  withResource(gather(lhsNoNulls, rEval)) { combinedVector =>
                    GpuColumnVector.from(
                      doColumnar(lhsBool, GpuColumnVector.from(combinedVector, dataType)),
                      dataType)
                }
              }
            }
          }
        }
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val rightExpr = right.asInstanceOf[GpuExpression]

    if (rightExpr.hasSideEffects) {
      columnarEvalWithSideEffects(batch)
    } else {
      super.columnarEval(batch)
    }
  }
}

case class GpuAnd(left: Expression, right: Expression) extends CudfBinaryPredicateWithSideEffect {

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

  override def binaryOp: BinaryOp = BinaryOp.NULL_LOGICAL_AND
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.NULL_LOGICAL_AND)

  override def shortCircuitVal: Boolean = false

  override def shouldShortCircuit(col: GpuColumnVector): Boolean =
    GpuExpressionWithSideEffectUtils.isAllFalse(col, false)

  override def processLHS(col: ColumnVector): ColumnVector =
    GpuExpressionWithSideEffectUtils.replaceNulls(col, true)
}

case class GpuOr(left: Expression, right: Expression) extends CudfBinaryPredicateWithSideEffect {

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

  override def binaryOp: BinaryOp = BinaryOp.NULL_LOGICAL_OR
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.NULL_LOGICAL_OR)

  override def shortCircuitVal: Boolean = true

  override def shouldShortCircuit(col: GpuColumnVector): Boolean =
    GpuExpressionWithSideEffectUtils.isAllTrue(col)

  override def processLHS(col: ColumnVector): ColumnVector =
    GpuExpressionWithSideEffectUtils.boolInverted(col)
}

abstract class CudfBinaryComparison extends CudfBinaryOperator with Predicate {
  // Note that we need to give a superset of allowable input types since orderable types are not
  // finitely enumerable. The allowable types are checked below by checkInputDataTypes.
  override def inputType: AbstractDataType = AnyDataType

  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess =>
      TypeUtils.checkForOrderingExpr(left.dataType, this.getClass.getSimpleName)
    case failure => failure
  }

  def hasFloatingPointInputs: Boolean = left.dataType == FloatType || left.dataType == DoubleType ||
    right.dataType == FloatType || right.dataType == DoubleType

  def evaluateAndFixFloatingPointResult(
    result: => ColumnVector,
    lhsNanPredicate: => BinaryOperable with AutoCloseable,
    rhsNanPredicate: => BinaryOperable with AutoCloseable
  ): ColumnVector = {
    if (!hasFloatingPointInputs) {
      result
    } else {
      withResource(result) { result =>
        val lhsAndRhs = withResource(lhsNanPredicate) { lhsNanPredicate =>
          withResource(rhsNanPredicate) { rhsNanPredicate =>
            lhsNanPredicate.and(rhsNanPredicate)
          }
        }
        withResource(lhsAndRhs) { lhsAndRhs =>
          lhsAndRhs.or(result)
        }
      }
    }
  }
}

/**
 * The table below shows how the result is calculated for Equal-to. To make calculation easier we
 * are leveraging the fact that the cudf-result(r) always returns false. So that result is used in
 * place of false when needed.
 *
 * Return (lhs.nan && rhs.nan) || result[i]
 *
 *  +-------------+------------+------------------+---------------+----+
 *  |  lhs.isNan()|  rhs.isNan |   cudf-result(r) |  final-result | eq |
 *  +-------------+------------+------------------+---------------+----+
 *  |    t        |     f      |       f          |      r        | f  |
 *  |    f        |     t      |       f          |      r        | f  |
 *  |    t        |     t      |       f          |      t        | t  |
 *  |    f        |     f      |       r          |      r        | na |
 *  +-------------+------------+------------------+---------------+----+
 */
case class GpuEqualTo(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = "="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.EQUAL

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      rhs.getBase.isNan)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), Scalar.fromBool(lhs.isNan),
      rhs.getBase.isNan)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      Scalar.fromBool(rhs.isNan))
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    // Currently AST computeColumn assumes nulls compare true for EQUAL, but NOT_EQUAL will
    // return null for null input.
    new ast.UnaryOperation(ast.UnaryOperator.NOT,
      new ast.BinaryOperation(ast.BinaryOperator.NOT_EQUAL,
        left.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns),
        right.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)))
  }
}

case class GpuEqualNullSafe(left: Expression, right: Expression) extends CudfBinaryComparison
  with NullIntolerantShim {
  override def symbol: String = "<=>"
  override def nullable: Boolean = false
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.NULL_EQUALS

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      rhs.getBase.isNan)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), Scalar.fromBool(lhs.isNan),
      rhs.getBase.isNan())
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      Scalar.fromBool(rhs.isNan))
  }
}

/**
 * This implementation leverages the default implementation of equal-to on the GPU
 * to perform the binary equals comparison. This is used for operations like PivotFirst,
 * where NaN != NaN (unlike most other cases) when pivoting on a float or double column.
 */
case class GpuEqualToNoNans(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = "="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.EQUAL

}

/**
 * The table below shows how the result is calculated for greater-than. To make calculation easier
 * we are leveraging the fact that the cudf-result(r) always returns false. So that result is used
 * in place of false when needed.
 *
 * In this case return (lhs.nan && !lhs.nan) || result[i]
 *
 *  +-------------+------------+-----------------+---------------+----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | gt |
 *  +-------------+------------+-----------------+---------------+----+
 *  |    t        |     f      |      f          |      t        | t  |
 *  |    f        |     t      |      f          |      r        | f  |
 *  |    t        |     t      |      f          |      r        | f  |
 *  |    f        |     f      |      r          |      r        | na |
 *  +-------------+------------+-----------------+---------------+----+
 */
case class GpuGreaterThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = ">"

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.GREATER
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.GREATER)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      rhs.getBase.isNotNan)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNan,
      Scalar.fromBool(rhs.isNotNan))
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), Scalar.fromBool(lhs.isNan),
      rhs.getBase.isNotNan)
  }
}

/**
 * The table below shows how the result is calculated for Greater-than-Eq. To make calculation
 * easier we are leveraging the fact that the cudf-result(r) always returns false. So that result
 * is used in place of false when needed.
 *
 * In this case return lhs.isNan || result[i]
 *
 *  +-------------+------------+-----------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | gte |
 *  +-------------+------------+-----------------+---------------+-----+
 *  |    t        |     f      |      f          |      t        |   t |
 *  |    f        |     t      |      f          |      r        |   f |
 *  |    t        |     t      |      f          |      t        |   t |
 *  |    f        |     f      |      r          |      r        |  NA |
 *  +-------------+------------+-----------------+---------------+-----+
 */
case class GpuGreaterThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = ">="

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.GREATER_EQUAL
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.GREATER_EQUAL)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          lhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if(hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          lhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if ((lhs.getBase.getType == DType.FLOAT32 ||
         lhs.getBase.getType == DType.FLOAT64) && lhs.isNan) {
      withResource(Scalar.fromBool(true)) { trueScalar =>
        if (rhs.hasNull) {
          withResource(ColumnVector.fromScalar(trueScalar, rhs.getRowCount.toInt)) { trueVec =>
            trueVec.mergeAndSetValidity(BinaryOp.BITWISE_AND, rhs.getBase)
          }
        } else {
          ColumnVector.fromScalar(trueScalar, rhs.getRowCount.toInt)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

/**
 * The table below shows how the result is calculated for Less-than. To make calculation easier we
 * are leveraging the fact that the cudf-result(r) always returns false. So that result is used in
 * place of false when needed.
 *
 * In this case return !lhs.nan && rhs.nan || result[i]
 *
 *  +-------------+------------+-----------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | lt  |
 *  +-------------+------------+-----------------+---------------+-----+
 *  |    t        |     f      |      f          |      r        |   f |
 *  |    f        |     t      |      f          |      t        |   t |
 *  |    t        |     t      |      f          |      r        |   f |
 *  |    f        |     f      |      r          |      r        |  NA |
 *  +-------------+------------+-----------------+---------------+-----+
 */
case class GpuLessThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = "<"

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.LESS
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.LESS)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNotNan,
      rhs.getBase.isNan)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), lhs.getBase.isNotNan,
      Scalar.fromBool(rhs.isNan))
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    evaluateAndFixFloatingPointResult(super.doColumnar(lhs, rhs), Scalar.fromBool(lhs.isNotNan),
      rhs.getBase.isNan)
  }
}

/**
 * The table below shows how the result is calculated for Less-than-Eq. To make calculation easier
 * we are leveraging the fact that the cudf-result(r) always returns false. So that result is used
 * in place of false when needed.
 *
 * In this case, return rhs.nan || result[i]
 *
 *  +-------------+------------+------------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |   cudf-result(r) |  final-result | lte |
 *  +-------------+------------+------------------+---------------+-----+
 *  |    t        |     f      |       f          |      r        |   f |
 *  |    f        |     t      |       f          |      t        |   t |
 *  |    t        |     t      |       f          |      t        |   t |
 *  |    f        |     f      |       r          |      r        |  NA |
 *  +-------------+------------+------------------+---------------+-----+
 */
case class GpuLessThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerantShim {
  override def symbol: String = "<="

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.LESS_EQUAL
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.LESS_EQUAL)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(rhs.getBase.isNan) { rhsNan =>
          rhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if ((rhs.getBase.getType == DType.FLOAT32 ||
         rhs.getBase.getType == DType.FLOAT64) && rhs.isNan) {
      withResource(Scalar.fromBool(true)) { trueScalar =>
        if (lhs.hasNull) {
          withResource(ColumnVector.fromScalar(trueScalar, lhs.getRowCount.toInt)) { trueVec =>
            trueVec.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs.getBase)
          }
        } else {
          ColumnVector.fromScalar(trueScalar, lhs.getRowCount.toInt)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(rhs.getBase.isNan) { rhsNan =>
          rhsNan.or(result)
        }
      }
    } else {
      result
    }
  }
}
