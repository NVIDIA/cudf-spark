/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.util.function.Consumer

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.CaseWhen
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuExpressionWithSideEffectUtils {

  /**
   * Returns true only if all rows are true. Nulls are considered false.
   */
  def isAllTrue(col: GpuColumnVector): Boolean = {
    assert(BooleanType == col.dataType())
    if (col.getRowCount == 0) {
      return true
    }
    if (col.hasNull) {
      return false
    }
    withResource(col.getBase.all()) { allTrue =>
      // Guaranteed there is at least one row and no nulls so result must be valid
      allTrue.getBoolean
    }
  }

  /**
   * Used to shortcircuit predicates and filter conditions.
   *
   * @param nullsAsFalse when true, null values are considered false.
   * @param col the input being evaluated.
   * @return boolean. When nullsAsFalse is set, it returns True if none of the rows is true;
   *         Otherwise, returns true if at least one row exists and all rows are false.   
   */
  def isAllFalse(col: GpuColumnVector, nullsAsFalse: Boolean = true): Boolean = {
    assert(BooleanType == col.dataType())
    if (nullsAsFalse) {
      if (col.getRowCount == col.numNulls()) {
        return true
      }
    } else if (col.hasNull() || col.getRowCount == 0) {
      return false
    }
    withResource(col.getBase.any()) { anyTrue =>
      // null values are considered false values in the context of nullsAsFalse true
      !anyTrue.getBoolean
    }
  }

  def filterBatch(
      tbl: Table,
      pred: ColumnVector,
      colTypes: Array[DataType]): ColumnarBatch = {
    withResource(tbl.filter(pred)) { filteredData =>
      GpuColumnVector.from(filteredData, colTypes)
    }
  }

  private def boolToInt(cv: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(1, DataTypes.IntegerType)) { one =>
      withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
        cv.ifElse(one, zero)
      }
    }
  }

  /**
   * Invert boolean values and convert null values to true
   */
  def boolInverted(cv: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(true, DataTypes.BooleanType)) { t =>
      withResource(GpuScalar.from(false, DataTypes.BooleanType)) { f =>
        cv.ifElse(f, t)
      }
    }
  }

  def gather(predicate: ColumnVector, t: GpuColumnVector): ColumnVector = {
    // convert the predicate boolean column to numeric where 1 = true
    // and 0 (or null) = false and then use `scan` with `sum` to convert to
    // indices.
    //
    // For example, if the predicate evaluates to [F, null, T, F, T] then this
    // gets translated first to [0, 0, 1, 0, 1] and then the scan operation
    // will perform an exclusive sum on these values and
    // produce [0, 0, 0, 1, 1]. Combining this with the original
    // predicate boolean array results in the two T values mapping to
    // indices 0 and 1, respectively.

    val prefixSumExclusive = withResource(boolToInt(predicate)) { boolsAsInts =>
      boolsAsInts.scan(
        ScanAggregation.sum(),
        ScanType.EXCLUSIVE,
        NullPolicy.INCLUDE)
    }
    val gatherMap = withResource(prefixSumExclusive) { prefixSumExclusive =>
      // for the entries in the gather map that do not represent valid
      // values to be gathered, we change the value to -MAX_INT which
      // will be treated as null values in the gather algorithm
      withResource(Scalar.fromInt(Int.MinValue)) {
        outOfBoundsFlag => predicate.ifElse(prefixSumExclusive, outOfBoundsFlag)
      }
    }
    withResource(gatherMap) { _ =>
      withResource(new Table(t.getBase)) { tbl =>
        withResource(tbl.gather(gatherMap)) { gatherTbl =>
          gatherTbl.getColumn(0).incRefCount()
        }
      }
    }
  }

  def replaceNulls(cv: ColumnVector, bool: Boolean) : ColumnVector = {
    if (!cv.hasNulls) {
      return cv.incRefCount()
    }
    withResource(Scalar.fromBool(bool)) { booleanScalar =>
      cv.replaceNulls(booleanScalar)
    }
  }

  def shortCircuitWithBool(gpuCV: GpuColumnVector, bool: Boolean) : GpuColumnVector = {
    withResource(GpuScalar.from(bool, BooleanType)) { boolScalar =>
      GpuColumnVector.from(boolScalar, gpuCV.getRowCount.toInt, BooleanType)
    }
  }
}

trait GpuConditionalExpression extends ComplexTypeMergingExpression with GpuExpression
  with ShimExpression {

  protected def computeIfElse(
      batch: ColumnarBatch,
      predExpr: Expression,
      trueExpr: Expression,
      falseValue: Any): GpuColumnVector = {
    withResourceIfAllowed(falseValue) { falseRet =>
      withResource(predExpr.columnarEval(batch)) { pred =>
        withResourceIfAllowed(trueExpr.columnarEvalAny(batch)) { trueRet =>
          val finalRet = (trueRet, falseRet) match {
            case (t: GpuColumnVector, f: GpuColumnVector) =>
              pred.getBase.ifElse(t.getBase, f.getBase)
            case (t: GpuScalar, f: GpuColumnVector) =>
              pred.getBase.ifElse(t.getBase, f.getBase)
            case (t: GpuColumnVector, f: GpuScalar) =>
              pred.getBase.ifElse(t.getBase, f.getBase)
            case (t: GpuScalar, f: GpuScalar) =>
              pred.getBase.ifElse(t.getBase, f.getBase)
            case (t, f) =>
              throw new IllegalStateException(s"Unexpected inputs" +
                s" ($t: ${t.getClass}, $f: ${f.getClass})")
          }
          GpuColumnVector.from(finalRet, dataType)
        }
      }
    }
  }
}

case class GpuIf(
    predicateExpr: Expression,
    trueExpr: Expression,
    falseExpr: Expression) extends GpuConditionalExpression {

  import GpuExpressionWithSideEffectUtils._

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueExpr.dataType, falseExpr.dataType)
  }

  override def children: Seq[Expression] = predicateExpr :: trueExpr :: falseExpr :: Nil
  override def nullable: Boolean = trueExpr.nullable || falseExpr.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicateExpr.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in If should be boolean, " +
          s"not ${predicateExpr.dataType.catalogString}")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueExpr.dataType.catalogString} and ${falseExpr.dataType.catalogString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {

    val gpuTrueExpr = trueExpr.asInstanceOf[GpuExpression]
    val gpuFalseExpr = falseExpr.asInstanceOf[GpuExpression]
    val trueExprHasSideEffects = gpuTrueExpr.hasSideEffects
    val falseExprHasSideEffects = gpuFalseExpr.hasSideEffects

    withResource(predicateExpr.columnarEval(batch)) { pred =>
      // It is unlikely that pred is all true or all false, and in many cases it is as expensive
      // to calculate isAllTrue as it would be to calculate the expression so only do it when
      // it would help with side effect processing, because that is very expensive to do.
      if (falseExprHasSideEffects && isAllTrue(pred)) {
        trueExpr.columnarEval(batch)
      } else if (trueExprHasSideEffects && isAllFalse(pred)) {
        falseExpr.columnarEval(batch)
      } else if (trueExprHasSideEffects || falseExprHasSideEffects) {
        conditionalWithSideEffects(batch, pred, gpuTrueExpr, gpuFalseExpr)
      } else {
        withResourceIfAllowed(trueExpr.columnarEvalAny(batch)) { trueRet =>
          withResourceIfAllowed(falseExpr.columnarEvalAny(batch)) { falseRet =>
            val finalRet = (trueRet, falseRet) match {
              case (t: GpuColumnVector, f: GpuColumnVector) =>
                pred.getBase.ifElse(t.getBase, f.getBase)
              case (t: GpuScalar, f: GpuColumnVector) =>
                pred.getBase.ifElse(t.getBase, f.getBase)
              case (t: GpuColumnVector, f: GpuScalar) =>
                pred.getBase.ifElse(t.getBase, f.getBase)
              case (t: GpuScalar, f: GpuScalar) =>
                pred.getBase.ifElse(t.getBase, f.getBase)
              case (t, f) =>
                throw new IllegalStateException(s"Unexpected inputs" +
                  s" ($t: ${t.getClass}, $f: ${f.getClass})")
            }
            GpuColumnVector.from(finalRet, dataType)
          }
        }
      }
    }
  }

  /**
   * When computing conditional expressions on the CPU, the true and false
   * expressions are evaluated lazily, meaning that the true expression is
   * only evaluated for rows where the predicate is true, and the false
   * expression is only evaluated for rows where the predicate is false.
   * This is important in the case where the expressions can have
   * side-effects, such as throwing exceptions for invalid inputs.
   *
   * This method performs lazy evaluation on the GPU by first filtering the
   * input batch into two batches - one for rows where the predicate is true
   * and one for rows where the predicate is false. The expressions are
   * evaluated against these batches and then the results are combined
   * back into a single batch using the gather algorithm.
   */
  private def conditionalWithSideEffects(
      batch: ColumnarBatch,
      pred: GpuColumnVector,
      gpuTrueExpr: GpuExpression,
      gpuFalseExpr: GpuExpression): GpuColumnVector = {

    val colTypes = GpuColumnVector.extractTypes(batch)

    withResource(GpuColumnVector.from(batch)) { tbl =>
      // Use boolInverted instead of NOT so null predicates become true,
      // routing null-condition rows to the false branch (matching CPU
      // semantics where null is treated as "not true").
      withResource(boolInverted(pred.getBase)) { inverted =>
        // evaluate true expression against true batch
        val tt = withResource(filterBatch(tbl, pred.getBase, colTypes)) { trueBatch =>
          gpuTrueExpr.columnarEvalAny(trueBatch)
        }
        withResourceIfAllowed(tt) { _ =>
          // evaluate false expression against false batch
          val ff = withResource(filterBatch(tbl, inverted, colTypes)) { falseBatch =>
            gpuFalseExpr.columnarEvalAny(falseBatch)
          }
          withResourceIfAllowed(ff) { _ =>
            val finalRet = (tt, ff) match {
              case (t: GpuColumnVector, f: GpuColumnVector) =>
                withResource(gather(pred.getBase, t)) { trueValues =>
                  withResource(gather(inverted, f)) { falseValues =>
                    pred.getBase.ifElse(trueValues, falseValues)
                  }
                }
              case (t: GpuScalar, f: GpuColumnVector) =>
                withResource(gather(inverted, f)) { falseValues =>
                  pred.getBase.ifElse(t.getBase, falseValues)
                }
              case (t: GpuColumnVector, f: GpuScalar) =>
                withResource(gather(pred.getBase, t)) { trueValues =>
                  pred.getBase.ifElse(trueValues, f.getBase)
                }
              case (_: GpuScalar, _: GpuScalar) =>
                throw new IllegalStateException(
                  "scalar expressions can never have side effects")
            }
            GpuColumnVector.from(finalRet, dataType)
          }
        }
      }
    }
  }

  override def toString: String = s"if ($predicateExpr) $trueExpr else $falseExpr"

  override def sql: String = s"(IF(${predicateExpr.sql}, ${trueExpr.sql}, ${falseExpr.sql}))"
}


case class GpuCaseWhen(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression] = None,
    caseWhenFuseEnabled: Boolean = true)
    extends GpuConditionalExpression with Serializable {

  import GpuExpressionWithSideEffectUtils._

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    branches.map(_._2.dataType) ++ elseValue.map(_.dataType)
  }

  private lazy val haveSideEffects =
    branches.exists(_._2.asInstanceOf[GpuExpression].hasSideEffects) ||
      elseValue.exists(_.asInstanceOf[GpuExpression].hasSideEffects)

  override def nullable: Boolean = {
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.forall(_.nullable)
  }

  private lazy val useFusion = caseWhenFuseEnabled && branches.size > 2 &&
    isCaseWhenFusionSupportedType(inputTypesForMerging.head) &&
    (branches.map(_._2) ++ elseValue).forall(_.isInstanceOf[GpuLiteral])

  override def checkInputDataTypes(): TypeCheckResult = {
    if (TypeCoercion.haveSameType(inputTypesForMerging)) {
      // Make sure all branch conditions are boolean types.
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      val branchesStr = branches.map(_._2.dataType).map(dt => s"WHEN ... THEN ${dt.catalogString}")
        .mkString(" ")
      val elseStr = elseValue.map(expr => s" ELSE ${expr.dataType.catalogString}").getOrElse("")
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type," +
          s" got CASE $branchesStr$elseStr END")
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (haveSideEffects) {
      columnarEvalWithSideEffects(batch)
    } else {
      if (useFusion) {
        // when branches size > 2;
        // return type is supported types: Boolean/Byte/Int/String/Decimal ...
        // all the then and else expressions are Scalars.
        // Avoid to use multiple `computeIfElse`s which will create multiple temp columns

        // 1. select first true index from bool columns, if no true, index will be out of bound
        // e.g.:
        //  case when bool result column 0: true,  false, false
        //  case when bool result column 1: false, true,  false
        //  result is: [0, 1, 2]
        val whenBoolCols = branches.safeMap(_._1.columnarEval(batch).getBase).toArray
        val firstTrueIndex: ColumnVector = withResource(whenBoolCols) { _ =>
          CaseWhen.selectFirstTrueIndex(whenBoolCols)
        }

        withResource(firstTrueIndex) { _ =>
          val thenElseScalars = (branches.map(_._2) ++ elseValue).map(_.columnarEvalAny(batch)
              .asInstanceOf[GpuScalar])
          withResource(thenElseScalars) { _ =>
            // 2. generate a column to store all scalars
            withResource(createFromScalarList(thenElseScalars)) {
              scalarCol =>
                val finalRet = withResource(new Table(scalarCol)) { oneColumnTable =>
                  // 3. execute final select
                  // default gather OutOfBoundsPolicy is nullify,
                  // If index is out of bound, return null
                  withResource(oneColumnTable.gather(firstTrueIndex)) { resultTable =>
                    resultTable.getColumn(0).incRefCount()
                  }
                }
                // return final column vector
                GpuColumnVector.from(finalRet, dataType)
            }
          }
        }
      } else {
        // execute from tail to front recursively
        // `elseRet` will be closed in `computeIfElse`.
        val elseRet = elseValue
          .map(_.columnarEvalAny(batch))
          .getOrElse(GpuScalar(null, branches.last._2.dataType))
        val any = branches.foldRight[Any](elseRet) {
          case ((predicateExpr, trueExpr), falseRet) =>
            computeIfElse(batch, predicateExpr, trueExpr, falseRet)
        }
        GpuExpressionsUtils.resolveColumnVector(any, batch.numRows())
      }
    }
  }

  /**
   * Perform lazy evaluation of each branch so that we only evaluate the THEN expressions
   * against rows where the WHEN expression is true.
   */
  private def columnarEvalWithSideEffects(batch: ColumnarBatch): GpuColumnVector = {
    val colTypes = GpuColumnVector.extractTypes(batch)

    // track cumulative state of predicate evaluation per row so that we never evaluate expressions
    // for a row if an earlier expression has already been evaluated to true for that row
    var cumulativePred: Option[GpuColumnVector] = None

    // this variable contains the currently evaluated value for each row and gets updated
    // as each branch is evaluated
    var currentValue: Option[GpuColumnVector] = None

    try {
      withResource(GpuColumnVector.from(batch)) { tbl =>

        // iterate over the WHEN THEN branches first
        branches.foreach {
          case (whenExpr, thenExpr) =>
            // evaluate the WHEN predicate
            withResource(whenExpr.columnarEval(batch)) { whenBool =>
              // we only want to evaluate where this WHEN is true and no previous WHEN has been true
              val firstTrueWhen = isFirstTrueWhen(cumulativePred, whenBool)

              withResource(firstTrueWhen) { _ =>
                if (isAllTrue(firstTrueWhen)) {
                  // if this WHEN predicate is true for all rows and no previous predicate has
                  // been true then we can return immediately
                  return thenExpr.columnarEval(batch)
                }
                val thenValues = filterEvaluateWhenThen(colTypes, tbl, firstTrueWhen.getBase,
                  thenExpr)
                withResource(thenValues) { _ =>
                  currentValue = Some(calcCurrentValue(currentValue, firstTrueWhen, thenValues))
                }
                cumulativePred = Some(calcCumulativePredicate(
                  cumulativePred, whenBool, firstTrueWhen))

                if (isAllTrue(cumulativePred.get)) {
                  // no need to process any more branches or the else condition
                  return currentValue.get.incRefCount()
                }
              }
            }
        }

        // invert the cumulative predicate to get the ELSE predicate
        withResource(boolInverted(cumulativePred.get.getBase)) { elsePredNoNulls =>
          elseValue match {
            case Some(expr) =>
              if (isAllFalse(cumulativePred.get)) {
                expr.columnarEval(batch)
              } else {
                val elseValues = filterEvaluateWhenThen(colTypes, tbl, elsePredNoNulls, expr)
                withResource(elseValues) { _ =>
                  GpuColumnVector.from(elsePredNoNulls.ifElse(
                    elseValues, currentValue.get.getBase), dataType)
                }
              }

            case None =>
              // if there is no ELSE condition then we return NULL for any rows not matched by
              // previous branches
              withResource(GpuScalar.from(null, dataType)) { nullScalar =>
                if (isAllFalse(cumulativePred.get)) {
                  GpuColumnVector.from(nullScalar, elsePredNoNulls.getRowCount.toInt, dataType)
                } else {
                  GpuColumnVector.from(
                    elsePredNoNulls.ifElse(nullScalar, currentValue.get.getBase),
                      dataType)
                }
              }
          }
        }
      }
    } finally {
      currentValue.foreach(_.safeClose())
      cumulativePred.foreach(_.safeClose())
    }
  }

  /**
   * Filter the batch to just the rows where the WHEN condition is true and
   * then evaluate the THEN expression.
   */
  private def filterEvaluateWhenThen(
      colTypes: Array[DataType],
      tbl: Table,
      whenBool: ColumnVector,
      thenExpr: Expression): ColumnVector = {
    val filteredBatch = filterBatch(tbl, whenBool, colTypes)
    val thenValues = withResource(filteredBatch) { trueBatch =>
      thenExpr.columnarEval(trueBatch)
    }
    withResource(thenValues) { _ =>
      gather(whenBool, thenValues)
    }
  }

  /**
   * Calculate the cumulative predicate so far using the logical expression
   * `prevPredicate OR thisPredicate`.
   */
  private def calcCumulativePredicate(
      cumulativePred: Option[GpuColumnVector],
      whenBool: GpuColumnVector,
      firstTrueWhen: GpuColumnVector): GpuColumnVector = {
    cumulativePred match {
      case Some(prev) =>
        withResource(prev) { _ =>
          val result = prev.getBase.binaryOp(BinaryOp.NULL_LOGICAL_OR,
            whenBool.getBase, DType.BOOL8)
          GpuColumnVector.from(result, DataTypes.BooleanType)
        }
      case _ =>
        firstTrueWhen.incRefCount()
    }
  }

  /**
   * Calculate the current values by merging the THEN values for this branch (where the WHEN
   * predicate was true) with the previous values.
   */
  private def calcCurrentValue(
      prevValue: Option[GpuColumnVector],
      whenBool: GpuColumnVector,
      thenValues: ColumnVector): GpuColumnVector = {
    prevValue match {
      case Some(v) =>
        withResource(v) { _ =>
          GpuColumnVector.from(whenBool.getBase.ifElse(thenValues, v.getBase), dataType)
        }
      case _ =>
        GpuColumnVector.from(thenValues.incRefCount(), dataType)
    }
  }

  /**
   * Determine for each row whether this is the first WHEN predicate so far to evaluate to true
   */
  private def isFirstTrueWhen(
      cumulativePred: Option[GpuColumnVector],
      whenBool: GpuColumnVector): GpuColumnVector = {
    cumulativePred match {
      case Some(prev) =>
        withResource(boolInverted(prev.getBase)) { notPrev =>
          withResource(replaceNulls(whenBool.getBase, false)) { whenReplaced =>
            GpuColumnVector.from(whenReplaced.and(notPrev), DataTypes.BooleanType)
          }
        }
      case None =>
        whenBool.incRefCount()
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  private def isCaseWhenFusionSupportedType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case StringType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  /**
   * Create column vector from scalars
   *
   * @param scalars literals
   * @return column vector for the specified scalars
   */
  private def createFromScalarList(scalars: Seq[GpuScalar]): ColumnVector = {
    scalars.head.dataType match {
      case BooleanType =>
        val booleans = scalars.map(s => s.getValue.asInstanceOf[java.lang.Boolean])
        ColumnVector.fromBoxedBooleans(booleans: _*)
      case ByteType =>
        val bytes = scalars.map(s => s.getValue.asInstanceOf[java.lang.Byte])
        ColumnVector.fromBoxedBytes(bytes: _*)
      case ShortType =>
        val shorts = scalars.map(s => s.getValue.asInstanceOf[java.lang.Short])
        ColumnVector.fromBoxedShorts(shorts: _*)
      case IntegerType =>
        val ints = scalars.map(s => s.getValue.asInstanceOf[java.lang.Integer])
        ColumnVector.fromBoxedInts(ints: _*)
      case LongType =>
        val longs = scalars.map(s => s.getValue.asInstanceOf[java.lang.Long])
        ColumnVector.fromBoxedLongs(longs: _*)
      case FloatType =>
        val floats = scalars.map(s => s.getValue.asInstanceOf[java.lang.Float])
        ColumnVector.fromBoxedFloats(floats: _*)
      case DoubleType =>
        val doubles = scalars.map(s => s.getValue.asInstanceOf[java.lang.Double])
        ColumnVector.fromBoxedDoubles(doubles: _*)
      case StringType =>
        val utf8Bytes = scalars.map(s => {
          val v = s.getValue
          if (v == null) {
            null
          } else {
            v.asInstanceOf[UTF8String].getBytes
          }
        })
        ColumnVector.fromUTF8Strings(utf8Bytes: _*)
      case dt: DecimalType =>
        val decimals = scalars.map(s => {
          val v = s.getValue
          if (v == null) {
            null
          } else {
            v.asInstanceOf[Decimal].toJavaBigDecimal
          }
        })
        fromDecimals(dt, decimals: _*)
      case _ =>
        throw new UnsupportedOperationException(s"Creating column vector from a GpuScalar list" +
            s" is not supported for type ${scalars.head.dataType}.")
    }
  }

  /**
   * Create decimal column vector according to DecimalType.
   * Note: it will create 3 types of column vector according to DecimalType precision
   *  - Decimal 32 bits
   *  - Decimal 64 bits
   *  - Decimal 128 bits
   *    E.g.: If the max of values are decimal 32 bits, but DecimalType is 128 bits,
   *    then return a Decimal 128 bits column vector
   */
  private def fromDecimals(dt: DecimalType, values: java.math.BigDecimal*): ColumnVector = {
    val hcv = HostColumnVector.build(
      DecimalUtil.createCudfDecimal(dt),
      values.length,
      new Consumer[HostColumnVector.Builder]() {
        override def accept(b: HostColumnVector.Builder): Unit = {
          b.appendBoxed(values: _*)
        }
      }
    )
    withResource(hcv) { _ =>
      hcv.copyToDevice()
    }
  }
}
