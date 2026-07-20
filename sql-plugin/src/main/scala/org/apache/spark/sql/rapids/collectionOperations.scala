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

package org.apache.spark.sql.rapids

import java.util.Optional

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, DuplicateKeepOption, ReductionAggregation, Scalar, SegmentedReductionAggregation, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.ArrayIndexUtils.firstIndexAndNumElementUnchecked
import com.nvidia.spark.rapids.BoolUtils.isAllValidTrue
import com.nvidia.spark.rapids.GpuListUtils
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{GpuListSliceUtils, MapUtils}
import com.nvidia.spark.rapids.shims.{GetSequenceSize, NullIntolerantShim, ShimExpression}

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ArraySort, ElementAt, ExpectsInputTypes, Expression, ImplicitCastInputTypes, LambdaFunction, NamedExpression, RowOrdering, Sequence, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

case class GpuConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  @transient override lazy val dataType: DataType = {
    if (children.isEmpty) {
      StringType
    } else {
      super.dataType
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val res = dataType match {
      // in Spark concat() will be considered as an empty string here
      case dt if children.isEmpty => GpuScalar("", dt)
      // For single column concat, we pass the result of child node to avoid extra cuDF call.
      case _ if children.length == 1 => children.head.columnarEval(batch)
      case StringType => stringConcat(batch)
      case ArrayType(_, _) => listConcat(batch)
      case _ => throw new IllegalArgumentException(s"unsupported dataType $dataType")
    }
    GpuExpressionsUtils.resolveColumnVector(res, batch.numRows())
  }

  private def stringConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
      // run string concatenate
      GpuColumnVector.from(
        cudf.ColumnVector.stringConcatenate(cols.toArray[ColumnView]), StringType)
    }
  }

  private def listConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
      // run list concatenate
      GpuColumnVector.from(cudf.ColumnVector.listConcatenateByRow(cols: _*), dataType)
    }
  }
}

case class GpuMapConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  override lazy val hasSideEffects: Boolean =
    GpuCreateMap.exceptionOnDupKeys || super.hasSideEffects

  @transient override lazy val dataType: MapType = {
    if (children.isEmpty) {
      MapType(StringType, StringType)
    } else {
      super.dataType.asInstanceOf[MapType]
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    (dataType, children.length) match {
      // Explicitly return null for empty concat as Spark, since cuDF doesn't support empty concat.
      case (dt, 0) => GpuColumnVector.fromNull(batch.numRows(), dt)
      // For single column concat, we pass the result of child node to avoid extra cuDF call.
      case (_, 1) => children.head.columnarEval(batch)
      case (_, _) => {
        withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
          withResource(cudf.ColumnVector.listConcatenateByRow(cols: _*)) {structs =>
            GpuCreateMap.createMapFromKeysValuesAsStructs(dataType, structs)
          }
        }
      }
    }
}

case class GpuSlice(x: Expression, start: Expression, length: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def dataType: DataType = x.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, IntegerType, IntegerType)
  override def prettyName: String = "slice"

  override def first: Expression = x
  override def second: Expression = start
  override def third: Expression = length

  override def doColumnar(numRows: Int, listS: GpuScalar, startS: GpuScalar,
      lengthS: GpuScalar): ColumnVector = {
    // When either start or length is null, return all nulls like the CPU does.
    if (!startS.isValid || !lengthS.isValid) {
      GpuColumnVector.columnVectorFromNull(numRows, dataType)
    } else {
      withResource(GpuColumnVector.from(listS, numRows)) { listCol =>
        doColumnar(listCol, startS, lengthS)
      }
    }
  }

  override def doColumnar(listS: GpuScalar, startS: GpuScalar,
      lengthCol: GpuColumnVector): ColumnVector = {
    val numRows = lengthCol.getRowCount.toInt
    // When start is null, return all nulls like the CPU does.
    if (!startS.isValid) {
      GpuColumnVector.columnVectorFromNull(numRows, dataType)
    } else {
      withResource(GpuColumnVector.from(listS, numRows)) { listCol =>
        doColumnar(listCol, startS, lengthCol)
      }
    }
  }

  override def doColumnar(listS: GpuScalar, startCol: GpuColumnVector,
      lengthS: GpuScalar): ColumnVector = {
    val numRows = startCol.getRowCount.toInt
    // When length is null, return all nulls like the CPU does.
    if (!lengthS.isValid) {
      GpuColumnVector.columnVectorFromNull(numRows, dataType)
    } else {
      withResource(GpuColumnVector.from(listS, numRows)) { listCol =>
        doColumnar(listCol, startCol, lengthS)
      }
    }
  }

  override def doColumnar(listS: GpuScalar, startCol: GpuColumnVector,
      lengthCol: GpuColumnVector): ColumnVector = {
    val numRows = startCol.getRowCount.toInt
    withResource(GpuColumnVector.from(listS, numRows)) { listCol =>
      doColumnar(listCol, startCol, lengthCol)
    }
  }

  override def doColumnar(listCol: GpuColumnVector, startS: GpuScalar,
      lengthS: GpuScalar): ColumnVector = {
    // When either start or length is null, return all nulls like the CPU does.
    if (listCol.getRowCount == listCol.numNulls() || !startS.isValid || !lengthS.isValid) {
      GpuColumnVector.columnVectorFromNull(listCol.getRowCount.toInt, dataType)
    } else {
      val list = listCol.getBase
      val start = startS.getValue.asInstanceOf[Int]
      val length = lengthS.getValue.asInstanceOf[Int]
      if (start == 0) {
        throw RapidsErrorUtils.unexpectedValueForStartInFunctionError(prettyName)
      }
      if (length < 0) {
        throw RapidsErrorUtils.unexpectedValueForLengthInFunctionError(prettyName, length)
      }
      GpuListSliceUtils.listSlice(list, start, length, false)
    }
  }

  override def doColumnar(listCol: GpuColumnVector, startS: GpuScalar,
      lengthCol: GpuColumnVector): ColumnVector = {
    // When list, start or length is null, return all nulls like the CPU does.
    if (listCol.getRowCount == listCol.numNulls() ||
        lengthCol.getRowCount == lengthCol.numNulls() ||
        !startS.isValid) {
      GpuColumnVector.columnVectorFromNull(listCol.getRowCount.toInt, dataType)
    } else {
      val list = listCol.getBase
      val start = startS.getValue.asInstanceOf[Int]
      if (start == 0) {
        throw RapidsErrorUtils.unexpectedValueForStartInFunctionError(prettyName)
      }
      // keep null as original, or null it out if `list[i]`  is null
      withResource(NullUtilities.mergeNulls(lengthCol.getBase, list)) { length =>
        withResource(length.min()) { minLen =>
          if (minLen.isValid && minLen.getInt < 0) {
            throw RapidsErrorUtils.unexpectedValueForLengthInFunctionError(prettyName,
              minLen.getInt)
          }
        }
        GpuListSliceUtils.listSlice(list, start, length, false)
      }
    }
  }

  override def doColumnar(listCol: GpuColumnVector, startCol: GpuColumnVector,
      lengthS: GpuScalar): ColumnVector = {
    // When list, start or length is null, return all nulls like the CPU does.
    if (listCol.getRowCount == listCol.numNulls() ||
        startCol.getRowCount == startCol.numNulls() ||
        !lengthS.isValid) {
      GpuColumnVector.columnVectorFromNull(listCol.getRowCount.toInt, dataType)
    } else {
      val list = listCol.getBase
      val start = startCol.getBase
      val length = lengthS.getValue.asInstanceOf[Int]
      withResource(Scalar.fromInt(0)) { zero =>
        if (start.contains(zero)) {
          throw RapidsErrorUtils.unexpectedValueForStartInFunctionError(prettyName)
        }
      }
      if (length < 0) {
        // we know not all rows in list/start are nulls since we checked for that at the
        // beginning of this function, and length is scalar: for _some_ rows we will have
        // non-null list/start, and a guaranteed negative index.
        throw RapidsErrorUtils.unexpectedValueForLengthInFunctionError(prettyName, length)
      }
      GpuListSliceUtils.listSlice(list, start, length, false)
    }
  }

  override def doColumnar(listCol: GpuColumnVector, startCol: GpuColumnVector,
      lengthCol: GpuColumnVector): ColumnVector = {
    // When list, start or length is null, return all nulls like the CPU does.
    if (listCol.getRowCount == listCol.numNulls() ||
        startCol.getRowCount == startCol.numNulls() ||
        lengthCol.getRowCount == lengthCol.numNulls()) {
      GpuColumnVector.columnVectorFromNull(listCol.getRowCount.toInt, dataType)
    } else {
      val list = listCol.getBase
      val start = startCol.getBase
      withResource(Scalar.fromInt(0)) { zero =>
        if (start.contains(zero)) {
          throw RapidsErrorUtils.unexpectedValueForStartInFunctionError(prettyName)
        }
      }
      // keep null as original, or null it out if `list[i]` or `start[i]` is null
      withResource(NullUtilities.mergeNulls(lengthCol.getBase, list, start)) { length =>
        withResource(length.min()) { minLen =>
          if (minLen.isValid && minLen.getInt < 0) {
            throw RapidsErrorUtils.unexpectedValueForLengthInFunctionError(prettyName,
              minLen.getInt)
          }
        }
        GpuListSliceUtils.listSlice(list, start, length, false)
      }
    }
  }
}

case class GpuArrayJoin(override val children : Seq[Expression])
  extends GpuExpression with ShimExpression {

  private val array = children(0)
  private val delimiter = children(1)
  private val nullReplacement = children.lift(2)

  override def dataType: DataType = array.dataType.asInstanceOf[ArrayType].elementType
  override def prettyName: String = "array_join"
  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private def concatArrayCol(arrayColumn: ColumnView,
                             sepScalar: GpuScalar,
                             naScalarOpt: Option[GpuScalar]): GpuColumnVector = {

    val ret = if (!sepScalar.isValid) {
        // Null separator is not valid and we don't have the same way to control it
        // as we do with the column separator API. so just return all nulls.
        withResource(Scalar.fromNull(DType.STRING)) { nullString =>
          ColumnVector.fromScalar(nullString, arrayColumn.getRowCount.toInt)
        }
      } else {
        naScalarOpt match {
          case None =>
            // Nulls are treated as if they are not in the string
            withResource(Scalar.fromString("")) { emptyString =>
              arrayColumn.stringConcatenateListElements(sepScalar.getBase,
                emptyString, false, true)
            }
          case Some(nullReplacement) if nullReplacement.isValid =>
            // TODO when https://github.com/rapidsai/cudf/issues/12766 is fixed remove
            //  this workaround
            withResource(replaceNullChild(arrayColumn, nullReplacement.getBase)) { workAround =>
              workAround.stringConcatenateListElements(sepScalar.getBase,
                nullReplacement.getBase, true, true)
            }
          case Some(_) => // The null replacement is not valid, so the result is all nulls
            withResource(Scalar.fromNull(DType.STRING)) { nullString =>
              ColumnVector.fromScalar(nullString, arrayColumn.getRowCount.toInt)
            }
        }
      }
    GpuColumnVector.from(ret, dataType)
  }

  private def concatArrayCol(arrayColumn: ColumnView,
                             sepColumn: ColumnView,
                             naScalarOpt: Option[GpuScalar]): GpuColumnVector = {
    val ret = withResource(Scalar.fromNull(DType.STRING)) { nullString =>
      naScalarOpt match {
        case None =>
          // Nulls are treated as if they are not in the string
          withResource(Scalar.fromString("")) { emptyString =>
            arrayColumn.stringConcatenateListElements(sepColumn,
              nullString, emptyString, false, true)
          }
        case Some(nullReplacement) if nullReplacement.isValid =>
          // TODO when https://github.com/rapidsai/cudf/issues/12766 is fixed remove
          //  this workaround
          withResource(replaceNullChild(arrayColumn, nullReplacement.getBase)) { workAround =>
            workAround.stringConcatenateListElements(sepColumn,
              nullString, nullReplacement.getBase, true, true)
          }
        case Some(_) => // The null replacement is not valid, so the result is all nulls
          ColumnVector.fromScalar(nullString, arrayColumn.getRowCount.toInt)
      }
    }
    GpuColumnVector.from(ret, dataType)
  }

  private def replaceNullChild(arrayColumn: ColumnView, replacement: Scalar): ColumnVector = {
    withResource(arrayColumn.getChildColumnView(0)) { dataView =>
      val replacedData = withResource(dataView.isNull) { isNull =>
        isNull.ifElse(replacement, dataView)
      }
      withResource(replacedData) { _ =>
        withResource(GpuListUtils.replaceListDataColumnAsView(arrayColumn,
          replacedData)) { replacedView =>
          replacedView.copyToColumnVector()
        }
      }
    }
  }

  private def getNaScalarOpt(batch: ColumnarBatch): Option[GpuScalar] =
    nullReplacement match {
      case None => None
      case Some(expr) =>
        withResourceIfAllowed(expr.columnarEvalAny(batch)) {
          case g: GpuScalar =>
            Some(g.incRefCount)
          case other =>
            throw new IllegalStateException(s"Only scalars are " +
              s"supported for null replacement $other")
        }
    }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(array.columnarEval(batch)) { arrayColumn =>
      withResourceIfAllowed(delimiter.columnarEvalAny(batch)) { sep =>
        withResource(getNaScalarOpt(batch)) { naScalarOpt =>
          sep match {
            case sepColumn: GpuColumnVector =>
              concatArrayCol(arrayColumn.getBase, sepColumn.getBase, naScalarOpt)
            case sepScalar: GpuScalar =>
              concatArrayCol(arrayColumn.getBase, sepScalar, naScalarOpt)
            case other =>
              throw new IllegalStateException(s"Unexpected separator type $other")
          }
        }
      }
    }
  }
}

object GpuElementAtMeta {
  /**
   * Construct the expression rule for `ElementAt`.
   * @param nullOnInvalidAccessToMap
   *  Returns `null` or throws an exception on invalid access to map column.
   *  For Spark 3.4+ and DB11.3+, this argument is `true`,
   *  and for other Spark versions, it is `false`.
   */
  def elementAtRule(nullOnInvalidAccessToMap: Boolean): ExprRule[_ <: Expression] = {
    GpuOverrides.expr[ElementAt](
      "Returns element of array at given(1-based) index in value if column is array. " +
        "Returns value for the given key in value if column is map.",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all,
        ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
          TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY) +
          TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
            TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY)
            .withPsNote(TypeEnum.MAP, "If it's map, only primitive key types are supported."),
          TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
        ("index/key", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128)
          .withPsNote(
            Seq(TypeEnum.BOOLEAN, TypeEnum.BYTE, TypeEnum.SHORT, TypeEnum.LONG,
              TypeEnum.FLOAT, TypeEnum.DOUBLE, TypeEnum.DATE, TypeEnum.TIMESTAMP,
              TypeEnum.STRING, TypeEnum.DECIMAL), "Unsupported as array index."),
          TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // To distinguish the supported nested type between Array and Map
          val checks = in.left.dataType match {
            case _: MapType =>
              // Match exactly with the checks for GetMapValue
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                TypeSig.all,
                ("map",
                  TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
                    TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
                  TypeSig.MAP.nested(TypeSig.all)),
                ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all))
            case _: ArrayType =>
              // Match exactly with the checks for GetArrayItem
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                TypeSig.all,
                ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                  TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP +
                  TypeSig.BINARY),
                  TypeSig.ARRAY.nested(TypeSig.all)),
                ("ordinal", TypeSig.INT, TypeSig.INT))
            case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
          }
          checks.tag(this)
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          val failOnError = if (nullOnInvalidAccessToMap) {
            in.failOnError && lhs.dataType.isInstanceOf[ArrayType]
          } else {
            in.failOnError
          }
          GpuElementAt(lhs, rhs, failOnError)(in.origin)
        }
      })
  }
}

case class GpuElementAt(left: Expression, right: Expression, failOnError: Boolean)(
    override val origin: Origin = CurrentOrigin.get)
  extends GpuBinaryExpression with ExpectsInputTypes {

  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  override def hasSideEffects: Boolean = super.hasSideEffects || failOnError || {
    right match {
      case GpuLiteral(index: Int, _) if index != 0 => false
      case _ => true
    }
  }

  override lazy val dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (arr: ArrayType, e2: IntegralType) if e2 != LongType =>
        Seq(arr, IntegerType)
      case (MapType(keyType, valueType, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(keyType, e2) match {
          case Some(dt) => Seq(MapType(dt, valueType, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: ArrayType, e2) if e2 != IntegerType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${ArrayType.simpleString} followed by a ${IntegerType.simpleString}, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (MapType(e1, _, _), e2) if !e2.sameType(e1) =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${MapType.simpleString} followed by a value of same key type, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (e1, _) if !e1.isInstanceOf[MapType] && !e1.isInstanceOf[ArrayType] =>
        TypeCheckResult.TypeCheckFailure(s"The first argument to function $prettyName should " +
          s"have been ${ArrayType.simpleString} or ${MapType.simpleString} type, but its " +
          s"${left.dataType.catalogString} type.")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }

  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true

  @transient
  private lazy val doElementAtV: (ColumnVector, ColumnVector) => cudf.ColumnVector =
    left.dataType match {
      case _: ArrayType =>
        (array, indices) => {
          if (failOnError) {
            // Check if any index is out of bound only when ansi mode is enabled. Nulls in either
            // array or indices are skipped during this computation. Then no exception will be
            // raised if no valid entry (An entry is valid when both the array row and its index
            // are not null), the same with what Spark does.
            withResource(array.countElements()) { numElements =>
              val hasLargerIndices = withResource(indices.abs()) { absIndices =>
                absIndices.greaterThan(numElements)
              }
              withResource(hasLargerIndices) { _ =>
                if (BoolUtils.isAnyValidTrue(hasLargerIndices)) {
                  val (index, numElem) = firstIndexAndNumElementUnchecked(hasLargerIndices,
                    indices, numElements)
                  throw RapidsErrorUtils.invalidArrayIndexError(index, numElem, true)
                }
              }
            }
          } // end of "if (failOnError)"

          // convert to zero-based indices for positive values
          val indicesCol = withResource(Scalar.fromInt(0)) { zeroS =>
            // No exception should be raised if no valid entry (An entry is valid when both
            // the array row and its index are not null), the same with what Spark does.
            val hasValidEntryCV = indices.mergeAndSetValidity(BinaryOp.BITWISE_AND,
              indices, array)
            withResource(hasValidEntryCV) { _ =>
              if (hasValidEntryCV.contains(zeroS)) {
                throw RapidsErrorUtils.sqlArrayIndexNotStartAtOneError()
              }
            }
            val zeroBasedIndices = withResource(Scalar.fromInt(1)) { oneS =>
              indices.sub(oneS, indices.getType)
            }
            withResource(zeroBasedIndices) { _ =>
              withResource(indices.greaterThan(zeroS)) { hasPositiveIndices =>
                hasPositiveIndices.ifElse(zeroBasedIndices, indices)
              }
            }
          }
          withResource(indicesCol) { _ =>
            array.extractListElement(indicesCol)
          }
        }
      case _: MapType =>
        (map, indices) => {
          if (failOnError) {
            GpuMapUtils.getMapValueOrThrow(map, indices, right.dataType, origin)
          }
          else {
            map.getMapValue(indices)
          }
        }
    }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): cudf.ColumnVector =
    doElementAtV(lhs.getBase, rhs.getBase)

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): cudf.ColumnVector =
    withResource(ColumnVector.fromScalar(lhs.getBase, rhs.getRowCount.toInt)) { expandedCV =>
      doElementAtV(expandedCV, rhs.getBase)
    }

  @transient
  private lazy val doElementAtS: (ColumnView, GpuScalar) => cudf.ColumnVector =
    left.dataType match {
      case _: ArrayType =>
        (array, indexS) => {
          if (!indexS.isValid || array.getRowCount == array.getNullCount) {
            // Return nulls when index is null or all the array rows are null,
            // the same with what Spark does.
            GpuColumnVector.columnVectorFromNull(array.getRowCount.toInt, dataType)
          } else {
            // The index is valid, and array column contains at least one non-null row.
            val index = indexS.getValue.asInstanceOf[Int]
            if (failOnError) {
              // Check if index is out of bound only when ansi mode is enabled, since cuDF
              // returns nulls if index is out of bound, the same with what Spark does when
              // ansi mode is disabled.
              withResource(array.countElements()) { numElementsCV =>
                withResource(numElementsCV.min) { minScalar =>
                  val minNumElements = minScalar.getInt
                  if (math.abs(index) > minNumElements) {
                    throw RapidsErrorUtils.invalidArrayIndexError(index, minNumElements, true)
                  }
                }
              }
            }
            // convert to zero-based index if it is positive
            val idx = if (index == 0) {
              throw RapidsErrorUtils.sqlArrayIndexNotStartAtOneError()
            } else if (index > 0) {
              index - 1
            } else {
              index
            }
            array.extractListElement(idx)
          }
        }
      case MapType(keyType, _, _) =>
        (map, keyS) => {
          val key = keyS.getBase
          if (failOnError) {
            withResource(map.getMapKeyExistence(key)){ keyExistenceColumn =>
              withResource(keyExistenceColumn.all()) { exist =>
                if (!exist.isValid || exist.getBoolean) {
                  map.getMapValue(key)
                } else {
                  throw RapidsErrorUtils.mapKeyNotExistError(keyS.getValue.toString, keyType,
                    origin)
                }
              }
            }
          } else {
            map.getMapValue(key)
          }
        }
    }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector =
    doElementAtS(lhs.getBase, rhs)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector =
    withResource(ColumnVector.fromScalar(lhs.getBase, numRows)) { expandedCV =>
      doElementAtS(expandedCV, rhs)
    }

  override def prettyName: String = "element_at"
}

case class GpuSize(child: Expression, legacySizeOfNull: Boolean)
    extends GpuUnaryExpression {

  require(child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType],
    s"The size function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {

    // Compute sizes of cuDF.ListType to get sizes of each ArrayData or MapData, considering
    // MapData is represented as List of Struct in terms of cuDF.
    withResource(input.getBase.countElements()) { collectionSize =>
      if (legacySizeOfNull) {
        withResource(Scalar.fromInt(-1)) { nullScalar =>
          withResource(input.getBase.isNull) { inputIsNull =>
            inputIsNull.ifElse(nullScalar, collectionSize)
          }
        }
      } else {
        collectionSize.incRefCount()
      }
    }
  }
}

case class GpuReverse(child: Expression) extends GpuUnaryExpression {
  require(child.dataType.isInstanceOf[StringType] || child.dataType.isInstanceOf[ArrayType],
    s"The reverse function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = child.dataType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.reverseStringsOrLists()
  }
}

case class GpuMapKeys(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def prettyName: String = "map_keys"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getKeysAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapValues(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = {
    val mt = child.dataType.asInstanceOf[MapType]
    ArrayType(mt.valueType, containsNull = mt.valueContainsNull)
  }

  override def prettyName: String = "map_values"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getValuesAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapEntries(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  @transient private lazy val childDataType: MapType = child.dataType.asInstanceOf[MapType]

  override def dataType: DataType = {
    ArrayType(
      StructType(
        StructField("key", childDataType.keyType, false) ::
            StructField("value", childDataType.valueType, childDataType.valueContainsNull) ::
            Nil),
      false)
  }

  override def prettyName: String = "map_entries"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Internally the format for a list of key/value structs is the same, so just
    // return the same thing, and let Spark think it is a different type.
    input.getBase.incRefCount()
  }
}

case class GpuMapFromEntries(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  private val mapKeyDedupPolicy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient
  private lazy val dataTypeDetails: Option[(MapType, Boolean, Boolean)] = child.dataType match {
    case ArrayType(
      StructType(Array(
        StructField(_, keyType, keyNullable, _),
        StructField(_, valueType, valueNullable, _))),
        containsNull) =>
      Some((MapType(keyType, valueType, valueNullable), keyNullable, containsNull))
    case _ => None
  }

  @transient override lazy val dataType: MapType = dataTypeDetails.get._1

  override def checkInputDataTypes(): TypeCheckResult = {
    dataTypeDetails match {
      case Some((mapType, _, _)) =>
        TypeUtils.checkForMapKeyType(mapType.keyType)
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"The input to function $prettyName should be an array of struct<key, value>, " +
          s"but it's ${child.dataType.catalogString}")
    }
  }

  override def prettyName: String = "map_from_entries"

  @transient private lazy val nullEntries: Boolean = dataTypeDetails.get._3

  override def nullable: Boolean = child.nullable || nullEntries

  // The CPU also sets the `stateful` flag to say that this function should
  // not be evaluated multiple times for a single row, which looks to be an issue for
  // interpreted execution. Not an issue for GPU execution.
  // override def stateful: Boolean = true

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // MapUtils handles Spark's null-struct-entry semantics and null-key validation.
    // Valid inputs can be returned zero-copy because Spark MAP<K,V> and
    // cuDF LIST<STRUCT<K,V>> share the same physical layout.
    val inputBase = input.getBase
    val effective: cudf.ColumnVector =
      if (MapUtils.isValidMap(inputBase, /*throwOnNullKey=*/true)) {
        inputBase.incRefCount()
      } else {
        MapUtils.mapFromEntries(inputBase, /*throwOnNullKey=*/true)
      }
    withResource(effective) { eff =>
      dedupByPolicy(eff)
    }
  }

  private def dedupByPolicy(col: cudf.ColumnVector): cudf.ColumnVector = {
    // Spark 4.1+ returns an enum value instead of String, so use toString first.
    mapKeyDedupPolicy.toString.toUpperCase match {
      case "EXCEPTION" =>
        // Check if any duplicate keys were removed
        withResource(col.dropListDuplicatesWithKeysValues()) { deduped =>
          withResource(deduped.getChildColumnView(0)) { dedupedChild =>
            withResource(col.getChildColumnView(0)) { originalChild =>
              if (dedupedChild.getRowCount != originalChild.getRowCount) {
                throw GpuMapUtils.duplicateMapKeyFoundError
              }
            }
          }
        }
        col.incRefCount()
      case "LAST_WIN" =>
        col.dropListDuplicatesWithKeysValues()
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported map key deduplication policy: $other")
    }
  }
}

case class GpuSortArray(base: Expression, ascendingOrder: Expression)
    extends GpuBinaryExpressionArgsAnyScalar with ExpectsInputTypes {

  override def left: Expression = base

  override def right: Expression = ascendingOrder

  override def dataType: DataType = base.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      ascendingOrder match {
        // replace Literal with GpuLiteral here
        case GpuLiteral(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case order =>
          TypeCheckResult.TypeCheckFailure(
            s"Sort order in second argument requires a boolean literal, but found $order")
      }
    case ArrayType(dt, _) =>
      val dtSimple = dt.catalogString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }


  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector = {
    val isDescending = isDescendingOrder(rhs)
    lhs.getBase.listSortRows(isDescending, true)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { cv =>
      doColumnar(cv, rhs)
    }
  }

  private def isDescendingOrder(scalar: GpuScalar): Boolean = scalar.getValue match {
    case ascending: Boolean => !ascending
    case invalidValue => throw new IllegalArgumentException(s"invalid value $invalidValue")
  }
}

case class GpuArraySort(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      TypeCheckResult.TypeCheckSuccess
    case ArrayType(dt, _) =>
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type ${dt.catalogString} " +
          "which is not orderable")
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // false, false => ascending order with nulls last (array_sort's default comparator)
    input.getBase.listSortRows(false, false)
  }
}

object GpuArraySort {
  /** True iff the lambda is array_sort's default comparator (ascending, nulls last). */
  def isDefaultComparator(arraySort: ArraySort): Boolean = arraySort.function match {
    case LambdaFunction(body, Seq(left, right), _) =>
      body.canonicalized == ArraySort.comparator(left, right).canonicalized
    case _ => false
  }
}

object GpuArrayMin {
  def apply(child: Expression): GpuArrayMin = {
    child.dataType match {
      case ArrayType(FloatType | DoubleType, _) => GpuFloatArrayMin(child)
      case ArrayType(_, _) => GpuBasicArrayMin(child)
      case _ => throw new IllegalStateException(s"array_min accepts only arrays.")
    }
  }
}

abstract class GpuArrayMin(child: Expression) extends GpuUnaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector =
    input.getBase.listReduce(SegmentedReductionAggregation.min())
}

/** ArrayMin without `Nan` handling */
case class GpuBasicArrayMin(child: Expression) extends GpuArrayMin(child)

/** ArrayMin for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is:
 *   if one list contains only `Nan`s or `null`s
 *   then
       if the list contains `Nan`
 *     then return `Nan`
 *     else return null
 *   else
 *     replace all `Nan`s with nulls;
 *     use cuDF kernel to find the min value
 */
case class GpuFloatArrayMin(child: Expression) extends GpuArrayMin(child) {
    @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(FloatType, _) => FloatType
    case ArrayType(DoubleType, _) => DoubleType
    case _ => throw new IllegalStateException(
      s"GpuFloatArrayMin accepts only float array and double array."
    )
  }

  protected def getNanScalar: Scalar = dataType match {
    case FloatType => Scalar.fromFloat(Float.NaN)
    case DoubleType => Scalar.fromDouble(Double.NaN)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  protected def getNullScalar: Scalar = dataType match {
    case FloatType => Scalar.fromNull(DType.FLOAT32)
    case DoubleType => Scalar.fromNull(DType.FLOAT64)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    val listAll = SegmentedReductionAggregation.all()
    val listAny = SegmentedReductionAggregation.max()
    val base = input.getBase()

    withResource(base.getChildColumnView(0)) { child =>
      withResource(child.isNan()){ childIsNan =>
        // if all values in each list are nans or nulls
        val allNanOrNull = {
          val childIsNanOrNull = withResource(child.isNull()) {_.or(childIsNan)}
          withResource(childIsNanOrNull) { newChild =>
            withResource(base.replaceListChild(newChild)) {
              _.listReduce(listAll)
            }
          }
        }
        withResource(allNanOrNull){ allNanOrNull =>
          // return nan if the list contains nan, else return null
          val trueOption = {
            val anyNan = withResource(base.replaceListChild(childIsNan)) {
              _.listReduce(listAny)
            }
            withResource(anyNan) { anyNan =>
              withResource(getNanScalar) { nanScalar =>
                withResource(getNullScalar) { nullScalar =>
                  anyNan.ifElse(nanScalar, nullScalar)
                }
              }
            }
          }
          withResource(trueOption){ trueOption =>
            // replace all nans to nulls, and then find the min value.
            val falseOption = withResource(child.nansToNulls()) { nanToNullChild =>
              withResource(base.replaceListChild(nanToNullChild)) { nanToNullList =>
                nanToNullList.listReduce(SegmentedReductionAggregation.min())
              }
            }
            // if a list contains values other than nan or null
            // return `trueOption`, else return `falseOption`.
            withResource(falseOption){ falseOption =>
              allNanOrNull.ifElse(trueOption, falseOption)
            }
          }
        }
      }
    }
  }
}

object GpuArrayMax {
  def apply(child: Expression): GpuArrayMax = {
    child.dataType match {
      case ArrayType(FloatType | DoubleType, _) => GpuFloatArrayMax(child)
      case ArrayType(_, _) => GpuBasicArrayMax(child)
      case _ => throw new IllegalStateException(s"array_max accepts only arrays.")
    }
  }
}

abstract class GpuArrayMax(child: Expression) extends GpuUnaryExpression
  with ImplicitCastInputTypes
  with Serializable{

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector =
    input.getBase.listReduce(SegmentedReductionAggregation.max())
}

/** ArrayMax without `NaN` handling */
case class GpuBasicArrayMax(child: Expression) extends GpuArrayMax(child)

/** ArrayMax for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is that, we firstly check if each list contains `Nan`.
 * If it is, the max value is `Nan`, else we use the cuDF kernel to
 * calculate the max value.
 */
case class GpuFloatArrayMax(child: Expression) extends GpuArrayMax(child){
  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(FloatType, _) => FloatType
    case ArrayType(DoubleType, _) => DoubleType
    case _ => throw new IllegalStateException(
      s"GpuFloatArrayMax accepts only float array and double array."
    )
  }

  protected def getNanSalar: Scalar = dataType match {
    case FloatType => Scalar.fromFloat(Float.NaN)
    case DoubleType => Scalar.fromDouble(Double.NaN)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(getNanSalar){nan =>
      withResource(input.getBase().listContains(nan)){hasNan =>
        withResource(input.getBase().listReduce(SegmentedReductionAggregation.max())) {max =>
          hasNan.ifElse(nan, max)
        }
      }
    }
  }
}

case class GpuArrayRepeat(left: Expression, right: Expression) extends GpuBinaryExpression {

  override def dataType: DataType = ArrayType(left.dataType, left.nullable)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // The primary issue of array_repeat is to workaround the null and negative count.
    // Spark returns a null (list) when encountering a null count, and an
    // empty list when encountering a negative count.
    // cudf does not handle these cases properly.

    // Step 1. replace invalid counts
    //  null -> 0
    //  negative values -> 0
    val refinedCount = withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
      withResource(rhs.getBase.replaceNulls(zero)) { notNull =>
        withResource(notNull.lessThan(zero)) { lessThanZero =>
          lessThanZero.ifElse(zero, notNull)
        }
      }
    }
    // Step 2. perform cuDF repeat
    val repeated = closeOnExcept(refinedCount) { cnt =>
      withResource(new Table(lhs.getBase)) { table =>
        table.repeat(cnt).getColumn(0)
      }
    }
    // Step 3. generate list offsets from refined counts
    val offsets = closeOnExcept(repeated) { _ =>
      withResource(refinedCount) { cnt =>
        cnt.generateListOffsets()
      }
    }
    // Step 4. make the result list column with offsets and child column
    val list = withResource(offsets) { offsets =>
      withResource(repeated) { repeated =>
        repeated.makeListFromOffsets(lhs.getRowCount, offsets)
      }
    }
    // Step 5. merge the validity of count column to the result
    withResource(list) { list =>
      list.mergeAndSetValidity(BinaryOp.BITWISE_AND, rhs.getBase)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val numRows = lhs.getRowCount.toInt
    if (!rhs.isValid) {
      GpuColumnVector.fromNull(numRows, dataType).getBase
    } else {
      val count = rhs.getValue.asInstanceOf[Int] max 0

      val offsets = withResource(GpuScalar.from(count, IntegerType)) { cntScalar =>
        withResource(GpuColumnVector.from(cntScalar, numRows, rhs.dataType)) { cnt =>
          cnt.getBase.generateListOffsets()
        }
      }

      withResource(offsets) { offsets =>
        withResource(new Table(lhs.getBase)) { table =>
          withResource(table.repeat(count).getColumn(0)) { repeated =>
            repeated.makeListFromOffsets(lhs.getRowCount, offsets)
          }
        }
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    if (!rhs.isValid) {
      GpuColumnVector.fromNull(numRows, dataType).getBase
    } else {
      withResource(GpuColumnVector.from(lhs, numRows)) { left =>
        doColumnar(left, rhs)
      }
    }
  }
}

case class GpuArraysZip(children: Seq[Expression]) extends GpuExpression with ShimExpression
  with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.length)(ArrayType)

  @transient override lazy val dataType: DataType = {
    val fields = children.zip(arrayElementTypes).zipWithIndex.map {
      case ((expr: NamedExpression, elementType), _) =>
        StructField(expr.name, elementType, nullable = true)
      case ((_, elementType), idx) =>
        StructField(idx.toString, elementType, nullable = true)
    }
    ArrayType(StructType(fields), containsNull = false)
  }

  override def nullable: Boolean = children.exists(_.nullable)

  @transient private lazy val arrayElementTypes =
    children.map(_.dataType.asInstanceOf[ArrayType].elementType)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector  = {
    val res = if (children.isEmpty) {
      GpuScalar(new GenericArrayData(Array.empty[Any]), dataType)
    } else {
      // Prepare input columns
      val inputs = children.safeMap { expr =>
        expr.columnarEval(batch).getBase
      }

      val cleanedInputs = withResource(inputs) { inputs =>
        normalizeNulls(inputs)
      }

      val padded = withResource(cleanedInputs) { cleanedInputs =>
        padArraysToMaxLength(cleanedInputs)
      }

      withResource(padded) { _ =>
        closeOnExcept(zipArrays(padded)) { ret =>
          GpuColumnVector.from(ret, dataType)
        }
      }
    }
    GpuExpressionsUtils.resolveColumnVector(res, batch.numRows())
  }

  /**
   * Segmented gather in CUDF produces a NULL output for a NULL input. But we need to produce
   * child columns that we can put together in a struct. This requires them all to have the
   * same length. To make this work we need to make sure all of the inputs have nulls in the
   * same places at the top level. That way when we gather things we get the same output
   * size for all of the children of the input columns. The result of this will have the same
   * validity for all top level columns, but possibly different offsets.
   */
  private def normalizeNulls(inputs: Seq[cudf.ColumnVector]): Seq[ColumnVector] = {
    // First let's figure out if there are any nulls at all, because if there are not we don't
    // need to do anything.
    if (inputs.exists(_.hasNulls)) {
      var nullOutput = inputs.head.isNull
      try {
        inputs.drop(1).foreach { cv =>
          val combinedIsNull = withResource(cv.isNull) { tmpIsNull =>
            tmpIsNull.or(nullOutput)
          }
          closeOnExcept(combinedIsNull) { _ =>
            nullOutput.close()
            nullOutput = combinedIsNull
          }
        }

        // input1: [[A, B, C], [D, E], [F], [G]]
        // input2: [[a, b], [c, d, e], null, [f, g]]
        // combinedIsNull, false, false, true, false

        // output1: [[A, B, C], [D, E], null, [G]]
        // output2: [[a, b], [c, d, e], null, [f, g]]

        inputs.zip(children).safeMap { case (cv, child) =>
          withResource(GpuScalar.from(null, child.dataType)) { nullArray =>
            nullOutput.ifElse(nullArray, cv)
          }
        }
      } finally {
        nullOutput.close()
      }
    } else {
      inputs.map(_.incRefCount())
    }
  }


  private def computeMaxArraySize(inputs: Seq[ColumnVector]): ColumnVector = {
    // Compute array sizes of input arrays
    val arraySizes = inputs.safeMap(_.countElements())

    // Pick max array size of each row.
    // Replace with zero if there exists null among input values.
    // [1, 3, 5, null, 4]
    // [2, 4, 3,    8, 0]   => [4, 4, 5, 0, 7]
    // [4, 2, 3,   10, 7]
    val arraySizeList = withResource(arraySizes) { sizes =>
      ColumnVector.makeList(sizes: _*)
    }
    val maxArraySizeWithNull = withResource(arraySizeList) { list =>
      list.listReduce(SegmentedReductionAggregation.max())
    }
    withResource(maxArraySizeWithNull) { max =>
      withResource(GpuScalar.from(0, IntegerType)) { zero =>
        max.replaceNulls(zero)
      }
    }
  }

  private def generateSeqIndices(maxArraySize: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(0, IntegerType)) { s =>
      withResource(ColumnVector.fromScalar(s, maxArraySize.getRowCount.toInt)) { zero =>
        ColumnVector.sequence(zero, maxArraySize)
      }
    }
  }

  /**
   * Do a segmented gather on the inputs so that they are padded with nulls to make sure each LIST
   * on the same row, at the top level, has the same length. The columns returned should have the
   * same offsets and the same validity. This assumes that the validity on the inputs all match.
   */
  private def padArraysToMaxLength(inputs: Seq[ColumnVector]): Seq[ColumnVector] = {
    if (inputs.head.getRowCount == 0) {
      return inputs.safeMap(_.incRefCount())
    }
    // Compute max size of input arrays for each row, this is to know how we need to pad things.
    //
    // input1: [[A, B, C], [D, E], null, [G]]
    // input2: [[a, b], [c, d, e], null, [f, g]]
    // max array size: [3, 3, 0, 2]
    val seqIndices = withResource(computeMaxArraySize(inputs)) { maxArraySize =>
      // Generate sequence indices for gathering children of input arrays
      //
      // [3, 3, 0, 2] => [[0, 1, 2], [0, 1, 2], [], [0, 1]]
      generateSeqIndices(maxArraySize)
    }

    // Perform segment gather on input columns with indices covering each element
    //
    // input1: [[A, B, C], [D, E], null, [G]]
    // input2: [[a, b], [c, d, e], null, [f, g]]
    // indices: [[0, 1, 2], [0, 1, 2], [], [0, 1]]
    // output1: [[A, B, C], [D, E, null], null, [G, null]]
    // output2: [[a, b, null], [c, d, e], null, [f, g]]
    withResource(seqIndices) { _ =>
      inputs.safeMap { cv =>
        cv.segmentedGather(seqIndices)
      }
    }
  }

  /**
   * This turns LIST[X], LIST[Y], ... into a LIST[ STRUCT[X, Y, ...] ] but requires that
   * the input LIST columns all have the same validity and offsets.
   */
  private def zipArrays(padded: Seq[ColumnVector]): ColumnVector = {
    // Get the data column from the children, without any offsets
    withResource(padded.safeMap(_.getChildColumnView(0))) { children =>
      // Put them into a struct column view
      withResource(ColumnView.makeStructView(children: _*)) { structView =>
        // Make the struct a list using the input's offsets and validity
        // in the cheapest way possible.
        val proto = padded.head
        withResource(proto.getValid) { valid =>
          withResource(proto.getOffsets) { offsets =>
            withResource(new ColumnView(DType.LIST, proto.getRowCount,
              java.util.Optional.of[java.lang.Long](proto.getNullCount),
              valid, offsets, Array(structView))) { retView =>
              // Finally copy the result out to a ColumnVector so we can return it
              retView.copyToColumnVector()
            }
          }
        }
      }
    }
  }
}

// Base class for GpuArrayExcept, GpuArrayUnion, GpuArrayIntersect
trait GpuArrayBinaryLike extends GpuComplexTypeMergingExpression with NullIntolerantShim {
  val left: Expression
  val right: Expression

  @transient override final lazy val children: Seq[Expression] = IndexedSeq(left, right)

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector
  def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(left.columnarEvalAny(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuColumnVector, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(batch.numRows(), l, r), dataType)
          case (l, r) =>
            throw new UnsupportedOperationException(s"Unsupported data '($l: " +
              s"${l.getClass}, $r: ${r.getClass})' for GPU binary expression.")
        }
      }
    }
  }
}

case class GpuArrayExcept(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_intersect requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsDifferenceDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArrayIntersect(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_intersect requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsIntersectDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArrayUnion(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_union requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsUnionDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArraysOverlap(left: Expression, right: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes with NullIntolerantShim {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_union requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsHaveOverlap(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuMapFromArrays(left: Expression, right: Expression) extends GpuBinaryExpression {

  private val mapKeyDedupPolicy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)

  override def dataType: MapType = {
    MapType(
      keyType = left.dataType.asInstanceOf[ArrayType].elementType,
      valueType = right.dataType.asInstanceOf[ArrayType].elementType,
      valueContainsNull = right.dataType.asInstanceOf[ArrayType].containsNull)
  }

  /**
   * Compare top level offsets to ensure there are equal number of elements in
   * keys array and values array for each row.
   */
  private def compareOffsets(lhs: ColumnVector, rhs: ColumnVector) : Boolean = {
    val boolScalar = withResource(lhs.getListOffsetsView) { lhsOffsets =>
      withResource(rhs.getListOffsetsView) { rhsOffsets =>
        withResource(lhsOffsets.equalToNullAware(rhsOffsets)) { compareOffsets =>
          compareOffsets.all
        }
      }
    }
    withResource(boolScalar) { all =>
      all.getBoolean
    }
  }

  /**
   * Build new keys column and values column where a row is not NULL only if
   * keys[row_id] and values[row_id] are not NULL.
   */
  private def nullSanitize(lhs: ColumnVector, rhs: ColumnVector) :
  (ColumnVector, ColumnVector) = {
    if (lhs.hasNulls || rhs.hasNulls) {
      val combinedNulls = withResource(lhs.isNull) { lhsNulls =>
        withResource(rhs.isNull) { rhsNulls =>
          lhsNulls.or(rhsNulls)
        }
      }
      withResource(combinedNulls) { combinedNulls =>
            withResource(GpuScalar.from(null, left.dataType)) { leftScalar =>
              withResource(GpuScalar.from(null, right.dataType)) { rightScalar =>
                //  lhs: [[1,2], NULL, [3]]
                //  rhs: [NULL, [2,5], [6]]
                //  newLhs: [NULL, NULL, [3]]
                //  newRhs: [NULL, NULL, [6]]
                val newLhs = combinedNulls.ifElse(leftScalar, lhs)
                val newRhs = combinedNulls.ifElse(rightScalar, rhs)
                (newLhs, newRhs)
              }
          }
        }
    } else {
      lhs.incRefCount()
      rhs.incRefCount()
      (lhs,rhs)
    }
  }

  /**
   * Spark by default does not allow keys array to contain duplicate values
   * Compare distinct key count before and after dropping duplicates per row
   */
  private def rowContainsDuplicates(keysList: ColumnVector): Boolean = {
    withResource(keysList.getChildColumnView(0)) { childView =>
      withResource(keysList.dropListDuplicates) { newKeyList =>
        withResource(newKeyList.getChildColumnView(0)) { newChildView =>
          childView.getRowCount != newChildView.getRowCount
        }
      }
    }
  }

  /**
   * Create list< struct < X, Y > > from list< X > and List< Y >
   */
  private def constructMapColumn(sanitizedLhsBase: ColumnVector, sanitizedRhsBase: ColumnVector)
  : ColumnVector = {
    withResource(ColumnView.makeStructView(sanitizedLhsBase.getChildColumnView(0),
      sanitizedRhsBase.getChildColumnView(0))) { structView =>
      withResource(sanitizedLhsBase.getValid) { valid =>
        withResource(sanitizedLhsBase.getOffsets) { offsets =>
          withResource(new ColumnView(DType.LIST, sanitizedLhsBase.getRowCount,
            java.util.Optional.of[java.lang.Long](sanitizedLhsBase.getNullCount),
            valid, offsets, Array(structView))) { retView =>
            retView.copyToColumnVector()
          }
        }
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {

    require(lhs.getRowCount == rhs.getRowCount,
      "All columns must have the same number of rows")

    // Set a row to NULL in both columns if it is NULL in either the keys
    // column or the values column
    val (sanitizedLhsBase, sanitizedRhsBase) = nullSanitize(lhs.getBase,rhs.getBase)

    withResource(sanitizedLhsBase) { sanitizedLhsBase =>
      withResource(sanitizedRhsBase) { sanitizedRhsBase =>

        if(mapKeyDedupPolicy.toString == "EXCEPTION") {
          val containsDuplicates = rowContainsDuplicates(sanitizedLhsBase)
          require(!containsDuplicates,
            "[DUPLICATED_MAP_KEY] Duplicate map key was found")
        }

        // Ensure keys array and values array have same length
        require(compareOffsets(sanitizedLhsBase, sanitizedRhsBase),
          "The key array and value array of MapData must have the same length")

        // Ensure keys array does not contain NULLs in any row
        val nullKeysCount = withResource(sanitizedLhsBase.getChildColumnView(0)) { childView =>
          childView.getNullCount
        }
        require(nullKeysCount == 0,
          "[NULL_MAP_KEY] Cannot use null as map key")

        val mapCol = constructMapColumn(sanitizedLhsBase, sanitizedRhsBase)

        val result = withResource(mapCol) { mapCol =>
          mapKeyDedupPolicy.toString match {
            case "LAST_WIN" if rowContainsDuplicates(sanitizedLhsBase) =>
                mapCol.dropListDuplicatesWithKeysValues
            case _ =>
              mapCol.incRefCount()
          }
        }
        result
      }
    }

  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }

}

case class GpuArrayRemove(left: Expression, right: Expression) extends GpuBinaryExpression {

  override def dataType: DataType = left.dataType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // Handle special case for null entries in rhs, replace corresponding rows in lhs with null
    //
    // lhs: [[1, 2, null], [1, 2, 2], [2, 3]]
    // rhs: [1, null, 2]
    // lhsWithNull: [[1, 2, null], null, [2, 3]]
    val lhsWithNull = withResource(rhs.getBase.isNull) { rhsIsNull =>
      withResource(GpuScalar.from(null, dataType)) { nullList =>
        rhsIsNull.ifElse(nullList, lhs.getBase)
      }
    }
    // Repeat entries in rhs N times as N is number of elements in corresponding row in lhsWithNull
    //
    // lhsWithNull: [[1, 2, null], null, [2, 3]]
    // rhs: [1, null, 2]
    // repeatedRhs: [1, 1, 1, 2, 2]
    val repeatedRhs = explodeRhs(rhs.getBase, lhsWithNull.countElements)
    withResource(lhsWithNull) { lhsWithNull =>
      // Construct boolean mask where true values correspond to entries to keep
      //
      // lhsWithNull: [[1, 2, null], null, [2, 3]]
      // repeatedRhs: [1, 1, 1, 2, 2]
      // boolMask: [[F, T, T], null, [F, T]]
      val boolMask = constructBooleanMask(lhsWithNull.getChildColumnView(0), repeatedRhs,
                                          lhsWithNull.getListOffsetsView, lhs.getRowCount)
      withResource(boolMask) { boolMask =>
        lhsWithNull.applyBooleanMask(boolMask)
      }
    }
  }

  private def explodeRhs(rhs: ColumnVector, counts: ColumnVector): ColumnVector = {
    withResource(counts) { counts =>
      withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
        withResource(counts.replaceNulls(zero)) { noNullCounts =>
          withResource(new Table(rhs)) { table =>
            table.repeat(noNullCounts).getColumn(0)
          }
        }
      }
    }
  }

  private def constructBooleanMask(lhs: ColumnView, rhs: ColumnView,
                                   offSets: ColumnView, rowCount: Long): ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        val boolMaskNoNans = lhs.equalToNullAware(rhs)
        val boolMaskWithNans = if (lhs.getType == DType.FLOAT32 || lhs.getType == DType.FLOAT64) {
            // Compare NaN values for arrays with float or double type
            withResource(booleanMaskNansOnly(lhs, rhs)) { boolMaskNansOnly =>
              withResource(boolMaskNoNans) { boolMaskNoNans =>
                boolMaskNoNans.or(boolMaskNansOnly)
              }
            }
          } else {
            boolMaskNoNans
          }
        withResource(boolMaskWithNans) { boolMaskWithNans =>
          withResource(boolMaskWithNans.not) { boolMaskToKeep =>
            withResource(offSets) { offSets =>
              boolMaskToKeep.makeListFromOffsets(rowCount, offSets)
            }
          }
        }
      }
    }
  }

  private def booleanMaskNansOnly(lhs: ColumnView, rhs: ColumnView): ColumnVector = {
    withResource(lhs.isNan) { lhsIsNan =>
      withResource(rhs.isNan) { rhsIsNan =>
        lhsIsNan.and(rhsIsNan)
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val lhsBase = lhs.getBase
    // Construct boolean mask where true values correspond to elements to keep
    val boolMask = withResource(lhsBase.getListOffsetsView) { offSets =>
      withResource(lhsBase.getChildColumnView(0)){ lhsFlatten =>
        withResource(lhsFlatten.equalToNullAware(rhs.getBase)) { boolMaskToRemove =>
          withResource(boolMaskToRemove.not) { boolMaskToKeep =>
            boolMaskToKeep.makeListFromOffsets(lhs.getRowCount, offSets)
          }
        }
      }
    }
    withResource(boolMask) { boolMask =>
      lhsBase.applyBooleanMask(boolMask)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuFlattenArray(child: Expression) extends GpuUnaryExpression with NullIntolerantShim {
  private def childDataType: ArrayType = child.dataType.asInstanceOf[ArrayType]
  override def nullable: Boolean = child.nullable || childDataType.containsNull
  override def dataType: DataType = childDataType.elementType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.flattenLists
  }
}

case class GpuArrayDistinct(child: Expression) extends GpuUnaryExpression with NullIntolerantShim {
  override def dataType: DataType = child.dataType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.dropListDuplicates(DuplicateKeepOption.KEEP_FIRST)
  }
}

class GpuSequenceMeta(
    expr: Sequence,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[Sequence](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    //  We have to fall back to the CPU if the timeZoneId is not UTC when
    //  we are processing date/timestamp.
    //  Date/Timestamp are not enabled right now so this is probably fine.
  }

  override def convertToGpuImpl(): GpuExpression = {
    val (startExpr, stopExpr, stepOpt) = if (expr.stepOpt.isDefined) {
        val Seq(start, stop, step) = childExprs.map(_.convertToGpu())
        (start, stop, Some(step))
      } else {
        val Seq(start, stop) = childExprs.map(_.convertToGpu())
        (start, stop, None)
      }
    GpuSequence(startExpr, stopExpr, stepOpt, expr.timeZoneId)
  }
}

object GpuSequenceUtil {

  def checkSequenceInputs(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): Unit = {
    // Keep the same requirement with Spark:
    // (step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop)
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      // The check should ignore each row (Row(start, stop, step)) that contains at least
      // one null element according to Spark's code. Thanks to the cudf binary ops, who ignore
      // nulls already, skipping nulls can be done without any additional process.
      //
      // Because the filtered table (e.g. upTbl) in each rule check excludes the rows that the
      // step is null. Next a null row will be produced when comparing start or stop when any
      // of them is null, and the nulls are skipped in the final assertion 'isAllValidTrue'.
      withResource(new Table(start, stop)) { startStopTable =>
        // (step > 0 && start <= stop)
        val upTbl = withResource(step.greaterThan(zero)) { positiveStep =>
          startStopTable.filter(positiveStep)
        }
        val allUp = withResource(upTbl) { _ =>
          upTbl.getColumn(0).lessOrEqualTo(upTbl.getColumn(1))
        }
        withResource(allUp) { _ =>
          require(isAllValidTrue(allUp), "Illegal sequence boundaries: step > 0 but start > stop")
        }

        // (step < 0 && start >= stop)
        val downTbl = withResource(step.lessThan(zero)) { negativeStep =>
          startStopTable.filter(negativeStep)
        }
        val allDown = withResource(downTbl) { _ =>
          downTbl.getColumn(0).greaterOrEqualTo(downTbl.getColumn(1))
        }
        withResource(allDown) { _ =>
          require(isAllValidTrue(allDown),
            "Illegal sequence boundaries: step < 0 but start < stop")
        }

        // (step == 0 && start == stop)
        val equalTbl = withResource(step.equalTo(zero)) { zeroStep =>
          startStopTable.filter(zeroStep)
        }
        val allEq = withResource(equalTbl) { _ =>
          equalTbl.getColumn(0).equalTo(equalTbl.getColumn(1))
        }
        withResource(allEq) { _ =>
          require(isAllValidTrue(allEq),
            "Illegal sequence boundaries: step == 0 but start != stop")
        }
      }
    } // end of zero
  }

  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) contains at least one null element will produce
   * a null in the output.
   *
   * The returned column should be closed.
   */
  def computeSequenceSize(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector,
      functionName: String): ColumnVector = {
    checkSequenceInputs(start, stop, step)
    val actualSize = GetSequenceSize(start, stop, step)
    val sizeAsLong = withResource(actualSize) { _ =>
      val mergedEquals = withResource(start.equalTo(stop)) { equals =>
        if (step.hasNulls) {
          // Also set the row to null where step is null.
          equals.mergeAndSetValidity(BinaryOp.BITWISE_AND, equals, step)
        } else {
          equals.incRefCount()
        }
      }
      withResource(mergedEquals) { _ =>
        withResource(Scalar.fromLong(1L)) { one =>
          mergedEquals.ifElse(one, actualSize)
        }
      }
    }
    withResource(sizeAsLong) { _ =>
      // check max size
      withResource(Scalar.fromInt(MAX_ROUNDED_ARRAY_LENGTH)) { maxLen =>
        withResource(sizeAsLong.lessOrEqualTo(maxLen)) { allValid =>
          withResource(sizeAsLong.reduce(ReductionAggregation.max())) { maxSizeScalar =>
            require(isAllValidTrue(allValid),
              RapidsErrorUtils.getTooLongSequenceErrorString(
                maxSizeScalar.getLong.asInstanceOf[Int],
                functionName))
          }
        }
      }
      // cast to int and return
      sizeAsLong.castTo(DType.INT32)
    }
  }
}

case class GpuSequence(start: Expression, stop: Expression, stepOpt: Option[Expression],
    timeZoneId: Option[String] = None) extends TimeZoneAwareExpression with GpuExpression
    with ShimExpression {

  import GpuSequenceUtil._

  override def dataType: ArrayType = ArrayType(start.dataType, containsNull = false)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = Seq(start, stop) ++ stepOpt

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  // can throw exceptions such as "Illegal sequence boundaries: step > 0 but start > stop"
  override def hasSideEffects: Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(start.columnarEval(batch)) { startGpuCol =>
      withResource(stepOpt.map(_.columnarEval(batch))) { stepGpuColOpt =>
        val startCol = startGpuCol.getBase

        // 1 Compute the sequence size for each row.
        val (sizeCol, stepCol) = withResource(stop.columnarEval(batch)) { stopGpuCol =>
          val stopCol = stopGpuCol.getBase
          val steps = stepGpuColOpt.map(_.getBase.incRefCount())
              .getOrElse(defaultStepsFunc(startCol, stopCol))
          closeOnExcept(steps) { _ =>
            (computeSequenceSize(startCol, stopCol, steps, prettyName), steps)
          }
        }

        // 2 Generate the sequence
        //
        // cudf 'sequence' requires 'step' has the same type with 'start'.
        // And the step type may differ due to the default steps.
        val castedStepCol = withResource(stepCol) { _ =>
          closeOnExcept(sizeCol) { _ =>
            stepCol.castTo(startCol.getType)
          }
        }
        withResource(Seq(sizeCol, castedStepCol)) { _ =>
          GpuColumnVector.from(genSequence(startCol, sizeCol, castedStepCol), dataType)
        }
      }
    }
  }

  @transient
  private lazy val defaultStepsFunc: (ColumnView, ColumnView) => ColumnVector =
      dataType.elementType match {
    case _: IntegralType =>
      // Default step:
      //   start > stop, step == -1
      //   start <= stop, step == 1
      (starts, stops) => {
        // It is ok to always use byte, since it will be casted to the same type before
        // going into cudf sequence. Besides byte saves memory, and does not cause any
        // type promotion during computation.
        withResource(Scalar.fromByte((-1).toByte)) { minusOne =>
          withResource(Scalar.fromByte(1.toByte)) { one =>
            withResource(starts.greaterThan(stops)) { decrease =>
              decrease.ifElse(minusOne, one)
            }
          }
        }
      }
    // Timestamp and Date will come soon
    // case TimestampType =>
    // case DateType =>
  }

  private def genSequence(
      start: ColumnView,
      size: ColumnView,
      step: ColumnView): ColumnVector = {
    // size is calculated from start, stop and step, so its validity mask is equal to
    // the merged validity of the three columns, and can be used as the final output
    // validity mask directly.
    // Then checking nulls only in size column is enough.
    if(size.getNullCount > 0) {
      // Nulls are not acceptable in cudf 'list::sequences'. (Pls refer to
      //     https://github.com/rapidsai/cudf/issues/10012),
      //
      // So replace the nulls with 0 for size, and create temp views for start and
      // stop with forcing null count to be 0.
      val sizeNoNull = withResource(Scalar.fromInt(0)) { zero =>
        size.replaceNulls(zero)
      }
      val ret = withResource(sizeNoNull) { _ =>
        val startNoNull = new ColumnView(start.getType, start.getRowCount, Optional.of(0L),
          start.getData, null)
        withResource(startNoNull) { _ =>
          val stepNoNull = new ColumnView(step.getType, step.getRowCount, Optional.of(0L),
            step.getData, null)
          withResource(stepNoNull) { _ =>
            ColumnVector.sequence(startNoNull, sizeNoNull, stepNoNull)
          }
        }
      }
      withResource(ret) { _ =>
        // Restore the null rows by setting the validity mask.
        ret.mergeAndSetValidity(BinaryOp.BITWISE_AND, size)
      }
    } else {
      ColumnVector.sequence(start, size, step)
    }
  }
}
