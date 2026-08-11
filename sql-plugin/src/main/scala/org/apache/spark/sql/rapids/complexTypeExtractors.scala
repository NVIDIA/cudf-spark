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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar}
import ai.rapids.cudf.ColumnView.FindOptions
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuExpressionsUtils, GpuListUtils, GpuMapUtils, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta, UnaryExprMeta}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.ArrayIndexUtils.firstIndexAndNumElementUnchecked
import com.nvidia.spark.rapids.BoolUtils.isAnyValidTrue
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, TypeUtils}
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, IntegralType, LongType, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuGetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
    extends ShimUnaryExpression
    with GpuExpression
    with ShimGetStructField
    with NullIntolerantShim {

  lazy val childSchema: StructType = child.dataType.asInstanceOf[StructType]

  override def dataType: DataType = childSchema(ordinal).dataType
  override def nullable: Boolean = child.nullable || childSchema(ordinal).nullable

  override def toString: String = {
    val fieldName = if (resolved) childSchema(ordinal).name else s"_$ordinal"
    s"$child.${name.getOrElse(fieldName)}"
  }

  override def sql: String =
    child.sql + s".${quoteIdentifier(name.getOrElse(childSchema(ordinal).name))}"

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    val dt = dataType
    withResourceIfAllowed(child.columnarEvalAny(batch)) {
      case cv: GpuColumnVector =>
        withResource(cv.getBase.getChildColumnView(ordinal)) { view =>
          GpuColumnVector.from(view.copyToColumnVector(), dt)
        }
      case s: GpuScalar =>
        // For a scalar in we want a scalar out.
        if (!s.isValid) {
          GpuScalar(null, dt)
        } else {
          withResource(s.getBase.getChildrenFromStructScalar) { children =>
            GpuScalar.wrap(children(ordinal).getScalarElement(0), dt)
          }
        }
      case other =>
        throw new IllegalArgumentException(s"Got an unexpected type out of columnarEvalAny $other")
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    GpuExpressionsUtils.resolveColumnVector(columnarEvalAny(batch), batch.numRows())
}

/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 */
case class GpuGetArrayItem(child: Expression, ordinal: Expression, failOnError: Boolean)
    extends GpuBinaryExpression
    with ExpectsInputTypes
    with ShimGetArrayItem {

  // We have done type checking for child in `ExtractValue`, so only need to check the `ordinal`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegralType)

  override def toString: String = s"$child[$ordinal]"
  override def sql: String = s"${child.sql}[${ordinal.sql}]"

  override def left: Expression = child
  override def right: Expression = ordinal
  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType

  override def hasSideEffects: Boolean = super.hasSideEffects || failOnError

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val (array, initialIndices) = (lhs.getBase, rhs.getBase)
    // Spark does not check for overflow/underflow when casting
    withResource(initialIndices.castTo(DType.INT32)) { indices =>
      getItemInternal(array, indices)
    }
  }

  private def getItemInternal(array: ColumnVector, indices: ColumnVector): ColumnVector = {
    val indicesCol = withResource(Scalar.fromInt(0)) { zeroS =>
      withResource(indices.lessThan(zeroS)) { hasNegativeIndicesCV =>
        if (failOnError) {
          // Check if any index is out of bound only when ansi mode is enabled.
          // No exception should be raised if no valid entry (An entry is valid when both
          // the array row and its index are not null), the same with what Spark does.
          withResource(array.countElements()) { numElements =>
            // Check negative indices. Should ignore the rows that are not valid
            val hasValidEntryCV = hasNegativeIndicesCV.mergeAndSetValidity(BinaryOp.BITWISE_AND,
                hasNegativeIndicesCV, array)
            withResource(hasValidEntryCV) { _ =>
              if (isAnyValidTrue(hasValidEntryCV)) {
                val (index, numElem) = firstIndexAndNumElementUnchecked(hasValidEntryCV,
                  indices, numElements)
                throw RapidsErrorUtils.invalidArrayIndexError(index, numElem)
              }
            }
            // Then check if any index is larger than its array size
            withResource(indices.greaterOrEqualTo(numElements)) { hasLargerIndicesCV =>
              // No need to check the validity of array column here, since the validity info
              // is included in this `hasLargerIndicesCV`.
              if (isAnyValidTrue(hasLargerIndicesCV)) {
                val (index, numElem) = firstIndexAndNumElementUnchecked(hasLargerIndicesCV,
                  indices, numElements)
                throw RapidsErrorUtils.invalidArrayIndexError(index, numElem)
              }
            }
          }
        } // end of "if (failOnError)"

        if (isAnyValidTrue(hasNegativeIndicesCV)) {
          // Replace negative indices with nulls no matter whether the corresponding array is
          // null, then cuDF will return nulls, the same with what Spark returns for negative
          // indices.
          withResource(Scalar.fromNull(DType.INT32)) { nullS =>
            hasNegativeIndicesCV.ifElse(nullS, indices)
          }
        } else {
          indices.incRefCount()
        }
      }
    }

    withResource(indicesCol) { _ =>
      array.extractListElement(indicesCol)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }

  override def doColumnar(lhs: GpuColumnVector, ordinalS: GpuScalar): ColumnVector = {
    if (!ordinalS.isValid || lhs.getRowCount == lhs.numNulls()) {
      // Return nulls when index is null or all the array rows are null,
      // the same with what Spark does.
      GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
    } else {
      // Spark does not worry about overflow/underflow on a long. This is almost
      // exactly what they do to get the index
      val ordinal = ordinalS.getValue.asInstanceOf[Number].intValue()
      if (failOnError) {
        // Check if index is out of bound only when ansi mode is enabled, since cuDF
        // returns nulls if index is out of bound, the same with what Spark does when
        // ansi mode is disabled.
        withResource(lhs.getBase.countElements()) { numElementsCV =>
          withResource(numElementsCV.min) { minScalar =>
            val minNumElements = minScalar.getInt
            if (ordinal < 0 || ordinal >= minNumElements) {
              throw RapidsErrorUtils.invalidArrayIndexError(ordinal, minNumElements)
            }
          }
        }
      } else if (ordinal < 0) {
        // Only take care of this case since cuDF treats negative index as offset to the
        // end of array, instead of invalid index.
        return GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
      }
      lhs.getBase.extractListElement(ordinal)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuGetMapValue(child: Expression, key: Expression, failOnError: Boolean)(
    override val origin: Origin = CurrentOrigin.get)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerantShim {

  override def otherCopyArgs: Seq[AnyRef] = origin :: Nil

  private def keyType = child.dataType.asInstanceOf[MapType].keyType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes() match {
      case f: TypeCheckResult.TypeCheckFailure => f
      case TypeCheckResult.TypeCheckSuccess =>
        TypeUtils.checkForOrderingExpr(keyType, s"function $prettyName")
    }
  }

  /**
   * `Null` is returned for invalid ordinals.
   */
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, keyType)

  override def prettyName: String = "getMapValue"

  override def hasSideEffects: Boolean = super.hasSideEffects || failOnError

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (failOnError) {
      withResource(lhs.getBase.getMapKeyExistence(rhs.getBase)) { keyExistenceColumn =>
        withResource(keyExistenceColumn.all) { exist =>
          if (exist.isValid && !exist.getBoolean) {
            throw RapidsErrorUtils.mapKeyNotExistError(rhs.getValue.toString, keyType, origin)
          }
        }
      }
    }
    lhs.getBase.getMapValue(rhs.getBase)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val map = lhs.getBase
    val indices = rhs.getBase
    if (failOnError) {
      GpuMapUtils.getMapValueOrThrow(map, indices, rhs.dataType(), origin)
    } else {
      map.getMapValue(indices)
    }
  }


  override def left: Expression = child

  override def right: Expression = key
}

/** Checks if the array (left) has the element (right)
*/
case class GpuArrayContains(left: Expression, right: Expression)
  extends GpuBinaryExpression with NullIntolerantShim {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  /**
   * Helper function to account for `libcudf`'s `listContains()` semantics.
   *
   * If a list row contains at least one null element, and is found not to contain
   * the search key, `libcudf` returns false instead of null.  SparkSQL expects to
   * return null in those cases.
   *
   * This method determines the result's validity mask by ORing the output of
   * `listContains()` with the NOT of `listContainsNulls()`.
   * A result row is thus valid if either the search key is found in the list,
   * or if the list does not contain any null elements.
   */
  private def orNotContainsNull(containsResult: ColumnVector,
                                inputListsColumn: ColumnView): ColumnVector = {
    val notContainsNull = withResource(inputListsColumn.listContainsNulls) {
      _.not
    }
    val containsKeyOrNotContainsNull = withResource(notContainsNull) {
      containsResult.or(_)
    }
    withResource(containsKeyOrNotContainsNull) { lcnn =>
      withResource(Scalar.fromNull(DType.BOOL8)) { nullVal =>
        lcnn.ifElse(containsResult, nullVal)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val inputListsColumn = lhs.getBase
    withResource(inputListsColumn.listContains(rhs.getBase)) {
      orNotContainsNull(_, inputListsColumn)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(ColumnVector.fromScalar(lhs.getBase, numRows)) { inputListsColumn =>
      withResource(ColumnVector.fromScalar(rhs.getBase, numRows)) { keysColumn =>
        //NOTE: listContainsColumn expects that input and the keys columns to have the same
        // number of rows
        withResource(inputListsColumn.listContainsColumn(keysColumn)) {
          orNotContainsNull(_, inputListsColumn)
        }
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(ColumnVector.fromScalar(lhs.getBase, rhs.getRowCount.toInt)) { inputListsColumn =>
      //NOTE: listContainsColumn expects that input and the keys columns to have the same
      // number of rows
      withResource(inputListsColumn.listContainsColumn(rhs.getBase)) {
        orNotContainsNull(_, inputListsColumn)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val inputListsColumn = lhs.getBase
    withResource(inputListsColumn.listContainsColumn(rhs.getBase)) {
      orNotContainsNull(_, inputListsColumn)
    }
  }

  override def prettyName: String = "array_contains"
}

class GpuArrayPositionMeta(
    expr: ArrayPosition,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends BinaryExprMeta[ArrayPosition](expr, conf, parent, rule) {

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuArrayPosition(lhs, rhs)
  }
}

case class GpuArrayPosition(left: Expression, right: Expression)
  extends GpuBinaryExpression with NullIntolerantShim {

  override def dataType: DataType = LongType

  override def prettyName: String = "array_position"

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val arrayCol = lhs.getBase
    val keyScalar = rhs.getBase
    // 1. `listIndexOf` with `FindOptions.FIND_FIRST` returns the zero-based index of the first
    //    occurrence of the key in each array if the key is found, otherwise returns -1.
    // 2. If the key is null, all output are set to null.
    // 3. If arrayCol[i] is null, output[i] is also null.
    withResource(arrayCol.listIndexOf(keyScalar, FindOptions.FIND_FIRST)) { zeroBasedPoses =>
      withResource(Scalar.fromInt(1)) { one =>
        zeroBasedPoses.add(one, DType.INT64)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val arrayCol = lhs.getBase
    val keyCol = rhs.getBase
    // 1. `listIndexOf` with `FindOptions.FIND_FIRST` returns the zero-based index of the first
    //    occurrence of each key in each array if the key is found, otherwise returns -1.
    // 2. If keyCol[i] is null or arrayCol[i] is null, output[i] is also null.
    withResource(arrayCol.listIndexOf(keyCol, FindOptions.FIND_FIRST)) { zeroBasedPoses =>
      withResource(Scalar.fromInt(1)) { one =>
        zeroBasedPoses.add(one, DType.INT64)
      }
    }
  }
}

class GpuGetArrayStructFieldsMeta(
     expr: GetArrayStructFields,
     conf: RapidsConf,
     parent: Option[RapidsMeta[_, _, _]],
     rule: DataFromReplacementRule)
  extends UnaryExprMeta[GetArrayStructFields](expr, conf, parent, rule) {

  def convertToGpu(child: Expression): GpuExpression =
    GpuGetArrayStructFields(child, expr.field, expr.ordinal, expr.numFields, expr.containsNull)
}

/**
 * For a child whose data type is an array of structs, extracts the `ordinal`-th fields of all array
 * elements, and returns them as a new array.
 *
 * No need to do type checking since it is handled by 'ExtractValue'.
 */
case class GpuGetArrayStructFields(
    child: Expression,
    field: StructField,
    ordinal: Int,
    numFields: Int,
    containsNull: Boolean) extends GpuUnaryExpression
    with ShimGetArrayStructFields
    with NullIntolerantShim {

  override def dataType: DataType = ArrayType(field.dataType, containsNull)
  override def toString: String = s"$child.${field.name}"
  override def sql: String = s"${child.sql}.${quoteIdentifier(field.name)}"

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val base = input.getBase
    val fieldView = withResource(base.getChildColumnView(0)) { structView =>
      structView.getChildColumnView(ordinal)
    }
    val listView = withResource(fieldView) { _ =>
      GpuListUtils.replaceListDataColumnAsView(base, fieldView)
    }
    withResource(listView) { _ =>
      listView.copyToColumnVector()
    }
  }
}
