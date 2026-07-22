/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuMapUtils, GpuScalar, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, RaiseError}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors.raiseError
import org.apache.spark.sql.types.{AbstractDataType, DataType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Implements `raise_error()` for Databricks 14.3 and Spark 4.0.
 * Note that while the arity `raise_error()` remains 1 for all user-facing APIs
 * (SQL, Scala, Python). But internally, the implementation uses a binary expression,
 * where the first argument indicates the "error-class" for the error being raised.
 */
case class GpuRaiseError(left: Expression, right: Expression, dataType: DataType)
  extends GpuBinaryExpression with ExpectsInputTypes {

  val errorClass: Expression = left
  val errorParams: Expression = right

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"raise_error($errorClass, $errorParams)"

  /** Could evaluating this expression cause side-effects, such as throwing an exception? */
  override def hasSideEffects: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new UnsupportedOperationException("Expected errorClass (lhs) to be a String literal")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    throw new UnsupportedOperationException("Expected errorClass (lhs) to be a String literal")

  private def extractScalaUTF8String(stringScalar: Scalar): UTF8String = {
    // This is guaranteed to be a string scalar.
    GpuScalar.extract(stringScalar).asInstanceOf[UTF8String]
  }

  private def extractStrings(stringsColumn: ColumnView): Array[UTF8String] = {
    val size = stringsColumn.getRowCount.asInstanceOf[Int] // Already checked if exceeds threshold.
    val output: Array[UTF8String] = new Array[UTF8String](size)
    for (i <- 0 until size) {
      output(i) = withResource(stringsColumn.getScalarElement(i)) {
        extractScalaUTF8String(_)
      }
    }
    output
  }

  private def makeMapData(listOfStructs: ColumnView): MapData = {
    val THRESHOLD: Int = 10 // Avoiding surprises with large maps.
                            // All testing indicates a map with 1 entry.
    val mapSize = listOfStructs.getRowCount

    if (mapSize > THRESHOLD) {
      throw new UnsupportedOperationException("Unexpectedly large error-parameter map")
    }

    val outputKeys: Array[UTF8String] =
      withResource(GpuMapUtils.getKeysAsListView(listOfStructs)) { listOfKeys =>
        withResource(listOfKeys.getChildColumnView(0)) { // Strings child of LIST column.
          extractStrings(_)
        }
      }

    val outputVals: Array[UTF8String] =
      withResource(GpuMapUtils.getValuesAsListView(listOfStructs)) { listOfVals =>
        withResource(listOfVals.getChildColumnView(0)) { // Strings child of LIST column.
          extractStrings(_)
        }
      }

    ArrayBasedMapData(outputKeys, outputVals)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if (rhs.getRowCount <= 0) {
      // For the case: when(condition, raise_error(col("a"))
      // When `condition` selects no rows, a vector of nulls should be returned,
      // instead of throwing.
      return GpuColumnVector.columnVectorFromNull(0, NullType)
    }

    val lhsErrorClass = lhs.getValue.asInstanceOf[UTF8String]

    val rhsMapData = withResource(rhs.getBase.slice(0,1)) { slices =>
      val firstRhsRow = slices(0)
      makeMapData(firstRhsRow)
    }

    throw raiseError(lhsErrorClass, rhsMapData)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
      if (numRows <= 0) {
        // For the case: when(condition, raise_error(col("a"))
        // When `condition` selects no rows, a vector of nulls should be returned,
        // instead of throwing.
        return GpuColumnVector.columnVectorFromNull(0, NullType)
      }

      val errorClass = lhs.getValue.asInstanceOf[UTF8String]
      // TODO (future):  Check if the map-data needs to be extracted differently.
      // All testing indicates that the host value of the map literal is set always pre-set.
      // But if it isn't, then GpuScalar.getValue might extract it incorrectly.
      // https://github.com/NVIDIA/spark-rapids/issues/11974
      val errorParams = rhs.getValue.asInstanceOf[MapData]
      throw raiseError(errorClass, errorParams)
  }
}

class RaiseErrorMeta(r: RaiseError,
                     conf: RapidsConf,
                     parent: Option[RapidsMeta[_, _, _]],
                     rule: DataFromReplacementRule )
  extends BinaryExprMeta[RaiseError](r, conf, parent, rule) {
  override def convertToGpu(lhsErrorClass: Expression, rhsErrorParams: Expression): GpuExpression
    = GpuRaiseError(lhsErrorClass, rhsErrorParams, r.dataType)
}