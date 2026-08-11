/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.functions

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType}
import com.nvidia.spark.rapids.{ExprMeta, GpuBinaryExpression, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.jni.iceberg.IcebergTruncate

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

case class GpuTruncateExpression(width: Expression, value: Expression)
  extends GpuBinaryExpression {

  private lazy val sanityCheckResult: Unit = sanityCheck()

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): CudfColumnVector = {
    throw new IllegalStateException("GpuTruncateExpression requires first argument to be scalar, " +
      "second to be column, but both are columns")
  }

  override def doColumnar(width: GpuScalar, rhs: GpuColumnVector): CudfColumnVector = {
    sanityCheckResult
    val widthInteger = width.getValue.asInstanceOf[java.lang.Integer]
    // `width` MUST be valid
    require (widthInteger != null)

    IcebergTruncate.truncate(rhs.getBase, widthInteger.intValue())
  }

  private def sanityCheck(): Unit = {
    require(width.dataType == DataTypes.IntegerType,
      s"truncates number must be an integer, got ${width.dataType}")

    require(GpuTruncateExpression.isSupportedValueType(value.dataType),
      s"Truncate function does not support type ${value.dataType} as values")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuTruncateExpression requires first argument to be scalar, " +
      "second to be column, but first is column, second is scalar")
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuTruncateExpression requires first argument to be scalar, " +
      "second to be column, but both are scalars")
  }

  override def left: Expression = width

  override def right: Expression = value

  override def dataType: DataType = value.dataType
}

object GpuTruncateExpression {

  private lazy val supportedFunctionClasses: Set[Class[_]] =
    Set(
      classOf[TruncateFunction.TruncateInt],
      classOf[TruncateFunction.TruncateBigInt],
      classOf[TruncateFunction.TruncateString],
      classOf[TruncateFunction.TruncateBinary],
      classOf[TruncateFunction.TruncateDecimal]
    )

  def isSupportedValueType(dataType: DataType): Boolean = {
    dataType match {
      case IntegerType => true
      case LongType => true
      case StringType => true
      case BinaryType => true
      case dt: DecimalType => dt.precision <= DType.DECIMAL128_MAX_PRECISION
      case _ => false
    }
  }

  def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    require(meta.childExprs.length == 2,
      s"TruncateFunction should have exactly two arguments, got ${meta.childExprs.length}")
    val exprCls = meta.wrapped.staticObject

    if (!supportedFunctionClasses.contains(exprCls)) {
      meta.willNotWorkOnGpu(s"Supported iceberg partition function classes are: " +
        s"${supportedFunctionClasses.mkString("[", ",", "]")},  actual: $exprCls")
    }

    val widthExpr = meta.wrapped.arguments.head
    if (widthExpr.dataType != DataTypes.IntegerType) {
      throw new IllegalStateException(
        s"The width of TruncateFunction must be an integer, got ${widthExpr.dataType}")
    }

    val valueExpr = meta.wrapped.arguments(1)
    if (!isSupportedValueType(valueExpr.dataType)) {
      meta.willNotWorkOnGpu(s"Gpu truncate function does not support type ${valueExpr.dataType} " +
        s"as values")
    }
  }
}
