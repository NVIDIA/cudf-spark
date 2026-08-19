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

import ai.rapids.cudf.{ast, BinaryOp, ColumnVector, DType, Scalar, UnaryOp}
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object ShiftHelper {
  // From the java language specification for Java 8
  // If the promoted type of the left-hand operand is int, then only the five lowest-order bits of
  // the right-hand operand are used as the shift distance. It is as if the right-hand operand
  // were subjected to a bitwise logical AND operator & ( §15.22.1) with the mask value
  // 0x1f (0b11111). The shift distance actually used is therefore always in the range 0 to 31,
  // inclusive.
  //
  // If  the  promoted  type  of  the  left-hand  operand  is  long,  then  only  the  six
  // lowest-order  bits  of  the  right-hand  operand  are  used  as  the  shift  distance.  It  is
  // as  if  the right-hand operand were subjected to a bitwise logical AND operator & ( §15.22.1)
  // with the mask value 0x3f (0b111111). The shift distance actually used is therefore always in
  // the range 0 to 63, inclusive.
  private def maskForDistance(t: DType): Int = t match {
    case DType.INT32 =>  0x1F // 0b11111
    case DType.INT64 =>  0x3F //0b111111
    case t => throw new IllegalArgumentException(s"$t is not a supported type for java bit shifts")
  }

  def fixupDistanceNoClose(t: DType, distance: ColumnVector): ColumnVector = {
    withResource(Scalar.fromInt(maskForDistance(t))) { mask =>
      distance.bitAnd(mask)
    }
  }

  def fixupDistanceNoClose(t: DType, distance: GpuScalar): Scalar = {
    if (distance.isValid) {
      Scalar.fromInt(distance.getValue.asInstanceOf[Int] & maskForDistance(t))
    } else {
      distance.getBase.incRefCount()
    }
  }
}

trait GpuShiftBase extends GpuBinaryExpression with ImplicitCastInputTypes {
  def shiftOp: BinaryOp

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val lBase = lhs.getBase
    withResource(ShiftHelper.fixupDistanceNoClose(lBase.getType, rhs.getBase)) { distance =>
      lBase.binaryOp(shiftOp, distance, lBase.getType)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(ShiftHelper.fixupDistanceNoClose(lhs.getBase.getType, rhs.getBase)) { distance =>
      lhs.getBase.binaryOp(shiftOp, distance, lhs.getBase.getType)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val lBase = lhs.getBase
    withResource(ShiftHelper.fixupDistanceNoClose(lBase.getType, rhs)) { distance =>
      lBase.binaryOp(shiftOp, distance, lBase.getType)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuShiftLeft(left: Expression, right: Expression) extends GpuShiftBase {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def shiftOp: BinaryOp = BinaryOp.SHIFT_LEFT

  override def dataType: DataType = left.dataType
}

case class GpuShiftRight(left: Expression, right: Expression) extends GpuShiftBase {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def shiftOp: BinaryOp = BinaryOp.SHIFT_RIGHT

  override def dataType: DataType = left.dataType

}

case class GpuShiftRightUnsigned(left: Expression, right: Expression) extends GpuShiftBase {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def shiftOp: BinaryOp = BinaryOp.SHIFT_RIGHT_UNSIGNED

  override def dataType: DataType = left.dataType
}

case class GpuBitwiseAnd(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "&"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_AND
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.BITWISE_AND)

  override val failOnError: Boolean = false
}

case class GpuBitwiseOr(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "|"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_OR
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.BITWISE_OR)

  override val failOnError: Boolean = false
}

case class GpuBitwiseXor(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "^"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_XOR
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.BITWISE_XOR)

  override val failOnError: Boolean = false
}

case class GpuBitwiseNot(child: Expression) extends CudfUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def unaryOp: UnaryOp = UnaryOp.BIT_INVERT

  override def dataType: DataType = child.dataType

  override def toString: String = s"~${child.sql}"
}

case class GpuBitwiseCount(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegralType, BooleanType))

  override def dataType: DataType = IntegerType

  override def toString: String = s"bit_count(${child.sql})"

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    if (input.dataType() == LongType || input.dataType() == BooleanType) {
      input.getBase.bitCount()
    } else {
      // For integer type, Spark always evaluates bit_count as if the input is UINT64.
      withResource(input.getBase.castTo(DType.UINT64)) { castedInput =>
        castedInput.bitCount()
      }
    }
  }
}
