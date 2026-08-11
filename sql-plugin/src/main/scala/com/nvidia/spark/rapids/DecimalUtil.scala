/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{DecimalUtils, DType}

import org.apache.spark.sql.types._

object DecimalUtil {

  def createCudfDecimal(dt: DecimalType): DType =
    DecimalUtils.createDecimalType(dt.precision, dt.scale)

  def outOfBounds(input: cudf.ColumnView, to: DecimalType): cudf.ColumnVector =
    DecimalUtils.outOfBounds(input, to.precision, to.scale)

  /**
   * Return the size in bytes of the Fixed-width data types.
   * WARNING: Do not use this method for variable-width data types
   */
  private[rapids] def getDataTypeSize(dt: DataType): Int = {
    dt match {
      case d: DecimalType if d.precision <= Decimal.MAX_INT_DIGITS => 4
      case t => t.defaultSize
    }
  }

  // The following types were copied from Spark's DecimalType class
  private val BooleanDecimal = DecimalType(1, 0)

  def optionallyAsDecimalType(t: DataType): Option[DecimalType] = t match {
    case dt: DecimalType => Some(dt)
    case ByteType | ShortType | IntegerType | LongType =>
      Some(DecimalType(GpuColumnVector.getNonNestedRapidsType(t).getPrecisionForInt, 0))
    case BooleanType => Some(BooleanDecimal)
    case _ => None
  }

  def asDecimalType(t: DataType): DecimalType = optionallyAsDecimalType(t) match {
    case Some(dt) => dt
    case _ =>
      throw new IllegalArgumentException(
        s"Internal Error: type $t cannot automatically be cast to a supported DecimalType")
  }
}
