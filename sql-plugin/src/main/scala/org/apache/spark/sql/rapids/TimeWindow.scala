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


package org.apache.spark.sql.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{AbstractDataType, DataType}

/**
 * Expression used internally to convert the TimestampType to Long and back without losing
 * precision, i.e. in microseconds. Used in time windowing.
 */
case class GpuPreciseTimestampConversion(
    child: Expression,
    fromType: DataType,
    toType: DataType) extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(fromType)
  override def dataType: DataType = toType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val outDType = GpuColumnVector.getNonNestedRapidsType(toType)
    withResource(input.getBase.bitCastTo(outDType)) { bitCast =>
      bitCast.copyToColumnVector()
    }
  }
}