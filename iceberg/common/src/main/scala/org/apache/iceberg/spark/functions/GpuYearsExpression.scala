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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{ExprMeta, GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.jni.iceberg.IcebergDateTimeUtil

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{DataType, IntegerType}

case class GpuYearsExpression(child: Expression) extends GpuUnaryExpression {
  override def dataType: DataType = IntegerType

  override def sql: String = s"iceberg.years(${child.sql})"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    IcebergDateTimeUtil.yearsFromEpoch(input.getBase);
}

object GpuYearsExpression {

  def tagExprForGpu(meta: ExprMeta[StaticInvoke], expectedDataType: DataType): Unit = {
    val valueExpr = meta.wrapped.arguments.head
    if (valueExpr.dataType != expectedDataType) {
      meta.willNotWorkOnGpu(s"Gpu years function does not support type ${valueExpr.dataType} " +
        s"as values")
    }
  }
}
