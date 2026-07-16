/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.types.DecimalType

object RoundingShims {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[RoundCeil](
      "Computes the ceiling of the given expression to d decimal places",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (ceil, conf, p, r) => new BinaryExprMeta[RoundCeil](ceil, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          ceil.child.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > DType.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
          GpuOverrides.extractLit(ceil.scale).foreach { scale =>
            if (scale.value != null &&
                scale.value.asInstanceOf[Integer] != 0) {
              willNotWorkOnGpu("Scale other than 0 is not supported")
            }
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          // use Spark `RoundCeil.dataType` to keep consistent between Spark versions.
          GpuCeil(lhs, ceil.dataType)
        }
      }),
    GpuOverrides.expr[RoundFloor](
      "Computes the floor of the given expression to d decimal places",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (floor, conf, p, r) => new BinaryExprMeta[RoundFloor](floor, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          floor.child.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > DType.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
          GpuOverrides.extractLit(floor.scale).foreach { scale =>
            if (scale.value != null &&
                scale.value.asInstanceOf[Integer] != 0) {
              willNotWorkOnGpu("Scale other than 0 is not supported")
            }
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          // use Spark `RoundFloor.dataType` to keep consistent between Spark versions.
          GpuFloor(lhs, floor.dataType)
        }
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
