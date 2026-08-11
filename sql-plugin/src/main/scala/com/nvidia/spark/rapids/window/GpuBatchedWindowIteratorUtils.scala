/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import ai.rapids.cudf
import ai.rapids.cudf.{DType, Scalar}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq

object GpuBatchedWindowIteratorUtils {
  def cudfAnd(lhs: cudf.ColumnVector,
      rhs: cudf.ColumnVector): cudf.ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        lhs.and(rhs)
      }
    }
  }

  def areRowPartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector],
      indexes: Seq[Int]): Array[Boolean] = {
    withResourceIfAllowed(arePartsEqual(scalars, columns)) {
      case scala.util.Right(ret) => Seq.fill(indexes.length)(ret).toArray
      case scala.util.Left(column) =>
        indexes.map { index =>
          withResource(column.getScalarElement(index)) { scalar =>
            scalar.isValid && scalar.getBoolean
          }
        }.toArray
    }
  }

  def arePartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector]): Either[cudf.ColumnVector, Boolean] = {
    if (scalars.length != columns.length) {
      scala.util.Right(false)
    } else if (scalars.isEmpty && columns.isEmpty) {
      scala.util.Right(true)
    } else {
      scala.util.Left(computeMask(scalars, columns))
    }
  }

  private def computeMask(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector]): cudf.ColumnVector = {
    val dType = scalars.head.getType
    if (dType == DType.FLOAT32 || dType == DType.FLOAT64) {
      // We need to handle nans and nulls
      scalars.zip(columns).map {
        case (scalar, column) =>
          withResource(scalar.equalToNullAware(column)) { eq =>
            dType match {
              case DType.FLOAT32 if scalar.getFloat.isNaN =>
                withResource(column.isNan) { isNan =>
                  isNan.or(eq)
                }
              case DType.FLOAT64 if scalar.getDouble.isNaN =>
                withResource(column.isNan) { isNan =>
                  isNan.or(eq)
                }
              case _ => eq.incRefCount()
            }
          }
      }.reduce(cudfAnd)
    } else {
      scalars.zip(columns).map {
        case (scalar, column) => scalar.equalToNullAware(column)
      }.reduce(cudfAnd)
    }
  }

  def areOrdersEqual(
      scalars: Seq[Scalar],
      columns: Seq[cudf.ColumnVector],
      partsEqual: Either[cudf.ColumnVector, Boolean]): Either[cudf.ColumnVector, Boolean] = {
    if (scalars.length != columns.length) {
      scala.util.Right(false)
    } else if (scalars.isEmpty && columns.isEmpty) {
      // they are equal but only so far as the parts are also equal
      partsEqual match {
        case r@scala.util.Right(_) => r
        case scala.util.Left(mask) => scala.util.Left(mask.incRefCount())
      }
    } else {
      // Part mask and order by equality mask
      partsEqual match {
        case r@scala.util.Right(false) => r
        case scala.util.Right(true) =>
          scala.util.Left(computeMask(scalars, columns))
        case scala.util.Left(partMask) =>
          withResource(computeMask(scalars, columns)) { orderMask =>
            scala.util.Left(orderMask.and(partMask))
          }
      }
    }
  }

  def getScalarRow(index: Int, columns: Seq[cudf.ColumnVector]): Array[Scalar] =
    columns.safeMap(_.getScalarElement(index)).toArray
}
