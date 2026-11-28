/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType, Scalar, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Floating-point key identities distinguish signed zeros and canonicalize NaNs. */
object GpuFloatingPointKeyUtils {
  def encode(batch: ColumnarBatch, orderPreserving: Boolean = false): ColumnarBatch = {
    val columns = (0 until batch.numCols()).safeMap { i =>
      val col = batch.column(i).asInstanceOf[GpuColumnVector]
      col.dataType() match {
        case FloatType => GpuColumnVector.from(encodeKey(col, orderPreserving), IntegerType)
        case DoubleType => GpuColumnVector.from(encodeKey(col, orderPreserving), LongType)
        case _ => col.incRefCount()
      }
    }
    closeOnExcept(columns) { _ => new ColumnarBatch(columns.toArray, batch.numRows()) }
  }

  private def encodeKey(col: GpuColumnVector, orderPreserving: Boolean): CudfColumnVector = {
    val bits = encodeFloat(col)
    if (orderPreserving) {
      withResource(bits)(flipNegativeBits)
    } else {
      bits
    }
  }

  // This involution preserves numeric order as signed integers, with -0.0 before +0.0.
  private def flipNegativeBits(bits: ai.rapids.cudf.ColumnView): CudfColumnVector = {
    val mask = if (bits.getType == DType.INT32) {
      Scalar.fromInt(Int.MaxValue)
    } else {
      Scalar.fromLong(Long.MaxValue)
    }
    withResource(mask) { positiveBits =>
      withResource(Scalar.fromInt(0)) { zero =>
        withResource(bits.lessThan(zero)) { negative =>
          withResource(bits.bitXor(positiveBits)) { flipped =>
            negative.ifElse(flipped, bits)
          }
        }
      }
    }
  }

  def encodeFloat(col: GpuColumnVector): CudfColumnVector = {
    val (bitsType, nanBits) = col.dataType() match {
      case FloatType => (DType.INT32, Scalar.fromInt(java.lang.Float.floatToIntBits(Float.NaN)))
      case DoubleType =>
        (DType.INT64, Scalar.fromLong(java.lang.Double.doubleToLongBits(Double.NaN)))
    }
    withResource(nanBits) { nan =>
      withResource(col.getBase.isNan) { isNan =>
        withResource(col.getBase.bitCastTo(bitsType)) { bits =>
          isNan.ifElse(nan, bits)
        }
      }
    }
  }

  def decode(table: Table, types: Array[DataType],
      orderPreserving: Boolean = false): ColumnarBatch = {
    val columns = types.indices.safeMap { i =>
      val input = table.getColumn(i)
      val col = if (orderPreserving && (types(i) == FloatType || types(i) == DoubleType)) {
        flipNegativeBits(input)
      } else {
        input.incRefCount()
      }
      val restored: CudfColumnVector = withResource(col) { _ =>
        types(i) match {
          case FloatType => withResource(col.bitCastTo(DType.FLOAT32))(_.copyToColumnVector())
          case DoubleType => withResource(col.bitCastTo(DType.FLOAT64))(_.copyToColumnVector())
          case _ => col.incRefCount()
        }
      }
      GpuColumnVector.from(restored, types(i))
    }
    closeOnExcept(columns) { _ => new ColumnarBatch(columns.toArray, table.getRowCount.toInt) }
  }
}
