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
package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{Arithmetic, RoundMode}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution for CheckOverflow. This cannot match the Spark CheckOverflow 100% because
 * Spark will calculate values in BigDecimal with unbounded precision and then see if there was an
 * overflow. This will check bounds, but can only detect that an overflow happened if the result is
 * outside the bounds of what the Spark type supports, but did not yet overflow the bounds for what
 * the CUDF type supports. For most operations when this is a possibility for the given precision
 * then the operator should fall back to the CPU, or have alternative ways of checking for overflow
 * prior to this being called.
 */
case class GpuCheckOverflow(child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends GpuUnaryExpression {
  private[this] val expectedCudfScale = -dataType.scale
  private[this] lazy val resultDType = if (dataType.precision > DType.DECIMAL64_MAX_PRECISION) {
    DType.create(DType.DTypeEnum.DECIMAL128, expectedCudfScale)
  } else if (dataType.precision > DType.DECIMAL32_MAX_PRECISION) {
    DType.create(DType.DTypeEnum.DECIMAL64, expectedCudfScale)
  } else {
    DType.create(DType.DTypeEnum.DECIMAL32, expectedCudfScale)
  }

  // SPARK-39190: snapshot the Origin at construction so the SQL query context survives the
  // reflection-based plan reconstruction (e.g. GpuBindReferences → mapChildren → makeCopy)
  // that otherwise resets `origin` to its default.
  val capturedOrigin: Origin = origin

  // Defensive: re-enter the captured origin around the case-class copy.  Spark's `mapChildren` /
  // `withNewChildren` already wrap with `CurrentOrigin.withOrigin(this.origin)`, but if any future
  // reconstruction path (e.g. direct reflection) bypasses that wrap, this override keeps the
  // parser-set SQL context attached to the rebound instance.
  override def withNewChildInternal(newChild: Expression): Expression =
    CurrentOrigin.withOrigin(capturedOrigin) {
      copy(child = newChild)
    }

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    CurrentOrigin.withOrigin(capturedOrigin) {
      val base = input.getBase
      val rounded = if (resultDType.equals(base.getType)) {
        base.incRefCount()
      } else {
        withResource(Arithmetic.round(base, dataType.scale, RoundMode.HALF_UP)) { rounded =>
          if (resultDType.getTypeId != base.getType.getTypeId) {
            rounded.castTo(resultDType)
          } else {
            rounded.incRefCount()
          }
        }
      }
      withResource(rounded) { rounded =>
        GpuCast.checkNFixDecimalBounds(rounded, dataType, !nullOnOverflow)
      }
    }

  override def nullable: Boolean = true
}

/**
 * A GPU substitution of PromotePrecision, which is a NOOP in Spark too.
 */
case class GpuPromotePrecision(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.incRefCount()
  override def dataType: DataType = child.dataType
}

case class GpuUnscaledValue(child: Expression) extends GpuUnaryExpression {
  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    if (input.getBase.getType.isBackedByInt) {
      withResource(input.getBase.bitCastTo(DType.INT32)) { int32View =>
        int32View.castTo(DType.INT64)
      }
    } else {
      withResource(input.getBase.bitCastTo(DType.INT64)) { view =>
        view.copyToColumnVector()
      }
    }
  }
}

case class GpuMakeDecimal(
    child: Expression,
    precision: Int,
    sparkScale: Int,
    nullOnOverflow: Boolean) extends GpuUnaryExpression {

  override def dataType: DecimalType = DecimalType(precision, sparkScale)
  override def nullable: Boolean = child.nullable || nullOnOverflow
  override def toString: String = s"MakeDecimal($child,$precision,$sparkScale)"

  private lazy val outputType =
    ai.rapids.cudf.DecimalUtils.createDecimalType(precision, sparkScale)

  private lazy val (minValue, maxValue) = {
    val bounds = ai.rapids.cudf.DecimalUtils.bounds(dataType.precision, dataType.scale)
    (bounds.getKey.unscaledValue().longValue(), bounds.getValue.unscaledValue().longValue())
  }

  private def makeResult(
      outOfBounds: ColumnVector,
      outputView: ColumnView): ColumnVector = {
    if (!nullOnOverflow) {
      withResource(outOfBounds.any()) { isAny =>
        if (isAny.isValid && isAny.getBoolean) {
          throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
        }
      }
      outputView.copyToColumnVector()
    } else {
      withResource(Scalar.fromNull(outputType)) { nullVal =>
        outOfBounds.ifElse(nullVal, outputView)
      }
    }
  }

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val base = input.getBase
    val outOfBounds = withResource(Scalar.fromLong(maxValue)) { maxScalar =>
      withResource(base.greaterThan(maxScalar)) { over =>
        withResource(Scalar.fromLong(minValue)) { minScalar =>
          withResource(base.lessThan(minScalar)) { under =>
            over.or(under)
          }
        }
      }
    }

    withResource(outOfBounds) { outOfBounds =>
      if (outputType.isBackedByInt) {
        // In production Spark, this path is unreachable: MakeDecimal is only
        // used in the SUM optimization where the input is always Long and
        // output precision = input precision + 10, so the minimum output
        // precision is 11 (always INT64-backed). We support it here for
        // test completeness when MakeDecimal is constructed directly with
        // precision <= 9.
        withResource(base.castTo(DType.INT32)) { int32 =>
          withResource(int32.bitCastTo(outputType)) {
            makeResult(outOfBounds, _)
          }
        }
      } else if (!outputType.isBackedByLong) {
        // DECIMAL128 (precision > 18) is not needed: MakeDecimal only
        // operates on Long inputs (max ~18 digits), and the GPU TypeSig
        // restricts output to DECIMAL_64. Higher precisions fall back to CPU.
        throw new IllegalStateException(
          s"Unexpected decimal output type in GpuMakeDecimal: $outputType")
      } else {
        // Precision 10-18: decimal is backed by INT64.
        // bitCastTo is zero-copy since widths already match.
        withResource(base.bitCastTo(outputType)) {
          makeResult(outOfBounds, _)
        }
      }
    }
  }
}
