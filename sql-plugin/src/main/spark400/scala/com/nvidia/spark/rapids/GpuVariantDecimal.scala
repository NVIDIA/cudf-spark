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

/*** spark-rapids-shim-json-lines
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.math.BigInteger

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, Table, VariantOperationResult,
  VariantOperationStatus, VariantUtils}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.DecimalType

/** Spark decimal TRY semantics over cuDF's exact decimal payload decoder. */
private[rapids] object GpuVariantDecimal {
  // Parquet Variant primitive IDs 8/9/10, shifted past the two basic-type bits, identify the
  // DECIMAL4/8/16 encodings. Unlike the native decoder, Spark limits each original encoding's
  // coefficient precision AND scale to 9/18/38. Other headers must not pass validation.
  private val SourcePrecisions = Array.tabulate(256) {
    case 32 => DType.DECIMAL32_MAX_PRECISION
    case 36 => DType.DECIMAL64_MAX_PRECISION
    case 40 => DType.DECIMAL128_MAX_PRECISION
    case _ => -1
  }
  private val UpperBounds = (0 to DecimalType.MAX_PRECISION).map { precision =>
    BigInteger.TEN.pow(precision).subtract(BigInteger.ONE)
  }
  private val LowerBounds = UpperBounds.map(_.negate())

  def isSupportedTargetType(target: DecimalType): Boolean =
    target.precision >= 1 && target.precision <= DecimalType.MAX_PRECISION &&
      target.scale >= 0 && target.scale <= target.precision

  def decode(
      extraction: VariantOperationResult,
      target: DecimalType)(fallback: => ColumnVector): ColumnVector = {
    require(isSupportedTargetType(target), s"unsupported Variant decimal target: $target")
    // One guard digit preserves HALF_UP despite native truncation. At scale38 every valid
    // source decimal has scale<=38, so decoding at scale38 is exact and needs no guard/round.
    val guardScale = math.min(target.scale + 1, DecimalType.MAX_PRECISION)
    val intermediate = DecimalType(DecimalType.MAX_PRECISION, guardScale)
    val nativeType = DType.create(DType.DTypeEnum.DECIMAL128, -guardScale)
    withResource(VariantUtils.castVariantValueWithStatus(
        extraction.getValue, nativeType, extraction.getStatus)) { decoded =>
      if (!GpuVariantExtraction.allRowsCovered(extraction.getStatus, decoded.getStatus) ||
          !fitsIntermediatePrecision(decoded.getValue, intermediate) ||
          !isValidSource(extraction, decoded.getValue, guardScale)) {
        // This is not final target overflow: the temporary scale may have overflowed even
        // though Spark can return a valid result. Preserve Spark errors/coercions as well.
        fallback
      } else if (guardScale == target.scale) {
        // Do not rely on castDecimalToDecimal's same-type shortcut to enforce precision.
        withResource(GpuCast.checkNFixDecimalBounds(decoded.getValue, target, ansiMode = false)) {
          _.castTo(DecimalUtil.createCudfDecimal(target))
        }
      } else {
        GpuCast.castDecimalToDecimal(decoded.getValue, intermediate, target, ansiMode = false)
      }
    }
  }

  private def fitsIntermediatePrecision(value: ColumnView, intermediate: DecimalType): Boolean = {
    // Native storage accepts 39-digit int128 values. Besides making the declared intermediate
    // truthful, this check prevents overflow inside the existing HALF_UP round helper.
    withResource(DecimalUtil.outOfBounds(value, intermediate)) { outOfBounds =>
      !BoolUtils.isAnyValidTrue(outOfBounds)
    }
  }

  private def isValidSource(
      extraction: VariantOperationResult,
      guard: ColumnView,
      guardScale: Int): Boolean = {
    withResource(sourcePrecisions(extraction.getValue)) { precision =>
      withResource(extraction.getValue.extractListElement(1)) { encodedScale =>
        withResource(encodedScale.castTo(DType.INT32)) { scale =>
          if (!sourceScalesValid(scale, precision, extraction.getStatus)) {
            false
          } else {
            withResource(sourceExponents(precision, scale, guardScale)) { exponent =>
              sourceCoefficientsValid(guard, exponent, guardScale, extraction.getStatus)
            }
          }
        }
      }
    }
  }

  private def sourceScalesValid(
      scale: ColumnView,
      precision: ColumnView,
      extractionStatus: ColumnView): Boolean = {
    withResource(scale.lessOrEqualTo(precision)) { validScale =>
      allRowsValid(validScale, extractionStatus)
    }
  }

  private def sourceExponents(
      precision: ColumnView,
      scale: ColumnView,
      guardScale: Int): ColumnVector = {
    withResource(Scalar.fromInt(guardScale)) { guardScaleValue =>
      withResource(precision.add(guardScaleValue)) { precisionAndGuard =>
        precisionAndGuard.sub(scale)
      }
    }
  }

  private def sourcePrecisions(raw: ColumnView): ColumnVector = {
    withResource(raw.extractListElement(0)) { header =>
      withResource(header.castTo(DType.INT32)) { indexes =>
        withResource(Scalar.fromInt(0)) { zero =>
          withResource(indexes.replaceNulls(zero)) { nonNullIndexes =>
            lookupSourcePrecisions(nonNullIndexes)
          }
        }
      }
    }
  }

  private def lookupSourcePrecisions(indexes: ColumnView): ColumnVector = {
    withResource(ColumnVector.fromInts(SourcePrecisions: _*)) { dictionary =>
      withResource(new Table(dictionary)) { table =>
        withResource(table.gather(indexes)) { mapped =>
          mapped.getColumn(0).incRefCount()
        }
      }
    }
  }

  private def sourceCoefficientsValid(
      guard: ColumnView,
      exponent: ColumnView,
      guardScale: Int,
      extractionStatus: ColumnView): Boolean = {
    // For original coefficient c, scale a, precision p and guard scale g>=1, a<=p implies
    // e=p+g-a>=1. Then |c|<10^p iff |trunc(c*10^(g-a))|<10^e, even when rescaling divides:
    // the threshold is an exact integer multiple of the divisor. Thus native truncation
    // cannot hide malformed source precision. Bounds are coefficients at scale -g, not
    // numeric powers at scale0. The earlier 38-digit guard check makes clamping e>38 safe.
    withResource(boundedExponents(exponent)) { indexes =>
      withResource(sourceBounds(indexes, guardScale)) { bounds =>
        coefficientsWithinBounds(guard, bounds, extractionStatus)
      }
    }
  }

  private def boundedExponents(exponent: ColumnView): ColumnVector = {
    withResource(Scalar.fromInt(0)) { zero =>
      withResource(Scalar.fromInt(DecimalType.MAX_PRECISION)) { maximum =>
        withResource(exponent.clamp(zero, maximum)) { clamped =>
          clamped.replaceNulls(zero)
        }
      }
    }
  }

  private def sourceBounds(indexes: ColumnView, guardScale: Int): Table = {
    withResource(ColumnVector.decimalFromBigInt(-guardScale, UpperBounds: _*)) { upper =>
      withResource(ColumnVector.decimalFromBigInt(-guardScale, LowerBounds: _*)) { lower =>
        withResource(new Table(upper, lower)) { table =>
          table.gather(indexes)
        }
      }
    }
  }

  private def coefficientsWithinBounds(
      guard: ColumnView,
      bounds: Table,
      extractionStatus: ColumnView): Boolean = {
    // Signed comparisons deliberately avoid abs(int128_min).
    withResource(guard.lessOrEqualTo(bounds.getColumn(0))) { belowUpper =>
      withResource(guard.greaterOrEqualTo(bounds.getColumn(1))) { aboveLower =>
        withResource(belowUpper.and(aboveLower)) { valid =>
          allRowsValid(valid, extractionStatus)
        }
      }
    }
  }

  private def allRowsValid(valid: ColumnView, extractionStatus: ColumnView): Boolean = {
    withResource(Scalar.fromBool(false)) { invalid =>
      withResource(valid.replaceNulls(invalid)) { nonNullValid =>
        withResource(validOrSqlNull(nonNullValid, extractionStatus)) { covered =>
          BoolUtils.isAllValidTrue(covered)
        }
      }
    }
  }

  private def validOrSqlNull(valid: ColumnView, extractionStatus: ColumnView): ColumnVector = {
    withResource(Scalar.fromUnsignedByte(VariantOperationStatus.ROW_NULL.getNativeId.toByte)) {
      sqlNull =>
        withResource(extractionStatus.equalTo(sqlNull)) { rowNull =>
          valid.or(rowNull)
        }
    }
  }
}
