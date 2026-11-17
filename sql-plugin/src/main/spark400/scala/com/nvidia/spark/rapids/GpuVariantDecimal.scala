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
  // One small lookup/gather avoids multiple full-column header comparison/select pairs.
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

  /** Returns an owning GPU column, or None so the caller can fall back after extraction closes. */
  def decode(
      extraction: VariantOperationResult,
      target: DecimalType): Option[ColumnVector] = {
    require(isSupportedTargetType(target), s"unsupported Variant decimal target: $target")
    // One guard digit preserves HALF_UP despite native truncation. At scale38 every valid
    // source decimal has scale<=38, so decoding at scale38 is exact and needs no guard/round.
    val guardScale = math.min(target.scale + 1, DecimalType.MAX_PRECISION)
    val intermediate = DecimalType(DecimalType.MAX_PRECISION, guardScale)
    val nativeType = DType.create(DType.DTypeEnum.DECIMAL128, -guardScale)
    withResource(VariantUtils.castVariantValueWithStatus(
        extraction.getValue, nativeType, extraction.getStatus)) { decoded =>
      if (!GpuVariantExtraction.allRowsCovered(extraction.getStatus, decoded.getStatus) ||
          !isValidDecimal(extraction, decoded.getValue, intermediate)) {
        // The guard scale may have overflowed even though Spark can return a valid result.
        // Fall back to preserve Spark coercions and decimal-payload errors.
        None
      } else if (guardScale == target.scale) {
        // Do not rely on castDecimalToDecimal's same-type shortcut to enforce precision.
        Some(withResource(GpuCast.checkNFixDecimalBounds(
            decoded.getValue, target, ansiMode = false)) {
          _.castTo(DecimalUtil.createCudfDecimal(target))
        })
      } else {
        Some(GpuCast.castDecimalToDecimal(decoded.getValue, intermediate, target, ansiMode = false))
      }
    }
  }

  private def fitsIntermediatePrecision(
      value: ColumnView,
      intermediate: DecimalType): ColumnVector = {
    // Native storage accepts 39-digit int128 values. Besides making the declared intermediate
    // truthful, this check prevents overflow inside the existing HALF_UP round helper.
    withResource(DecimalUtil.outOfBounds(value, intermediate)) { outOfBounds =>
      outOfBounds.not()
    }
  }

  private def isValidDecimal(
      extraction: VariantOperationResult,
      guard: ColumnView,
      intermediate: DecimalType): Boolean = {
    withResource(withResource(sourcePrecisions(extraction.getValue)) { precision =>
      withResource(sourceScales(extraction.getValue)) { scale =>
        validSource(guard, precision, scale, intermediate.scale)
      }
    }) { sourceValid =>
      // Release the INT32 precision/scale columns before the final validation allocations.
      decimalRowsValid(guard, sourceValid, extraction.getStatus, intermediate)
    }
  }

  private def sourceScales(raw: ColumnView): ColumnVector = {
    withResource(raw.extractListElement(1)) { encodedScale =>
      encodedScale.castTo(DType.INT32)
    }
  }

  private def validSource(
      guard: ColumnView,
      precision: ColumnView,
      scale: ColumnView,
      guardScale: Int): ColumnVector = {
    withResource(sourceExponents(precision, scale, guardScale)) { exponent =>
      withResource(sourceCoefficientsValid(guard, exponent, guardScale)) { coefficientsValid =>
        withResource(scale.lessOrEqualTo(precision)) { scaleValid =>
          coefficientsValid.and(scaleValid)
        }
      }
    }
  }

  private def decimalRowsValid(
      guard: ColumnView,
      sourceValid: ColumnView,
      extractionStatus: ColumnView,
      intermediate: DecimalType): Boolean = {
    // Keep status coverage separate so unsupported rows avoid these validation allocations.
    // Combine the three decimal checks before their single device-to-host reduction.
    withResource(fitsIntermediatePrecision(guard, intermediate)) { fits =>
      withResource(sourceValid.and(fits)) { valid =>
        allRowsValid(valid, extractionStatus)
      }
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
    // Parquet Variant primitive IDs 8/9/10, shifted past the two basic-type bits, identify the
    // DECIMAL4/8/16 encodings. Spark limits their coefficient precision AND scale to 9/18/38.
    // All other headers retain the invalid precision.
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
      guardScale: Int): ColumnVector = {
    // For original coefficient c, scale a, precision p and guard scale g>=1, a<=p implies
    // e=p+g-a>=1. Then |c|<10^p iff |trunc(c*10^(g-a))|<10^e, even when rescaling divides:
    // the threshold is an exact integer multiple of the divisor. Thus native truncation
    // cannot hide malformed source precision. Bounds are coefficients at scale -g, not
    // numeric powers at scale0. The combined 38-digit guard check makes clamping e>38 safe;
    // invalid source scales are rejected separately, with gather indexes bounded in all cases.
    withResource(boundedExponents(exponent)) { indexes =>
      withResource(sourceBounds(indexes, guardScale)) { bounds =>
        coefficientsWithinBounds(guard, bounds)
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
      bounds: Table): ColumnVector = {
    // Signed comparisons deliberately avoid abs(int128_min).
    withResource(guard.lessOrEqualTo(bounds.getColumn(0))) { belowUpper =>
      withResource(guard.greaterOrEqualTo(bounds.getColumn(1))) { aboveLower =>
        belowUpper.and(aboveLower)
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
