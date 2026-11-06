/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import ai.rapids.cudf._
import com.nvidia.spark.RapidsUDF
import com.udf.Arm.withResource

/**
 * GPU-accelerated version of [[ArraySegmentSumUDF]].
 */
class ArraySegmentSumRapidsUDF extends ArraySegmentSumUDF with RapidsUDF {

  /** Columnar implementation that runs on the GPU. */
  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 3, s"Unexpected argument count: ${args.length}")
    val values = args(0)
    val start = args(1)
    val length = args(2)
    require(numRows == values.getRowCount, s"Expected $numRows rows, received ${values.getRowCount}")
    require(values.getType == DType.LIST, s"values must be a LIST, got ${values.getType}")
    require(start.getType == DType.INT32, s"start must be INT32, got ${start.getType}")
    require(length.getType == DType.INT32, s"length must be INT32, got ${length.getType}")

    val zero = Scalar.fromInt(0)
    closeAllOnExit(zero) {
      withResource(withResource(values.countElements())(_.replaceNulls(zero))) { n =>
        withResource(start.replaceNulls(zero)) { startNN =>
          withResource(length.replaceNulls(zero)) { lenNN =>
            val (from, segLen) = computeFromAndSegLen(n, startNN, lenNN, zero)
            withResource(from) { f =>
              withResource(segLen) { sl =>
                withResource(sumSegments(values, f, sl)) { summed =>
                  applyOuterNulls(summed, values, start, length)
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Compute per-row `from` (segment start index) and `segLen` (segment length).
   * Rows that should sum to 0 (out-of-range start or non-positive length) yield segLen == 0.
   * All inputs are non-null INT32 columns; outputs are non-null INT32 columns.
   */
  private def computeFromAndSegLen(
      n: ColumnView,
      startNN: ColumnView,
      lenNN: ColumnView,
      zero: Scalar): (ColumnVector, ColumnVector) = {
    withResource(startNN.greaterOrEqualTo(zero)) { startNonNeg =>
      withResource(startNN.lessThan(n)) { startLtN =>
        withResource(lenNN.greaterThan(zero)) { lenPositive =>
          withResource(startNonNeg.and(startLtN)) { startInRange =>
            withResource(startInRange.and(lenPositive)) { valid =>
              withResource(n.sub(startNN, DType.INT32)) { remaining =>
                withResource(minColumns(lenNN, remaining)) { rawSegLen =>
                  val from = valid.ifElse(startNN, zero)
                  val segLen = closeOnExceptCV(from) {
                    valid.ifElse(rawSegLen, zero)
                  }
                  (from, segLen)
                }
              }
            }
          }
        }
      }
    }
  }

  /** Build gather map [from, from+segLen), segmentedGather the values, then SUM each segment. */
  private def sumSegments(
      values: ColumnView,
      from: ColumnView,
      segLen: ColumnView): ColumnVector = {
    withResource(ColumnVector.sequence(from, segLen)) { gatherMap =>
      withResource(values.segmentedGather(gatherMap, OutOfBoundsPolicy.NULLIFY)) { segments =>
        withResource(
          segments.listReduce(SegmentedReductionAggregation.sum(), NullPolicy.EXCLUDE, DType.INT64)
        ) { summed =>
          // Empty segments reduce to null -> replace with 0. Non-empty all-null segments also
          // reduce to null (EXCLUDE drops all) -> 0, matching the CPU "skip nulls" + acc=0 path.
          withResource(Scalar.fromLong(0L)) { zeroLong =>
            summed.replaceNulls(zeroLong)
          }
        }
      }
    }
  }

  /** Reapply outer null semantics: result is null where values, start, or length is null. */
  private def applyOuterNulls(
      summed: ColumnView,
      values: ColumnView,
      start: ColumnView,
      length: ColumnView): ColumnVector = {
    withResource(values.isNotNull()) { valuesValid =>
      withResource(start.isNotNull()) { startValid =>
        withResource(length.isNotNull()) { lengthValid =>
          withResource(valuesValid.and(startValid)) { valuesAndStartValid =>
            withResource(valuesAndStartValid.and(lengthValid)) { allValid =>
              withResource(Scalar.fromNull(DType.INT64)) { nullLong =>
                allValid.ifElse(summed, nullLong)
              }
            }
          }
        }
      }
    }
  }

  /** Element-wise min(a, b) for two INT32 columns. */
  private def minColumns(a: ColumnView, b: ColumnView): ColumnVector =
    a.binaryOp(BinaryOp.NULL_MIN, b, DType.INT32)

  /** Run a block then close the scalar afterwards. */
  private def closeAllOnExit[R](scalar: Scalar)(block: => R): R = {
    try block finally scalar.close()
  }

  /** Close a ColumnVector if the block throws. */
  private def closeOnExceptCV[R](cv: ColumnVector)(block: => R): R = {
    try block catch { case t: Throwable => cv.close(); throw t }
  }
}
