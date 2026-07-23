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
{"spark": "403"}
{"spark": "404"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.GpuUnaryMathExpression

/**
 * Hyperbolic inverse functions that match Spark's default implementation for this shim.
 *
 * Spark changed the CPU behavior for very large inputs so the naive formulas do not overflow
 * during the x*x intermediate:
 *   acosh(x) = log(x) + log(2), when x >= sqrt(Double.MaxValue)
 *   asinh(x) = sign(x) * (log(abs(x)) + log(2)), when abs(x) >= sqrt(Double.MaxValue) - 1
 *
 * For smaller values we intentionally keep Spark's default formulas, instead of using cudf's
 * improved unary operators, so the GPU result follows Spark CPU compatibility mode. The improved
 * RAPIDS path is still selected from SparkShims when improved float ops are enabled.
 */
object HyperbolicMathExpressions {
  private val LOG_2 = StrictMath.log(2.0)

  private[shims] val LARGE_ACOSH: Double = Math.sqrt(Double.MaxValue)
  private[shims] val LARGE_ASINH: Double = Math.sqrt(Double.MaxValue) - 1.0

  /**
   * Holds both the large-input predicate and the value for asinh's large branch. They need to stay
   * alive together until the final ifElse chooses between the large-input value and the basic
   * formula. Use safeClose so both columns are closed even if the first close throws.
   */
  private case class LargeAsinhResult(mask: ColumnVector, value: ColumnVector)
      extends AutoCloseable {
    override def close(): Unit = {
      Seq(mask, value).safeClose()
    }
  }

  private def logPlusLog2(input: ColumnVector): ColumnVector = {
    withResource(input.log()) { logInput =>
      withResource(Scalar.fromDouble(LOG_2)) { log2 =>
        logInput.add(log2)
      }
    }
  }

  private def largeAsinhResult(x: ColumnVector): LargeAsinhResult = {
    withResource(x.abs()) { ax =>
      // Compute the branch predicate while abs(x) is still available, then compute
      // sign(x) * (log(abs(x)) + log(2)). The returned mask/value pair is closed by
      // the caller after the final ifElse.
      val mask = withResource(Scalar.fromDouble(LARGE_ASINH)) { large =>
        ax.greaterOrEqualTo(large)
      }
      closeOnExcept(mask) { _ =>
        withResource(logPlusLog2(ax)) { absValue =>
          val negativeAbsValue = withResource(Scalar.fromDouble(-1.0)) { negativeOne =>
            absValue.mul(negativeOne)
          }
          val value = withResource(negativeAbsValue) { negativeAbsValue =>
            val isNegative = withResource(Scalar.fromDouble(0.0)) { zero =>
              x.lessThan(zero)
            }
            withResource(isNegative) { isNegative =>
              isNegative.ifElse(negativeAbsValue, absValue)
            }
          }
          closeOnExcept(value) { _ =>
            LargeAsinhResult(mask, value)
          }
        }
      }
    }
  }

  def acosh(x: ColumnVector): ColumnVector = {
    // The large and basic results must both be live for ifElse, but compute the large branch as
    // late as possible so it is not held while building the basic formula.
    val xSquaredMinusOne = withResource(x.mul(x)) { xSquared =>
      withResource(Scalar.fromDouble(1.0)) { one =>
        xSquared.sub(one)
      }
    }
    val sqrt = withResource(xSquaredMinusOne) { xSquaredMinusOne =>
      xSquaredMinusOne.sqrt()
    }
    val basicResult = withResource(sqrt) { sqrt =>
      withResource(x.add(sqrt)) { logInput =>
        logInput.log()
      }
    }
    withResource(basicResult) { basicResult =>
      val isLarge = withResource(Scalar.fromDouble(LARGE_ACOSH)) { large =>
        x.greaterOrEqualTo(large)
      }
      withResource(isLarge) { isLarge =>
        val largeResult = logPlusLog2(x)
        withResource(largeResult) { largeResult =>
          val largeOrBasic = isLarge.ifElse(largeResult, basicResult)
          withResource(largeOrBasic) { largeOrBasic =>
            // Spark returns NaN for values outside acosh's domain. Apply this after the
            // large/basic selection so invalid negative inputs cannot leak through as +/-Inf.
            val isInvalid = withResource(Scalar.fromDouble(1.0)) { one =>
              x.lessThan(one)
            }
            withResource(isInvalid) { isInvalid =>
              withResource(Scalar.fromDouble(Double.NaN)) { nan =>
                isInvalid.ifElse(nan, largeOrBasic)
              }
            }
          }
        }
      }
    }
  }

  def asinh(x: ColumnVector): ColumnVector = {
    // Keep the large branch predicate/value alive together until the final selection.
    // The basic formula intermediates are released step-by-step before that final ifElse.
    withResource(largeAsinhResult(x)) { large =>
      val xSquaredPlusOne = withResource(x.mul(x)) { xSquared =>
        withResource(Scalar.fromDouble(1.0)) { one =>
          xSquared.add(one)
        }
      }
      val sqrt = withResource(xSquaredPlusOne) { xSquaredPlusOne =>
        xSquaredPlusOne.sqrt()
      }
      val basicResult = withResource(sqrt) { sqrt =>
        withResource(x.add(sqrt)) { logInput =>
          logInput.log()
        }
      }
      withResource(basicResult) { basicResult =>
        large.mask.ifElse(large.value, basicResult)
      }
    }
  }
}

case class GpuAcosh(child: Expression) extends GpuUnaryMathExpression("ACOSH") {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    import HyperbolicMathExpressions._

    acosh(input.getBase)
  }
}

case class GpuAsinh(child: Expression) extends GpuUnaryMathExpression("ASINH") {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    import HyperbolicMathExpressions._

    asinh(input.getBase)
  }
}
