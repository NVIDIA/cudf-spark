/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

/**
 * Abstraction for possible range-boundary specifications.
 *
 * This provides type disjunction for Long, BigInt and Double,
 * the three types that might represent a range boundary.
 */
abstract class RangeBoundaryValue {
  def long: Long = RangeBoundaryValue.long(this)
  def bigInt: BigInt = RangeBoundaryValue.bigInt(this)
  def double: Double = RangeBoundaryValue.double(this)
}

case class LongRangeBoundaryValue(value: Long) extends RangeBoundaryValue
case class BigIntRangeBoundaryValue(value: BigInt) extends RangeBoundaryValue
case class DoubleRangeBoundaryValue(value: Double) extends RangeBoundaryValue

object RangeBoundaryValue {

  def long(boundary: RangeBoundaryValue): Long = boundary match {
    case LongRangeBoundaryValue(l) => l
    case other => throw new NoSuchElementException(s"Cannot get `long` from $other")
  }

  def bigInt(boundary: RangeBoundaryValue): BigInt = boundary match {
    case BigIntRangeBoundaryValue(b) => b
    case other => throw new NoSuchElementException(s"Cannot get `bigInt` from $other")
  }

  def double(boundary: RangeBoundaryValue): Double = boundary match {
    case DoubleRangeBoundaryValue(d) => d
    case other => throw new NoSuchElementException(s"Cannot get `double` from $other")
  }

  def long(value: Long): LongRangeBoundaryValue = LongRangeBoundaryValue(value)

  def bigInt(value: BigInt): BigIntRangeBoundaryValue = BigIntRangeBoundaryValue(value)

  def double(value: Double): DoubleRangeBoundaryValue = DoubleRangeBoundaryValue(value)
}

case class ParsedBoundary(isUnbounded: Boolean, value: RangeBoundaryValue)
