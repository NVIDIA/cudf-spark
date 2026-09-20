/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.{lang => jl}
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.util.AccumulatorV2

case class NanoTime(value: java.lang.Long) {
  override def toString: String = {
    val hours = TimeUnit.NANOSECONDS.toHours(value)
    var remaining = value - TimeUnit.HOURS.toNanos(hours)
    val minutes = TimeUnit.NANOSECONDS.toMinutes(remaining)
    remaining = remaining - TimeUnit.MINUTES.toNanos(minutes)
    val seconds = remaining.toDouble / TimeUnit.SECONDS.toNanos(1)
    val locale = Locale.US
    "%02d:%02d:%06.3f".formatLocal(locale, hours, minutes, seconds)
  }
}

// Format example:
//  10.74GB (11534336000 bytes)
//  1.23MB (1289750 bytes)
//  1020.10KB (1044585 bytes)
case class SizeInBytes(value: jl.Long) {
  override def toString: String = {
    var unitVal = value
    var remainVal = 0L
    var unitIndex = 0
    while (unitIndex < SizeInBytes.SizeUnitNames.length && unitVal >= 1024) {
      val nextUnitVal = unitVal >> 10
      remainVal = unitVal - (nextUnitVal << 10)
      unitVal = nextUnitVal
      unitIndex += 1
    }
    val finalVal = "%.2f".format(unitVal + (remainVal.toDouble / 1024))
    s"$finalVal${SizeInBytes.SizeUnitNames(unitIndex)} ($value bytes)"
  }
}

private object SizeInBytes {
  private val SizeUnitNames: Array[String] = Array("B", "KB", "MB", "GB", "TB", "PB", "EB")
}

class NanoSecondAccumulator extends AccumulatorV2[jl.Long, NanoTime] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0


  override def copy(): NanoSecondAccumulator = {
    val newAcc = new NanoSecondAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  def add (v: Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, NanoTime]): Unit = other match {
    case ns: NanoSecondAccumulator =>
      _sum += ns._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: NanoTime = NanoTime(_sum)
}

/**
 * Accumulator to sum up size in bytes, almost identical to LongAccumulator but with
 * a user-friendly representation of the value.
 */
class SizeInBytesAccumulator extends AccumulatorV2[jl.Long, SizeInBytes] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0

  override def copy(): SizeInBytesAccumulator = {
    val newAcc = new SizeInBytesAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, SizeInBytes]): Unit = other match {
    case sb: SizeInBytesAccumulator =>
      _sum += sb._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: SizeInBytes = SizeInBytes(_sum)

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue
}

class HighWatermarkAccumulator extends AccumulatorV2[jl.Long, SizeInBytes] {
  private var _value = 0L
  override def isZero: Boolean = _value == 0

  override def copy(): HighWatermarkAccumulator = {
    val newAcc = new HighWatermarkAccumulator
    newAcc._value = this._value
    newAcc
  }

  override def reset(): Unit = {
    _value = 0
  }

  override def add(v: jl.Long): Unit = {
    _value += v
  }

  override def merge(other: AccumulatorV2[jl.Long, SizeInBytes]): Unit = other match {
    case wa: HighWatermarkAccumulator =>
      _value = _value.max(wa._value)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: SizeInBytes = SizeInBytes(_value)
}

class MaxLongAccumulator extends AccumulatorV2[jl.Long, jl.Long] {
  private var _v = 0L

  override def isZero: Boolean = _v == 0

  override def copy(): MaxLongAccumulator = {
    val newAcc = new MaxLongAccumulator
    newAcc._v = this._v
    newAcc
  }

  override def reset(): Unit = {
    _v = 0L
  }

  override def add(v: jl.Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  def add(v: Long): Unit = {
    if(v > _v) {
      _v = v
    }
  }

  override def merge(other: AccumulatorV2[jl.Long, jl.Long]): Unit = other match {
    case o: MaxLongAccumulator =>
      add(o.value)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Long = _v
}

class AvgLongAccumulator extends AccumulatorV2[jl.Long, jl.Double] {
  private var _sum = 0L
  private var _count = 0L

  override def isZero: Boolean = _count == 0L

  override def copy(): AvgLongAccumulator = {
    val newAcc = new AvgLongAccumulator
    newAcc._sum = this._sum
    newAcc._count = this._count
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: AccumulatorV2[jl.Long, jl.Double]): Unit = other match {
    case o: AvgLongAccumulator =>
      _sum += o._sum
      _count += o._count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = if (_count != 0) {
    1.0 * _sum / _count
  } else 0;
}
