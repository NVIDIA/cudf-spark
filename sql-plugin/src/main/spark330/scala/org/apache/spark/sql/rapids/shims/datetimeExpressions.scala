/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import java.util.concurrent.TimeUnit

import ai.rapids.cudf.{BinaryOp, BinaryOperable, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

case class GpuTimeAdd(start: Expression,
                      interval: Expression,
                      timeZoneId: Option[String] = None)
  extends ShimBinaryExpression
    with GpuExpression
    with TimeZoneAwareExpression
    with ExpectsInputTypes
    with Serializable {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  val microSecondsInOneDay: Long = TimeUnit.DAYS.toMicros(1)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyTimestampType, TypeCollection(CalendarIntervalType, DayTimeIntervalType))

  override def dataType: DataType = start.dataType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) { rhs =>
        // lhs is start, rhs is interval
        (lhs, rhs) match {
          case (l, intervalS: GpuScalar) =>
            // get long type interval
            val interval = intervalS.dataType match {
              case CalendarIntervalType =>
                // Scalar does not support 'CalendarInterval' now, so use
                // the Scala value instead.
                // Skip the null check because it wll be detected by the following calls.
                val calendarI = intervalS.getValue.asInstanceOf[CalendarInterval]
                if (calendarI.months != 0) {
                  throw new UnsupportedOperationException("Months aren't supported at the moment")
                }
                calendarI.days * microSecondsInOneDay + calendarI.microseconds
              case _: DayTimeIntervalType =>
                intervalS.getValue.asInstanceOf[Long]
              case _ =>
                throw new UnsupportedOperationException(
                  "GpuTimeAdd unsupported data type: " + intervalS.dataType)
            }

            // add interval
            if (interval != 0) {
              withResource(Scalar.durationFromLong(DType.DURATION_MICROSECONDS, interval)) { d =>
                GpuColumnVector.from(timestampAddDuration(l.getBase, d), dataType)
              }
            } else {
              l.incRefCount()
            }
          case (l, r: GpuColumnVector) =>
            (l.dataType(), r.dataType) match {
              case (_: TimestampType, _: DayTimeIntervalType) =>
                // DayTimeIntervalType is stored as long
                // bitCastTo is similar to reinterpret_cast, it's fast, the time can be ignored.
                withResource(r.getBase.bitCastTo(DType.DURATION_MICROSECONDS)) { duration =>
                  GpuColumnVector.from(timestampAddDuration(l.getBase, duration), dataType)
                }
              case _ =>
                throw new UnsupportedOperationException(
                  "GpuTimeAdd takes column and interval as an argument only")
            }
          case _ =>
            throw new UnsupportedOperationException(
              "GpuTimeAdd takes column and interval as an argument only, the types " +
                s"passed are, left: ${lhs.getClass} right: ${rhs.getClass}")
        }
      }
    }
  }

  private def timestampAddDuration(cv: ColumnView, duration: BinaryOperable): ColumnVector = {
    // Not use cv.add(duration), because of it invoke BinaryOperable.implicitConversion,
    // and currently BinaryOperable.implicitConversion return Long
    // Directly specify the return type is TIMESTAMP_MICROSECONDS
    cv.binaryOp(BinaryOp.ADD, duration, DType.TIMESTAMP_MICROSECONDS)
  }
}
