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
package com.nvidia.spark.rapids.shims

import java.math.BigInteger
import java.util.concurrent.TimeUnit.{DAYS, HOURS, MINUTES, SECONDS}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, RegexProgram, Scalar}
import com.nvidia.spark.rapids.{BoolUtils, CloseableHolder}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, MONTHS_PER_YEAR}
import org.apache.spark.sql.rapids.shims.IntervalUtils
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType => DT, YearMonthIntervalType => YM}

/**
 * Parse DayTimeIntervalType string column to long column of micro seconds
 * Spark DayTimeIntervalType type:
 * https://spark.apache.org/docs/latest/sql-ref-datatypes.html
 * Spark parse DayTime:
 * https://github.com/apache/spark/blob/v3.2.1/
 * sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/IntervalUtils.scala#L275
 *
 * DayTimeIntervalType have 10 sub types, 10 types and examples are as following:
 *
 * INTERVAL DAY: INTERVAL '100' DAY
 * INTERVAL DAY TO HOUR: INTERVAL '100 10' DAY TO HOUR
 * INTERVAL DAY TO MINUTE: INTERVAL '100 10:30' DAY TO MINUTE
 * INTERVAL DAY TO SECOND: INTERVAL '100 10:30:40.999999' DAY TO SECOND
 * INTERVAL HOUR: INTERVAL '123' HOUR
 * INTERVAL HOUR TO MINUTE: INTERVAL '123:10' HOUR TO MINUTE
 * INTERVAL HOUR TO SECOND: INTERVAL '123:10:59' HOUR TO SECOND
 * INTERVAL MINUTE: INTERVAL '1000' MINUTE
 * INTERVAL MINUTE TO SECOND: INTERVAL '1000:01.001' MINUTE TO SECOND
 * INTERVAL SECOND: INTERVAL '1000.000001' SECOND
 *
 * For each sub type, there 2 valid forms, take DAY TO SECOND for example:
 * INTERVAL '100 10:30:40.999999' DAY TO SECOND         the normal mode
 * 100 10:30:40.999999                                  the short mode
 *
 * Note: Currently not supporting the short mode, because of CSV writing generates the normal mode
 *
 * DAY, days in the range [0..106751991]
 * HOUR, hours within days [0..23]
 * Note if HOUR is leading item, range is [0, Long.max/ micros in one hour]
 * MINUTE, minutes within hours [0..59]
 * Note if MINUTE is leading item, range is [0, Long.max/ micros in one minute]
 * SECOND, seconds within minutes and possibly fractions of a second [0..59.999999]
 * Note if SECOND is leading item, range is [0, Long.max/ micros in one second]
 * Max second within minutes should be 59,
 * but Spark use 99, see this issue: https://issues.apache.org/jira/browse/SPARK-38324,
 * should update correspondingly if Spark fixes this issue
 *
 */
trait GpuIntervalUtilsBase {
  val MAX_DAY: Long = Long.MaxValue / DAYS.toMicros(1)
  val MAX_HOUR: Long = Long.MaxValue / HOURS.toMicros(1)
  val MAX_MINUTE: Long = Long.MaxValue / MINUTES.toMicros(1)
  val MAX_SECOND: Long = Long.MaxValue / SECONDS.toMicros(1)
  val MAX_HOUR_IN_DAY = 23L
  val MAX_MINUTE_IN_HOUR = 59L

  val prefixStr = "INTERVAL '"
  // used for casting Long.MinValue to string
  val minLongBaseStr = "-106751991 04:00:54.775808000"

  // literals ignore upper and lower cases
  private val INTERVAL = "[iI][nN][tT][eE][rR][vV][aA][lL]"
  private val DAY = "[dD][aA][yY]"
  private val HOUR = "[hH][oO][uU][rR]"
  private val MINUTE = "[mM][iI][nN][uU][tT][eE]"
  private val SECOND = "[sS][eE][cC][oO][nN][dD]"
  private val TO = "[tT][oO]"

  // + or -
  private val sign = "([+\\-])?"
  private val blanks = "\\s+"
  private val normalPattern = "(\\d{1,2})"
  private val dayBoundPattern = "(\\d{1,9})"
  private val hourBoundPattern = "(\\d{1,10})"
  private val minuteBoundPattern = "(\\d{1,12})"
  private val secondBoundPattern = "(\\d{1,13})"
  private val microPattern = "(\\.\\d{1,9})?"

  private val dayPatternString = s"$sign$dayBoundPattern"
  private val dayLiteralRegex = s"^$INTERVAL$blanks$sign'$dayPatternString'$blanks$DAY$$"

  private val dayHourPatternString = s"$sign$dayBoundPattern $normalPattern"
  private val dayHourLiteralRegex =
    s"^$INTERVAL$blanks$sign'$dayHourPatternString'$blanks$DAY$blanks$TO$blanks$HOUR$$"

  private val dayMinutePatternString = s"$sign$dayBoundPattern $normalPattern:$normalPattern"
  private val dayMinuteLiteralRegex =
    s"^$INTERVAL$blanks$sign'$dayMinutePatternString'$blanks$DAY$blanks$TO$blanks$MINUTE$$"

  private val daySecondPatternString =
    s"$sign$dayBoundPattern $normalPattern:$normalPattern:$normalPattern$microPattern"
  private val daySecondLiteralRegex =
    s"^$INTERVAL$blanks$sign'$daySecondPatternString'$blanks$DAY$blanks$TO$blanks$SECOND$$"

  private val hourPatternString = s"$sign$hourBoundPattern"
  private val hourLiteralRegex = s"^$INTERVAL$blanks$sign'$hourPatternString'$blanks$HOUR$$"

  private val hourMinutePatternString = s"$sign$hourBoundPattern:$normalPattern"
  private val hourMinuteLiteralRegex =
    s"^$INTERVAL$blanks$sign'$hourMinutePatternString'$blanks$HOUR$blanks$TO$blanks$MINUTE$$"

  private val hourSecondPatternString =
    s"$sign$hourBoundPattern:$normalPattern:$normalPattern$microPattern"
  private val hourSecondLiteralRegex =
    s"^$INTERVAL$blanks$sign'$hourSecondPatternString'$blanks$HOUR$blanks$TO$blanks$SECOND$$"

  private val minutePatternString = s"$sign$minuteBoundPattern"
  private val minuteLiteralRegex = s"^$INTERVAL$blanks$sign'$minutePatternString'$blanks$MINUTE$$"

  private val minuteSecondPatternString =
    s"$sign$minuteBoundPattern:$normalPattern$microPattern"
  private val minuteSecondLiteralRegex =
    s"^$INTERVAL$blanks$sign'$minuteSecondPatternString'$blanks$MINUTE$blanks$TO$blanks$SECOND$$"

  private val secondPatternString = s"$sign$secondBoundPattern$microPattern"
  private val secondLiteralRegex = s"^$INTERVAL$blanks$sign'$secondPatternString'$blanks$SECOND$$"

  def castStringToDayTimeIntervalWithThrow(cv: ColumnView, t: DataType): ColumnVector = {
    castStringToDayTimeIntervalWithThrow(cv, t.asInstanceOf[DT])
  }

  /**
   * Cast string column to long column, throw exception if the casting of a row failed
   * Fail reasons includes: regexp not match, range check failed, overflow when adding
   * @param cv string column
   * @param t  day-time interval type
   * @return long column of micros
   * @throws IllegalArgumentException if have a row failed
   */
  def castStringToDayTimeIntervalWithThrow(cv: ColumnView, t: DT): ColumnVector = {
    withResource(castStringToDTInterval(cv, t)) { ret =>
      if(ret.getNullCount > cv.getNullCount) {
        throw new IllegalArgumentException("Cast string to day time interval failed, " +
            "may be the format is invalid, range check failed or overflow")
      } else {
        ret.incRefCount()
      }
    }
  }

  /**
   * Cast string column to long column, if the casting of a row failed set null by default.
   * Fail reason includes: regexp not match, range check failed, overflow when adding
   *
   * @param cv             string column
   * @param t              day-time interval type
   * @return long column of micros
   */
  def castStringToDTInterval(cv: ColumnView, t: DT): ColumnVector = {
    (t.startField, t.endField) match {
      case (DT.DAY, DT.DAY) =>
        withResource(cv.extractRe(new RegexProgram(dayLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromDayToDay(sign,
              groupsTable.getColumn(2) // day
            )
          }
        }

      case (DT.DAY, DT.HOUR) =>
        withResource(cv.extractRe(new RegexProgram(dayHourLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromDayToHour(sign,
              groupsTable.getColumn(2), // day
              groupsTable.getColumn(3) // hour
            )
          }
        }

      case (DT.DAY, DT.MINUTE) =>
        withResource(cv.extractRe(new RegexProgram(dayMinuteLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromDayToMinute(sign,
              groupsTable.getColumn(2), // day
              groupsTable.getColumn(3), // hour
              groupsTable.getColumn(4) // minute
            )
          }
        }

      case (DT.DAY, DT.SECOND) =>
        withResource(cv.extractRe(new RegexProgram(daySecondLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromDayToSecond(sign,
              groupsTable.getColumn(2), // day
              groupsTable.getColumn(3), // hour
              groupsTable.getColumn(4), // minute
              groupsTable.getColumn(5), // second
              groupsTable.getColumn(6) // micro
            )
          }
        }

      case (DT.HOUR, DT.HOUR) =>
        withResource(cv.extractRe(new RegexProgram(hourLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromHourToHour(sign,
              groupsTable.getColumn(2) // hour
            )
          }
        }

      case (DT.HOUR, DT.MINUTE) =>
        withResource(cv.extractRe(new RegexProgram(hourMinuteLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromHourToMinute(sign,
              groupsTable.getColumn(2), // hour
              groupsTable.getColumn(3) // minute
            )
          }
        }

      case (DT.HOUR, DT.SECOND) =>
        withResource(cv.extractRe(new RegexProgram(hourSecondLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromHourToSecond(sign,
              groupsTable.getColumn(2), // hour
              groupsTable.getColumn(3), // minute
              groupsTable.getColumn(4), // second
              groupsTable.getColumn(5) // micros
            )
          }
        }

      case (DT.MINUTE, DT.MINUTE) =>
        withResource(cv.extractRe(new RegexProgram(minuteLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromMinuteToMinute(sign,
              groupsTable.getColumn(2) // minute
            )
          }
        }

      case (DT.MINUTE, DT.SECOND) =>
        withResource(cv.extractRe(new RegexProgram(minuteSecondLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromMinuteToSecond(sign,
              groupsTable.getColumn(2), // minute
              groupsTable.getColumn(3), // second
              groupsTable.getColumn(4) // micro
            )
          }
        }

      case (DT.SECOND, DT.SECOND) =>
        withResource(cv.extractRe(new RegexProgram(secondLiteralRegex))) { groupsTable =>
          withResource(finalSign(groupsTable.getColumn(0), groupsTable.getColumn(1))) { sign =>
            addFromSecondToSecond(sign,
              groupsTable.getColumn(2), // second
              groupsTable.getColumn(3) // micro
            )
          }
        }

      case _ =>
        throw new RuntimeException(
          s"Not supported DayTimeIntervalType(${t.startField}, ${t.endField})")
    }
  }

  // get sign column of long type with 1L or -1L in it
  // not close firstSignInTable and secondSignInTable here, outer table.close will close them
  private def finalSign(
      firstSignInTable: ColumnVector, secondSignInTable: ColumnVector): ColumnVector = {
    val negatives = withResource(Scalar.fromString("-")) { negScalar =>
      withResource(Seq(firstSignInTable, secondSignInTable).safeMap(negScalar.equalTo)) {
        signMatches =>
          // cuDF returns null for optional capture groups that did not participate in the match.
          // A missing sign is positive, so normalize those null comparisons to false before XOR.
          withResource(Scalar.fromBool(false)) { falseScalar =>
            withResource(signMatches.safeMap(_.replaceNulls(falseScalar))) {
              case Seq(neg1, neg2) => neg1.bitXor(neg2)
            }
          }
      }
    }

    withResource(negatives) { _ =>
      withResource(Seq(1L, -1L).safeMap(Scalar.fromLong)) { case Seq(posOne, negOne) =>
        negatives.ifElse(negOne, posOne)
      }
    }
  }

  /**
   * get micro seconds from decimal string column and truncate the nano seconds
   * e.g.: .123456789 => 123456
   *
   * @param decimal string column
   * @return micros column
   */
  protected def getMicrosFromDecimal(sign: ColumnVector, decimal: ColumnVector): ColumnVector = {
    val decimalType64_6 = DType.create(DType.DTypeEnum.DECIMAL64, -6)
    val timesMillion = withResource(Scalar.fromLong(1000000L)) { million =>
      // An absent optional fractional-seconds capture is null. It represents zero micros rather
      // than a failed parse, so normalize it before the decimal conversion.
      withResource(Scalar.fromString("0")) { zero =>
        withResource(decimal.replaceNulls(zero)) { normalizedDecimal =>
          withResource(normalizedDecimal.castTo(decimalType64_6)) {
            _.mul(million)
          }
        }
      }
    }
    val timesMillionLongs = withResource(timesMillion) {
      _.asLongs()
    }
    withResource(timesMillionLongs) {
      _.mul(sign)
    }
  }

  private def addFromDayToDay(
      sign: ColumnVector,
      daysInTable: ColumnVector
  ): ColumnVector = {
    daysToMicros(sign, daysInTable, MAX_DAY)
  }

  private def addFromDayToHour(
      sign: ColumnVector,
      daysInTable: ColumnVector,
      hoursInTable: ColumnVector
  ): ColumnVector = {
    add(daysToMicros(sign, daysInTable, MAX_DAY),
      hoursToMicros(sign, hoursInTable, MAX_HOUR_IN_DAY))
  }

  private def addFromDayToMinute(
      sign: ColumnVector,
      daysInTable: ColumnVector,
      hoursInTable: ColumnVector,
      minutesInTable: ColumnVector
  ): ColumnVector = {
    add(daysToMicros(sign, daysInTable, MAX_DAY),
      add(hoursToMicros(sign, hoursInTable, MAX_HOUR_IN_DAY),
        minutesToMicros(sign, minutesInTable, MAX_MINUTE_IN_HOUR)))
  }

  protected def addFromDayToSecond(
      sign: ColumnVector,
      daysInTable: ColumnVector,
      hoursInTable: ColumnVector,
      minutesInTable: ColumnVector,
      secondsInTable: ColumnVector,
      microsInTable: ColumnVector
  ): ColumnVector = {
    add(daysToMicros(sign, daysInTable, MAX_DAY),
      add(hoursToMicros(sign, hoursInTable, MAX_HOUR_IN_DAY),
        add(minutesToMicros(sign, minutesInTable, MAX_MINUTE_IN_HOUR),
          add(secondsToMicros(sign, secondsInTable), // max value is 99, no overflow
            getMicrosFromDecimal(sign, microsInTable))))) // max value is 999999999, no overflow
  }

  private def addFromHourToHour(
      sign: ColumnVector,
      hoursInTable: ColumnVector
  ): ColumnVector = {
    hoursToMicros(sign, hoursInTable, MAX_HOUR)
  }

  private def addFromHourToMinute(
      sign: ColumnVector,
      hoursInTable: ColumnVector,
      minutesInTable: ColumnVector
  ): ColumnVector = {
    add(hoursToMicros(sign, hoursInTable, MAX_HOUR),
      minutesToMicros(sign, minutesInTable, MAX_MINUTE_IN_HOUR))
  }

  private def addFromHourToSecond(
      sign: ColumnVector,
      hoursInTable: ColumnVector,
      minutesInTable: ColumnVector,
      secondsInTable: ColumnVector,
      microsInTable: ColumnVector
  ): ColumnVector = {
    add(hoursToMicros(sign, hoursInTable, MAX_HOUR),
      add(minutesToMicros(sign, minutesInTable, MAX_MINUTE_IN_HOUR),
        add(secondsToMicros(sign, secondsInTable),
          getMicrosFromDecimal(sign, microsInTable))))
  }

  private def addFromMinuteToMinute(
      sign: ColumnVector,
      minutesInTable: ColumnVector
  ): ColumnVector = {
    minutesToMicros(sign, minutesInTable, MAX_MINUTE)
  }

  private def addFromMinuteToSecond(
      sign: ColumnVector,
      minutesInTable: ColumnVector,
      secondsInTable: ColumnVector,
      microsInTable: ColumnVector
  ): ColumnVector = {
    add(minutesToMicros(sign, minutesInTable, MAX_MINUTE),
      add(secondsToMicros(sign, secondsInTable),
        getMicrosFromDecimal(sign, microsInTable)))
  }

  private def addFromSecondToSecond(
      sign: ColumnVector,
      secondsInTable: ColumnVector,
      microsInTable: ColumnVector
  ): ColumnVector = {
    add(secondsToMicros(sign, secondsInTable, MAX_SECOND),
      getMicrosFromDecimal(sign, microsInTable))
  }

  // Check overflow. It is true when both arguments have the opposite sign of the result.
  // Which is equal to "((x ^ r) & (y ^ r)) < 0" in the form of arithmetic.
  private def getOverflow(lhs: ColumnVector, rhs: ColumnVector, ret: ColumnVector): ColumnVector = {
    val signCV = withResource(ret.bitXor(lhs)) { lXor =>
      withResource(ret.bitXor(rhs)) { rXor =>
        lXor.bitAnd(rXor)
      }
    }
    withResource(signCV) { sign =>
      withResource(Scalar.fromInt(0)) { zero =>
        sign.lessThan(zero)
      }
    }
  }

  // set null if overflow
  private def setNullIfOverflow(
      lhs: ColumnVector, rhs: ColumnVector, ret: ColumnVector): ColumnVector = {
    withResource(getOverflow(lhs, rhs, ret)) { overflow =>
      withResource(Scalar.fromNull(DType.INT64)) { nullScalar =>
        // if overflow, set as null
        overflow.ifElse(nullScalar, ret)
      }
    }
  }

  // Add left and right to a new one and then close them.
  // Check overflow and set null
  protected def add(left: ColumnVector, right: ColumnVector): ColumnVector = {
    withResource(left) { l =>
      withResource(right) { r =>
        withResource(l.add(r)) { result =>
          setNullIfOverflow(l, r, result)
        }
      }
    }
  }

  protected def daysToMicros(
      sign: ColumnVector, daysInGroupTable: ColumnVector, maxDay: Long): ColumnVector = {
    multiple(sign, daysInGroupTable, DAYS.toMicros(1), maxDay)
  }

  protected def hoursToMicros(
      sign: ColumnVector, hoursInGroupTable: ColumnVector, maxHour: Long): ColumnVector = {
    multiple(sign, hoursInGroupTable, HOURS.toMicros(1), maxHour)
  }

  protected def minutesToMicros(
      sign: ColumnVector, minutesInGroupTable: ColumnVector, maxMinute: Long): ColumnVector = {
    multiple(sign, minutesInGroupTable, MINUTES.toMicros(1), maxMinute)
  }

  protected def secondsToMicros(
      sign: ColumnVector, secondsInGroupTable: ColumnVector, maxSecond: Long): ColumnVector = {
    multiple(sign, secondsInGroupTable, SECONDS.toMicros(1), maxSecond)
  }

  private def secondsToMicros(
      sign: ColumnVector, secondsInGroupTable: ColumnVector): ColumnVector = {
    multiple(sign, secondsInGroupTable, SECONDS.toMicros(1))
  }

  /**
   * Check range, return sign * base * multiple.
   *
   * @param sign      long column with 1L or -1L in it
   * @param base      string column contains positive long
   * @param multiple  const long value
   * @param maxInBase the max value for base column
   * @return
   */
  private def multiple(
      sign: ColumnVector, base: ColumnVector, multiple: Long, maxInBase: Long): ColumnVector = {
    // check max limit, set null if exceeds the max value
    val baseWithFixCv = withResource(Scalar.fromLong(maxInBase)) { maxScalar =>
      withResource(Scalar.fromNull(DType.INT64)) { nullScalar =>
        withResource(base.castTo(DType.INT64)) { baseLong =>
          withResource(baseLong.greaterThan(maxScalar)) { greater =>
            greater.ifElse(nullScalar, baseLong)
          }
        }
      }
    }
    val baseWithSignCv = withResource(baseWithFixCv) { baseWithFix =>
      baseWithFix.mul(sign)
    }
    withResource(baseWithSignCv) { baseWithSign =>
      withResource(Scalar.fromLong(multiple)) { multipleScalar =>
        baseWithSign.mul(multipleScalar)
      }
    }
  }

  /**
   * Return sign * base * multiple.
   *
   * @param sign         long column
   * @param groupInTable string column contains positive long
   * @param multiple     const long value
   * @return sign * base * multiple
   */
  private def multiple(
      sign: ColumnVector, groupInTable: ColumnVector, multiple: Long): ColumnVector = {
    val baseWithSignCv = withResource(groupInTable.castTo(DType.INT64)) { baseLong =>
      baseLong.mul(sign)
    }
    withResource(baseWithSignCv) { baseWithSign =>
      withResource(Scalar.fromLong(multiple)) { multipleScalar =>
        baseWithSign.mul(multipleScalar)
      }
    }
  }

  def toDayTimeIntervalString(micros: ColumnView, dayTimeType: DataType): ColumnVector = {
    val t = dayTimeType.asInstanceOf[DT]
    toDayTimeIntervalString(micros, t.startField, t.endField)
  }

  /**
   * Cast day-time interval to string
   * Rewrite from org.apache.spark.sql.catalyst.util.IntervalUtils.toDayTimeIntervalString
   *
   * @param micros     long micro seconds
   * @param startField start field, valid values are [0, 3] indicates [DAY, HOUR, MINUTE, SECOND]
   * @param endField   end field, should >= startField,
   *                   valid values are [0, 3] indicates [DAY, HOUR, MINUTE, SECOND]
   * @return ANSI day-time interval string, e.g.: interval '01 08:30:30.001' DAY TO SECOND
   */
  def toDayTimeIntervalString(
      micros: ColumnView,
      startField: Byte,
      endField: Byte): ColumnVector = {

    val numRows = micros.getRowCount
    val from = DT.fieldToString(startField).toUpperCase
    val to = DT.fieldToString(endField).toUpperCase
    val postfixStr = s"' ${if (startField == endField) from else s"$from TO $to"}"

    val retCv = withResource(new ArrayBuffer[ColumnView]) { parts =>
        // prefix with sign part: INTERVAL ' or INTERVAL '-
        parts += withResource(Scalar.fromLong(0L)) { zero =>
          withResource(micros.lessThan(zero)) { less =>
            withResource(Scalar.fromString(prefixStr + "-")) { negPrefix =>
              withResource(Scalar.fromString(prefixStr)) { prefix =>
                less.ifElse(negPrefix, prefix)
              }
            }
          }
        }

        // calculate abs, abs(Long.MinValue) will overflow, handle in the last as special case
        withResource(new CloseableHolder(micros.abs())) { restHolder =>

        startField match {
          case DT.DAY =>
            // start day part
            parts += divResult(restHolder.get, MICROS_PER_DAY)
            restHolder.setAndCloseOld(getRest(restHolder.get, MICROS_PER_DAY))
          case DT.HOUR =>
            // start hour part
            parts += divResultWithPadding(restHolder.get, MICROS_PER_HOUR)
            restHolder.setAndCloseOld(getRest(restHolder.get, MICROS_PER_HOUR))
          case DT.MINUTE =>
            // start minute part
            parts += divResultWithPadding(restHolder.get, MICROS_PER_MINUTE)
            restHolder.setAndCloseOld(getRest(restHolder.get, MICROS_PER_MINUTE))
          case DT.SECOND =>
            // start second part
            addDecimalParts(restHolder.get, parts)
        }

        if (startField < DT.HOUR && DT.HOUR <= endField) {
          // if hour has precedent part
          parts += getConstStringVector(" ", numRows)
          parts += divResultWithPadding(restHolder.get, MICROS_PER_HOUR)
          restHolder.setAndCloseOld(getRest(restHolder.get, MICROS_PER_HOUR))
        }

        if (startField < DT.MINUTE && DT.MINUTE <= endField) {
          // if minute has precedent part
          parts += getConstStringVector(":", numRows)
          parts += divResultWithPadding(restHolder.get, MICROS_PER_MINUTE)
          restHolder.setAndCloseOld(getRest(restHolder.get, MICROS_PER_MINUTE))
        }

        if (startField < DT.SECOND && DT.SECOND <= endField) {
          // if second has precedent part
          parts += getConstStringVector(":", numRows)

          // start second part
          addDecimalParts(restHolder.get, parts)
        }

        // trailing part, e.g.:  ' DAY TO SECOND
        parts += getConstStringVector(postfixStr, numRows)

        // concatenate all the parts
        ColumnVector.stringConcatenate(parts.toArray[ColumnView])
      }
    }

    // special handling for Long.MinValue
    // if micros are Long.MinValue, directly replace with const string
    withResource(retCv) { ret =>
      val firstStr = startField match {
        case DT.DAY => s"-$MAX_DAY"
        case DT.HOUR => s"-$MAX_HOUR"
        case DT.MINUTE => s"-$MAX_MINUTE"
        case DT.SECOND => s"-$MAX_SECOND.775808"
      }
      val followingStr = if (startField == endField) {
        ""
      } else {
        val substrStart = startField match {
          case DT.DAY => 10
          case DT.HOUR => 13
          case DT.MINUTE => 16
        }
        val substrEnd = endField match {
          case DT.HOUR => 13
          case DT.MINUTE => 16
          case DT.SECOND => 26
        }
        minLongBaseStr.substring(substrStart, substrEnd)
      }
      val minStr = s"$prefixStr$firstStr$followingStr$postfixStr"
      withResource(Scalar.fromString(minStr)) { minStrScalar =>
        withResource(Scalar.fromLong(Long.MinValue)) { minS =>
          withResource(micros.equalTo(minS)) { eq =>
            eq.ifElse(minStrScalar, ret)
          }
        }
      }
    }
  }

  /**
   * return (micros / div).toString
   */
  private def divResult(micros: ColumnVector, div: Long): ColumnVector = {
    withResource(Scalar.fromLong(div)) { divS =>
      withResource(micros.div(divS)) { ret =>
        ret.castTo(DType.STRING)
      }
    }
  }

  /**
   * return (micros / div).toString with padding zero
   */
  private def divResultWithPadding(micros: ColumnVector, div: Long): ColumnVector = {
    withResource(divResult(micros, div)) { s =>
      // pad 0 if value < 10, e.g.:  9 => 09
      s.zfill(2)
    }
  }

  /**
   * return (micros % div)
   */
  private def getRest(micros: ColumnVector, div: Long): ColumnVector = {
    withResource(Scalar.fromLong(div)) { divS =>
      micros.mod(divS)
    }
  }

  private def getConstStringVector(s: String, numRows: Long): ColumnVector = {
    withResource(Scalar.fromString(s)) { scalar =>
      ai.rapids.cudf.ColumnVector.fromScalar(scalar, numRows.toInt)
    }
  }

  /**
   * Generate second decimal part with strip trailing `0` and `.`
   */
  private def addDecimalParts(rest: ColumnVector, parts: ArrayBuffer[ColumnView]) = {
    // if second less than 10, should pad zero. e.g.: `9.500000` => `09.500000`
    withResource(Scalar.fromString("0")) { padZero =>
      withResource(Scalar.fromString("")) { empty =>
        parts += withResource(Scalar.fromLong(10 * MICROS_PER_SECOND)) { tenSeconds =>
          withResource(rest.lessThan(tenSeconds)) { lessThan10 =>
            lessThan10.ifElse(padZero, empty)
          }
        }
      }
    }

    // get the second strings with fractional microseconds
    // the values always have dot and 6 fractional digits
    val decimalType = DType.create(DType.DTypeEnum.DECIMAL128, -6)
    // use big int to generate Decimal128 decimal scalar
    val microsPerSecondBigInt = new BigInteger(MICROS_PER_SECOND.toString)
    val decimalStrCv = withResource(rest.castTo(decimalType)) { decimal =>
      withResource(Scalar.fromDecimal(0, microsPerSecondBigInt)) { microsPerSecond =>
        withResource(decimal.div(microsPerSecond)) { r =>
          r.castTo(DType.STRING)
        }
      }
    }

    // strip trailing `0`
    // e.g.: 0.001000 => 0.001, 0.000000 => 0.
    val stripedCv = withResource(decimalStrCv) { decimalStr =>
      withResource(Scalar.fromString("0")) { zero =>
        decimalStr.rstrip(zero)
      }
    }

    // strip trailing `.` for spacial case:  0. => 0
    parts += withResource(stripedCv) { striped =>
      withResource(Scalar.fromString(".")) { dot =>
        striped.rstrip(dot)
      }
    }
  }

  def dayTimeIntervalToLong(dtCv: ColumnView, dt: DataType): ColumnVector = {
    dt.asInstanceOf[DT].endField match {
      case DT.DAY => withResource(Scalar.fromLong(MICROS_PER_DAY)) { micros =>
        dtCv.div(micros)
      }
      case DT.HOUR => withResource(Scalar.fromLong(MICROS_PER_HOUR)) { micros =>
        dtCv.div(micros)
      }
      case DT.MINUTE => withResource(Scalar.fromLong(MICROS_PER_MINUTE)) { micros =>
        dtCv.div(micros)
      }
      case DT.SECOND => withResource(Scalar.fromLong(MICROS_PER_SECOND)) { micros =>
        dtCv.div(micros)
      }
    }
  }

  def dayTimeIntervalToInt(dtCv: ColumnView, dt: DataType): ColumnVector = {
    withResource(dayTimeIntervalToLong(dtCv, dt)) { longCv =>
      castToTargetWithOverflowCheck(longCv, DType.INT32)
    }
  }

  def dayTimeIntervalToShort(dtCv: ColumnView, dt: DataType): ColumnVector = {
    withResource(dayTimeIntervalToLong(dtCv, dt)) { longCv =>
      castToTargetWithOverflowCheck(longCv, DType.INT16)
    }
  }

  def dayTimeIntervalToByte(dtCv: ColumnView, dt: DataType): ColumnVector = {
    withResource(dayTimeIntervalToLong(dtCv, dt)) { longCv =>
      castToTargetWithOverflowCheck(longCv, DType.INT8)
    }
  }

  def yearMonthIntervalToLong(ymCv: ColumnView, ym: DataType): ColumnVector = {
    ym.asInstanceOf[YM].endField match {
      case YM.YEAR => withResource(Scalar.fromLong(MONTHS_PER_YEAR)) { monthsPerYear =>
        ymCv.div(monthsPerYear)
      }
      case YM.MONTH => ymCv.castTo(DType.INT64)
    }
  }

  def yearMonthIntervalToInt(ymCv: ColumnView, ym: DataType): ColumnVector = {
    ym.asInstanceOf[YM].endField match {
      case YM.YEAR => withResource(Scalar.fromInt(MONTHS_PER_YEAR)) { monthsPerYear =>
        ymCv.div(monthsPerYear)
      }
      case YM.MONTH => ymCv.copyToColumnVector()
    }
  }

  def yearMonthIntervalToShort(ymCv: ColumnView, ym: DataType): ColumnVector = {
    withResource(yearMonthIntervalToInt(ymCv, ym)) { i =>
      castToTargetWithOverflowCheck(i, DType.INT16)
    }
  }

  def yearMonthIntervalToByte(ymCv: ColumnView, ym: DataType): ColumnVector = {
    withResource(yearMonthIntervalToInt(ymCv, ym)) { i =>
      castToTargetWithOverflowCheck(i, DType.INT8)
    }
  }

  private def castToTargetWithOverflowCheck(cv: ColumnView, dType: DType): ColumnVector = {
    withResource(cv.castTo(dType)) { retTarget =>
      withResource(cv.notEqualTo(retTarget)) { notEqual =>
        if (BoolUtils.isAnyValidTrue(notEqual)) {
          throw new ArithmeticException(s"overflow occurs when casting to $dType")
        } else {
          retTarget.incRefCount()
        }
      }
    }
  }

  /**
   * Convert long cv to `day time interval`
   */
  def longToDayTimeInterval(longCv: ColumnView, dt: DataType): ColumnVector = {
    val microsScalar = dt.asInstanceOf[DT].endField match {
      case DT.DAY => Scalar.fromLong(MICROS_PER_DAY)
      case DT.HOUR => Scalar.fromLong(MICROS_PER_HOUR)
      case DT.MINUTE => Scalar.fromLong(MICROS_PER_MINUTE)
      case DT.SECOND => Scalar.fromLong(MICROS_PER_SECOND)
    }
    withResource(microsScalar) { micros =>
      // leverage `Decimal 128` to check the overflow
      IntervalUtils.multipleToLongWithOverflowCheck(longCv, micros)
    }
  }

  /**
   * Convert (byte | short | int) cv to `day time interval`
   */
  def intToDayTimeInterval(intCv: ColumnView, dt: DataType): ColumnVector = {
    dt.asInstanceOf[DT].endField match {
      case DT.DAY => withResource(Scalar.fromLong(MICROS_PER_DAY)) { micros =>
        if (intCv.getType.equals(DType.INT32)) {
          // leverage `Decimal 128` to check the overflow
          // Int.MaxValue * `micros` can cause overflow
          IntervalUtils.multipleToLongWithOverflowCheck(intCv, micros)
        } else {
          // no need to check overflow for short byte types
          intCv.mul(micros)
        }
      }
      case DT.HOUR => withResource(Scalar.fromLong(MICROS_PER_HOUR)) { micros =>
        // no need to check overflow
        intCv.mul(micros)
      }
      case DT.MINUTE => withResource(Scalar.fromLong(MICROS_PER_MINUTE)) { micros =>
        // no need to check overflow
        intCv.mul(micros)
      }
      case DT.SECOND => withResource(Scalar.fromLong(MICROS_PER_SECOND)) { micros =>
        // no need to check overflow
        intCv.mul(micros)
      }
    }
  }

  /**
   * Convert long cv to `year month interval`
   */
  def longToYearMonthInterval(longCv: ColumnView, ym: DataType): ColumnVector = {
    ym.asInstanceOf[YM].endField match {
      case YM.YEAR => withResource(Scalar.fromLong(MONTHS_PER_YEAR)) { num12 =>
        // leverage `Decimal 128` to check the overflow
        IntervalUtils.multipleToIntWithOverflowCheck(longCv, num12)
      }
      case YM.MONTH => IntervalUtils.castLongToIntWithOverflowCheck(longCv)
    }
  }

  /**
   * Convert (byte | short | int) cv to `year month interval`
   */
  def intToYearMonthInterval(intCv: ColumnView, ym: DataType): ColumnVector = {
    (ym.asInstanceOf[YM].endField, intCv.getType) match {
      case (YM.YEAR, DType.INT32) => withResource(Scalar.fromInt(MONTHS_PER_YEAR)) { num12 =>
        // leverage `Decimal 128` to check the overflow
        IntervalUtils.multipleToIntWithOverflowCheck(intCv, num12)
      }
      case (YM.YEAR, DType.INT16 | DType.INT8) => withResource(Scalar.fromInt(MONTHS_PER_YEAR)) {
        num12 => intCv.mul(num12)
      }
      case (YM.MONTH, _) => intCv.castTo(DType.INT32)
    }
  }
}
