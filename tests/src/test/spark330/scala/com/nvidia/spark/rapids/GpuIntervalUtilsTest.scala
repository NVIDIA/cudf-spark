/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
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
{"spark": "411"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.GpuIntervalUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.util.{IntervalStringStyles, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND}
import org.apache.spark.sql.types.{DayTimeIntervalType => DT}

/**
 * Unit test cases for testing `GpuIntervalUtils.toDayTimeIntervalString`
 *
 */
class GpuIntervalUtilsTest extends AnyFunSuite {

  def testDayTimeToString(fromField: Byte, endField: Byte,
      testData: Array[(Long, String)]): Unit = {
    val caseName = s"testToDayTimeIntervalString for " +
        s"from ${DT.fieldToString(fromField)} to ${DT.fieldToString(endField)}"
    test(caseName) {
      withResource(ColumnVector.fromStrings(testData.map(e => e._2): _*)) { expected =>
        withResource(ColumnVector.fromStrings(testData.map(e =>
          IntervalUtils.toDayTimeIntervalString(e._1, IntervalStringStyles.ANSI_STYLE,
            fromField, endField)): _*)) { cpuRet =>
          withResource(ColumnVector.fromLongs(testData.map(e => e._1): _*)) { micros =>
            withResource(
              GpuIntervalUtils.toDayTimeIntervalString(micros, fromField, endField)
            ) { gpuRet =>
                  CudfTestHelper.assertColumnsAreEqual(expected, gpuRet)
                  CudfTestHelper.assertColumnsAreEqual(cpuRet, gpuRet)
            }
          }
        }
      }
    }
  }

  testDayTimeToString(DT.DAY, DT.DAY, Array(
    (0L, "INTERVAL '0' DAY"),
    (Long.MinValue, "INTERVAL '-106751991' DAY"),
    (Long.MaxValue, "INTERVAL '106751991' DAY"),
    (3 * MICROS_PER_DAY, "INTERVAL '3' DAY"),
    (-3 * MICROS_PER_DAY, "INTERVAL '-3' DAY"),
    (23 * MICROS_PER_DAY, "INTERVAL '23' DAY"),
    (-23 * MICROS_PER_DAY, "INTERVAL '-23' DAY"),
    (1 * MICROS_PER_DAY + 23 * MICROS_PER_HOUR, "INTERVAL '1' DAY"),
    (-123 * MICROS_PER_DAY - 23 * MICROS_PER_HOUR, "INTERVAL '-123' DAY")
  ))

  testDayTimeToString(DT.DAY, DT.HOUR, Array(
    (0L, "INTERVAL '0 00' DAY TO HOUR"),
    (Long.MinValue, "INTERVAL '-106751991 04' DAY TO HOUR"),
    (Long.MaxValue, "INTERVAL '106751991 04' DAY TO HOUR"),
    (3 * MICROS_PER_HOUR, "INTERVAL '0 03' DAY TO HOUR"),
    (123 * MICROS_PER_DAY + 3 * MICROS_PER_HOUR, "INTERVAL '123 03' DAY TO HOUR"),
    (-123 * MICROS_PER_DAY - 23 * MICROS_PER_HOUR, "INTERVAL '-123 23' DAY TO HOUR")
  ))

  testDayTimeToString(DT.DAY, DT.MINUTE, Array(
    (0L, "INTERVAL '0 00:00' DAY TO MINUTE"),
    (Long.MinValue, "INTERVAL '-106751991 04:00' DAY TO MINUTE"),
    (Long.MaxValue, "INTERVAL '106751991 04:00' DAY TO MINUTE"),
    (3 * MICROS_PER_MINUTE, "INTERVAL '0 00:03' DAY TO MINUTE"),
    (123 * MICROS_PER_DAY + 3 * MICROS_PER_HOUR + 4 * MICROS_PER_MINUTE,
        "INTERVAL '123 03:04' DAY TO MINUTE"),
    (-123 * MICROS_PER_DAY - 23 * MICROS_PER_HOUR - 59 * MICROS_PER_MINUTE,
        "INTERVAL '-123 23:59' DAY TO MINUTE")
  ))

  testDayTimeToString(DT.DAY, DT.SECOND, Array(
    (0L, "INTERVAL '0 00:00:00' DAY TO SECOND"),
    (Long.MinValue, "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND"),
    (Long.MaxValue, "INTERVAL '106751991 04:00:54.775807' DAY TO SECOND"),
    (3 * MICROS_PER_SECOND, "INTERVAL '0 00:00:03' DAY TO SECOND"),
    (3 * MICROS_PER_SECOND + 667L, "INTERVAL '0 00:00:03.000667' DAY TO SECOND"),
    (123 * MICROS_PER_DAY + 23 * MICROS_PER_HOUR, "INTERVAL '123 23:00:00' DAY TO SECOND"),
    (-123 * MICROS_PER_DAY - 23 * MICROS_PER_HOUR, "INTERVAL '-123 23:00:00' DAY TO SECOND"),
    (MICROS_PER_DAY + MICROS_PER_HOUR + MICROS_PER_MINUTE + MICROS_PER_SECOND + 10L,
        "INTERVAL '1 01:01:01.00001' DAY TO SECOND"),
    (-(MICROS_PER_DAY + MICROS_PER_HOUR + MICROS_PER_MINUTE + MICROS_PER_SECOND + 10L),
        "INTERVAL '-1 01:01:01.00001' DAY TO SECOND")
  ))

  testDayTimeToString(DT.HOUR, DT.HOUR, Array(
    (0L, "INTERVAL '00' HOUR"),
    (Long.MinValue, s"INTERVAL '${Long.MinValue / MICROS_PER_HOUR}' HOUR"),
    (Long.MaxValue, s"INTERVAL '${Long.MaxValue / MICROS_PER_HOUR}' HOUR"),
    (23 * MICROS_PER_HOUR, "INTERVAL '23' HOUR"),
    (3 * MICROS_PER_HOUR, "INTERVAL '03' HOUR"),
    (-3 * MICROS_PER_HOUR, "INTERVAL '-03' HOUR"),
    (23 * MICROS_PER_HOUR + 59 * MICROS_PER_MINUTE, "INTERVAL '23' HOUR"),
    (-100 * MICROS_PER_HOUR - 60 * MICROS_PER_MINUTE, "INTERVAL '-101' HOUR")
  ))

  testDayTimeToString(DT.HOUR, DT.MINUTE, Array(
    (0L, "INTERVAL '00:00' HOUR TO MINUTE"),
    (Long.MinValue, "INTERVAL '-2562047788:00' HOUR TO MINUTE"),
    (Long.MaxValue, "INTERVAL '2562047788:00' HOUR TO MINUTE"),
    (3 * MICROS_PER_HOUR, "INTERVAL '03:00' HOUR TO MINUTE"),
    (23 * MICROS_PER_HOUR, "INTERVAL '23:00' HOUR TO MINUTE"),
    (123 * MICROS_PER_HOUR + 23 * MICROS_PER_MINUTE, "INTERVAL '123:23' HOUR TO MINUTE"),
    (-123 * MICROS_PER_HOUR - 60 * MICROS_PER_MINUTE, "INTERVAL '-124:00' HOUR TO MINUTE")
  ))

  testDayTimeToString(DT.HOUR, DT.SECOND, Array(
    (0L, "INTERVAL '00:00:00' HOUR TO SECOND"),
    (Long.MinValue, "INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND"),
    (Long.MaxValue, "INTERVAL '2562047788:00:54.775807' HOUR TO SECOND"),
    (3 * MICROS_PER_HOUR, "INTERVAL '03:00:00' HOUR TO SECOND"),
    (23 * MICROS_PER_HOUR, "INTERVAL '23:00:00' HOUR TO SECOND"),
    (3 * MICROS_PER_HOUR + 4 * MICROS_PER_MINUTE + 5 * MICROS_PER_SECOND + 1000,
        "INTERVAL '03:04:05.001' HOUR TO SECOND"),
    (-(3 * MICROS_PER_HOUR + 4 * MICROS_PER_MINUTE + 5 * MICROS_PER_SECOND + 1000),
        "INTERVAL '-03:04:05.001' HOUR TO SECOND"),
    (123 * MICROS_PER_HOUR + 23 * MICROS_PER_MINUTE, "INTERVAL '123:23:00' HOUR TO SECOND"),
    (-123 * MICROS_PER_HOUR - 60 * MICROS_PER_MINUTE, "INTERVAL '-124:00:00' HOUR TO SECOND")
  ))

  testDayTimeToString(DT.MINUTE, DT.MINUTE, Array(
    (0L, "INTERVAL '00' MINUTE"),
    (Long.MinValue, s"INTERVAL '${Long.MinValue / MICROS_PER_MINUTE}' MINUTE"),
    (Long.MaxValue, s"INTERVAL '${Long.MaxValue / MICROS_PER_MINUTE}' MINUTE"),
    (23 * MICROS_PER_MINUTE, "INTERVAL '23' MINUTE"),
    (3 * MICROS_PER_MINUTE, "INTERVAL '03' MINUTE"),
    (-3 * MICROS_PER_MINUTE, "INTERVAL '-03' MINUTE"),
    (123 * MICROS_PER_MINUTE + 59 * MICROS_PER_SECOND, "INTERVAL '123' MINUTE"),
    (-100 * MICROS_PER_MINUTE - 60 * MICROS_PER_SECOND, "INTERVAL '-101' MINUTE")
  ))

  testDayTimeToString(DT.MINUTE, DT.SECOND, Array(
    (0L, "INTERVAL '00:00' MINUTE TO SECOND"),
    (Long.MinValue, s"INTERVAL '-153722867280:54.775808' MINUTE TO SECOND"),
    (Long.MaxValue, s"INTERVAL '153722867280:54.775807' MINUTE TO SECOND"),
    (23 * MICROS_PER_MINUTE, "INTERVAL '23:00' MINUTE TO SECOND"),
    (3 * MICROS_PER_MINUTE, "INTERVAL '03:00' MINUTE TO SECOND"),
    (123 * MICROS_PER_MINUTE + 23 * MICROS_PER_SECOND, "INTERVAL '123:23' MINUTE TO SECOND"),
    (-123 * MICROS_PER_MINUTE - 60 * MICROS_PER_SECOND, "INTERVAL '-124:00' MINUTE TO SECOND")
  ))

  testDayTimeToString(DT.SECOND, DT.SECOND, Array(
    (0L, "INTERVAL '00' SECOND"),
    (Long.MinValue, s"INTERVAL '-9223372036854.775808' SECOND"),
    (Long.MaxValue, s"INTERVAL '9223372036854.775807' SECOND"),
    (3 * MICROS_PER_SECOND, "INTERVAL '03' SECOND"),
    (-3 * MICROS_PER_SECOND, "INTERVAL '-03' SECOND"),
    (123 * MICROS_PER_SECOND + 999, "INTERVAL '123.000999' SECOND"),
    (-100 * MICROS_PER_SECOND - MICROS_PER_SECOND, "INTERVAL '-101' SECOND")
  ))
}
