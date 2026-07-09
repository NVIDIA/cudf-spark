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
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.GpuIntervalUtils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DayTimeIntervalType, StructField, StructType}

class CsvScanForIntervalSuite extends SparkQueryCompareTestSuite {
  test("test castStringToDTInterval format valid") {
    withResource(ColumnVector.fromLongs(
      86400000000L,
      -86400000000L,
      86400000000L,

      86400000000L,
      -86400000000L,
      86400000000L,

      86400000000L,
      86400000000L,
      86400000000L,

      -86400000000L,
      -86400000000L,
      86400000000L
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval '1' day",
        "inTERval '-1' DAY",
        "INTERVAL -'-1' DAY",

        "interval '01' DAY",
        "interval '-01' DAY",
        "interval -'-01' DAY",

        "inTerVal             +'01'                 day",
        "INTeRVAL '+01' DaY",
        "INTERvAL             +'+01' dAY",

        "interval +'-01'                   dAY",
        "INTERVAL -'+01' DAy",
        "INTERVAL +'+01' DAy"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }
  }

  test("test castStringToDTInterval for 10 sub types") {
    val micros1Day = 86400L * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Day,
      -micros1Day,
      micros1Day,
      micros1Day
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1' day",
        "inTERval '-1' DAY",
        "INTERVAL -'-1' DAY",
        "INTERVAL +'+1' DAY"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Day1Hour = (86400L + 3600L) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Day1Hour,
      -micros1Day1Hour,
      micros1Day1Hour,
      micros1Day1Hour
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1 1' day to hour",
        "inTERval '-1 1' DAY to HOUR",
        "INTERVAL -'-1 1' DAY TO HOUR",
        "INTERVAL +'+1 1' DAY TO HOUR"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.HOUR))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Day1Hour1Minute = (86400L + 3600L + 60) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Day1Hour1Minute,
      -micros1Day1Hour1Minute,
      micros1Day1Hour1Minute,
      micros1Day1Hour1Minute
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1 1:1' day to MINUTE",
        "inTERval '-1 1:1' DAY to MINUTE",
        "INTERVAL -'-1 1:1' DAY TO MINUTE",
        "INTERVAL +'+1 1:1' DAY TO MINUTE"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.MINUTE))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Day1Hour1Minute1Second = (86400L + 3600L + 60 + 1) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Day1Hour1Minute1Second,
      -micros1Day1Hour1Minute1Second,
      micros1Day1Hour1Minute1Second,
      micros1Day1Hour1Minute1Second
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1 1:1:1' day to SECOND",
        "inTERval '-1 1:1:1' DAY to SECOND",
        "INTERVAL -'-1 1:1:1' DAY TO SECOND",
        "INTERVAL +'+1 1:1:1' DAY TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Day1Hour1Minute1Second1Micros = (86400L + 3600L + 60 + 1) * 1000000L + 100000L
    withResource(ColumnVector.fromLongs(
      -micros1Day1Hour1Minute1Second1Micros,
      -micros1Day1Hour1Minute1Second1Micros,
      micros1Day1Hour1Minute1Second1Micros,
      micros1Day1Hour1Minute1Second1Micros
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1 1:1:1.1' day to SECOND",
        "inTERval '-1 1:1:1.1' DAY to SECOND",
        "INTERVAL -'-1 1:1:1.1' DAY TO SECOND",
        "INTERVAL +'+1 1:1:1.1' DAY TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Hour = 3600L * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Hour,
      -micros1Hour,
      micros1Hour,
      micros1Hour
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1' hour",
        "inTERval '-1' hour",
        "INTERVAL -'-1' hour",
        "INTERVAL +'+1' hour"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.HOUR))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }


    val micros1Hour1Minute = (3600L + 60) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Hour1Minute,
      -micros1Hour1Minute,
      micros1Hour1Minute,
      micros1Hour1Minute
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1:1' hour to minute",
        "inTERval '-1:1' hour to MINUTE",
        "INTERVAL -'-1:1' hour TO MINUTE",
        "INTERVAL +'+1:1' hour TO MINUTE"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.MINUTE))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Hour1Minute1Second = (3600L + 60 + 1) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Hour1Minute1Second,
      -micros1Hour1Minute1Second,
      micros1Hour1Minute1Second,
      micros1Hour1Minute1Second
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1:1:1' HOUR to SECOND",
        "inTERval '-1:1:1' HOUR to SECOND",
        "INTERVAL -'-1:1:1' HOUR TO SECOND",
        "INTERVAL +'+1:1:1' HOUR TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Hour1Minute1Second1Micros = (3600L + 60 + 1) * 1000000L + 100000L
    withResource(ColumnVector.fromLongs(
      -micros1Hour1Minute1Second1Micros,
      -micros1Hour1Minute1Second1Micros,
      micros1Hour1Minute1Second1Micros,
      micros1Hour1Minute1Second1Micros
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1:1:1.1' HOUR to SECOND",
        "inTERval '-1:1:1.1' HOUR to SECOND",
        "INTERVAL -'-1:1:1.1' HOUR TO SECOND",
        "INTERVAL +'+1:1:1.1' HOUR TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND))) { actualCV =>
          CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Minute = 60 * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Minute,
      -micros1Minute,
      micros1Minute,
      micros1Minute
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1' MINUTE",
        "inTERval '-1' MINUTE",
        "INTERVAL -'-1' MINUTE",
        "INTERVAL +'+1' MINUTE"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.MINUTE))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Minute1Second = (60 + 1) * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Minute1Second,
      -micros1Minute1Second,
      micros1Minute1Second,
      micros1Minute1Second
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1:1' MINUTE to SECOND",
        "inTERval '-1:1' MINUTE to SECOND",
        "INTERVAL -'-1:1' MINUTE TO SECOND",
        "INTERVAL +'+1:1' MINUTE TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Minute1Second1Micros = (60 + 1) * 1000000L + 100000L
    withResource(ColumnVector.fromLongs(
      -micros1Minute1Second1Micros,
      -micros1Minute1Second1Micros,
      micros1Minute1Second1Micros,
      micros1Minute1Second1Micros
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1:1.1' MINUTE to SECOND",
        "inTERval '-1:1.1' MINUTE to SECOND",
        "INTERVAL -'-1:1.1' MINUTE TO SECOND",
        "INTERVAL +'+1:1.1' MINUTE TO SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Second = 1 * 1000000L
    withResource(ColumnVector.fromLongs(
      -micros1Second,
      -micros1Second,
      micros1Second,
      micros1Second
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1' SECOND",
        "inTERval '-1' SECOND",
        "INTERVAL -'-1' SECOND",
        "INTERVAL +'+1' SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    val micros1Second1Micros = 1 * 1000000L + 100000L
    withResource(ColumnVector.fromLongs(
      -micros1Second1Micros,
      -micros1Second1Micros,
      micros1Second1Micros,
      micros1Second1Micros
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        "interval -'1.1' SECOND",
        "inTERval '-1.1' SECOND",
        "INTERVAL -'-1.1' SECOND",
        "INTERVAL +'+1.1' SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }
  }

  test("test castStringToDTInterval min max") {
    withResource(ColumnVector.fromLongs(
      Long.MaxValue,
      Long.MinValue,
      0L
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        s"interval +'+${GpuIntervalUtils.MAX_DAY} 4:0:54.775807' day to SECOND",
        s"interval -'${GpuIntervalUtils.MAX_DAY} 4:0:54.775808' day to SECOND",
        "INTERVAL '-0 00:00:00' day to SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    withResource(ColumnVector.fromLongs(
      Long.MaxValue / (86400L * 1000000L) * (86400L * 1000000L),
      Long.MinValue / (86400L * 1000000L) * (86400L * 1000000L)
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        s"interval +'+${GpuIntervalUtils.MAX_DAY}' day",
        s"interval -'${GpuIntervalUtils.MAX_DAY}' day"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    withResource(ColumnVector.fromLongs(
      Long.MaxValue / (3600L * 1000000L) * (3600L * 1000000L),
      Long.MinValue / (3600L * 1000000L) * (3600L * 1000000L)
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        s"interval +'+${GpuIntervalUtils.MAX_HOUR}' HOUR",
        s"interval -'${GpuIntervalUtils.MAX_HOUR}' HOUR"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.HOUR))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    withResource(ColumnVector.fromLongs(
      Long.MaxValue / (60L * 1000000L) * (60L * 1000000L),
      Long.MinValue / (60L * 1000000L) * (60L * 1000000L)
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        s"interval +'+${GpuIntervalUtils.MAX_MINUTE}' minute",
        s"interval -'${GpuIntervalUtils.MAX_MINUTE}' minute"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.MINUTE))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }

    withResource(ColumnVector.fromLongs(
      Long.MaxValue / 1000000L * 1000000L,
      Long.MinValue / 1000000L * 1000000L
    )) { expectCV =>
      withResource(ColumnVector.fromStrings(
        s"interval +'+${GpuIntervalUtils.MAX_SECOND}' SECOND",
        s"interval -'${GpuIntervalUtils.MAX_SECOND}' SECOND"
      )) { intervalCV =>
        withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
          DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))) {
          actualCV =>
            CudfTestHelper.assertColumnsAreEqual(expectCV, actualCV)
        }
      }
    }
  }

  test("test castStringToDTInterval for overflow and range") {
    // check the overflow
    withResource(ColumnVector.fromStrings(
      s"interval '${GpuIntervalUtils.MAX_DAY} 4:0:54.775808' day to second", // Long.MaxValue + 1
      s"interval '-${GpuIntervalUtils.MAX_DAY} 4:0:54.775809' day to second", // Long.MinValue - 1
      s"interval '${GpuIntervalUtils.MAX_DAY} 5:0:0.0' day to second" // > Long.MaxValue
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max hour and max second which are not leading items
    withResource(ColumnVector.fromStrings(
      "interval '1 24:0:54.775808' day to second", // 24 hour is invalid
      "interval '1 2:60:54.775808' day to second" // 60 minute is invalid
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max hour and max second which are not leading items
    withResource(ColumnVector.fromStrings(
      "interval '2:60:54.775808' day to second" // 60 minute is invalid
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))

      }
    }

    // check max leading day
    withResource(ColumnVector.fromStrings(
      s"interval '${Long.MaxValue / (86400L * 1000000L) + 1} 0:0:0.0' day to second"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max leading hour
    withResource(ColumnVector.fromStrings(
      s"interval '${Long.MaxValue / (3600L * 1000000L) + 1}:0:0.0' hour to second"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max leading minute
    withResource(ColumnVector.fromStrings(
      s"interval '${Long.MaxValue / (60L * 1000000L) + 1}:0.0' minute to second"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max leading second
    withResource(ColumnVector.fromStrings(
      s"interval '${Long.MaxValue / 1000000L + 1}' second"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }

    // check max leading second
    withResource(ColumnVector.fromStrings(
      s"interval '-${Long.MinValue / 1000000L + 1}' second"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }
  }

  test("test castStringToDTInterval format invalid") {
    withResource(ColumnVector.fromStrings(
      "INTERVAL xx DAY",
      "INTERVAL 3 day",
      "INTERVAL 3' day",
      "INTERVAL '3 day",
      "INTERVAL '-      3' day",
      "INTERVAL -       '3' day",
      "INTERVAL '100 10:30:40.' DAY TO SECOND"
    )) { intervalCV =>
      withResource(GpuIntervalUtils.castStringToDTInterval(intervalCV,
        DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))) { actualCV =>
        // return null because invalid
        assert(allNulls(actualCV))
      }
    }
  }

  def readCsv(spark: SparkSession, path: String): DataFrame = {
    def dayTime(s: Byte, e: Byte): DayTimeIntervalType = DayTimeIntervalType(s, e)

    val schema = StructType(Seq(
      StructField("c01", dayTime(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY)),
      StructField("c02", dayTime(DayTimeIntervalType.DAY, DayTimeIntervalType.HOUR)),
      StructField("c03", dayTime(DayTimeIntervalType.DAY, DayTimeIntervalType.MINUTE)),
      StructField("c04", dayTime(DayTimeIntervalType.DAY, DayTimeIntervalType.SECOND)),
      StructField("c05", dayTime(DayTimeIntervalType.HOUR, DayTimeIntervalType.HOUR)),
      StructField("c06", dayTime(DayTimeIntervalType.HOUR, DayTimeIntervalType.MINUTE)),
      StructField("c07", dayTime(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND)),
      StructField("c08", dayTime(DayTimeIntervalType.MINUTE, DayTimeIntervalType.MINUTE)),
      StructField("c09", dayTime(DayTimeIntervalType.MINUTE, DayTimeIntervalType.SECOND)),
      StructField("c10", dayTime(DayTimeIntervalType.SECOND, DayTimeIntervalType.SECOND))
    ))
    fromCsvDf(path, schema)(spark)
  }

  testSparkResultsAreEqual(
    "test read day-time interval csv file",
    spark => readCsv(spark, "day-time-interval.csv")
  ) {
    df => df
  }

  /**
   * TODO: Blocked by Spark overflow issue: https://issues.apache.org/jira/browse/SPARK-38520
   *
   *    // days overflow
   *    scala> val schema = StructType(Seq(StructField("c1",
   *      DayTimeIntervalType(DayTimeIntervalType.DAY, DayTimeIntervalType.DAY))))
   *    scala> spark.read.csv(path).show(false)
   *    +------------------------+
   *    |_c0                     |
   *    +------------------------+
   *    |interval '106751992' day|
   *    +------------------------+
   *    scala> spark.read.schema(schema).csv(path).show(false)
   *    +-------------------------+
   *    |c1                       |
   *    +-------------------------+
   *    |INTERVAL '-106751990' DAY|
   *    +-------------------------+
   *
   *     // hour overflow
   *    scala> val schema = StructType(Seq(StructField("c1",
   *      DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.HOUR))))
   *    scala> spark.read.csv(path).show(false)
   *    +----------------------------+
   *    |_c0                         |
   *    +----------------------------+
   *    |INTERVAL +'+2562047789' hour|
   *    +----------------------------+
   *    scala> spark.read.schema(schema).csv(path).show(false)
   *    +---------------------------+
   *    |c1                         |
   *    +---------------------------+
   *    |INTERVAL '-2562047787' HOUR|
   *    +---------------------------+
   *
   *    // minute overflow
   *    scala> val schema = StructType(Seq(StructField("c1",
   *      DayTimeIntervalType(DayTimeIntervalType.MINUTE, DayTimeIntervalType.MINUTE))))
   *    scala> spark.read.csv(path).show(false)
   *    +------------------------------+
   *    |_c0                           |
   *    +------------------------------+
   *    |interval '153722867281' minute|
   *    +------------------------------+
   *    scala> spark.read.schema(schema).csv(path).show(false)
   *    +-------------------------------+
   *    |c1                             |
   *    +-------------------------------+
   *    |INTERVAL '-153722867280' MINUTE|
   *    +-------------------------------+
   *
   */
  testSparkResultsAreEqual(
    "test read day-time interval overflow file",
    spark => readCsv(spark, "day-time-interval-to-be-fix.csv"),
    assumeCondition = _ => (false,
        "check if issue is fixed: https://issues.apache.org/jira/browse/SPARK-38520")
  ) {
    df => df
  }

  def allNulls(col: ColumnVector): Boolean = {
    withResource(col.isNull) { isNull =>
      BoolUtils.isAllValidTrue(isNull)
    }
  }
}
