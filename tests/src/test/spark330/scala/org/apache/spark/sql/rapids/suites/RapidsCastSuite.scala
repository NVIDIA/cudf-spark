/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, CastSuite, Expression, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait
import org.apache.spark.sql.types._

class RapidsCastSuite extends CastSuite with RapidsTestsTrait {
  // example to enhance logging for base suite
  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression =>
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
    }
  }
  
  private val specialTs = Seq(
    "0001-01-01T00:00:00", // the fist timestamp of Common Era
    "1582-10-15T23:59:59", // the cutover date from Julian to Gregorian calendar
    "1970-01-01T00:00:00", // the epoch timestamp
    "9999-12-31T23:59:59"  // the last supported timestamp according to SQL standard
  )

  val outstandingTimezonesIds: Seq[String] = Seq(
    "UTC",
    PST.getId,
    CET.getId,
    "Africa/Dakar",
    LA.getId,
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels")
  val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(getZoneId)

  testRapids("SPARK-35711: cast timestamp without time zone to timestamp with local time zone") {
    outstandingZoneIds.foreach { zoneId =>
      println(s"zoneId: $zoneId")
      withDefaultTimeZone(zoneId) {
        specialTs.foreach { s =>
          val input = LocalDateTime.parse(s)
          val expectedTs = Timestamp.valueOf(s.replace("T", " "))
          checkEvaluation(cast(input, TimestampType), expectedTs)
        }
      }
    }
  }

  testRapids("SPARK-35719: cast timestamp with local time zone to timestamp without timezone") {
    outstandingZoneIds.foreach { zoneId =>
      println(s"zoneId: $zoneId")
      withDefaultTimeZone(zoneId) {
        specialTs.foreach { s =>
          val input = Timestamp.valueOf(s.replace("T", " "))
          val expectedTs = LocalDateTime.parse(s)
          checkEvaluation(cast(input, TimestampNTZType), expectedTs)
        }
      }
    }
  }
}
