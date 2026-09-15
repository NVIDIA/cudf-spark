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

package org.apache.spark.sql.rapids

import java.time.{ZoneId, ZoneOffset}
import java.util.TimeZone

import org.apache.spark.sql.catalyst.util.RebaseDateTime

/** Access Spark's runtime-specific timestamp rebase records from the sql package. */
object RebaseDateTimeBridge {
  final case class RebaseInfo(switches: Array[Long], diffs: Array[Long])

  private val MicrosPerSecond = 1000000L

  private lazy val julianToGregorianMicros =
    RebaseDateTime.loadRebaseRecords("julian-gregorian-rebase-micros.json")

  val lastSwitchJulianTs: Long = RebaseDateTime.lastSwitchJulianTs

  private def copyInfo(timeZoneId: String): Option[RebaseInfo] = {
    julianToGregorianMicros.get(timeZoneId).map { info =>
      RebaseInfo(info.switches.clone(), info.diffs.clone())
    }
  }

  def getJulianToGregorianMicros(timeZoneId: String): Option[RebaseInfo] = {
    copyInfo(timeZoneId).orElse {
      val zoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)
      copyInfo(zoneId.getId).orElse {
        zoneId.normalized() match {
          case offset: ZoneOffset =>
            copyInfo("UTC").map { utcInfo =>
              // A fixed-offset local midnight is shifted by the inverse offset from UTC.
              val switchShift = Math.multiplyExact(
                -offset.getTotalSeconds.toLong, MicrosPerSecond)
              RebaseInfo(
                utcInfo.switches.map(switch => Math.addExact(switch, switchShift)),
                utcInfo.diffs)
            }
          case _ => None
        }
      }
    }
  }

  def rebaseJulianToGregorianMicros(timeZoneId: String, micros: Long): Long = {
    val sparkTimeZoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS) match {
      // ZoneId normalizes short IDs such as EST to -05:00. Spark's Calendar fallback
      // uses TimeZone.getTimeZone(String), which silently treats that spelling as GMT.
      case offset: ZoneOffset => TimeZone.getTimeZone(offset).getID
      case _ => timeZoneId
    }
    RebaseDateTime.rebaseJulianToGregorianMicros(sparkTimeZoneId, micros)
  }
}
