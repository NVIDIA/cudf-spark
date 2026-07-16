/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.TimeZone

import ai.rapids.cudf.{ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Mirror of Spark's LegacyBehaviorPolicy.
 * <p>
 * This is to provides a stable reference to other Java code in our codebase and also mitigate
 * from Spark's breaking changes that may cause issues if our code uses Spark's
 * LegacyBehaviorPolicy.
 */
sealed abstract class DateTimeRebaseMode(val value: String) extends Serializable

object DateTimeRebaseMode {
  def fromName(name: String): DateTimeRebaseMode = name match {
    case DateTimeRebaseException.value => DateTimeRebaseException
    case DateTimeRebaseLegacy.value => DateTimeRebaseLegacy
    case DateTimeRebaseCorrected.value => DateTimeRebaseCorrected
    case _ => throw new IllegalArgumentException(
      DateTimeRebaseUtils.invalidRebaseModeMessage(name))
  }
}

/**
 * Mirror of Spark's LegacyBehaviorPolicy.EXCEPTION.
 */
case object DateTimeRebaseException extends DateTimeRebaseMode("EXCEPTION")

/**
 * Mirror of Spark's LegacyBehaviorPolicy.LEGACY.
 */
case object DateTimeRebaseLegacy extends DateTimeRebaseMode("LEGACY")

/**
 * Mirror of Spark's LegacyBehaviorPolicy.CORRECTED.
 */
case object DateTimeRebaseCorrected extends DateTimeRebaseMode("CORRECTED")

object DateTimeRebaseUtils {
  def invalidRebaseModeMessage(name: String): String =
    s"Invalid datetime rebase mode: $name (must be either 'EXCEPTION', 'LEGACY', or 'CORRECTED')"

  // Copied from Spark
  private val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"
  private val SPARK_LEGACY_DATETIME_METADATA_KEY = "org.apache.spark.legacyDateTime"
  private val SPARK_LEGACY_INT96_METADATA_KEY = "org.apache.spark.legacyINT96"
  private val SPARK_TIMEZONE_METADATA_KEY = "org.apache.spark.timeZone"

  private def rebaseModeFromFileMeta(lookupFileMeta: String => String,
      modeByConfig: String,
      minVersion: String,
      metadataKey: String,
      hasDateTimeInReadSchema: Boolean =  true): DateTimeRebaseMode = {

    // If there is no version, we return the mode specified by the config.
    val mode = Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by `minVersion` and latter may also need the rebase if they were written
      // with the "LEGACY" rebase mode.
      if (version < minVersion || lookupFileMeta(metadataKey) != null) {
        DateTimeRebaseLegacy
      } else {
        DateTimeRebaseCorrected
      }
    }.getOrElse(DateTimeRebaseMode.fromName(modeByConfig))

    // Check the timezone of the file if the mode is LEGACY.
    if (mode == DateTimeRebaseLegacy) {
      val fileTimeZoneId = Option(lookupFileMeta(SPARK_TIMEZONE_METADATA_KEY)).map { str =>
        DateTimeUtils.getZoneId(str)
      }.getOrElse {
        // Use the default JVM time zone for backward compatibility
        TimeZone.getDefault.toZoneId
      }
      if (hasDateTimeInReadSchema && fileTimeZoneId.normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
        throw new UnsupportedOperationException(
          "LEGACY datetime rebase mode is only supported for files written in UTC timezone. " +
            s"Actual file timezone: $fileTimeZoneId")
      }
    }

    mode
  }

  def datetimeRebaseMode(lookupFileMeta: String => String,
      modeByConfig: String,
      hasDateTimeInReadSchema: Boolean = true): DateTimeRebaseMode = {
    rebaseModeFromFileMeta(lookupFileMeta, modeByConfig, "3.0.0",
      SPARK_LEGACY_DATETIME_METADATA_KEY, hasDateTimeInReadSchema)
  }

  def int96RebaseMode(lookupFileMeta: String => String,
      modeByConfig: String): DateTimeRebaseMode = {
    rebaseModeFromFileMeta(lookupFileMeta, modeByConfig, "3.1.0",
      SPARK_LEGACY_INT96_METADATA_KEY)
  }

  private[this] def isRebaseNeeded(column: ColumnView, checkType: DType,
      minGood: Scalar): Boolean = {
    val dtype = column.getType
    require(!dtype.hasTimeResolution || dtype == DType.TIMESTAMP_MICROSECONDS)

    dtype match {
      case `checkType` =>
        withResource(column.lessThan(minGood)) { hasBad =>
          withResource(hasBad.any()) { anyBad =>
            anyBad.isValid && anyBad.getBoolean
          }
        }

      case DType.LIST | DType.STRUCT => (0 until column.getNumChildren).exists(i =>
        withResource(column.getChildColumnView(i)) { child =>
          isRebaseNeeded(child, checkType, minGood)
        })

      case _ => false
    }
  }

  private[this] def isDateRebaseNeeded(column: ColumnView, startDay: Int): Boolean = {
    withResource(Scalar.timestampDaysFromInt(startDay)) { minGood =>
      isRebaseNeeded(column, DType.TIMESTAMP_DAYS, minGood)
    }
  }

  private[this] def isTimeRebaseNeeded(column: ColumnView, startTs: Long): Boolean = {
    withResource(Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, startTs)) { minGood =>
      isRebaseNeeded(column, DType.TIMESTAMP_MICROSECONDS, minGood)
    }
  }

  def isDateRebaseNeededInRead(column: ColumnView): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchJulianDay)

  def isTimeRebaseNeededInRead(column: ColumnView): Boolean =
    isTimeRebaseNeeded(column, RebaseDateTime.lastSwitchJulianTs)

  def isDateRebaseNeededInWrite(column: ColumnView): Boolean =
    isDateRebaseNeeded(column, RebaseDateTime.lastSwitchGregorianDay)

  def isTimeRebaseNeededInWrite(column: ColumnView): Boolean =
    isTimeRebaseNeeded(column, RebaseDateTime.lastSwitchGregorianTs)

  def newRebaseExceptionInRead(format: String): Exception = {
    val config = if (format == "Parquet") {
      SparkShimImpl.parquetRebaseReadKey
    } else if (format == "Avro") {
      SparkShimImpl.avroRebaseReadKey
    } else {
      throw new IllegalStateException("unrecognized format " + format)
    }
    TrampolineUtil.makeSparkUpgradeException("3.0",
      "reading dates before 1582-10-15 or timestamps before " +
      s"1900-01-01T00:00:00Z from $format files can be ambiguous, as the files may be written by " +
      "Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar that is " +
      "different from Spark 3.0+'s Proleptic Gregorian calendar. See more details in " +
      s"SPARK-31404. The RAPIDS Accelerator does not support reading these 'LEGACY' files. To do " +
      s"so you should disable $format support in the RAPIDS Accelerator " +
      s"or set $config to 'CORRECTED' to read the datetime values as it is.",
      null)
  }
}
