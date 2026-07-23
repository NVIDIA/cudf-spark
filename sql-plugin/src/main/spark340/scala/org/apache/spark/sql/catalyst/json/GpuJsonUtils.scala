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

/*** spark-rapids-shim-json-lines
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
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.catalyst.json

import com.nvidia.spark.rapids.shims.LegacyBehaviorPolicyShim

import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.internal.SQLConf

object GpuJsonUtils {

  def optionalDateFormatInRead(options: JSONOptions): Option[String] =
    options.dateFormatInRead

  def optionalDateFormatInRead(options: Map[String, String]): Option[String] = {
    optionalDateFormatInRead(parseJSONReadOptions(options))
  }

  /**
   * Return the dateFormat that Spark will use in JSONOptions, or return a default. Note that this
   * does not match Spark's behavior in all cases, because dateFormat is intentionally optional.
   *
   * For example, in `org.apache.spark.sql.catalyst.util.DateFormatter#getFormatter`, a legacy
   * formatter will be used if no dateFormat is specified, and it does not correspond to
   * `DateFormatter.defaultPattern`.
   */
  def dateFormatInRead(options: JSONOptions): String =
    options.dateFormatInRead.getOrElse(DateFormatter.defaultPattern)

  def optionalTimestampFormatInRead(options: JSONOptions): Option[String] =
    options.timestampFormatInRead

  def optionalTimestampFormatInRead(options: Map[String, String]): Option[String] =
    optionalTimestampFormatInRead(parseJSONReadOptions(options))

  def timestampFormatInRead(options: JSONOptions): String = options.timestampFormatInRead.getOrElse(
    if (LegacyBehaviorPolicyShim.isLegacyTimeParserPolicy()) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  def dateFormatInWrite(options: JSONOptions): String =
    options.dateFormatInWrite

  def timestampFormatInWrite(options: JSONOptions): String =
    options.timestampFormatInWrite

  def enableDateTimeParsingFallback(options: JSONOptions): Boolean =
    options.enableDateTimeParsingFallback.getOrElse(false)

  def parseJSONOptions(options: Map[String, String]) = {
    new JSONOptions(
      options,
      SQLConf.get.sessionLocalTimeZone,
      SQLConf.get.columnNameOfCorruptRecord)
  }

  def parseJSONReadOptions(options: Map[String, String]) = {
    new JSONOptionsInRead(
      options,
      SQLConf.get.sessionLocalTimeZone,
      SQLConf.get.columnNameOfCorruptRecord)
  }
}
