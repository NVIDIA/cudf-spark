/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.catalyst.json

import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.internal.SQLConf

object GpuJsonUtils {

  def optionalDateFormatInRead(options: Map[String, String]): Option[String] =
    optionalDateFormatInRead(parseJSONReadOptions(options))

  def optionalDateFormatInRead(options: JSONOptions): Option[String] =
    options.dateFormatInRead

  def dateFormatInRead(options: JSONOptions): String =
    options.dateFormatInRead.getOrElse(DateFormatter.defaultPattern)

  def dateFormatInRead(options: Map[String, String]): String =
    dateFormatInRead(parseJSONReadOptions(options))

  def optionalTimestampFormatInRead(options: JSONOptions): Option[String] =
    options.timestampFormatInRead

  def optionalTimestampFormatInRead(options: Map[String, String]): Option[String] =
    optionalTimestampFormatInRead(parseJSONReadOptions(options))

  def timestampFormatInRead(options: JSONOptions): String = options.timestampFormatInRead.getOrElse(
    if (SQLConf.get.legacyTimeParserPolicy == SQLConf.LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  def dateFormatInWrite(options: JSONOptions): String =
    options.dateFormatInWrite

  def timestampFormatInWrite(options: JSONOptions): String =
    options.timestampFormatInWrite

  def enableDateTimeParsingFallback(options: JSONOptions): Boolean = false

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
