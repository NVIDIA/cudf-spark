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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.catalyst.csv

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Metadata

object GpuCsvUtils {
  def csvLineToParse(line: Array[Byte], offset: Int, len: Int,
      options: CSVOptions, normalizedNewline: Char): String =
    new String(line, offset, len, StandardCharsets.UTF_8)

  def dateFormatInRead(options: CSVOptions): String =
    options.dateFormatInRead.getOrElse(DateFormatter.defaultPattern)

  // Follow vanilla Spark's format-selection path, without consulting column metadata.
  def dateFormatInRead(options: CSVOptions, metadata: Metadata): Option[String] =
    options.dateFormatInRead

  def timestampFormatInRead(options: CSVOptions): String = options.timestampFormatInRead.getOrElse(
    if (SQLConf.get.legacyTimeParserPolicy == SQLConf.LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  def enableDateTimeParsingFallback(options: CSVOptions): Boolean = false

  // Spark 3.3 always retries stringToDate after a CSV date formatter failure.
  def enableDateParsingFallback(options: CSVOptions): Boolean = true
}
