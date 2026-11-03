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
{"spark": "350db143"}
{"spark": "400db173"}
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.catalyst.csv

import java.nio.charset.StandardCharsets

import com.nvidia.spark.rapids.shims.LegacyBehaviorPolicyShim

import org.apache.spark.sql.catalyst.util.{DateFormatDetector, DateFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Metadata

object GpuCsvUtils {
  def csvLineToParse(line: Array[Byte], offset: Int, len: Int,
      options: CSVOptions, normalizedNewline: Char): String = {
    // Databricks' TextInputCSVDataSource restores the terminator before tokenizing each line.
    val separator = options.lineSeparatorInRead.getOrElse(Array(normalizedNewline.toByte))
    val terminated = new Array[Byte](len + separator.length)
    System.arraycopy(line, offset, terminated, 0, len)
    System.arraycopy(separator, 0, terminated, len, separator.length)
    new String(terminated, StandardCharsets.UTF_8)
  }

  def dateFormatInRead(options: CSVOptions): String =
    options.dateFormatInRead.getOrElse(DateFormatter.defaultPattern)

  def dateFormatInRead(options: CSVOptions, metadata: Metadata): Option[String] = {
    if (options.detectDateFormat) {
      // DBR enables detection when preferDate is true and dateFormat is unspecified.
      // UnivocityParser then selects a format from each column's metadata, even with
      // an explicit schema. Missing metadata selects DefaultDateFormatter, whose
      // CAST-style grammar accepts "2020" as 2020-01-01. dateFormatInRead can still be
      // Some("yyyy-MM-dd"), which selects a different formatter and rejects "2020".
      // Preserve None so JNI receives null and uses the same default parsing semantics.
      val key = DateFormatDetector.DETECTED_DATE_FORMAT_ATTR_KEY
      if (metadata.contains(key)) Some(metadata.getString(key)) else None
    } else {
      // Use the same format-selection path as vanilla Spark. DBR also reaches this
      // branch when dateFormat is specified or preferDate is false.
      options.dateFormatInRead
    }
  }

  def timestampFormatInRead(options: CSVOptions): String = options.timestampFormatInRead.getOrElse(
    if (LegacyBehaviorPolicyShim.isLegacyTimeParserPolicy()) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
    })

  def enableDateTimeParsingFallback(options: CSVOptions): Boolean =
    options.enableDateTimeParsingFallback.getOrElse(false)

  def enableDateParsingFallback(options: CSVOptions): Boolean =
    options.enableDateTimeParsingFallback
      .orElse(SQLConf.get.csvEnableDateTimeParsingFallback)
      .getOrElse {
        LegacyBehaviorPolicyShim.isLegacyTimeParserPolicy() || options.dateFormatOption.isEmpty
      }
}
