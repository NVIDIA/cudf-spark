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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
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
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.io.FileNotFoundException

import org.apache.spark.sql.connector.read.PartitionReaderFactory

object MissingFileErrorShim {
  private val RECREATE_HINT = "recreating the Dataset/DataFrame involved"
  private val REFRESH_HINT = "REFRESH TABLE"

  def wrapReaderFactory(readerFactory: PartitionReaderFactory): PartitionReaderFactory =
    readerFactory

  def convert(
      filePath: Option[String],
      error: FileNotFoundException,
      includeRefreshHint: Boolean): Throwable = {
    val message = Option(error.getMessage).getOrElse(error.toString)
    if (message.contains(RECREATE_HINT) &&
        (!includeRefreshHint || message.contains(REFRESH_HINT))) {
      error
    } else {
      val recoveryHint = if (includeRefreshHint) {
        "It is possible the underlying files have been updated. " +
          "You can explicitly invalidate the cache in Spark by " +
          "running 'REFRESH TABLE tableName' command in SQL or " +
          "by recreating the Dataset/DataFrame involved."
      } else {
        "It is possible the underlying files have been updated. " +
          "You can explicitly invalidate the cache in Spark by " +
          "recreating the Dataset/DataFrame involved."
      }
      val enrichedException = new FileNotFoundException(s"$message\n$recoveryHint")
      enrichedException.initCause(error)
      enrichedException
    }
  }
}
