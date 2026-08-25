/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{DateUtils, GpuCast, GpuOverrides, RapidsMeta}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.rapids.ExceptionTimeParserPolicy

object GpuJsonToStructsShim {
  def tagDateFormatSupport(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
    // dateFormat is ignored by JsonToStructs in Spark 3.2.x and 3.3.x because it just
    // performs a regular cast from string to date
  }

  def castJsonStringToDate(input: ColumnView, options: JSONOptions): ColumnVector = {
    // dateFormat is ignored in from_json in Spark 3.2.x and 3.3.x
    withResource(Scalar.fromString(" ")) { space =>
      withResource(input.strip(space)) { trimmed =>
        GpuCast.castStringToDate(trimmed, ansiMode = false)
      }
    }
  }

  def tagDateFormatSupportFromScan(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
  }

  def castJsonStringToDateFromScan(input: ColumnView, dt: DType,
      dateFormat: Option[String]): ColumnVector = {
    dateFormat match {
      case None =>
        // legacy behavior
        withResource(input.strip()) { trimmed =>
          GpuCast.castStringToDate(trimmed, ansiMode =
            GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy)
        }
      case Some(fmt) =>
        withResource(input.strip()) { trimmed =>
          val regexRoot = fmt
            .replace("yyyy", raw"\d{4}")
            .replace("MM", raw"\d{1,2}")
            .replace("dd", raw"\d{1,2}")
          val cudfFormat = DateUtils.toStrf(fmt, parseString = true)
          GpuCast.convertDateOrNull(trimmed, "^" + regexRoot + "$", cudfFormat,
            failOnInvalid = GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy)
        }
    }
  }


  def castJsonStringToTimestamp(input: ColumnView,
      options: JSONOptions): ColumnVector = {
    // legacy behavior
    withResource(Scalar.fromString(" ")) { space =>
      withResource(input.strip(space)) { trimmed =>
        // from_json doesn't respect ansi mode
        GpuCast.castStringToTimestamp(trimmed, ansiMode = false)
      }
    }
  }
}
