/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.spark

import org.apache.iceberg.Schema
import org.apache.iceberg.types.{Types, TypeUtil}


object GpuSparkUtil {
  val HANDLE_TIMESTAMP_WITHOUT_TIMEZONE: String = "spark.sql.iceberg" +
    ".handle-timestamp-without-timezone"
  val HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT = false


  val TIMESTAMP_WITHOUT_TIMEZONE_ERROR: String = "Cannot handle timestamp without" +
    " timezone fields in Spark. Spark does not natively support this type but if you would like " +
    "to handle all timestamps as timestamp with timezone set " +
    s"'$HANDLE_TIMESTAMP_WITHOUT_TIMEZONE' " +
    "to true. This will not change the underlying values stored but will change their displayed " +
    "values in Spark. For more information please see " +
    "https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html#" +
    "ansi-sql-and-spark-sql-timestamps"

  def hasTimestampWithoutZone(schema: Schema): Boolean = {
    TypeUtil.find(schema, Types.TimestampType.withoutZone().equals(_)) != null
  }
}
