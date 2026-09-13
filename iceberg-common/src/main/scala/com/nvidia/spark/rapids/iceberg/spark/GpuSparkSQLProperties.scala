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

object GpuSparkSQLProperties {
  // Controls whether reading/writing timestamps without timezones is allowed
  val HANDLE_TIMESTAMP_WITHOUT_TIMEZONE = "spark.sql.iceberg.handle-timestamp-without-timezone"
  val HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT = false

  // Controls whether to report available column statistics to Spark for query optimization.
  val REPORT_COLUMN_STATS = "spark.sql.iceberg.report-column-stats"
  val REPORT_COLUMN_STATS_DEFAULT = true
}
