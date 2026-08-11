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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.DateFunctionsSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * Test suite for date functions on GPU.
 * This suite inherits from Spark's DateFunctionsSuite and runs the tests on GPU.
 *
 * DateFunctionsSuite tests DataFrame/SQL-level date functions including:
 * - current_date, current_timestamp, now
 * - date/timestamp comparisons
 * - date_add, date_sub, add_months
 * - datediff, months_between
 * - date_format, to_date, to_timestamp
 * - year, month, dayofmonth, dayofyear, dayofweek
 * - hour, minute, second
 * - weekofyear, last_day, next_day
 * - from_unixtime, unix_timestamp
 * - trunc, date_trunc
 * - make_date, make_timestamp
 * - and many more date/timestamp functions
 */
class RapidsDateFunctionsSuite extends DateFunctionsSuite with RapidsSQLTestsTrait {
  // All tests from DateFunctionsSuite will be inherited and run on GPU
}

