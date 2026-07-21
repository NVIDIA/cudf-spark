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
{"spark": "350"}
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
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.ColumnVector

object GpuIntervalUtils extends GpuIntervalUtilsBase {

  val MAX_SECONDS_IN_MIN = 59L

  /**
   * This method overrides the base class method to add overflow check to the seconds conversion
   * The valid range for seconds in Spark 3.5.0+ is [0,59]
   */
  override protected def addFromDayToSecond(
      sign: ColumnVector,
      daysInTable: ColumnVector,
      hoursInTable: ColumnVector,
      minutesInTable: ColumnVector,
      secondsInTable: ColumnVector,
      microsInTable: ColumnVector
  ): ColumnVector = {
    add(daysToMicros(sign, daysInTable, MAX_DAY),
      add(hoursToMicros(sign, hoursInTable, MAX_HOUR_IN_DAY),
        add(minutesToMicros(sign, minutesInTable, MAX_MINUTE_IN_HOUR),
          add(secondsToMicros(sign, secondsInTable, MAX_SECONDS_IN_MIN), // max value is 59
            getMicrosFromDecimal(sign, microsInTable))))) // max value is 999999999, no overflow
  }

}