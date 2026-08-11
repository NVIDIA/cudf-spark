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
{"spark": "334"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
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
package org.apache.spark.sql.rapids.shims

import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

trait SequenceSizeTooLongUnsuccessfulErrorBuilder {
  def getTooLongSequenceErrorString(sequenceSize: Int, functionName: String): String = {
    // The errant function's name does not feature in the exception message
    // prior to Spark 4.0.  Neither does the attempted allocation size.
    "Unsuccessful try to create array with elements exceeding the array " +
      s"size limit $MAX_ROUNDED_ARRAY_LENGTH"
  }
}
