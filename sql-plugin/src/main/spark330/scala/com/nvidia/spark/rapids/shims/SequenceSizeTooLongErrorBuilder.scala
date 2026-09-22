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
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

trait SequenceSizeTooLongErrorBuilder {

  def getTooLongSequenceErrorString(sequenceSize: Int, functionName: String): String = {
    // For these Spark versions, the sequence length and function name
    // do not appear in the exception message.
    s"Too long sequence found. Should be <= $MAX_ROUNDED_ARRAY_LENGTH"
  }
}
