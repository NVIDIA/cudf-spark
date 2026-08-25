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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.json4s._
import org.json4s.jackson.Serialization

object EventLogJsonShims {
  private implicit val formats: Formats = org.json4s.DefaultFormats

  def parseJson(line: String): JValue = Serialization.read[JValue](line)

  def extractLong(jval: JValue): Option[Long] =
    jval.extractOpt[BigInt].map(_.toLong)
}
