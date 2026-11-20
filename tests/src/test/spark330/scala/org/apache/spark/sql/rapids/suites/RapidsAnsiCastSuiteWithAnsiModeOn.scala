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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.catalyst.expressions.AnsiCastSuiteWithAnsiModeOn
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsTestsTrait

class RapidsAnsiCastSuiteWithAnsiModeOn extends AnsiCastSuiteWithAnsiModeOn
  with RapidsTestsTrait {
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Preserve the original suite's ANSI-on contract after the plugin session is created.
    _spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }
}
