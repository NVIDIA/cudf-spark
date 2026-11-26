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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AnsiCastSuiteWithAnsiModeOn, Expression}
import org.apache.spark.sql.internal.SQLConf

class RapidsAnsiCastSuiteWithAnsiModeOn extends AnsiCastSuiteWithAnsiModeOn
  with RapidsCastTimezoneTests {
  override def beforeAll(): Unit = {
    // The inherited setup sets ANSI on the current SQLConf before creating _spark.
    val initialConf = SQLConf.get
    val ansiKey = SQLConf.ANSI_ENABLED.key
    val originalAnsi = initialConf.getAllConfs.get(ansiKey)
    try {
      super.beforeAll()
    } finally {
      originalAnsi match {
        case Some(value) => initialConf.setConfString(ansiKey, value)
        case None => initialConf.unsetConf(SQLConf.ANSI_ENABLED)
      }
    }
    // Preserve the original suite's ANSI-on contract after the plugin session is created.
    _spark.conf.set(ansiKey, true)
  }

  override def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {
    // Spark checks these exception messages using in-process Catalyst evaluation.
    shouldNotFallback()
    super.checkExceptionInExpression[T](expression, inputRow, expectedErrMsg)
  }
}
