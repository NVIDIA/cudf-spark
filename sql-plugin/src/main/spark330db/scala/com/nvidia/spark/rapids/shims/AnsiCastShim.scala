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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
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
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuCast, GpuEvalMode}

import org.apache.spark.sql.catalyst.expressions.{Cast, EvalMode, Expression}
import org.apache.spark.sql.types.DataType

object AnsiCastShim {
  def isAnsiCast(e: Expression): Boolean = e match {
    case c: GpuCast => c.ansiMode
    case c: Cast => c.evalMode == EvalMode.ANSI
    case _ => false
  }

  def getEvalMode(c: Cast): GpuEvalMode.Value = {
    c.evalMode match {
      case EvalMode.LEGACY => GpuEvalMode.LEGACY
      case EvalMode.ANSI => GpuEvalMode.ANSI
      case EvalMode.TRY => GpuEvalMode.TRY
    }
  }

  def extractAnsiCastTypes(e: Expression): (DataType, DataType) = e match {
    case c: Cast => (c.child.dataType, c.dataType)
    case _ => throw new UnsupportedOperationException(s"${e.getClass} is Cast type")
  }
}
