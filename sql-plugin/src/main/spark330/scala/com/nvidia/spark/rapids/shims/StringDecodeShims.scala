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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Locale

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, StringDecode}
import org.apache.spark.sql.rapids.GpuStringDecode

object StringDecodeShims {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[StringDecode](
      "Decodes binary data from a charset to a UTF-8 string",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("bin", TypeSig.BINARY, TypeSig.BINARY),
        ("charset", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[StringDecode](a, conf, p, r) {
        private var charsetName: String = _
        override def tagExprForGpu(): Unit = {
          GpuOverrides.extractStringLit(a.charset) match {
            case Some(cs) if cs != null =>
              charsetName = cs.toUpperCase(Locale.ROOT)
              if (charsetName != "GBK") {
                willNotWorkOnGpu(s"only GBK charset is supported on GPU, got: $charsetName")
              }
            case _ =>
              willNotWorkOnGpu("charset must be a string literal for GPU StringDecode")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStringDecode(lhs, charsetName)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
