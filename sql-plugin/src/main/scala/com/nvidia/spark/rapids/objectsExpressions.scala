/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.rapids.{ExternalSource, GpuStringDecode}

/**
 * Meta class for overriding StaticInvoke expressions.
 * <br/>
 * Handles two cases:
 * - Iceberg partition value computation via StaticInvoke
 * - Spark 4.0+ StringDecode (RuntimeReplaceable replaced by StaticInvoke)
 */
class StaticInvokeMeta(expr: StaticInvoke,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule) extends ExprMeta[StaticInvoke](expr, conf, parent, rule) {

  private val isStringDecode: Boolean = {
    expr.staticObject.getName == "org.apache.spark.sql.catalyst.expressions.StringDecode" &&
      expr.functionName == "decode"
  }

  private var charsetName: String = null
  // True when spark.sql.legacy.codingErrorAction=false: the GPU kernel must signal malformed
  // input so the caller can raise MALFORMED_CHARACTER_CODING instead of silently replacing.
  private var reportMalformed: Boolean = false

  override val childExprs: Seq[BaseExprMeta[_]] = if (isStringDecode) {
    // StringDecode StaticInvoke: decode(bin, charset, legacyCharsets, legacyErrorAction)
    // Only wrap the first child (bin) as the GPU expression input
    expr.arguments.take(1).map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  } else {
    expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagExprForGpu(): Unit = {
    if (isStringDecode) {
      tagStringDecode()
    } else {
      ExternalSource.tagForGpu(expr, this)
    }
  }

  private def tagStringDecode(): Unit = {
    // Spark 4.0+ StringDecode.replacement always builds the StaticInvoke with 4 arguments:
    // (bin, charset, legacyCharsets, legacyErrorAction).
    if (expr.arguments.size < 4) {
      willNotWorkOnGpu("StringDecode StaticInvoke has unexpected argument count")
      return
    }
    // charset is the second argument, must be a foldable string literal
    val charsetExpr = expr.arguments(1)
    GpuOverrides.extractLit(charsetExpr).map(_.value) match {
      case Some(cs: org.apache.spark.unsafe.types.UTF8String) if cs != null =>
        charsetName = cs.toString.toUpperCase(Locale.ROOT)
        if (charsetName != "GBK") {
          willNotWorkOnGpu(s"only GBK charset is supported on GPU, got: $charsetName")
        }
      case _ =>
        willNotWorkOnGpu("charset must be a string literal for GPU StringDecode")
    }
    // spark.sql.legacy.javaCharsets=false rejects GBK at CharsetProvider.forName on the CPU
    // before decoding ever starts; in that case we leave the work to the CPU so the charset
    // error is raised consistently. GPU accepts GBK unconditionally and has no equivalent
    // rejection path.
    expr.arguments(2) match {
      case Literal(true, _) =>
      case _ => willNotWorkOnGpu(
        "GPU StringDecode requires spark.sql.legacy.javaCharsets=true")
    }
    // spark.sql.legacy.codingErrorAction selects REPLACE (legacy, true) vs REPORT (false).
    // Both are supported on the GPU; REPORT is delegated to the kernel via CharsetDecode.REPORT
    // and the resulting malformed signal is translated to MALFORMED_CHARACTER_CODING.
    expr.arguments(3) match {
      case Literal(true, _)  => reportMalformed = false
      case Literal(false, _) => reportMalformed = true
      case _ => willNotWorkOnGpu(
        "legacyCodingErrorAction argument to StringDecode is not a boolean literal")
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    if (isStringDecode) {
      val bin = childExprs.head.convertToGpu().asInstanceOf[Expression]
      GpuStringDecode(bin, charsetName, reportMalformed)
    } else {
      ExternalSource.convertToGpu(expr, this)
    }
  }
}
