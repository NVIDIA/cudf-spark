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


/*** spark-rapids-shim-json-lines
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

package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, ParseUrl, StructsToJson}
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.expressions.url.ParseUrlEvaluator
import org.apache.spark.sql.rapids.{GpuParseUrl, GpuStructsToJson}
import org.apache.spark.sql.types._

case class InvokeExprMeta(
    invoke: Invoke,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[Invoke](invoke, conf, p, r) {

  private object SupportedTargetEnum extends Enumeration {
    type SupportedTargetEnum = Value

    val UNSUPPORTED: Value = Value
    val STRUCTS_TO_JSON_EVALUATOR: Value = Value
    val PARSE_URL_EVALUATOR: Value = Value
  }

  private var targetType: SupportedTargetEnum.Value = SupportedTargetEnum.UNSUPPORTED

  /**
   * Return the warped children.
   * Note: `childExprs` ignored the first child of Spark `invoke`: literal(xxEvaluator).
   * If `literal` is not ignored, we need to add type check for ObjectType since `literal` wraps
   * a ObjectType `xxEvaluator`.
   * E.g.: StructsToJson is replaced by
   * invoke(literal(StructsToJsonEvaluator), "evaluate", string_type, arguments, ...)
   * The children of Spark invoke is: literal, arguments
   * The `childExprs` only uses arguments
   */
  override val childExprs: Seq[BaseExprMeta[_]] =
    invoke.arguments.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override final def tagExprForGpu(): Unit = {
    invoke match {
      case Invoke(
      Literal(_: StructsToJsonEvaluator, _: ObjectType),
      functionName: String,
      StringType,
      arguments: Seq[Expression],
      methodInputTypes: Seq[AbstractDataType],
      _: Boolean,
      _: Boolean,
      _: Boolean) if (functionName == "evaluate"
        && arguments.size == 1
        && methodInputTypes.size == 1) =>
        tagStructsToJson(invoke)
        targetType = SupportedTargetEnum.STRUCTS_TO_JSON_EVALUATOR
      case Invoke(
      Literal(_: ParseUrlEvaluator, _: ObjectType),
      functionName: String,
      StringType,
      arguments: Seq[Expression],
      methodInputTypes: Seq[AbstractDataType],
      _: Boolean,
      _: Boolean,
      _: Boolean) if (functionName == "evaluate"
        && (arguments.size == 2 || arguments.size == 3)
        && methodInputTypes.size == arguments.size) =>
        tagParseUrl(invoke)
        targetType = SupportedTargetEnum.PARSE_URL_EVALUATOR
      case _ =>
        // Unsupported invoke expr
        willNotWorkOnGpu(s"Unsupported invoke expr: $invoke")
    }
  }

  /**
   * Invoke this tag for `invoke` if it's expected to do `StructsToJson`.
   * It reuses the existing tag code. First construct `StructsToJson`/`StructsToJson`, then tag and
   * copy the messages into this meta.
   * `StructsToJson` is replaced to
   * invoke(literal(StructsToJsonEvaluator), "evaluate", string_type, arguments, ...)
   * From Spark 400, we can only see the replaced expressions, and can not see `StructsToJson`
   * @param invoke the replaced expressions for `StructsToJson`
   */
  private def tagStructsToJson(invoke: Invoke): Unit = {
    val child = invoke.arguments(0)
    val evaluator = invoke.targetObject.eval(null).asInstanceOf[StructsToJsonEvaluator]
    val stj = StructsToJson(evaluator.options, child, evaluator.timeZoneId)
    val stjMeta = GpuOverrides.wrapExpr(stj, conf, None)
    // forward the tag
    stjMeta.initReasons()
    stjMeta.tagForGpu()
    if (!stjMeta.canThisBeReplaced) {
      val sb = new StringBuilder()
      stjMeta.print(sb, depth = 0, all = false)
      // copy the messages to this meta
      willNotWorkOnGpu(sb.toString())
    }
  }

  /**
   * Invoke this tag for `invoke` if it's expected to do `ParseUrl`.
   * It reuses the existing tag code. First construct `ParseUrl`, then tag and
   * copy the messages into this meta.
   * `ParseUrl` is replaced to
   * invoke(ParseUrlEvaluator), "evaluate", string_type, arguments, ...)
   * From Spark 400, we can only see the replaced expressions, and can not see `ParseUrl`
   * @param invoke the replaced expressions for `ParseUrl`
   */
  private def tagParseUrl(invoke: Invoke): Unit = {
    val evaluator = invoke.targetObject.eval(null).asInstanceOf[ParseUrlEvaluator]
    // ParseUrl takes url, partToExtract, and optionally key arguments
    val parseUrl = ParseUrl(invoke.arguments, evaluator.failOnError)
    val parseUrlMeta = GpuOverrides.wrapExpr(parseUrl, conf, None)
    // forward the tag
    parseUrlMeta.initReasons()
    parseUrlMeta.tagForGpu()
    if (!parseUrlMeta.canThisBeReplaced) {
      val sb = new StringBuilder()
      parseUrlMeta.print(sb, depth = 0, all = false)
      // copy the messages to this meta
      willNotWorkOnGpu(sb.toString())
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    targetType match {
      case SupportedTargetEnum.STRUCTS_TO_JSON_EVALUATOR =>
        val evaluator = invoke.targetObject.eval(null).asInstanceOf[StructsToJsonEvaluator]
        val child = childExprs.head.convertToGpu().asInstanceOf[Expression]
        GpuStructsToJson(evaluator.options, child, evaluator.timeZoneId)
      case SupportedTargetEnum.PARSE_URL_EVALUATOR =>
        val evaluator = invoke.targetObject.eval(null).asInstanceOf[ParseUrlEvaluator]
        val gpuChildren = childExprs.map(_.convertToGpu())
        GpuParseUrl(gpuChildren, evaluator.failOnError)
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported invoke expr: $invoke")
    }
  }
}
