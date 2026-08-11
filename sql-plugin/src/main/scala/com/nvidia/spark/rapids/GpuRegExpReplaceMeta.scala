/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.rapids.{GpuRegExpReplace, GpuRegExpReplaceWithBackref, GpuRegExpUtils}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

trait GpuRegExpReplaceOpt extends Serializable

@SerialVersionUID(100L)
object GpuRegExpStringReplace extends GpuRegExpReplaceOpt

@SerialVersionUID(100L)
object GpuRegExpStringReplaceMulti extends GpuRegExpReplaceOpt

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends QuaternaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  private var javaPattern: Option[String] = None
  private var cudfPattern: Option[String] = None
  private var replacement: Option[String] = None
  private var searchList: Option[Seq[String]] = None
  private var replaceOpt: Option[GpuRegExpReplaceOpt] = None
  private var containsBackref: Boolean = false

  override def tagExprForGpu(): Unit = {
    replacement = expr.rep match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null => Some(s.toString)
      case _ => None
    }

    expr.regexp match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null =>
        javaPattern = Some(s.toString())
        try {
          val (pat, repl) =
              new CudfRegexTranspiler(RegexReplaceMode).getTranspiledAST(s.toString, None,
                  replacement)
          // Use the user's Java pattern group count for the greedy-with-backoff parse so that
          // user-authored backref tokens (e.g. `$13` on a 12-group pattern) follow Java's
          // `Matcher.appendReplacement` spec. Backrefs already emitted in braced `${N}` form
          // are treated as resolved by the replacement AST and do not need to be covered by
          // the user pattern's group count.
          val userNumCaptureGroups =
            java.util.regex.Pattern.compile(s.toString).matcher("").groupCount()
          repl.map { r =>
            GpuRegExpUtils.backrefConversion(r.toRegexString, userNumCaptureGroups)
          }.foreach {
              case (hasBackref, convertedRep) =>
                containsBackref = hasBackref
                replacement = Some(GpuRegExpUtils.unescapeReplaceString(convertedRep))
          }
          if (!containsBackref && GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
            replaceOpt = Some(GpuRegExpStringReplace)
          } else {
              searchList = GpuRegExpUtils.getChoicesFromRegex(pat)
              searchList match {
                case Some(_) if !containsBackref =>
                  replaceOpt = Some(GpuRegExpStringReplaceMulti)
                case _ =>
                  GpuRegExpUtils.tagForRegExpEnabled(this)
                  cudfPattern = Some(pat.toRegexString)
              }
          }
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }

      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    GpuOverrides.extractLit(expr.pos).foreach { lit =>
      if (lit.value.asInstanceOf[Int] != 1) {
        willNotWorkOnGpu("only a search starting position of 1 is supported")
      }
    }
  }

  override def convertToGpu(
      lhs: Expression,
      regexp: Expression,
      rep: Expression,
      pos: Expression): GpuExpression = {
    // ignore the pos expression which must be a literal 1 after tagging check
    require(childExprs.length == 4,
      s"Unexpected child count for RegExpReplace: ${childExprs.length}")
    replaceOpt match {
      case None =>
        (javaPattern, cudfPattern, replacement) match {
          case (Some(javaPattern), Some(cudfPattern), Some(cudfReplacement)) =>
            if (containsBackref) {
              GpuRegExpReplaceWithBackref(lhs, regexp, rep)(javaPattern,
                cudfPattern, cudfReplacement)
            } else {
              GpuRegExpReplace(lhs, regexp, rep)(javaPattern, cudfPattern, cudfReplacement,
                  None, None)
            }
          case _ =>
            throw new IllegalStateException("Expression has not been tagged correctly")
        }
      case _ =>
        (javaPattern, replacement) match {
          case (Some(javaPattern), Some(replacement)) =>
            GpuRegExpReplace(lhs, regexp, rep)(javaPattern, javaPattern, replacement,
                searchList, replaceOpt)
          case _ =>
            throw new IllegalStateException("Expression has not been tagged correctly")
        }
    }
  }
}
