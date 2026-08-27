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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.GpuOverrides._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class StringLocateRuleMeta(
    in: StringLocate,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[StringLocate](in, conf, p, r) {
  override def convertToGpu(
      val0: Expression,
      val1: Expression,
      val2: Expression): GpuExpression =
    GpuStringLocate(val0, val1, val2)
}

case class StringInstrRuleMeta(
    in: StringInstr,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[StringInstr](in, conf, p, r) {
  override def convertToGpu(
      str: Expression,
      substr: Expression): GpuExpression =
    GpuStringInstr(str, substr)
}

case class SubstringRuleMeta(
    in: Substring,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[Substring](in, conf, p, r) {
  override def convertToGpu(
      column: Expression,
      position: Expression,
      length: Expression): GpuExpression =
    GpuSubstring(column, position, length)
}

case class StringRepeatRuleMeta(
    in: StringRepeat,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[StringRepeat](in, conf, p, r) {
  override def convertToGpu(
      input: Expression,
      repeatTimes: Expression): GpuExpression = GpuStringRepeat(input, repeatTimes)
}

case class StringReplaceRuleMeta(
    in: StringReplace,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[StringReplace](in, conf, p, r) {
  override def convertToGpu(
      column: Expression,
      target: Expression,
      replace: Expression): GpuExpression =
    GpuStringReplace(column, target, replace)
}

case class StringTrimRuleMeta(
    in: StringTrim,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends String2TrimExpressionMeta[StringTrim](in, conf, p, r) {
  override def convertToGpu(
      column: Expression,
      target: Option[Expression] = None): GpuExpression =
    GpuStringTrim(column, target)
}

case class StringTrimLeftRuleMeta(
    in: StringTrimLeft,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends String2TrimExpressionMeta[StringTrimLeft](in, conf, p, r) {
  override def convertToGpu(
    column: Expression,
    target: Option[Expression] = None): GpuExpression =
    GpuStringTrimLeft(column, target)
}

case class StringTrimRightRuleMeta(
    in: StringTrimRight,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends String2TrimExpressionMeta[StringTrimRight](in, conf, p, r) {
  override def convertToGpu(
      column: Expression,
      target: Option[Expression] = None): GpuExpression =
    GpuStringTrimRight(column, target)
}

case class StringTranslateRuleMeta(
    in: StringTranslate,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[StringTranslate](in, conf, p, r) {
  override def convertToGpu(
      input: Expression,
      from: Expression,
      to: Expression): GpuExpression =
    GpuStringTranslate(input, from, to)
}

case class StartsWithRuleMeta(
    a: StartsWith,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[StartsWith](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuStartsWith(lhs, rhs)
}

case class EndsWithRuleMeta(
    a: EndsWith,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[EndsWith](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuEndsWith(lhs, rhs)
}

case class ConcatRuleMeta(
    a: Concat,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
  override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuConcat(child)
}

case class FormatNumberRuleMeta(
    in: FormatNumber,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[FormatNumber](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    in.children.head.dataType match {
      case FloatType | DoubleType if !this.conf.isFloatFormatNumberEnabled =>
        willNotWorkOnGpu("format_number with floating point types on the GPU returns " +
            "results that have a different precision than the default results of Spark. " +
            "To enable this operation on the GPU, set" +
            s" ${RapidsConf.ENABLE_FLOAT_FORMAT_NUMBER} to true.")
      case _ =>
    }
  }
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuFormatNumber(lhs, rhs)
}

case class MapConcatRuleMeta(
    a: MapConcat,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ComplexTypeMergingExprMeta[MapConcat](a, conf, p, r) {
  override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuMapConcat(child)
}

case class SliceRuleMeta(
    in: Slice,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[Slice](in, conf, p, r) {
  override def convertToGpu(
      x: Expression,
      start: Expression,
      length: Expression): GpuExpression =
    GpuSlice(x, start, length)
}

case class ArrayJoinRuleMeta(
    a: ArrayJoin,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArrayJoin](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    if (a.children.size > 3) {
      willNotWorkOnGpu(s"array_join has more parameters than we expected " +
        s"to see. Found ${a.children.size}")
    }
  }
  override def convertToGpuImpl(): GpuExpression =
    GpuArrayJoin(childExprs.map(_.convertToGpu()))
}

case class ConcatWsRuleMeta(
    a: ConcatWs,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ConcatWs](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    if (a.children.size <= 1) {
      // If only a separator specified and its a column, Spark returns an empty
      // string for all entries unless they are null, then it returns null.
      // This seems like edge case so instead of handling on GPU just fallback.
      willNotWorkOnGpu("Only specifying separator column not supported on GPU")
    }
  }
  override final def convertToGpuImpl(): GpuExpression =
    GpuConcatWs(childExprs.map(_.convertToGpu()))
}

case class HiveHashRuleMeta(
    a: HiveHash,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[HiveHash](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    def getMaxStackDepth(inputType: DataType): Int = {
      inputType match {
        case at: ArrayType => 1 + getMaxStackDepth(at.elementType)
        case st: StructType =>
          1 + st.map(f => getMaxStackDepth(f.dataType)).max
        case _ => 0 // primitive types
      }
    }
    val maxDepth = a.children.map(c => getMaxStackDepth(c.dataType)).max
    val supportedDepth = XxHash64Utils.MAX_STACK_DEPTH
    if (maxDepth > supportedDepth) {
      willNotWorkOnGpu(s"the data type requires a stack size of $maxDepth, " +
        s"which exceeds the GPU limit of $supportedDepth")
    }
  }

  def convertToGpuImpl(): GpuExpression =
    GpuHiveHash(childExprs.map(_.convertToGpu()))
}

case class ContainsRuleMeta(
    a: Contains,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Contains](a, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuContains(lhs, rhs)
}

case class LikeRuleMeta(
    a: Like,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[Like](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    a.right match {
      case Literal(v: UTF8String, _) =>
        val pattern = v.toString
        val esc = a.escapeChar
        var i = 0
        while (i < pattern.length) {
          if (pattern.charAt(i) == esc) {
            val j = i + 1
            if (j >= pattern.length) {
              willNotWorkOnGpu(
                "invalid LIKE escape pattern")
              return
            }
            val c = pattern.charAt(j)
            if (c != '_' && c != '%' && c != esc) {
              willNotWorkOnGpu(
                "invalid LIKE escape pattern")
              return
            }
            i = j + 1
          } else {
            i += 1
          }
        }
      case _ =>
    }
  }
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuLike(lhs, rhs, a.escapeChar)
}

case class ParseUrlRuleMeta(
    a: ParseUrl,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ParseUrl](a, conf, p, r) {
  override def tagExprForGpu(): Unit = {

    extractStringLit(a.children(1)) match {
      // In Spark, the key in parse_url could act like a regex, but GPU will match the key
      // exactly. When key is literal, GPU will check if the key contains regex special and
      // fallbcak to CPU if it does, but we are not able to fallback when key is column.
      // see Spark issue: https://issues.apache.org/jira/browse/SPARK-44500
      case Some("QUERY") if (a.children.size == 3) => {
        extractLit(a.children(2)).foreach { key =>
          if (key.value != null) {
            val keyStr = key.value.asInstanceOf[UTF8String].toString
            if (regexMetaChars.exists(keyStr.contains(_))) {
              willNotWorkOnGpu(s"Key $keyStr could act like a regex which is not " +
                  "supported on GPU")
            }
          }
        }
      }
      case Some(part) if GpuParseUrl.isSupportedPart(part) =>
      case Some(other) =>
        willNotWorkOnGpu(s"Part to extract $other is not supported on GPU")
      case None =>
        // Should never get here, but just in case
        willNotWorkOnGpu("GPU only supports a literal for the part to extract")
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    GpuParseUrl(childExprs.map(_.convertToGpu()), a.failOnError)
  }
}

case class LengthRuleMeta(
    a: Length,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Length](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuLength(child)
}

case class SizeRuleMeta(
    a: Size,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Size](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuSize(child, a.legacySizeOfNull)
}

case class ReverseRuleMeta(
    a: Reverse,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Reverse](a, conf, p, r) {
  override def convertToGpu(input: Expression): GpuExpression =
    GpuReverse(input)
}

case class UnscaledValueRuleMeta(
    a: UnscaledValue,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[UnscaledValue](a, conf, p, r) {
  override val isFoldableNonLitAllowed: Boolean = true
  override def convertToGpu(child: Expression): GpuExpression = GpuUnscaledValue(child)
}

case class MakeDecimalRuleMeta(
    a: MakeDecimal,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MakeDecimal](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMakeDecimal(child, a.precision, a.scale, a.nullOnOverflow)
}

case class ExplodeRuleMeta(
    a: Explode,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GeneratorExprMeta[Explode](a, conf, p, r) {
  override val supportOuter: Boolean = true
  override def convertToGpuImpl(): GpuExpression = GpuExplode(childExprs.head.convertToGpu())
}

case class PosExplodeRuleMeta(
    a: PosExplode,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GeneratorExprMeta[PosExplode](a, conf, p, r) {
  override val supportOuter: Boolean = true
  override def convertToGpuImpl(): GpuExpression =
    GpuPosExplode(childExprs.head.convertToGpu())
}

case class ReplicateRowsRuleMeta(
    a: ReplicateRows,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ReplicateRowsExprMeta[ReplicateRows](a, conf, p, r) {
  override def convertToGpu(childExpr: Seq[Expression]): GpuExpression =
    GpuReplicateRows(childExpr)
}

case class CollectListRuleMeta(
    c: CollectList,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TypedImperativeAggExprMeta[CollectList](c, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    if (context == WindowAggExprContext && !this.conf.isWindowCollectListEnabled) {
      willNotWorkOnGpu("collect_list is disabled for window operations because " +
          "the output explodes in size proportional to the window size squared. If " +
          "you know the window is small you can try it by setting " +
          s"${RapidsConf.ENABLE_WINDOW_COLLECT_LIST} to true")
    }
  }

  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuCollectList(childExprs.head, c.mutableAggBufferOffset, c.inputAggBufferOffset,
      TypeUtilsShims.collectListIgnoreNulls(c))

  override def aggBufferAttribute: AttributeReference = {
    val aggBuffer = c.aggBufferAttributes.head
    aggBuffer.copy(dataType = c.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
  }

  override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
    new CpuToGpuCollectBufferConverter(c.child.dataType,
      !TypeUtilsShims.collectListIgnoreNulls(c))

  override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
    new GpuToCpuCollectBufferConverter()

  override val supportBufferConversion: Boolean = true

  // Last does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class CollectSetRuleMeta(
    c: CollectSet,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TypedImperativeAggExprMeta[CollectSet](c, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    if (context == WindowAggExprContext && !this.conf.isWindowCollectSetEnabled) {
      willNotWorkOnGpu("collect_set is disabled for window operations because " +
          "the output can explode in size proportional to the window size squared. If " +
          "you know the window is small you can try it by setting " +
          s"${RapidsConf.ENABLE_WINDOW_COLLECT_SET} to true")
    }
  }

  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuCollectSet(childExprs.head, c.mutableAggBufferOffset, c.inputAggBufferOffset,
      TypeUtilsShims.collectSetIgnoreNulls(c))

  override def aggBufferAttribute: AttributeReference = {
    val aggBuffer = c.aggBufferAttributes.head
    // Match Spark 4.2+ CollectSet buffer layout for float/double (normalized bit keys).
    val ignoreNulls = TypeUtilsShims.collectSetIgnoreNulls(c)
    val bufferElementType =
      TypeUtilsShims.collectSetCpuBufferElementType(c.child.dataType)
    aggBuffer.copy(dataType = ArrayType(bufferElementType, !ignoreNulls))(
      aggBuffer.exprId, aggBuffer.qualifier)
  }

  override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter = {
    val ignoreNulls = TypeUtilsShims.collectSetIgnoreNulls(c)
    new CpuToGpuCollectBufferConverter(
      TypeUtilsShims.collectSetCpuBufferElementType(c.child.dataType),
      !ignoreNulls)
  }

  override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
    new GpuToCpuCollectBufferConverter()

  override val supportBufferConversion: Boolean = true

  // Last does not overflow, so it doesn't need the ANSI check
  override val needsAnsiCheck: Boolean = false
}

case class StddevPopRuleMeta(
    a: StddevPop,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[StddevPop](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
    GpuStddevPop(childExprs.head, !legacyStatisticalAggregate)
  }
}

case class StddevSampRuleMeta(
    a: StddevSamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[StddevSamp](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
    GpuStddevSamp(childExprs.head, !legacyStatisticalAggregate)
  }
}

case class VariancePopRuleMeta(
    a: VariancePop,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[VariancePop](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
    GpuVariancePop(childExprs.head, !legacyStatisticalAggregate)
  }
}

case class VarianceSampRuleMeta(
    a: VarianceSamp,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends AggExprMeta[VarianceSamp](a, conf, p, r) {
  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val legacyStatisticalAggregate = SQLConf.get.legacyStatisticalAggregate
    GpuVarianceSamp(childExprs.head, !legacyStatisticalAggregate)
  }
}

case class PercentileRuleMeta(
    c: Percentile,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TypedImperativeAggExprMeta[Percentile](c, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    // Check if the input percentage can be supported on GPU.
    GpuOverrides.extractLit(childExprs(1).wrapped.asInstanceOf[Expression]) match {
      case None =>
        willNotWorkOnGpu("percentile on GPU only supports literal percentages")
      case Some(Literal(null, _)) =>
        willNotWorkOnGpu("percentile on GPU only supports non-null literal percentages")
      case Some(Literal(a: ArrayData, _)) => {
        if((0 until a.numElements).exists(a.isNullAt)) {
          willNotWorkOnGpu(
            "percentile on GPU does not support percentage arrays containing nulls")
        }
        if (a.toDoubleArray().exists(percentage => percentage < 0.0 || percentage > 1.0)) {
          willNotWorkOnGpu(
            "percentile requires the input percentages given in the range [0, 1]")
        }
      }
      case Some(_) => // This is fine
    }
  }

  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
    val exprMeta = p.get.asInstanceOf[BaseExprMeta[_]]
    val isReduction = exprMeta.context match {
      case ReductionAggExprContext => true
      case GroupByAggExprContext => false
      case _ => throw new IllegalStateException(
        s"Invalid aggregation context: ${exprMeta.context}")
    }
    GpuPercentile(childExprs.head, childExprs(1).asInstanceOf[GpuLiteral], childExprs(2),
      isReduction)
  }
  // Declare the data type of the internal buffer so it can be serialized and
  // deserialized correctly during shuffling.
  override def aggBufferAttribute: AttributeReference = {
    val aggBuffer = c.aggBufferAttributes.head
    val dataType: DataType = ArrayType(StructType(Seq(
      StructField("value", childExprs.head.dataType),
      StructField("frequency", LongType))), containsNull = false)
    aggBuffer.copy(dataType = dataType)(aggBuffer.exprId, aggBuffer.qualifier)
  }

  override val needsAnsiCheck: Boolean = false
  override val supportBufferConversion: Boolean = true
  override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
    CpuToGpuPercentileBufferConverter(childExprs.head.dataType)
  override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
    GpuToCpuPercentileBufferConverter(childExprs.head.dataType)
}

case class ApproximatePercentileRuleMeta(
    c: ApproximatePercentile,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TypedImperativeAggExprMeta[ApproximatePercentile](c, conf, p, r) {
  override def tagAggForGpu(): Unit = {
    // check if the percentile expression can be supported on GPU
    childExprs(1).wrapped match {
      case lit: Literal => lit.value match {
        case null =>
          willNotWorkOnGpu(
            "approx_percentile on GPU only supports non-null literal percentiles")
        case a: ArrayData if a.numElements == 0 =>
          willNotWorkOnGpu(
            "approx_percentile on GPU does not support empty percentiles arrays")
        case a: ArrayData if (0 until a.numElements).exists(a.isNullAt) =>
          willNotWorkOnGpu(
            "approx_percentile on GPU does not support percentiles arrays containing nulls")
        case _ =>
          // this is fine
      }
      case _ =>
        willNotWorkOnGpu("approx_percentile on GPU only supports literal percentiles")
    }
  }

  override def convertToGpu(childExprs: Seq[Expression]): GpuExpression =
    GpuApproximatePercentile(childExprs.head,
        childExprs(1).asInstanceOf[GpuLiteral],
        childExprs(2).asInstanceOf[GpuLiteral])

  override def aggBufferAttribute: AttributeReference = {
    // Spark's ApproxPercentile has an aggregation buffer named "buf" with type "BinaryType"
    // so we need to replace that here with the GPU aggregation buffer reference, which is
    // a t-digest type
    val aggBuffer = c.aggBufferAttributes.head
    aggBuffer.copy(dataType = CudfTDigest.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
  }
}


case class SubstringIndexConstructorRuleMeta(
    in: SubstringIndex,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SubstringIndexMeta(in, conf, parent, r)

case class RLikeConstructorRuleMeta(
    a: RLike,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuRLikeMeta(a, conf, parent, r)

case class RegExpReplaceConstructorRuleMeta(
    a: RegExpReplace,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuRegExpReplaceMeta(a, conf, parent, r)

case class RegExpExtractConstructorRuleMeta(
    a: RegExpExtract,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuRegExpExtractMeta(a, conf, parent, r)

case class RegExpExtractAllConstructorRuleMeta(
    a: RegExpExtractAll,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuRegExpExtractAllMeta(a, conf, parent, r)

case class StackConstructorRuleMeta(
    a: Stack,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuStackMeta(a, conf, parent, r)
