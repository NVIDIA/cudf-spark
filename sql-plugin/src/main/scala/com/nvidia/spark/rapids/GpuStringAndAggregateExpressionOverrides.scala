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

private[rapids] object GpuStringAndAggregateExpressionOverrides {
  val rules: Seq[ExprRule[_ <: Expression]] = Seq(
    expr[StringLocate](
      "Substring search operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("start", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      StringLocateRuleMeta),
    expr[StringInstr](
      "Instr string operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
            ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringInstrRuleMeta),
    expr[Substring](
      "Substring operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
          ParamCheck("pos", TypeSig.INT, TypeSig.INT),
          ParamCheck("len", TypeSig.INT, TypeSig.INT))),
      SubstringRuleMeta),
    expr[SubstringIndex](
      "substring_index operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("delim", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("count", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      SubstringIndexConstructorRuleMeta),
    expr[StringRepeat](
      "StringRepeat operator that repeats the given strings with numbers of times " +
        "given by repeatTimes",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("input", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("repeatTimes", TypeSig.INT, TypeSig.INT))),
      StringRepeatRuleMeta),
    expr[StringReplace](
      "StringReplace operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("search", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("replace", TypeSig.STRING, TypeSig.STRING))),
      StringReplaceRuleMeta),
    expr[StringTrim](
      "StringTrim operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringTrimRuleMeta),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringTrimLeftRuleMeta),
    expr[StringTrimRight](
      "StringTrimRight operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringTrimRightRuleMeta),
    expr[StringTranslate](
      "StringTranslate operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("input", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("from", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("to", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringTranslateRuleMeta)
        .incompat("the GPU implementation supports all unicode code points. In Spark versions " +
          "< 3.2.0, translate() does not support unicode characters with code point >= U+10000 " +
          "(See SPARK-34094)"),
    expr[StartsWith](
      "Starts with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      StartsWithRuleMeta),
    expr[EndsWith](
      "Ends with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      EndsWithRuleMeta),
    expr[Concat](
      "List/String concatenate",
      ExprChecks.projectOnly((TypeSig.STRING + TypeSig.ARRAY).nested(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      ConcatRuleMeta),
    expr[Conv](
      desc = "Convert string representing a number from one base to another",
      pluginChecks = ExprChecks.projectOnly(
        outputCheck = TypeSig.STRING,
        paramCheck = Seq(
          ParamCheck(
            name = "num",
            cudf = TypeSig.STRING,
            spark = TypeSig.STRING),
          ParamCheck(
            name = "from_base",
            cudf = TypeSig.INT,
            spark = TypeSig.INT),
          ParamCheck(
            name = "to_base",
            cudf = TypeSig.INT,
            spark = TypeSig.INT)),
        sparkOutputSig = TypeSig.STRING),
      (convExpr, conf, parentMetaOpt, dataFromReplacementRule) =>
        new GpuConvMeta(convExpr, conf, parentMetaOpt, dataFromReplacementRule)
    ),
    expr[FormatNumber](
      "Formats the number x like '#,###,###.##', rounded to d decimal places.",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("x", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
        ("d", TypeSig.lit(TypeEnum.INT), TypeSig.INT+TypeSig.STRING)),
      FormatNumberRuleMeta
    ),
    expr[MapConcat](
      "Returns the union of all the given maps",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.MAP.nested(TypeSig.all)))),
      MapConcatRuleMeta),
    expr[Slice](
      "Subsets array x starting from index start (array indices start at 1, " +
        "or starting from the end if start is negative) with the specified length.",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("x",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("start", TypeSig.INT, TypeSig.INT),
          ParamCheck("length", TypeSig.INT, TypeSig.INT))),
      SliceRuleMeta),
    expr[ArrayJoin](
      "Concatenates the elements of the given array using the delimiter and an optional " +
        "string to replace nulls. If no value is set for nullReplacement, any null value " +
        "is filtered.",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("array",
          TypeSig.ARRAY.nested(TypeSig.STRING),
          TypeSig.ARRAY.nested(TypeSig.STRING)),
          ParamCheck("delimiter",
            TypeSig.STRING,
            TypeSig.STRING)),
        repeatingParamCheck = Some(RepeatingParamCheck("nullReplacement",
          TypeSig.lit(TypeEnum.STRING),
          TypeSig.STRING))),
      ArrayJoinRuleMeta
    ),
    expr[ConcatWs](
      "Concatenates multiple input strings or array of strings into a single " +
        "string using a given separator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING),
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING)))),
      ConcatWsRuleMeta),
    expr[Murmur3Hash](
      "Murmur3 hash operator",
      HashExprChecks.murmur3ProjectChecks,
      Murmur3HashExprMeta.apply),
    expr[XxHash64](
      "xxhash64 hash operator",
      HashExprChecks.xxhash64ProjectChecks,
      XxHash64ExprMeta.apply),
    expr[HiveHash](
      "hive hash operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested() +
              TypeSig.psNote(TypeEnum.ARRAY, "The nesting depth has a certain limit") +
              TypeSig.psNote(TypeEnum.STRUCT, "The nesting depth has a certain limit"),
          TypeSig.all))),
      HiveHashRuleMeta),
    expr[Contains](
      "Contains",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.STRING, TypeSig.STRING)),
      ContainsRuleMeta),
    expr[Like](
      "Like",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.STRING, TypeSig.STRING)),
      LikeRuleMeta),
    expr[RLike](
      "Regular expression version of Like",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("str", TypeSig.STRING, TypeSig.STRING),
        ("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      RLikeConstructorRuleMeta),
    expr[RegExpReplace](
      "String replace using a regular expression pattern",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regex", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("rep", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("pos", TypeSig.lit(TypeEnum.INT)
              .withPsNote(TypeEnum.INT, "only a value of 1 is supported"),
            TypeSig.lit(TypeEnum.INT)))),
      RegExpReplaceConstructorRuleMeta),
    expr[RegExpExtract](
      "Extract a specific group identified by a regular expression",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("idx", TypeSig.lit(TypeEnum.INT),
            TypeSig.lit(TypeEnum.INT)))),
      RegExpExtractConstructorRuleMeta),
    expr[RegExpExtractAll](
      "Extract all strings matching a regular expression corresponding to the regex group index",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("idx", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      RegExpExtractAllConstructorRuleMeta),
    expr[ParseUrl](
      "Extracts a part from a URL",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("url", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("partToExtract", TypeSig.lit(TypeEnum.STRING).withPsNote(
            TypeEnum.STRING, "only support partToExtract = PROTOCOL | HOST | QUERY | PATH"),
            TypeSig.STRING)),
          // Should really be an OptionalParam
          Some(RepeatingParamCheck("key", TypeSig.STRING, TypeSig.STRING))),
      ParseUrlRuleMeta),
    expr[Length](
      "String character length or binary byte length",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      LengthRuleMeta),
    expr[Size](
      "The size of an array or a map",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL
            + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      SizeRuleMeta),
    expr[Reverse](
      "Returns a reversed string or an array with reverse order of elements",
      ExprChecks.unaryProject(TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.STRING + TypeSig.ARRAY.nested(TypeSig.all)),
      ReverseRuleMeta),
    expr[UnscaledValue](
      "Convert a Decimal to an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.LONG, TypeSig.LONG,
        TypeSig.DECIMAL_64, TypeSig.DECIMAL_128),
      UnscaledValueRuleMeta),
    expr[MakeDecimal](
      "Create a Decimal from an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.DECIMAL_64, TypeSig.DECIMAL_128,
        TypeSig.LONG, TypeSig.LONG),
      MakeDecimalRuleMeta),
    expr[Explode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      ExplodeRuleMeta),
    expr[PosExplode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      PosExplodeRuleMeta),
    expr[Stack](
      "Separates expr1, ..., exprk into n rows.",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("n", TypeSig.lit(TypeEnum.INT), TypeSig.INT)),
        Some(RepeatingParamCheck("expr",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      StackConstructorRuleMeta
    ),
    expr[ReplicateRows](
      "Given an input row replicates the row N times",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      ReplicateRowsRuleMeta),
    expr[CollectList](
      "Collect a list of non-unique elements, not supported in reduction",
      ExprChecks.fullAgg(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP)
            .withPsNote(TypeEnum.ARRAY, "window operations are disabled by default due " +
                "to extreme memory usage"),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all))),
      CollectListRuleMeta),
    expr[CollectSet](
      "Collect a set of unique elements, not supported in reduction",
      ExprChecks.fullAgg(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY)
            .withPsNote(TypeEnum.ARRAY, "window operations are disabled by default due " +
                "to extreme memory usage"),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.NULL +
            TypeSig.STRUCT +
            TypeSig.ARRAY).nested(),
          TypeSig.all))),
      CollectSetRuleMeta),
    expr[StddevPop](
      "Aggregation computing population standard deviation",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      StddevPopRuleMeta),
    expr[StddevSamp](
      "Aggregation computing sample standard deviation",
      ExprChecks.fullAgg(
          TypeSig.DOUBLE, TypeSig.DOUBLE,
          Seq(ParamCheck("input", TypeSig.DOUBLE,
            TypeSig.DOUBLE))),
        StddevSampRuleMeta),
    expr[VariancePop](
      "Aggregation computing population variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      VariancePopRuleMeta),
    expr[VarianceSamp](
      "Aggregation computing sample variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      VarianceSampRuleMeta),
    expr[Percentile](
      "Aggregation computing exact percentile",
      ExprChecks.reductionAndGroupByAgg(
        // The output can be a single number or array depending on whether percentiles param
        // is a single number or an array.
        TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
        TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
        Seq(
          // ANSI interval types are new in Spark 3.2.0 and are not yet supported by the
          // current GPU implementation.
          ParamCheck("input", TypeSig.integral + TypeSig.fp, TypeSig.integral + TypeSig.fp),
          ParamCheck("percentage",
            TypeSig.lit(TypeEnum.DOUBLE) + TypeSig.ARRAY.nested(TypeSig.lit(TypeEnum.DOUBLE)),
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE)),
          ParamCheck("frequency",
            TypeSig.LONG + TypeSig.ARRAY.nested(TypeSig.LONG),
            TypeSig.LONG + TypeSig.ARRAY.nested(TypeSig.LONG)))),
      PercentileRuleMeta),
    expr[ApproximatePercentile](
      "Approximate percentile",
      ExprChecks.reductionAndGroupByAgg(
        // note that output can be single number or array depending on whether percentiles param
        // is a single number or an array
        TypeSig.gpuNumeric +
            TypeSig.ARRAY.nested(TypeSig.gpuNumeric),
        TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.ARRAY.nested(
          TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
        Seq(
          ParamCheck("input",
            TypeSig.gpuNumeric,
            TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
          ParamCheck("percentage",
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE)),
          ParamCheck("accuracy", TypeSig.INT, TypeSig.INT))),
      ApproximatePercentileRuleMeta)
        .incompat("the GPU implementation of approx_percentile is not bit-for-bit " +
          s"compatible with Apache Spark"),
  )
}
