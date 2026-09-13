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
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero


private[rapids] object GpuMathAndDateExpressionOverrides {
  val rules: Seq[ExprRule[_ <: Expression]] = Seq(
    expr[PreciseTimestampConversion](
      "Expression used internally to convert the TimestampType to Long and back without losing " +
          "precision, i.e. in microseconds. Used in time windowing",
      ExprChecks.unaryProject(
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG),
      PreciseTimestampConversionRuleMeta),
    expr[UnaryMinus](
      "Negate a numeric value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval),
      UnaryMinusRuleMeta),
    expr[UnaryPositive](
      "A numeric value with a + in front of it",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval),
      UnaryPositiveRuleMeta),
    expr[Year](
      "Returns the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      YearRuleMeta),
    expr[Month](
      "Returns the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      MonthRuleMeta),
    expr[Quarter](
      "Returns the quarter of the year for date, in the range 1 to 4",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      QuarterRuleMeta),
    expr[DayOfMonth](
      "Returns the day of the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      DayOfMonthRuleMeta),
    expr[DayOfYear](
      "Returns the day of the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      DayOfYearRuleMeta),
    expr[SecondsToTimestamp](
      "Converts the number of seconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.gpuNumeric, TypeSig.cpuNumeric),
      SecondsToTimestampRuleMeta),
    expr[MillisToTimestamp](
      "Converts the number of milliseconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.integral, TypeSig.integral),
      MillisToTimestampRuleMeta),
    expr[MicrosToTimestamp](
      "Converts the number of microseconds from unix epoch to a timestamp",
      ExprChecks.unaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
      TypeSig.integral, TypeSig.integral),
      MicrosToTimestampRuleMeta),
    expr[Acos](
      "Inverse cosine",
      ExprChecks.mathUnaryWithAst,
      AcosRuleMeta),
    expr[Acosh](
      "Inverse hyperbolic cosine",
      ExprChecks.mathUnaryWithAst,
      AcoshRuleMeta),
    expr[Asin](
      "Inverse sine",
      ExprChecks.mathUnaryWithAst,
      AsinRuleMeta),
    expr[Asinh](
      "Inverse hyperbolic sine",
      ExprChecks.mathUnaryWithAst,
      AsinhRuleMeta),
    expr[Sqrt](
      "Square root",
      ExprChecks.mathUnaryWithAst,
      SqrtRuleMeta),
    expr[Cbrt](
      "Cube root",
      ExprChecks.mathUnaryWithAst,
      CbrtRuleMeta),
    expr[Hypot](
      "Pythagorean addition (Hypotenuse) of real numbers",
      ExprChecks.binaryProject(
        TypeSig.DOUBLE,
        TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      HypotRuleMeta),
    expr[Floor](
      "Floor of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      FloorRuleMeta),
    expr[Ceil](
      "Ceiling of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      CeilRuleMeta),
    expr[Not](
      "Boolean not operator",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN),
      NotRuleMeta),
    expr[IsNull](
      "Checks if a value is null",
      ExprChecks.unaryProjectAndAst(TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalPredicateSupportedTypes).nested(),
        TypeSig.all),
      IsNullRuleMeta),
    expr[IsNotNull](
      "Checks if a value is not null",
      ExprChecks.unaryProjectAndAst(TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalPredicateSupportedTypes).nested(),
        TypeSig.all),
      IsNotNullRuleMeta),
    expr[IsNaN](
      "Checks if a value is NaN",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.DOUBLE + TypeSig.FLOAT, TypeSig.DOUBLE + TypeSig.FLOAT),
      IsNaNRuleMeta),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      ExprChecks.mathUnaryWithAst,
      RintRuleMeta),
    expr[AtLeastNNonNulls](
      "Checks if number of non null/Nan values is greater than a given value",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      AtLeastNNonNullsRuleMeta),
    expr[DateAdd](
      "Returns the date that is num_days after start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      DateAddRuleMeta),
    expr[DateSub](
      "Returns the date that is num_days before start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      DateSubRuleMeta),
    expr[NaNvl](
      "Evaluates to `left` iff left is not NaN, `right` otherwise",
      ExprChecks.binaryProject(TypeSig.fp, TypeSig.fp,
        ("lhs", TypeSig.fp, TypeSig.fp),
        ("rhs", TypeSig.fp, TypeSig.fp)),
      NaNvlRuleMeta),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      ShiftLeftRuleMeta),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      ShiftRightRuleMeta),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      ShiftRightUnsignedRuleMeta),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      BitwiseAndRuleMeta),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      BitwiseOrRuleMeta),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      BitwiseXorRuleMeta),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral),
      BitwiseNotRuleMeta),
    expr[BitwiseCount](
      "Returns the number of bits that are set in the input as unsigned 64-bit integer",
      ExprChecks.unaryProject(
        TypeSig.INT, TypeSig.INT,
        TypeSig.integral + TypeSig.BOOLEAN, TypeSig.integral + TypeSig.BOOLEAN),
      BitwiseCountRuleMeta),
    expr[BitAndAgg](
      "Returns the bitwise AND of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      BitAndAggRuleMeta),
    expr[BitOrAgg](
      "Returns the bitwise OR of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      BitOrAggRuleMeta),
    expr[BitXorAgg](
      "Returns the bitwise XOR of all non-null input values",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.integral, TypeSig.integral,
        Seq(ParamCheck("input", TypeSig.integral, TypeSig.integral))),
      BitXorAggRuleMeta),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null",
      ExprChecks.projectOnly(
        (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY +
          TypeSig.MAP + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
        TypeSig.all,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY +
            TypeSig.MAP + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
          TypeSig.all))),
      CoalesceRuleMeta),
    expr[Least] (
      "Returns the least value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      LeastRuleMeta),
    expr[Greatest] (
      "Returns the greatest value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      GreatestRuleMeta),
    expr[Atan](
      "Inverse tangent",
      ExprChecks.mathUnaryWithAst,
      AtanRuleMeta),
    expr[Atanh](
      "Inverse hyperbolic tangent",
      ExprChecks.mathUnaryWithAst,
      AtanhRuleMeta),
    expr[Cos](
      "Cosine",
      ExprChecks.mathUnaryWithAst,
      CosRuleMeta),
    expr[Exp](
      "Euler's number e raised to a power",
      ExprChecks.mathUnaryWithAst,
      ExpRuleMeta),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      ExprChecks.mathUnaryWithAst,
      Expm1RuleMeta),
    expr[InitCap](
      "Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      InitCapRuleMeta).incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Log](
      "Natural log",
      ExprChecks.mathUnary,
      LogRuleMeta),
    expr[Log1p](
      "Natural log 1 + expr",
      ExprChecks.mathUnary,
      Log1pRuleMeta),
    expr[Log2](
      "Log base 2",
      ExprChecks.mathUnary,
      Log2RuleMeta),
    expr[Log10](
      "Log base 10",
      ExprChecks.mathUnary,
      Log10RuleMeta),
    expr[Logarithm](
      "Log variable base",
      ExprChecks.binaryProject(TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("value", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("base", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      LogarithmRuleMeta),
    expr[Sin](
      "Sine",
      ExprChecks.mathUnaryWithAst,
      SinRuleMeta),
    expr[Sinh](
      "Hyperbolic sine",
      ExprChecks.mathUnaryWithAst,
      SinhRuleMeta),
    expr[Cosh](
      "Hyperbolic cosine",
      ExprChecks.mathUnaryWithAst,
      CoshRuleMeta),
    expr[Cot](
      "Cotangent",
      ExprChecks.mathUnaryWithAst,
      CotRuleMeta),
    expr[Tanh](
      "Hyperbolic tangent",
      ExprChecks.mathUnaryWithAst,
      TanhRuleMeta),
    expr[Tan](
      "Tangent",
      ExprChecks.mathUnaryWithAst,
      TanRuleMeta),
    expr[NormalizeNaNAndZero](
      "Normalize NaN and zero",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.FLOAT,
        TypeSig.DOUBLE + TypeSig.FLOAT),
      NormalizeNaNAndZeroRuleMeta),
    expr[KnownFloatingPointNormalized](
      "Tag to prevent redundant normalization",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.all, TypeSig.all),
      KnownFloatingPointNormalizedRuleMeta),
    expr[KnownNotNull](
      "Tag an expression as known to not be null",
      ExprChecks.unaryProjectInputMatchesOutput(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.CALENDAR +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested(), TypeSig.all),
      KnownNotNullRuleMeta),
    expr[DateDiff](
      "Returns the number of days from startDate to endDate",
      ExprChecks.binaryProject(TypeSig.INT, TypeSig.INT,
        ("lhs", TypeSig.DATE, TypeSig.DATE),
        ("rhs", TypeSig.DATE, TypeSig.DATE)),
      DateDiffRuleMeta),
    // TimeAdd moved to TimeAddShims to handle API differences across Spark versions
    expr[DateAddInterval](
      "Adds interval to date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("start", TypeSig.DATE, TypeSig.DATE),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
          TypeSig.CALENDAR)),
      DateAddIntervalRuleMeta),
    expr[DateFormatClass](
      "Converts timestamp to a value of string in the format specified by the date format",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("strfmt", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      DateFormatClassRuleMeta
    ),
    expr[ToUnixTimestamp](
      "Returns the UNIX timestamp of the given time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      ToUnixTimestampRuleMeta),
    expr[UnixTimestamp](
      "Returns the UNIX timestamp of current or specified time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      UnixTimestampRuleMeta),
    expr[Hour](
      "Returns the hour component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      HourRuleMeta),
    expr[Minute](
      "Returns the minute component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      MinuteRuleMeta),
    expr[Second](
      "Returns the second component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      SecondRuleMeta),
    expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      WeekDayRuleMeta),
    expr[DayOfWeek](
      "Returns the day of the week (1 = Sunday...7=Saturday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      DayOfWeekRuleMeta),
    expr[LastDay](
      "Returns the last day of the month which the date belongs to",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DATE, TypeSig.DATE),
      LastDayRuleMeta),
    expr[FromUnixTime](
      "Get the string from a unix timestamp",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("sec", TypeSig.LONG, TypeSig.LONG),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "Only a limited number of formats are supported"),
            TypeSig.STRING)),
      FromUnixTimeConstructorRuleMeta),
    expr[FromUTCTimestamp](
      "Render the input UTC timestamp in the input timezone",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("timezone", TypeSig.lit(TypeEnum.STRING)
          .withPsNote(TypeEnum.STRING,
            "Only non-DST(Daylight Savings Time) timezones are supported"),
          TypeSig.lit(TypeEnum.STRING))),
      FromUTCTimestampConstructorRuleMeta
    ),
    expr[ToUTCTimestamp](
      "Render the input timestamp in UTC",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("timezone", TypeSig.lit(TypeEnum.STRING)
          .withPsNote(TypeEnum.STRING,
            "Only non-DST(Daylight Savings Time) timezones are supported"),
          TypeSig.lit(TypeEnum.STRING))),
      ToUTCTimestampConstructorRuleMeta
    ),
    expr[MonthsBetween](
      "If `timestamp1` is later than `timestamp2`, then the result " +
        "is positive. If `timestamp1` and `timestamp2` are on the same day of month, or both " +
        "are the last day of month, time of day will be ignored. Otherwise, the difference is " +
        "calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false.",
      ExprChecks.projectOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("timestamp1", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          ParamCheck("timestamp2", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          ParamCheck("round", TypeSig.lit(TypeEnum.BOOLEAN), TypeSig.BOOLEAN))),
      MonthsBetweenConstructorRuleMeta
    ),
    expr[TruncDate](
      "Truncate the date to the unit specified by the given string format",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("date", TypeSig.DATE, TypeSig.DATE),
        ("format", TypeSig.STRING, TypeSig.STRING)),
      TruncDateConstructorRuleMeta
    ),
    expr[TruncTimestamp](
      "Truncate the timestamp to the unit specified by the given string format",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("format", TypeSig.STRING, TypeSig.STRING),
        ("date", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP)),
      TruncTimestampConstructorRuleMeta
    ),
  )
}
