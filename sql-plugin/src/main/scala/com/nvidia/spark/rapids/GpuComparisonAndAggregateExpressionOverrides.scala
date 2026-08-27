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
import org.apache.spark.sql.rapids.aggregate._
import org.apache.spark.sql.types._


private[rapids] object GpuComparisonAndAggregateExpressionOverrides {
  val rules: Seq[ExprRule[_ <: Expression]] = Seq(
    expr[Pmod](
      "Pmod",
      // Decimal support disabled https://github.com/NVIDIA/spark-rapids/issues/7553
      ExprChecks.binaryProject(TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric,
        ("lhs", (TypeSig.integral + TypeSig.fp).withPsNote(TypeEnum.DECIMAL,
          s"decimals with precision ${DecimalType.MAX_PRECISION} are not supported"),
            TypeSig.cpuNumeric),
        ("rhs", (TypeSig.integral + TypeSig.fp), TypeSig.cpuNumeric)),
      PmodRuleMeta),
    expr[Add](
      "Addition",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval)),
      AddRuleMeta),
    expr[Subtract](
      "Subtraction",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
            TypeSig.numericAndInterval)),
      SubtractRuleMeta),
    expr[And](
      "Logical AND",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      AndRuleMeta),
    expr[Or](
      "Logical OR",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      OrRuleMeta),
    expr[EqualNullSafe](
      "Check if the values are equal including nulls <=>",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable)),
      EqualNullSafeRuleMeta),
    expr[EqualTo](
      "Check if the values are equal",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.comparable)),
      EqualToRuleMeta),
    expr[GreaterThan](
      "> operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      GreaterThanRuleMeta),
    expr[GreaterThanOrEqual](
      ">= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      GreaterThanOrEqualRuleMeta),
    expr[In](
      "IN operator",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(ParamCheck("value", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.comparable)),
        Some(RepeatingParamCheck("list",
          TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
          TypeSig.comparable))),
      InRuleMeta).note(
        "Non-literal list expressions must be deterministic and side-effect-free"),
    expr[InSet](
      "INSET operator",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.comparable),
      InSetRuleMeta),
    expr[LessThan](
      "< operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      LessThanRuleMeta),
    expr[LessThanOrEqual](
      "<= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable),
        ("rhs", (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            GpuTypeShims.additionalPredicateSupportedTypes + TypeSig.STRUCT).nested(),
            TypeSig.orderable)),
      LessThanOrEqualRuleMeta),
    expr[CaseWhen](
      "CASE WHEN expression",
      CaseWhenCheck,
      GpuCaseWhenMeta),
    expr[If](
      "IF expression",
      ExprChecks.projectOnly(
        (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all,
        Seq(ParamCheck("predicate", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
          ParamCheck("trueValue",
            (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
                TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
            TypeSig.all),
          ParamCheck("falseValue",
            (gpuCommonTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
                TypeSig.BINARY + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
            TypeSig.all))),
      GpuIfMeta),
    expr[Pow](
      "lhs ^ rhs",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      PowRuleMeta),
    expr[AggregateExpression](
      "Aggregate expression",
      // Let the underlying expression checks decide whether this can be on the GPU.
      ExprChecks.fullAgg(
        TypeSig.all,
        TypeSig.all,
        Seq(ParamCheck("aggFunc", TypeSig.all, TypeSig.all)),
        Some(RepeatingParamCheck("filter", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      GpuAggregateExpressionMeta),
    expr[SortOrder](
      "Sort order",
      ExprChecks.projectOnly(
        pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
            .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
        TypeSig.orderable,
        Seq(ParamCheck(
          "input",
          pluginSupportedOrderableSig + TypeSig.ARRAY.nested(gpuCommonTypes)
             .withPsNote(TypeEnum.ARRAY, "STRUCT is not supported as a child type for ARRAY"),
          TypeSig.orderable))),
      GpuSortOrderMeta),
    expr[PivotFirst](
      "PivotFirst operator",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128),
        TypeSig.all,
        Seq(ParamCheck(
          "pivotColumn",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
          TypeSig.all),
          ParamCheck("valueColumn",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.all))),
      PivotFirstRuleMeta),
    expr[Count](
      "Count aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG, TypeSig.LONG,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", TypeSig.all, TypeSig.all))),
      CountRuleMeta),
    expr[Max](
      "Max aggregate operator",
      ExprChecksImpl(
        ExprChecks.reductionAndGroupByAgg(
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
            TypeSig.ARRAY).nested(),
          TypeSig.orderable,
          Seq(ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
            TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.orderable,
            Seq(ParamCheck("input",
              (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
              TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts),
      MaxRuleMeta),
    expr[Min](
      "Min aggregate operator",
      ExprChecksImpl(
        ExprChecks.reductionAndGroupByAgg(
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
          TypeSig.orderable,
          Seq(ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.STRUCT +
              TypeSig.ARRAY).nested(),
            TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.orderable,
            Seq(ParamCheck("input",
              (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
              TypeSig.orderable))).asInstanceOf[ExprChecksImpl].contexts),
      MinRuleMeta),
    expr[Sum](
      "Sum aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input", TypeSig.gpuNumeric, TypeSig.cpuNumeric))),
      SumRuleMeta),
    expr[NthValue](
      "nth window operator",
      ExprChecks.windowOnly(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all),
          ParamCheck("offset", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT)))
      ),
      NthValueRuleMeta),
    expr[First](
      "first aggregate operator",
      ExprChecks.fullAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all))
      ),
      FirstRuleMeta),
    expr[Last](
    "last aggregate operator",
      ExprChecks.fullAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(ParamCheck("input",
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all))
      ),
      LastRuleMeta),
    expr[MaxBy](
      "MaxBy aggregate operator. It may produce different results than CPU when " +
        "multiple rows in a group have same minimum value in the ordering column and " +
        "different associated values in the value column.",
      ExprChecks.reductionAndGroupByAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("value", (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY
            + TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all),
          ParamCheck("ordering", (TypeSig.commonCudfTypes - TypeSig.fp + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested(
              TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY),
            TypeSig.orderable))
      ),
      MaxByRuleMeta),
    expr[MinBy](
      "MinBy aggregate operator. It may produce different results than CPU when " +
        "multiple rows in a group have same minimum value in the ordering column and " +
        "different associated values in the value column.",
      ExprChecks.reductionAndGroupByAgg(
        (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY +
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("value", (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY
            + TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all),
          ParamCheck("ordering", (TypeSig.commonCudfTypes - TypeSig.fp + TypeSig.DECIMAL_128 +
            TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY).nested(
              TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.STRUCT + TypeSig.ARRAY),
            TypeSig.orderable))
      ),
      MinByRuleMeta),
    expr[BRound](
      "Round an expression to d decimal places using HALF_EVEN rounding mode",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      BRoundRuleMeta),
    expr[Round](
      "Round an expression to d decimal places using HALF_UP rounding mode",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      RoundRuleMeta),
  )
}
