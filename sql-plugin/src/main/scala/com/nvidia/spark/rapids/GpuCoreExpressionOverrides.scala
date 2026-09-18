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
import com.nvidia.spark.rapids.window._

import org.apache.spark.sql.catalyst.expressions._

private[rapids] object GpuCoreExpressionOverrides {
  val rules: Seq[ExprRule[_ <: Expression]] = Seq(
    expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.CALENDAR
          + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT + TypeSig.ansiIntervals)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      LiteralConstructorRuleMeta),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      ExprChecks.mathUnary,
      SignumRuleMeta),
    expr[Alias](
      "Gives a column a name",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes + GpuTypeShims.additionalCommonOperatorSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT
            + TypeSig.DECIMAL_128 + TypeSig.BINARY
            + GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      AliasRuleMeta),
    expr[BoundReference](
      "Reference to a bound variable",
      ExprChecks.projectAndAst(
        TypeSig.astTypes + GpuTypeShims.additionalCommonOperatorSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT +
          TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      BoundReferenceRuleMeta),
    expr[AttributeReference](
      "References an input column",
      ExprChecks.projectAndAst(
        TypeSig.astTypes + GpuTypeShims.additionalCommonOperatorSupportedTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
        AttributeReferenceRuleMeta),

    expr[ToDegrees](
      "Converts radians to degrees",
      ExprChecks.mathUnary,
      ToDegreesRuleMeta),
    expr[ToRadians](
      "Converts degrees to radians",
      ExprChecks.mathUnary,
      ToRadiansRuleMeta),
    expr[Bin](
      "Returns the string representation of the long value `expr` represented in binary",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING, TypeSig.LONG, TypeSig.LONG),
      BinRuleMeta),
    expr[Hex](
      "Returns the hex string representation of a value",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.LONG + TypeSig.STRING + TypeSig.BINARY,
        TypeSig.LONG + TypeSig.STRING + TypeSig.BINARY),
      HexRuleMeta),
    expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        TypeSig.all,
        TypeSig.all,
        Seq(ParamCheck("windowFunction", TypeSig.all, TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64,
            TypeSig.numericAndInterval))),
      WindowExpressionConstructorRuleMeta),
    expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_128 +
              TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_128 +
              TypeSig.FLOAT + TypeSig.DOUBLE,
            TypeSig.numericAndInterval))),
      SpecifiedWindowFrameConstructorRuleMeta ),
    expr[WindowSpecDefinition](
      "Specification of a window function, indicating the partitioning-expression, the row " +
        "ordering, and the width of the window",
      WindowSpecCheck,
      WindowSpecDefinitionConstructorRuleMeta),
    expr[CurrentRow.type](
      "Special boundary for a window frame, indicating stopping at the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      CurrentRowRuleMeta),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      UnboundedPrecedingRuleMeta),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      UnboundedFollowingRuleMeta),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      RowNumberRuleMeta),
    expr[Rank](
      "Window function that returns the rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      RankRuleMeta),
    expr[DenseRank](
      "Window function that returns the dense rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      DenseRankRuleMeta),
    expr[PercentRank](
      "Window function that returns the percent rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      PercentRankRuleMeta),
    expr[NTile](
      "Window function that divides the rows in each partition into buckets",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("buckets", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT)))),
      GpuNTileMeta),
    expr[Lead](
      "Window function that returns N entries ahead of this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      LeadRuleMeta),
    expr[Lag](
      "Window function that returns N entries behind this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      LagRuleMeta),
  )
}
