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
import com.nvidia.spark.rapids.shims.GpuTypeShims

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.rapids._

private[rapids] object GpuCollectionExpressionOverrides {
  val rules: Seq[ExprRule[_ <: Expression]] = Seq(
    expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
      ExprChecks.fullAggAndProject(
        // Different types of Pandas UDF support different sets of output type. Please refer to
        //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
        // for more details.
        // It is impossible to specify the exact type signature for each Pandas UDF type in a single
        // expression 'PythonUDF'.
        // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
        // plugin is also an union of all the types of Pandas UDF.
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
        TypeSig.unionOfPandasUdfOut,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "param",
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      PythonUDFRuleMeta),
    GpuScalaUDFMeta.exprMeta,
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      ExprChecks.projectOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("seed",
          (TypeSig.INT + TypeSig.LONG).withAllLit(),
          (TypeSig.INT + TypeSig.LONG).withAllLit()))),
      RandRuleMeta),
    expr[SparkPartitionID] (
      "Returns the current partition id",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT),
      SparkPartitionIDRuleMeta),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      MonotonicallyIncreasingIDRuleMeta),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING),
      InputFileNameRuleMeta),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      InputFileBlockStartRuleMeta),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      InputFileBlockLengthRuleMeta),
    expr[Md5] (
      "MD5 hash operator",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      Md5RuleMeta),
    expr[Sha1] (
      "Sha1 hash operator",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      Sha1RuleMeta),
    expr[Sha2] (
      "Sha2 hash operator",
      ExprChecks.binaryProject(
        TypeSig.STRING,
        TypeSig.STRING,
        ("input", TypeSig.BINARY, TypeSig.BINARY),
        ("bitLength", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      GpuSha2.Meta
    ),
    expr[Upper](
      "String uppercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      UpperRuleMeta)
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      LowerRuleMeta)
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLPad](
      "Pad a string on the left",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringLPadRuleMeta),
    expr[StringRPad](
      "Pad a string on the right",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      StringRPadRuleMeta),
    expr[StringSplit](
       "Splits `str` around occurrences that match `regex`",
      // Java's split API produces different behaviors than cudf when splitting with empty pattern
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "very limited subset of regex supported"),
            TypeSig.STRING),
          ParamCheck("limit", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      StringSplitConstructorRuleMeta),
    expr[GetStructField](
      "Gets the named field of the struct",
      ExprChecks.unaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalVariantSupportedTypes).nested(),
        TypeSig.all,
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalVariantSupportedTypes),
        TypeSig.STRUCT.nested(TypeSig.all)),
      GetStructFieldRuleMeta),
    expr[GetArrayItem](
      "Gets the field at `ordinal` in the Array",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ordinal", TypeSig.integral, TypeSig.integral)),
      GetArrayItemRuleMeta),
    expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
          TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all)),
      GetMapValueRuleMeta),
    GpuElementAtMeta.elementAtRule(false),
    expr[MapKeys](
      "Returns an unordered array containing the keys of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY).nested(),
        TypeSig.ARRAY.nested(TypeSig.all - TypeSig.MAP), // Maps cannot have other maps as keys
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      MapKeysRuleMeta),
    expr[MapValues](
      "Returns an unordered array containing the values of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      MapValuesRuleMeta),
    expr[MapEntries](
      "Returns an unordered array of all entries in the given map",
      ExprChecks.unaryProject(
        // Technically the return type is an array of struct, but we cannot really express that
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all)),
      MapEntriesRuleMeta),
    expr[MapFromEntries](
      "Creates a map from an array of entries (structs of key-value pairs)",
      ExprChecks.unaryProject(
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.MAP.nested(TypeSig.all),
        // Input is an array of struct<key, value>
        // The struct fields can contain the supported types
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all)),
      MapFromEntriesRuleMeta),
    expr[StringToMap](
      "Creates a map after splitting the input string into pairs of key-value strings",
      // Java's split API produces different behaviors than cudf when splitting with empty pattern
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("pairDelim", TypeSig.lit(TypeEnum.STRING), TypeSig.lit(TypeEnum.STRING)),
          ParamCheck("keyValueDelim", TypeSig.lit(TypeEnum.STRING), TypeSig.lit(TypeEnum.STRING)))),
      StringToMapConstructorRuleMeta),
    expr[ArrayMin](
      "Returns the minimum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      ArrayMinRuleMeta),
    expr[ArrayMax](
      "Returns the maximum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      ArrayMaxRuleMeta),
    expr[ArrayRepeat](
      "Returns the array containing the given input value (left) count (right) times",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
          + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("left", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
          + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
        ("right", TypeSig.integral, TypeSig.integral)),
      ArrayRepeatRuleMeta
    ),
    expr[CreateNamedStruct](
      "Creates a struct with the given field names and values",
      CreateNamedStructCheck,
      CreateNamedStructRuleMeta),
    expr[ArrayContains](
      "Returns a boolean if the array contains the passed in key",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN,
        TypeSig.BOOLEAN,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL),
          TypeSig.ARRAY.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes, TypeSig.all)),
      ArrayContainsRuleMeta),
    expr[ArrayPosition](
      "Returns the (1-based) index of the first matching element of the array as long, " +
        "or 0 if no match is found.",
      ExprChecks.binaryProject(
        TypeSig.LONG,
        TypeSig.LONG,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL
            + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY),
          TypeSig.ARRAY.nested(TypeSig.orderable)),
        ("key", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY).nested(),
          TypeSig.orderable)),
      ArrayPositionConstructorRuleMeta),
    expr[SortArray](
      "Returns a sorted array with the input array and the ascending / descending order",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array", TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ascendingOrder", TypeSig.lit(TypeEnum.BOOLEAN), TypeSig.lit(TypeEnum.BOOLEAN))),
      SortArrayRuleMeta
    ),
    expr[ArraySort](
      "Sorts the input array in ascending order with nulls last according to the natural " +
          "ordering of the elements (the default comparator). A custom comparator, or a nested " +
          "element type (array or struct), falls back to the CPU",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("array",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
            TypeSig.ARRAY.nested(TypeSig.all)))),
      ArraySortRuleMeta),
    expr[CreateArray](
      "Returns an array with the given elements",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.gpuNumeric +
          TypeSig.NULL + TypeSig.STRING + TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("arg",
          TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
              TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT + TypeSig.BINARY +
              TypeSig.ARRAY.nested(TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
                TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT +
                  TypeSig.ARRAY + TypeSig.BINARY),
          TypeSig.all))),
      CreateArrayRuleMeta),
    expr[ArrayDistinct](
      "Removes duplicate values from the array",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      GpuArrayDistinctMeta),
    expr[Flatten](
      "Creates a single array from an array of arrays",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(TypeSig.all)),
      FlattenRuleMeta),
    expr[LambdaFunction](
      "Holds a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("function",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all)),
        Some(RepeatingParamCheck("arguments",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      LambdaFunctionRuleMeta),
    expr[NamedLambdaVariable](
      "A parameter to a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      NamedLambdaVariableRuleMeta),
    expr[ArrayTransform](
      "Transform elements in an array using the transform function. This is similar to a `map` " +
          "in functional programming",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT +
        TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      ArrayTransformRuleMeta),
     expr[ArrayExists](
      "Return true if any element satisfies the predicate LambdaFunction",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      ArrayExistsRuleMeta),
    expr[ArrayFilter](
      "Filter an input array using a given predicate",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT +
        TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      ArrayFilterRuleMeta),
    expr[ArrayAggregate](
      "Aggregate elements in an array using an accumulator function and finishing " +
          "transformation. Currently only lambdas of the form (acc, x) -> op(acc, g(x)) with " +
          "an identity finish are executed on the GPU, where op is one of SUM/PRODUCT/MAX/" +
          "MIN/ALL/ANY. If/CaseWhen branches are accepted as long as each branch is itself " +
          "op-of-acc (or bare acc) with op consistent across branches; other shapes fall " +
          "back to CPU. SUM/PRODUCT on float/double share the same parallel-reduction " +
          "non-determinism as GpuSum and are gated by " +
          "spark.rapids.sql.variableFloatAgg.enabled. SUM/PRODUCT on integer/decimal in " +
          "ANSI mode fall back to CPU because cuDF segmented reduce wraps on overflow " +
          "instead of raising.",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
        TypeSig.all,
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("zero",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all),
          ParamCheck("merge",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all),
          ParamCheck("finish",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
            TypeSig.all))),
      ArrayAggregateConstructorRuleMeta),
    // TODO: fix the signature https://github.com/NVIDIA/spark-rapids/issues/5327
    expr[ArraysZip](
      "Returns a merged array of structs in which the N-th struct contains" +
        " all N-th values of input arrays.",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("children",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.ARRAY.nested(TypeSig.all)))),
      ArraysZipRuleMeta
    ),
    expr[ArrayExcept](
      "Returns an array of the elements in array1 but not in array2, without duplicates",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      ArrayExceptRuleMeta
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayIntersect](
      "Returns an array of the elements in the intersection of array1 and array2, without" +
        " duplicates",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      ArrayIntersectRuleMeta
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayUnion](
      "Returns an array of the elements in the union of array1 and array2, without duplicates.",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      ArrayUnionRuleMeta
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArraysOverlap](
      "Returns true if a1 contains at least a non-null element present also in a2. If the arrays " +
      "have no common element and they are both non-empty and either of them contains a null " +
      "element null is returned, false otherwise.",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("array1",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("array2",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL),
            TypeSig.ARRAY.nested(TypeSig.all))),
      ArraysOverlapRuleMeta
    ).incompat("the GPU implementation treats -0.0 and 0.0 as equal, but the CPU " +
        "implementation currently does not (see SPARK-39845). Also, Apache Spark " +
        "3.1.3 fixed issue SPARK-36741 where NaNs in these set like operators were " +
        "not treated as being equal. We have chosen to break with compatibility for " +
        "the older versions of Spark in this instance and handle NaNs the same as 3.1.3+"),
    expr[ArrayRemove](
      "Returns the array after removing all elements that equal to the input element (right) " +
      "from the input array (left)",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.STRUCT),
          TypeSig.all),
        ("element",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.STRUCT).nested(),
          TypeSig.all)),
      ArrayRemoveRuleMeta
    ),
    expr[MapFromArrays](
      "Creates a new map from two arrays",
      ExprChecks.binaryProject(
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        // Map values may themselves be maps. Map keys remain constrained by the key-array check.
        TypeSig.MAP.nested(TypeSig.all),
        ("keys",
          TypeSig.ARRAY.nested(
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT),
          TypeSig.ARRAY.nested(TypeSig.all - TypeSig.MAP)),
        ("values",
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.ARRAY.nested(TypeSig.all))),
      GpuMapFromArraysMeta
    ),
    expr[TransformKeys](
      "Transform keys in a map using a transform function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            // We need to be able to check for duplicate keys (equality)
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all - TypeSig.MAP.nested()))),
      TransformKeysRuleMeta),
    expr[TransformValues](
      "Transform values in a map using a transform function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      TransformValuesRuleMeta),
    expr[MapZipWith](
      "Filters entries in a map using the function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument1",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("argument2",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      MapZipWithRuleMeta),
    expr[MapFilter](
      "Filters entries in a map using the function",
      ExprChecks.projectOnly(TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.NULL + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.MAP.nested(TypeSig.all)),
          ParamCheck("function", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      MapFilterRuleMeta),
  )
}
