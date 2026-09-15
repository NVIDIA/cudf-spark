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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution.python.GpuPythonUDF
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PythonUDFRuleMeta(
    a: PythonUDF,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[PythonUDF](a, conf, p, r) {
  override def replaceMessage: String = "not block GPU acceleration"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"blocks running on GPU because $reasons"

  override def convertToGpuImpl(): GpuExpression =
    GpuPythonUDF(a.name, a.func, a.dataType,
      childExprs.map(_.convertToGpu()),
      a.evalType, a.udfDeterministic, a.resultId)
}

case class RandRuleMeta(
    a: Rand,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Rand](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuRand(child, this.conf.isRetryContextCheckEnabled)
}

case class SparkPartitionIDRuleMeta(
    a: SparkPartitionID,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[SparkPartitionID](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuSparkPartitionID()
}

case class MonotonicallyIncreasingIDRuleMeta(
    a: MonotonicallyIncreasingID,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuMonotonicallyIncreasingID()
}

case class InputFileNameRuleMeta(
    a: InputFileName,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[InputFileName](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuInputFileName()
}

case class InputFileBlockStartRuleMeta(
    a: InputFileBlockStart,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[InputFileBlockStart](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuInputFileBlockStart()
}

case class InputFileBlockLengthRuleMeta(
    a: InputFileBlockLength,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[InputFileBlockLength](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuInputFileBlockLength()
}

case class Md5RuleMeta(
    a: Md5,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Md5](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuMd5(child)
}

case class Sha1RuleMeta(
    a: Sha1,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Sha1](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuSha1(child)
}

case class UpperRuleMeta(
    a: Upper,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Upper](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuUpper(child)
}

case class LowerRuleMeta(
    a: Lower,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Lower](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuLower(child)
}

case class StringLPadRuleMeta(
    in: StringLPad,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[StringLPad](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    extractLit(in.pad).foreach { padLit =>
      if (padLit.value != null &&
          padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
        willNotWorkOnGpu("only a single character is supported for pad")
      }
    }
  }
  override def convertToGpu(
      str: Expression,
      width: Expression,
      pad: Expression): GpuExpression =
    GpuStringLPad(str, width, pad)
}

case class StringRPadRuleMeta(
    in: StringRPad,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends TernaryExprMeta[StringRPad](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    extractLit(in.pad).foreach { padLit =>
      if (padLit.value != null &&
          padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
        willNotWorkOnGpu("only a single character is supported for pad")
      }
    }
  }
  override def convertToGpu(
      str: Expression,
      width: Expression,
      pad: Expression): GpuExpression =
    GpuStringRPad(str, width, pad)
}

case class GetStructFieldRuleMeta(
    expr: GetStructField,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[GetStructField](expr, conf, p, r) {
  override def convertToGpu(arr: Expression): GpuExpression =
    GpuGetStructField(arr, expr.ordinal, expr.name)
}

case class GetArrayItemRuleMeta(
    in: GetArrayItem,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[GetArrayItem](in, conf, p, r) {
  override def convertToGpu(arr: Expression, ordinal: Expression): GpuExpression =
    GpuGetArrayItem(arr, ordinal, in.failOnError)
}

case class GetMapValueRuleMeta(
    in: GetMapValue,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GetMapValueMeta(in, conf, p, r) {}

case class MapKeysRuleMeta(
    in: MapKeys,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MapKeys](in, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMapKeys(child)
}

case class MapValuesRuleMeta(
    in: MapValues,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MapValues](in, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMapValues(child)
}

case class MapEntriesRuleMeta(
    in: MapEntries,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MapEntries](in, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMapEntries(child)
}

case class MapFromEntriesRuleMeta(
    in: MapFromEntries,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[MapFromEntries](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    // Spark 4.1+ returns an enum value instead of String, so use toString first
    SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase match {
      case "EXCEPTION" | "LAST_WIN" => // Good we can support this
      case other =>
        willNotWorkOnGpu(s"$other is not supported for config setting" +
            s" ${SQLConf.MAP_KEY_DEDUP_POLICY.key}")
    }
  }
  override def convertToGpu(child: Expression): GpuExpression =
    GpuMapFromEntries(child)
}

case class ArrayMinRuleMeta(
    in: ArrayMin,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[ArrayMin](in, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuArrayMin(child)
}

case class ArrayMaxRuleMeta(
    in: ArrayMax,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[ArrayMax](in, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuArrayMax(child)
}

case class ArrayRepeatRuleMeta(
    in: ArrayRepeat,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayRepeat](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuArrayRepeat(lhs, rhs)
}

case class CreateNamedStructRuleMeta(
    in: CreateNamedStruct,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[CreateNamedStruct](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuCreateNamedStruct(childExprs.map(_.convertToGpu()))
}

case class ArrayContainsRuleMeta(
    in: ArrayContains,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayContains](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuArrayContains(lhs, rhs)
}

case class SortArrayRuleMeta(
    sortExpression: SortArray,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[SortArray](sortExpression, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuSortArray(lhs, rhs)
  }
}

case class ArraySortRuleMeta(
    in: ArraySort,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArraySort](in, conf, p, r) {
  // Wrap only the array; listSortRows applies the default comparator natively, so the
  // comparator lambda is excluded from children (neither converted nor type-checked).
  // This must wrap exactly one child to stay 1:1 with the single ParamCheck above, which
  // the framework pairs to childExprs positionally.
  override val childExprs: Seq[BaseExprMeta[_]] =
    Seq(GpuOverrides.wrapExpr(in.arguments.head, this.conf, Some(this)))

  override def tagExprForGpu(): Unit = {
    if (!GpuArraySort.isDefaultComparator(in)) {
      willNotWorkOnGpu("array_sort with a custom comparator function is not supported " +
          "on the GPU; only the default ordering (ascending, nulls last) is supported")
    }
  }
  override def convertToGpuImpl(): GpuExpression =
    GpuArraySort(childExprs.head.convertToGpu())
}

case class CreateArrayRuleMeta(
    in: CreateArray,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[CreateArray](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    wrapped.dataType match {
      case ArrayType(ArrayType(ArrayType(_, _), _), _) =>
        willNotWorkOnGpu("Only support to create array or array of array, Found: " +
          s"${wrapped.dataType}")
      case _ =>
    }
  }

  override def convertToGpuImpl(): GpuExpression =
    GpuCreateArray(childExprs.map(_.convertToGpu()), wrapped.useStringTypeWhenEmpty)
}

case class FlattenRuleMeta(
    a: Flatten,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Flatten](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuFlattenArray(child)
}

case class LambdaFunctionRuleMeta(
    in: LambdaFunction,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[LambdaFunction](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    val func = childExprs.head
    val args = childExprs.tail
    GpuLambdaFunction(func.convertToGpu(),
      args.map(_.convertToGpu().asInstanceOf[NamedExpression]),
      in.hidden)
  }
}

case class NamedLambdaVariableRuleMeta(
    in: NamedLambdaVariable,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[NamedLambdaVariable](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuNamedLambdaVariable(in.name, in.dataType, in.nullable, in.exprId)
  }
}

case class ArrayTransformRuleMeta(
    in: ArrayTransform,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArrayTransform](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuArrayTransform(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
  }
}

case class ArrayExistsRuleMeta(
    in: ArrayExists,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArrayExists](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuArrayExists(
      childExprs.head.convertToGpu(),
      childExprs(1).convertToGpu(),
      SQLConf.get.getConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC)
    )
  }
}

case class ArrayFilterRuleMeta(
    in: ArrayFilter,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArrayFilter](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuArrayFilter(
      childExprs.head.convertToGpu(),
      childExprs(1).convertToGpu()
    )
  }
}

case class ArraysZipRuleMeta(
    in: ArraysZip,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[ArraysZip](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuArraysZip(childExprs.map(_.convertToGpu()))
  }
}

case class ArrayExceptRuleMeta(
    in: ArrayExcept,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayExcept](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuArrayExcept(lhs, rhs)
  }
}

case class ArrayIntersectRuleMeta(
    in: ArrayIntersect,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayIntersect](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuArrayIntersect(lhs, rhs)
  }
}

case class ArrayUnionRuleMeta(
    in: ArrayUnion,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayUnion](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuArrayUnion(lhs, rhs)
  }
}

case class ArraysOverlapRuleMeta(
    in: ArraysOverlap,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArraysOverlap](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    GpuArraysOverlap(lhs, rhs)
  }
}

case class ArrayRemoveRuleMeta(
    in: ArrayRemove,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends BinaryExprMeta[ArrayRemove](in, conf, p, r) {
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
    GpuArrayRemove(lhs, rhs)
}

case class TransformKeysRuleMeta(
    in: TransformKeys,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[TransformKeys](in, conf, p, r) {
  override def tagExprForGpu(): Unit = {
    // Spark 4.1+ returns an enum value instead of String, so use toString first
    SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase match {
      case "EXCEPTION"| "LAST_WIN" => // Good we can support this
      case other =>
        willNotWorkOnGpu(s"$other is not supported for config setting" +
            s" ${SQLConf.MAP_KEY_DEDUP_POLICY.key}")
    }
  }
  override def convertToGpuImpl(): GpuExpression = {
    GpuTransformKeys(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
  }
}

case class TransformValuesRuleMeta(
    in: TransformValues,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[TransformValues](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuTransformValues(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
  }
}

case class MapZipWithRuleMeta(
    in: MapZipWith,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[MapZipWith](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuMapZipWith(childExprs.head.convertToGpu(),
    childExprs(1).convertToGpu(), childExprs(2).convertToGpu())
  }
}

case class MapFilterRuleMeta(
    in: MapFilter,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[MapFilter](in, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    GpuMapFilter(childExprs.head.convertToGpu(), childExprs(1).convertToGpu())
  }
}


case class StringSplitConstructorRuleMeta(
    in: StringSplit,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuStringSplitMeta(in, conf, parent, r)

case class StringToMapConstructorRuleMeta(
    in: StringToMap,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuStringToMapMeta(in, conf, parent, r)

case class ArrayPositionConstructorRuleMeta(
    in: ArrayPosition,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuArrayPositionMeta(in, conf, parent, r)

case class ArrayAggregateConstructorRuleMeta(
    in: ArrayAggregate,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuArrayAggregateMeta(in, conf, parent, r)
