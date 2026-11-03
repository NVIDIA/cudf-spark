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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.aggregate.GpuHyperLogLogPlusPlus
import org.apache.spark.sql.rapids.shims.GpuAscii

case class ScalarSubqueryRuleMeta(
    a: org.apache.spark.sql.execution.ScalarSubquery,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[org.apache.spark.sql.execution.ScalarSubquery](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = GpuScalarSubquery(a.plan, a.exprId)
}

case class CreateMapRuleMeta(
    a: CreateMap,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ExprMeta[CreateMap](a, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression =
    GpuCreateMap(childExprs.map(_.convertToGpu()))
}

case class BitLengthRuleMeta(
    a: BitLength,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[BitLength](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuBitLength(child)
}

case class OctetLengthRuleMeta(
    a: OctetLength,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[OctetLength](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuOctetLength(child)
}

case class AsciiRuleMeta(
    a: Ascii,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[Ascii](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = GpuAscii(child)
}

case class DynamicPruningExpressionRuleMeta(
    a: DynamicPruningExpression,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[DynamicPruningExpression](a, conf, p, r) {
  override def convertToGpu(child: Expression): GpuExpression = {
    GpuDynamicPruningExpression(child)
  }
}

case class HyperLogLogPlusPlusRuleMeta(
    a: HyperLogLogPlusPlus,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends UnaryExprMeta[HyperLogLogPlusPlus](a, conf, p, r) {
  // It's the same as Xxhash64
  override def tagExprForGpu(): Unit = {
    val maxDepth = a.children.map(
      c => XxHash64Utils.computeMaxStackSize(c.dataType)).max
    if (maxDepth > XxHash64Utils.MAX_STACK_DEPTH) {
      willNotWorkOnGpu(s"The data type requires a stack depth of $maxDepth, " +
          s"which exceeds the GPU limit of ${XxHash64Utils.MAX_STACK_DEPTH}. " +
          "The algorithm to calculate stack depth: " +
          "1: Primitive type counts 1 depth; " +
          "2: Array of Structure counts:  1  + depthOf(Structure); " +
          "3: Array of Other counts: depthOf(Other); " +
          "4: Structure counts: 1 + max of depthOf(child); " +
          "5: Map counts: 2 + max(depthOf(key), depthOf(value)); "
      )
    }
    val precision = GpuHyperLogLogPlusPlus.computePrecision(a.relativeSD)
    // Spark supports precision range: [4, Infinity)
    // cuCollection supports precision range: [4, 18]
    // Spark-Rapids only supports precision range: [4, 14],
    // GPU does not perform well for precision > 14.
    if (precision < 4 || precision > 14) {
      willNotWorkOnGpu(s"The precision $precision from relativeSD ${a.relativeSD} is out of" +
        s" range, GPU only supports precision range [4, 14].")
    }
  }

  override def convertToGpu(child: Expression): GpuExpression = {
    GpuHyperLogLogPlusPlus(child, a.relativeSD)
  }
}


case class GetJsonObjectConstructorRuleMeta(
    a: GetJsonObject,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuGetJsonObjectMeta(a, conf, parent, r)

case class StructsToJsonConstructorRuleMeta(
    a: StructsToJson,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuStructsToJsonMeta(a, conf, parent, r)

case class SequenceConstructorRuleMeta(
    a: Sequence,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuSequenceMeta(a, conf, parent, r)

case class GetArrayStructFieldsConstructorRuleMeta(
    e: GetArrayStructFields,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuGetArrayStructFieldsMeta(e, conf, parent, r)

case class UuidConstructorRuleMeta(
    a: Uuid,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends GpuUuidMeta(a, conf, parent, r)

case class StaticInvokeConstructorRuleMeta(
    a: StaticInvoke,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends StaticInvokeMeta(a, conf, parent, r)
