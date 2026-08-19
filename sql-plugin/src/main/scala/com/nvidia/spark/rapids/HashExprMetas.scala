/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.XxHash64Shims

import org.apache.spark.sql.catalyst.expressions.HashExpression
import org.apache.spark.sql.rapids.{GpuMurmur3Hash, GpuXxHash64, XxHash64Utils}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, StructType}

/** Base meta for Murmur3-hash-like expressions. */
case class Murmur3HashExprMeta[HEINT <: HashExpression[Int]](
    expr: HEINT,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta(expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    val hasArrayOfStruct = expr.children.exists { e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType, {
        case ArrayType(_: StructType, _) => true
        case _ => false
      })
    }
    if (hasArrayOfStruct) {
      willNotWorkOnGpu("hashing arrays with structs is not supported")
    }
  }
  override def convertToGpuImpl(): GpuExpression =
    GpuMurmur3Hash(childExprs.map(_.convertToGpu()), expr.seed)
}

/** Base meta for xxhash64-like expressions. */
case class XxHash64ExprMeta[HE <: HashExpression[Long]](
    expr: HE,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta(expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    val maxDepth = expr.children.map(c => XxHash64Utils.computeMaxStackSize(c.dataType)).max
    if (maxDepth > XxHash64Utils.MAX_STACK_DEPTH) {
      willNotWorkOnGpu(
        s"The data type requires a stack depth of $maxDepth, " +
          s"which exceeds the GPU limit of ${XxHash64Utils.MAX_STACK_DEPTH}. " +
          "The algorithm to calculate stack depth: " +
          "1: Primitive type counts 1 depth; " +
          "2: Array of Structure counts:  1  + depthOf(Structure); " +
          "3: Array of Other counts: depthOf(Other); " +
          "4: Structure counts: 1 + max of depthOf(child); " +
          "5: Map counts: 2 + max(depthOf(key), depthOf(value)); ")
    }
  }

  override def convertToGpuImpl(): GpuExpression =
    GpuXxHash64(childExprs.map(_.convertToGpu()), expr.seed)
}


/** Shared ExprChecks for hash expressions */
object HashExprChecks {
  val murmur3InputTypes: TypeSig =
    (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
        TypeSig.STRUCT + TypeSig.ARRAY).nested() +
      TypeSig.psNote(TypeEnum.ARRAY, "Arrays of structs are not supported")

  val murmur3ProjectChecks: ExprChecks = ExprChecks.projectOnly(
    TypeSig.INT, TypeSig.INT,
    repeatingParamCheck = Some(RepeatingParamCheck(
      "input",
      murmur3InputTypes,
      TypeSig.all)))

  val xxhash64ProjectChecks: ExprChecks = ExprChecks.projectOnly(
    TypeSig.LONG, TypeSig.LONG,
    repeatingParamCheck = Some(RepeatingParamCheck(
      "input",
      XxHash64Shims.supportedTypes,
      TypeSig.all)))
}
