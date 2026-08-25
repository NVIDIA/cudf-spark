/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.jni.BloomFilter

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.rapids.aggregate.{CpuToGpuAggregateBufferConverter,
  CpuToGpuBloomFilterBufferConverter, GpuBloomFilterAggregate,
  GpuToCpuAggregateBufferConverter, GpuToCpuBloomFilterBufferConverter}

object BloomFilterShims {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[BloomFilterMightContain](
        "Bloom filter query",
        ExprChecks.binaryProject(
          TypeSig.BOOLEAN,
          TypeSig.BOOLEAN,
          ("lhs", TypeSig.BINARY + TypeSig.NULL, TypeSig.BINARY + TypeSig.NULL),
          ("rhs", TypeSig.LONG + TypeSig.NULL, TypeSig.LONG + TypeSig.NULL)),
        (a, conf, p, r) => new BinaryExprMeta[BloomFilterMightContain](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuBloomFilterMightContain(lhs, rhs)
        }),
      GpuOverrides.expr[BloomFilterAggregate](
        "Bloom filter build",
        ExprChecksImpl(Map(
          (ReductionAggExprContext,
            ContextChecks(TypeSig.BINARY, TypeSig.BINARY,
              Seq(ParamCheck("child", TypeSig.LONG, TypeSig.LONG),
                ParamCheck("estimatedItems",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG)),
                ParamCheck("numBits",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG))))))),
        (a, conf, p, r) => new TypedImperativeAggExprMeta[BloomFilterAggregate](a, conf, p, r) {
          private lazy val estimatedNumItems =
            GpuBloomFilterAggregate.clampEstimatedNumItems(
              a.estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue)

          private lazy val numBits =
            GpuBloomFilterAggregate.clampNumBits(
              a.numBitsExpression.eval().asInstanceOf[Number].longValue)

          override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
            GpuBloomFilterAggregate(
              childExprs.head,
              a.estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
              a.numBitsExpression.eval().asInstanceOf[Number].longValue,
              BloomFilterConstantsShims.BLOOM_FILTER_FORMAT_VERSION,
              BloomFilter.DEFAULT_SEED)
          }

          override def aggBufferAttribute: AttributeReference = {
            val aggBuffer = a.aggBufferAttributes.head
            aggBuffer.copy(dataType = a.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
          }

          // This is a defensive correctness fix for the rare mixed CPU/GPU bridge path.
          // BloomFilterAggregate crosses the CPU/GPU boundary as BinaryType in both directions,
          // but empty GPU partial buffers can be null while Spark CPU final expects a serialized
          // empty bloom filter. We still need a converter even though the runtime type
          // is unchanged.
          override def createCpuToGpuBufferConverter(): CpuToGpuAggregateBufferConverter =
            CpuToGpuBloomFilterBufferConverter()

          override def createGpuToCpuBufferConverter(): GpuToCpuAggregateBufferConverter =
            GpuToCpuBloomFilterBufferConverter(estimatedNumItems, numBits)

          override val supportBufferConversion: Boolean = true
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
