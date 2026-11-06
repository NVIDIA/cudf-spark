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

import com.nvidia.spark.rapids.shims.ShimSparkPlan

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.catalyst.plans.physical.{IdentityBroadcastMode, SinglePartition}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.rapids.execution.GpuBroadcastExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

class SparkPlanMetaSuite extends SparkQueryCompareTestSuite {
  test("AQE exchange conversion preserves all tags") {
    val child = RangeExec(Range(0L, 10L, 1L, Some(1)))
    val exchanges = Seq(
      BroadcastExchangeExec(IdentityBroadcastMode, child),
      ShuffleExchangeExec(SinglePartition, child))
    val opaqueTag = TreeNodeTag[String]("test.opaque.exchange.tag")
    val logical = LocalRelation()
    val conf = new RapidsConf(Map.empty[String, String])

    exchanges.foreach { cpu =>
      cpu.setTagValue(opaqueTag, "carried-value")
      cpu.setTagValue(GpuTransitionOverrides.aqeQueryStageExchange, true)
      cpu.setLogicalLink(logical)
      val meta = GpuOverrides.wrapPlan(cpu, conf, None)
      meta.tagForGpu()
      assert(meta.canThisBeReplaced)
      val converted = meta.convertIfNeeded()

      cpu match {
        case _: BroadcastExchangeExec => assert(converted.isInstanceOf[GpuBroadcastExchangeExec])
        case _: ShuffleExchangeExec => assert(converted.isInstanceOf[GpuShuffleExchangeExec])
      }
      assert(converted.getTagValue(opaqueTag).contains("carried-value"))
      assert(converted.getTagValue(GpuTransitionOverrides.aqeQueryStageExchange).contains(true))
      assert(converted.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq logical))
      assert(converted.children.head.getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
        .exists(_ eq logical))
    }
  }

  test("GPU conversion preserves opaque SparkPlan tags and direct logical plan tag") {
    case class CpuExec() extends ShimSparkPlan {
      override protected def doExecute(): RDD[InternalRow] =
        throw new IllegalStateException("should not be called")
      override def output: Seq[Attribute] = Nil
      override def children: Seq[SparkPlan] = Nil
    }
    case class TestGpuExec() extends ShimSparkPlan with GpuExec {
      override protected def doExecute(): RDD[InternalRow] =
        throw new IllegalStateException("should not be called")
      override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
        throw new IllegalStateException("should not be called")
      override def output: Seq[Attribute] = Nil
      override def children: Seq[SparkPlan] = Nil
    }
    class TestMeta(plan: CpuExec, conf: RapidsConf, gpu: TestGpuExec)
        extends SparkPlanMeta[CpuExec](
          plan, conf, None, new NoRuleDataFromReplacementRule) {
      override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil
      override val childExprs: Seq[BaseExprMeta[_]] = Nil
      override val childScans: Seq[ScanMeta[_]] = Nil
      override val childParts: Seq[PartMeta[_]] = Nil
      override def convertToGpu(): GpuExec = gpu
    }
    val opaqueTag = TreeNodeTag[String]("test.opaque.spark.plan.tag")
    val logical = LocalRelation()
    val cpu = CpuExec()
    cpu.setTagValue(opaqueTag, "carried-value")
    cpu.setLogicalLink(logical)
    val gpu = TestGpuExec()
    val meta = new TestMeta(cpu, new RapidsConf(Map.empty[String, String]), gpu)

    meta.tagForGpu()
    assert(meta.canThisBeReplaced)
    val converted = meta.convertIfNeeded()

    assert(converted eq gpu)
    assert(converted.getTagValue(opaqueTag).contains("carried-value"))
    assert(converted.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq logical))

    // A conversion can return an existing child. Its tags must retain their original meaning.
    val sourceOnlyTag = TreeNodeTag[String]("test.source.only.tag")
    cpu.setTagValue(sourceOnlyTag, "source-only-value")
    val existingGpu = TestGpuExec()
    val existingLogical = LocalRelation()
    existingGpu.setTagValue(opaqueTag, "existing-value")
    existingGpu.setLogicalLink(existingLogical)
    val existingMeta = new TestMeta(cpu, new RapidsConf(Map.empty[String, String]), existingGpu)
    existingMeta.tagForGpu()
    val existingConverted = existingMeta.convertIfNeeded()
    assert(existingConverted eq existingGpu)
    assert(existingConverted.getTagValue(opaqueTag).contains("existing-value"))
    assert(existingConverted.getTagValue(sourceOnlyTag).isEmpty)
    assert(existingConverted.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).exists(_ eq existingLogical))
  }
}
