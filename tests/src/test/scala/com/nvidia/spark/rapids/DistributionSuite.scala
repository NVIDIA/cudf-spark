/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
import org.mockito.Mockito._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, BroadcastMode, Distribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.JoinTypeChecks

class DistributionSuite extends SparkQueryCompareTestSuite {

  test("test Custom BroadcastMode") {
    case class CustomBroadcastExec() extends ShimSparkPlan {
      override protected def doExecute(): RDD[InternalRow] =
        throw new IllegalStateException("should not be here")

      override def output: Seq[Attribute] = Nil

      override def children: Seq[SparkPlan] = Nil

      override def requiredChildDistribution: Seq[Distribution] =
        Seq(BroadcastDistribution(customBroadcastMode))

      // custom broadcast mode isn't IdentityBroadcastMode or HashRelationBroadcastMode
      private val customBroadcastMode = mock(classOf[BroadcastMode])
    }

    class CustomExecMeta(
        plan: CustomBroadcastExec,
        conf: RapidsConf,
        parent: Option[RapidsMeta[_, _, _]],
        rule: DataFromReplacementRule)
        extends SparkPlanMeta[CustomBroadcastExec](plan, conf, parent, rule) {
      override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil
      override val childExprs: Seq[BaseExprMeta[_]] = Nil
      override val childScans: Seq[ScanMeta[_]] = Nil
      override val childParts: Seq[PartMeta[_]] = Nil

      override def convertToGpu(): GpuExec =
        throw new IllegalStateException("should not be called")
    }
    val rapidsConf = new RapidsConf(Map[String, String]())
    val execRule = GpuOverrides.exec[CustomBroadcastExec]("Custom BroadcastMode Check",
      JoinTypeChecks.equiJoinExecChecks,
      (join, conf, p, r) => new CustomExecMeta(join, conf, p, r))

    val meta = execRule.wrap(
      new CustomBroadcastExec, rapidsConf, None, new NoRuleDataFromReplacementRule)
    meta.tagForGpu()
    assert(!meta.canThisBeReplaced)
    assert(meta.explain(false).contains(" cannot run on GPU because unsupported " +
      "required distribution: BroadcastDistribution"))
  }
}
