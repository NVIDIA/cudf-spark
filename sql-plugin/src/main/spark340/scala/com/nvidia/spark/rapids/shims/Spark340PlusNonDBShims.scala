/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.{exec, pluginSupportedOrderableSig}

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.{Empty2Null, Expression, KnownNullable,
  NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.objects.{ExternalMapToCatalyst, InvokeLike}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{GpuWriteFilesMeta, WriteFilesExec}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.rapids.GpuElementAtMeta
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null

trait Spark340PlusNonDBShims extends Spark331PlusNonDBShims {
  override def isExpressionStateful(expr: Expression): Boolean = expr match {
    case _: InvokeLike | _: ExternalMapToCatalyst => true
    case _ => expr.stateful && !isBridgeCloneSafeStatefulExpression(expr)
  }

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimit, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimit, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(
              globalLimit.limit, childPlans.head.convertIfNeeded(), globalLimit.offset)
        }),
    exec[TakeOrderedAndProjectExec](
      "Take the first limit elements after offset as defined by the sortOrder, and do " +
          "projection if needed",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data
      // type. The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
      (takeExec, conf, p, r) =>
        new SparkPlanMeta[TakeOrderedAndProjectExec](takeExec, conf, p, r) {
          val sortOrder: Seq[BaseExprMeta[SortOrder]] =
            takeExec.sortOrder.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
          val projectList: Seq[BaseExprMeta[NamedExpression]] =
            takeExec.projectList.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = sortOrder ++ projectList

          override def convertToGpu(): GpuExec = {
            // To avoid metrics confusion we split a single stage up into multiple parts but only
            // if there are multiple partitions to make it worth doing.
            val so = sortOrder.map(_.convertToGpu().asInstanceOf[SortOrder])
            if (takeExec.child.outputPartitioning.numPartitions == 1) {
              GpuTopN(takeExec.limit, so,
                projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
                childPlans.head.convertIfNeeded(), takeExec.offset)(takeExec.sortOrder)
            } else {
              // We are applying the offset only after the batch has been sorted into a single
              // partition. To further clarify we are doing the following
              // GpuTopN(0, end) -> Shuffle(single partition) -> GpuTopN(offset, end)
              // So the leaf GpuTopN (left most) doesn't take offset into account so all the
              // results can perculate above, where we shuffle into a single partition then
              // we drop the offset number of rows before projecting.
              GpuTopN(
                takeExec.limit,
                so,
                projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
                GpuShuffleExchangeExec(
                  GpuSinglePartitioning,
                  GpuTopN(
                    takeExec.limit,
                    so,
                    takeExec.child.output,
                    childPlans.head.convertIfNeeded())(takeExec.sortOrder),
                  ENSURE_REQUIREMENTS
                )(SinglePartition),
                takeExec.offset)(takeExec.sortOrder)
            }
          }
        }),
    GpuOverrides.exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimit, conf, p, r) => new GpuCollectLimitMeta(collectLimit, conf, p, r) {
        override def convertToGpu(): GpuExec =
          buildCollectLimitGpu(collectLimit.offset)
      }
    ).disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
        "of rows in a batch it could help by limiting the number of rows transferred from " +
        "GPU to CPU"),
    GpuOverrides.exec[WriteFilesExec](
      "v1 write files",
      // WriteFilesExec always has patterns:
      //   InsertIntoHadoopFsRelationCommand(WriteFilesExec) or InsertIntoHiveTable(WriteFilesExec)
      // The parent node of `WriteFilesExec` will check the types, here just let type check pass
      ExecChecks(TypeSig.all, TypeSig.all),
      (write, conf, p, r) => new GpuWriteFilesMeta(write, conf, p, r)
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      // Empty2Null is pulled out of FileFormatWriter by default since Spark 3.4.0,
      // so it is visible in the overriding stage.
      GpuOverrides.expr[Empty2Null](
        "Converts the empty string to null for writing data",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.STRING, TypeSig.STRING),
        (a, conf, p, r) => new UnaryExprMeta[Empty2Null](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuEmpty2Null(child)
        }
      ),
      GpuOverrides.expr[KnownNullable](
        "Tags the expression as being nullable",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.all, TypeSig.all),
        (a, conf, p, r) => new UnaryExprMeta[KnownNullable](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuKnownNullable(child)
        }
      ),
      GpuElementAtMeta.elementAtRule(true)
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }

  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Map.empty
  }

  override def hasGpuWriteFiles: Boolean = true

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[CreateDataSourceTableAsSelectCommand](
        "Write to a data source",
        (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }
}
