/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.rapids.ExternalSource
import org.apache.spark.sql.sources.CreatableRelationProvider

class SaveIntoDataSourceCommandMeta(
    saveCmd: SaveIntoDataSourceCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[SaveIntoDataSourceCommand](saveCmd, conf, parent, rule)
{
  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  private val providerMeta: CreatableRelationProviderMeta[_] =
    ExternalSource.wrapCreatableRelationProvider(wrapped.dataSource, conf, Some(this))

  override def tagSelfForGpu(): Unit = {
    providerMeta.tagForGpu()
    if (!providerMeta.canThisBeReplaced) {
      willNotWorkOnGpu("the table creation provider cannot be replaced")
    }
  }

  override def convertToGpu(): SaveIntoDataSourceCommand = {
    SaveIntoDataSourceCommand(
      wrapped.query,
      providerMeta.convertToGpu(),
      wrapped.options,
      wrapped.mode)
  }

  override def print(strBuilder: StringBuilder, depth: Int, all: Boolean): Unit = {
    super.print(strBuilder, depth, all)
    providerMeta.print(strBuilder, depth + 1, all)
  }
}

abstract class CreatableRelationProviderMeta[INPUT <: CreatableRelationProvider](
    provider: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RapidsMeta[
        INPUT,
        CreatableRelationProvider,
        GpuCreatableRelationProvider](provider, conf, parent, rule) {
  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty
}

final class RuleNotFoundCreatableRelationProviderMeta[INPUT <: CreatableRelationProvider](
    i: INPUT,
    c: RapidsConf,
    p: Option[RapidsMeta[_, _, _]])
    extends CreatableRelationProviderMeta[INPUT](i, c, p, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpu(): Unit =
    willNotWorkOnGpu(s"GPU does not currently support the create table provider ${i.getClass}")

  override def convertToGpu(): GpuCreatableRelationProvider =
    throw new IllegalStateException("Cannot be converted to GPU")
}

class CreatableRelationProviderRule[INPUT <: CreatableRelationProvider](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => CreatableRelationProviderMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[
        INPUT,
        CreatableRelationProvider,
        CreatableRelationProviderMeta[INPUT]](doWrap, desc, None, tag) {

  override protected val confKeyPart: String = "output"
  override val operationName: String = "Output"
}

/** Trait to mark a GPU version of a CreatableRelationProvider */
trait GpuCreatableRelationProvider extends CreatableRelationProvider
