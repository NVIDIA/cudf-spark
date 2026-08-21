/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims.GpuTypeShims

import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExec, OverwriteByExpressionExecV1, OverwritePartitionsDynamicExec}
import org.apache.spark.sql.rapids.ExternalSource

class AtomicCreateTableAsSelectExecMeta(
    wrapped: AtomicCreateTableAsSelectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AtomicCreateTableAsSelectExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class AtomicReplaceTableAsSelectExecMeta(
    wrapped: AtomicReplaceTableAsSelectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AtomicReplaceTableAsSelectExec](wrapped, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

trait HasCustomTaggingData {
  private var customData: Option[Object] = None

  def setCustomTaggingData(data: Object): Unit = {
    assert(customData.isEmpty, "custom tagging data already exists")
    customData = Some(data)
  }

  def getCustomTaggingData: Option[Object] = customData
}

sealed trait V2WriteCommand {
  def execName: String
}

object V2WriteCommand {
  // These CPU commands are shared by writer implementations such as noop and Iceberg. Other
  // Iceberg write commands have provider-specific CPU plan classes and do not need recognition.
  case object Append extends V2WriteCommand {
    override val execName: String = "AppendDataExec"
  }

  case object OverwriteByExpression extends V2WriteCommand {
    override val execName: String = "OverwriteByExpressionExec"
  }

  val all: Seq[V2WriteCommand] = Seq(Append, OverwriteByExpression)
}

sealed trait V2WriteRecognizerId

object V2WriteRecognizerId {
  case object Noop extends V2WriteRecognizerId
  case object Iceberg extends V2WriteRecognizerId
}

case class V2WriteRecognizerSupport(
    id: V2WriteRecognizerId,
    name: String,
    typeChecks: Map[V2WriteCommand, ExecChecks]) {
  def supports(command: V2WriteCommand): Boolean = typeChecks.contains(command)

  def checksFor(command: V2WriteCommand): ExecChecks = typeChecks.getOrElse(command,
    throw new IllegalArgumentException(s"$name does not support ${command.execName}"))
}

object V2WriteCommandRecognizers {
  // These checks cover the command's GPU-columnar input. Source-specific checks, such as
  // Iceberg file-format checks, are applied separately by the recognizer.
  private val gpuColumnarTypes = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
    TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
    GpuTypeShims.additionalCommonOperatorSupportedTypes).nested()

  private def checksForAllCommands(checks: ExecChecks): Map[V2WriteCommand, ExecChecks] =
    V2WriteCommand.all.map(_ -> checks).toMap

  val noop = V2WriteRecognizerSupport(
    V2WriteRecognizerId.Noop,
    "Spark noop write",
    checksForAllCommands(ExecChecks(gpuColumnarTypes + TypeSig.NULL, TypeSig.all)))
  val iceberg = V2WriteRecognizerSupport(
    V2WriteRecognizerId.Iceberg,
    "Apache Iceberg write",
    checksForAllCommands(ExecChecks(gpuColumnarTypes, TypeSig.all)))
  val all: Seq[V2WriteRecognizerSupport] = Seq(noop, iceberg)
}

trait HasV2WriteRecognizer {
  // Tagging owns recognizer selection. Conversion must reuse that exact decision so changes in
  // provider availability or overlapping recognizers cannot select a different implementation.
  private var selectedRecognizer: Option[V2WriteRecognizerSupport] = None

  def setV2WriteRecognizer(recognizer: V2WriteRecognizerSupport): Unit = {
    assert(selectedRecognizer.isEmpty, "V2 write recognizer already selected")
    selectedRecognizer = Some(recognizer)
  }

  def getV2WriteRecognizer: Option[V2WriteRecognizerSupport] = selectedRecognizer
}

class AppendDataExecV1Meta(
    wrapped: AppendDataExecV1,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AppendDataExecV1](wrapped, conf, parent, rule) with HasCustomTaggingData {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class OverwriteByExpressionExecV1Meta(
    wrapped: OverwriteByExpressionExecV1,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[OverwriteByExpressionExecV1](wrapped, conf, parent, rule)
  with HasCustomTaggingData {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class AppendDataExecMeta(
  wrapped: AppendDataExec,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule)
  extends SparkPlanMeta[AppendDataExec](wrapped, conf, parent, rule) with HasV2WriteRecognizer {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class OverwritePartitionsDynamicExecMeta(
    wrapped: OverwritePartitionsDynamicExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[OverwritePartitionsDynamicExec](wrapped, conf, parent, rule)
  with HasCustomTaggingData {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

class OverwriteByExpressionExecMeta(
                                     wrapped: OverwriteByExpressionExec,
                                     conf: RapidsConf,
                                     parent: Option[RapidsMeta[_, _, _]],
                                     rule: DataFromReplacementRule)
  extends SparkPlanMeta[OverwriteByExpressionExec](wrapped, conf, parent, rule)
  with HasV2WriteRecognizer {

  override def tagPlanForGpu(): Unit = {
    ExternalSource.tagForGpu(wrapped, this)
  }

  override def convertToGpu(): GpuExec = {
    ExternalSource.convertToGpu(wrapped, this)
  }
}

