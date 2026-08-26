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

import scala.annotation.tailrec
import scala.util.control.NonFatal

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.rapids.{GpuSequenceFileRDDScanExec, SequenceFileRddReadProof}

private[rapids] final class GpuSequenceFileSerializeFromObjectExecMeta(
    plan: SerializeFromObjectExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SerializeFromObjectExec](plan, conf, parent, rule) {

  private val inspection = SequenceFileRddReadProof.inspect(plan)
  private val isProvenCandidate = inspection.isInstanceOf[SequenceFileRddReadProof.Proven]
  private var provenRead: Option[SequenceFileRddReadProof.Proven] = None

  // The proof covers both nodes, so exposing the RDD scan as a child would make replacement
  // non-atomic.
  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = if (isProvenCandidate) {
    Seq.empty
  } else {
    plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  }
  override val childExprs: Seq[BaseExprMeta[_]] = if (isProvenCandidate) {
    Seq.empty
  } else {
    plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagPlanForGpu(): Unit = {
    inspection match {
      case proven: SequenceFileRddReadProof.Proven =>
        if (proven.columns.size != 2 ||
            proven.columns.count(_ == SequenceFileRddReadProof.Key) != 1 ||
            proven.columns.count(_ == SequenceFileRddReadProof.Value) != 1) {
          willNotWorkOnGpu("SequenceFile RDD replacement requires key and value exactly once")
        } else if (hasUnprovenPartitionAncestor(parent)) {
          willNotWorkOnGpu(
            "SequenceFile RDD replacement cannot prove ancestor partition semantics")
        } else {
          sourceIgnoreFlags(proven.sourceRdd) match {
            case Right(false) => provenRead = Some(proven)
            case Right(true) => willNotWorkOnGpu(
              "SequenceFile RDD replacement does not support ignoring missing or corrupt files")
            case Left(reason) => willNotWorkOnGpu(reason)
          }
        }
      case SequenceFileRddReadProof.Rejected(reason) =>
        willNotWorkOnGpu(reason)
    }
  }

  override def convertToGpu(): GpuExec = {
    val proven = provenRead.getOrElse {
      throw new IllegalStateException("SequenceFile RDD read was not proven")
    }
    GpuSequenceFileRDDScanExec(
      wrapped.output,
      proven.columns,
      proven.sourceRdd)(conf)
  }

  override def convertToCpu(): SparkPlan = {
    if (isProvenCandidate) wrapped else super.convertToCpu()
  }

  private def hasPartitionSensitiveExpression(plan: SparkPlan): Boolean = {
    InputFileBlockRule.hasInputFileExpression(plan) || plan.expressions.exists(_.exists {
      case _: Nondeterministic => true
      case _ => false
    })
  }

  private def isPartitionTransparent(meta: SparkPlanMeta[_]): Boolean = {
    val ancestor = meta.wrapped.asInstanceOf[SparkPlan]
    val exactRowLocalExec = ancestor match {
      case project: ProjectExec => project.getClass == classOf[ProjectExec]
      case filter: FilterExec => filter.getClass == classOf[FilterExec]
      case _ => false
    }
    exactRowLocalExec && meta.canExprTreeBeReplaced &&
      !meta.hasDirectCpuBridgeExpressions && !hasPartitionSensitiveExpression(ancestor)
  }

  @tailrec
  private def hasUnprovenPartitionAncestor(
      current: Option[RapidsMeta[_, _, _]]): Boolean = current match {
    case Some(meta: SparkPlanMeta[_]) if meta.wrapped.isInstanceOf[Exchange] => false
    case Some(meta: SparkPlanMeta[_]) if isPartitionTransparent(meta) =>
      hasUnprovenPartitionAncestor(meta.parent)
    case Some(_) => true
    case None => false
  }

  private def sourceIgnoreFlags(
      source: NewHadoopRDD[Any, Any]): Either[String, Boolean] = {
    try {
      val sourceClass = classOf[NewHadoopRDD[_, _]]
      val methods = sourceClass.getDeclaredMethods
      val fields = sourceClass.getDeclaredFields
      val flags = Seq("ignoreMissingFiles", "ignoreCorruptFiles").map { suffix =>
        val methodValue = methods.find(method => method.getName.endsWith(suffix) &&
            method.getParameterCount == 0 && method.getReturnType == java.lang.Boolean.TYPE)
          .map { method =>
            method.setAccessible(true)
            method.invoke(source).asInstanceOf[Boolean]
          }
        methodValue.orElse(fields.find(field => field.getName.endsWith(suffix) &&
            field.getType == java.lang.Boolean.TYPE).map { field =>
          field.setAccessible(true)
          field.getBoolean(source)
        }).toRight(s"cannot inspect NewHadoopRDD $suffix")
      }
      flags.collectFirst { case Left(reason) => reason } match {
        case Some(reason) => Left(reason)
        case None => Right(flags.collect { case Right(value) => value }.exists(identity))
      }
    } catch {
      case NonFatal(e) =>
        Left(s"cannot inspect NewHadoopRDD file handling: ${e.getClass.getSimpleName}")
    }
  }
}
