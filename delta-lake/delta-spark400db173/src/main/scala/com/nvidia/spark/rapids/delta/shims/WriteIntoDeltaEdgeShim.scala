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

package com.nvidia.spark.rapids.delta.shims

import java.lang.reflect.{Constructor, InvocationTargetException, Method}

import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOptions}
import com.databricks.sql.transaction.tahoe.commands.WriteIntoDeltaEdge

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.types.StructType

/** Resolves the write command for a validated DBR 17.3 patch before GPU planning. */
object WriteIntoDeltaEdgeShim extends Logging {
  private val explicitTypes: Seq[Class[_]] = Seq(
    classOf[DeltaLog], classOf[SaveMode], classOf[DeltaOptions], classOf[Seq[_]],
    classOf[Map[_, _]], classOf[Dataset[_]])
  private val optionType = classOf[Option[_]]

  private case class Layout(patch: String, argumentCount: Int, schemaPosition: Int)
  private case class Binding(
      layout: Layout,
      constructor: Constructor[WriteIntoDeltaEdge],
      defaults: Seq[Method])

  // The DBR version exposed to Spark identifies the 17.3 family. The Databricks
  // build revision distinguishes these two tested patch images.
  private def layoutFor(buildRevision: String): Option[Layout] = buildRevision match {
    case "74719ffead181613a6858637ae9d6771bb243479" => Some(Layout("17.3.24", 13, 8))
    case "2c6d9e667780c1deb163769ed814a3b4509e3d39" => Some(Layout("17.3.25", 14, 9))
    case _ => None
  }

  private lazy val binding: Either[String, Binding] = {
    try {
      val revision = com.databricks.BuildInfo.gitHash
      layoutFor(revision).toRight(s"unrecognized Databricks build $revision").map { layout =>
        val parameterTypes = explicitTypes ++
          Seq.fill(layout.argumentCount - explicitTypes.size)(optionType)
        val ctor = classOf[WriteIntoDeltaEdge].getConstructor(parameterTypes: _*)
        val companion = WriteIntoDeltaEdge.getClass
        val defaults = (explicitTypes.size + 1 to layout.argumentCount).map { position =>
          val getter = companion.getMethod(s"apply$$default$$$position")
          require(getter.getReturnType == optionType, s"unexpected return type for $getter")
          getter
        }
        logInfo(s"Selected WriteIntoDeltaEdge layout for DBR ${layout.patch}")
        Binding(layout, ctor, defaults)
      }
    } catch {
      case e: LinkageError => Left(e.toString)
      case e: ReflectiveOperationException => Left(e.toString)
      case e: IllegalArgumentException => Left(e.toString)
      case e: SecurityException => Left(e.toString)
    }
  }

  def incompatibility: Option[String] = binding.left.toOption.map { reason =>
    s"GPU Delta writes cannot use WriteIntoDeltaEdge on this DBR patch: $reason"
  }

  def create(
      deltaLog: DeltaLog,
      mode: SaveMode,
      options: DeltaOptions,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      data: DataFrame,
      schemaInCatalogOverride: Option[Option[StructType]] = None): WriteIntoDeltaEdge = {
    val resolved = binding.fold(reason => throw new IllegalStateException(reason), identity)
    val explicit = Seq[AnyRef](deltaLog, mode, options, partitionColumns, configuration, data)
    val defaults: Seq[AnyRef] = resolved.defaults.zipWithIndex.map { case (getter, index) =>
      val position = explicit.size + 1 + index
      schemaInCatalogOverride.filter(_ => position == resolved.layout.schemaPosition)
        .getOrElse(invoke(getter.invoke(WriteIntoDeltaEdge)))
    }
    invoke(resolved.constructor.newInstance((explicit ++ defaults): _*))
  }

  private def invoke[T](call: => T): T = {
    try {
      call
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }
}
