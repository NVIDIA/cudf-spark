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
import com.nvidia.spark.rapids.RapidsConf

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
  private[shims] final class Binding[T](
      schemaPosition: Int,
      constructor: Constructor[T],
      companion: AnyRef,
      defaults: Seq[Method]) {

    def newWrite(
        explicit: Seq[AnyRef],
        schemaInCatalogOverride: Option[Option[StructType]]): T = {
      val options: Seq[AnyRef] = defaults.zipWithIndex.map { case (getter, index) =>
        val position = explicit.size + 1 + index
        schemaInCatalogOverride.filter(_ => position == schemaPosition)
          .getOrElse(invoke(getter.invoke(companion)))
      }
      invoke(constructor.newInstance((explicit ++ options): _*))
    }
  }

  // The DBR version exposed to Spark identifies the 17.3 family. The Databricks
  // build revision distinguishes these two tested patch images.
  private def layoutFor(buildRevision: String): Option[Layout] = buildRevision match {
    case "74719ffead181613a6858637ae9d6771bb243479" => Some(Layout("17.3.24", 13, 8))
    case "2c6d9e667780c1deb163769ed814a3b4509e3d39" => Some(Layout("17.3.25", 14, 9))
    case _ => None
  }

  private lazy val binding =
    resolve(com.databricks.BuildInfo.gitHash, classOf[WriteIntoDeltaEdge], WriteIntoDeltaEdge)

  private[shims] def resolve[T](
      buildRevision: => String,
      commandClass: => Class[T],
      defaultsCompanion: => AnyRef): Either[String, Binding[T]] = {
    try {
      val revision = buildRevision
      layoutFor(revision).toRight(s"unrecognized Databricks build $revision").map { layout =>
        val parameterTypes = explicitTypes ++
          Seq.fill(layout.argumentCount - explicitTypes.size)(optionType)
        val ctor = commandClass.getConstructor(parameterTypes: _*)
        val companion = defaultsCompanion
        val defaults = (explicitTypes.size + 1 to layout.argumentCount).map { position =>
          val getter = companion.getClass.getMethod(s"apply$$default$$$position")
          require(getter.getReturnType == optionType, s"unexpected return type for $getter")
          getter
        }
        logInfo(s"Selected WriteIntoDeltaEdge layout for DBR ${layout.patch}")
        new Binding(layout.schemaPosition, ctor, companion, defaults)
      }
    } catch {
      case e: LinkageError => Left(e.toString)
      case e: ReflectiveOperationException => Left(e.toString)
      case e: IllegalArgumentException => Left(e.toString)
      case e: SecurityException => Left(e.toString)
    }
  }

  def incompatibility(conf: RapidsConf): Option[String] = {
    val rejected = binding.left.toOption.orElse {
      // A test may reject an otherwise compatible runtime. It cannot enable
      // GPU writes on an unsupported image or change the binding used to write.
      if (conf.isTestEnabled) {
        conf.get(RapidsConf.TEST_DELTA_WRITE_BUILD_REVISION).flatMap { revision =>
          resolve(revision, classOf[WriteIntoDeltaEdge], WriteIntoDeltaEdge).left.toOption
        }
      } else {
        None
      }
    }
    rejected.map { reason =>
      s"GPU Delta writes cannot use WriteIntoDeltaEdge on this DBR patch: $reason"
    }
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
    resolved.newWrite(explicit, schemaInCatalogOverride)
  }

  private def invoke[T](call: => T): T = {
    try {
      call
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }
}
