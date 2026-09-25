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

/*** spark-rapids-shim-json-lines
{"spark":"341db"}
{"spark":"350db143"}
{"spark":"400db173"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.lang.reflect.Method

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.protobuf._

/**
 * Probes descriptor APIs because Databricks backports may differ from the matching Spark version.
 */
private[shims] object SparkProtobufCompat extends SparkProtobufCompatBase {
  override protected def reflectDescriptorSource(
      e: Expression): Either[String, ProtobufDescriptorSource] = {
    reflectDescFilePath(e).map(ProtobufDescriptorSource.DescriptorPath).orElse(
      reflectDescriptorBytes(e).map(ProtobufDescriptorSource.DescriptorBytes.apply)).toRight(
      "from_protobuf requires a descriptor set (descFilePath or binaryFileDescriptorSet)")
  }

  private def reflectDescFilePath(e: Expression): Option[String] =
    Try(ProtobufReflection.invoke0[Option[String]](e, "descFilePath")).toOption.flatten

  private def reflectDescriptorBytes(e: Expression): Option[Array[Byte]] = {
    val spark35Result =
      Try(ProtobufReflection.invoke0[Option[Array[Byte]]](e, "binaryFileDescriptorSet"))
      .toOption.flatten
    spark35Result.orElse {
      val direct = Try(ProtobufReflection.invoke0[Array[Byte]](e, "binaryDescriptorSet")).toOption
      direct.orElse {
        Try(ProtobufReflection.invoke0[Option[Array[Byte]]](e, "binaryDescriptorSet"))
          .toOption.flatten
      }
    }
  }

  // Vendor runtimes can backport the descriptor-and-extensions return type.
  override private[shims] def unwrapMessageDescriptor(raw: AnyRef): AnyRef =
    Try(ProtobufReflection.invoke0[AnyRef](raw, "descriptor")).getOrElse(raw)

  override private[shims] def invokeBuildDescriptor(
      buildMethod: Method,
      module: AnyRef,
      messageName: String,
      descriptorSource: ProtobufDescriptorSource,
      readDescriptorFile: String => Array[Byte]): AnyRef = {
    descriptorSource match {
      case source: ProtobufDescriptorSource.DescriptorBytes =>
        buildMethod.invoke(module, messageName, Some(source.bytes)).asInstanceOf[AnyRef]
      case ProtobufDescriptorSource.DescriptorPath(filePath) =>
        try {
          buildMethod.invoke(module, messageName, Some(filePath)).asInstanceOf[AnyRef]
        } catch {
          case ex: java.lang.reflect.InvocationTargetException =>
            val cause = ex.getCause
            // Spark 3.5+ changed the descriptor payload from Option[String] to
            // Option[Array[Byte]] while keeping the same erased JVM signature.
            // Retry with file contents when the path-based invocation clearly hit that
            // binary-descriptor variant.
            if (cause != null && (cause.isInstanceOf[ClassCastException] ||
                cause.isInstanceOf[MatchError])) {
              Try {
                buildMethod.invoke(
                  module, messageName, Some(readDescriptorFile(filePath))).asInstanceOf[AnyRef]
              }.recoverWith { case retryEx =>
                val wrapped = buildDescriptorRetryFailure(cause, retryEx)
                wrapped.addSuppressed(ex)
                scala.util.Failure(wrapped)
              }.get
            } else {
              throw ex
            }
        }
    }
  }

  private def buildDescriptorRetryFailure(
      originalCause: Throwable,
      retryFailure: Throwable): RuntimeException = {
    val retryCause = unwrapInvocationFailure(retryFailure)
    new RuntimeException(
      s"Spark 3.5+ descriptor bytes retry failed after initial path invocation error " +
        s"(${describeThrowable(originalCause)}); retry error (${describeThrowable(retryCause)})",
      retryCause)
  }

  private def unwrapInvocationFailure(t: Throwable): Throwable = t match {
    case ex: java.lang.reflect.InvocationTargetException if ex.getCause != null => ex.getCause
    case other => other
  }

  private def describeThrowable(t: Throwable): String = {
    val suffix = Option(t.getMessage).filter(_.nonEmpty).map(msg => s": $msg").getOrElse("")
    s"${t.getClass.getSimpleName}$suffix"
  }

}
