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
{"spark":"340"}
{"spark":"341"}
{"spark":"342"}
{"spark":"343"}
{"spark":"344"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.lang.reflect.Method

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.protobuf._

private[shims] object SparkProtobufCompat extends SparkProtobufCompatBase {
  override protected def reflectDescriptorSource(
      e: Expression): Either[String, ProtobufDescriptorSource] =
    Try(ProtobufReflection.invoke0[Option[String]](e, "descFilePath"))
      .toEither.left.map(t => s"Cannot read descFilePath: ${t.getMessage}")
      .flatMap(_.map(ProtobufDescriptorSource.DescriptorPath).toRight(
        "from_protobuf requires a descriptor path"))

  override private[shims] def invokeBuildDescriptor(
      buildMethod: Method,
      module: AnyRef,
      messageName: String,
      descriptorSource: ProtobufDescriptorSource,
      readDescriptorFile: String => Array[Byte]): AnyRef = descriptorSource match {
    case ProtobufDescriptorSource.DescriptorPath(path) =>
      buildMethod.invoke(module, messageName, Some(path))
    case _: ProtobufDescriptorSource.DescriptorBytes =>
      throw new UnsupportedOperationException("Spark 3.4 requires a descriptor path")
  }
}
