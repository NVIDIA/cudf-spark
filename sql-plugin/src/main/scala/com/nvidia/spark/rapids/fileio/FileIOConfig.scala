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

package com.nvidia.spark.rapids.fileio

import java.lang.reflect.{Constructor, InvocationTargetException}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/** Session settings captured on the driver. The Hadoop configuration stays in its job broadcast. */
final class FileIOConfig private (
    impl: Option[String],
    options: Map[String, String]) extends Serializable {

  def load(hadoopConf: Configuration): RapidsFileIO = impl match {
    case None => new HadoopFileIO(hadoopConf)
    case Some(name) => FileIOConfig.instantiate(name, options, hadoopConf, FileIOConfig.IMPL_KEY)
  }
}

object FileIOConfig {
  private val IMPL_KEY = RapidsConf.FILE_IO_IMPL.key
  private[fileio] val PREFIX = IMPL_KEY.stripSuffix("impl")

  def from(conf: SQLConf): FileIOConfig = {
    val impl = RapidsConf.FILE_IO_IMPL.get(conf)
    val options = impl.map { name =>
      if (!isResolver(name) && name != classOf[HadoopFileIO].getName) {
        // Check the signature during planning without opening a storage client on the driver.
        constructor(name, IMPL_KEY)
      }
      scoped(conf.getAllConfs, PREFIX)
    }.getOrElse(Map.empty[String, String])
    new FileIOConfig(impl, options)
  }

  private[fileio] def scoped(properties: Map[String, String], prefix: String): Map[String, String] =
    properties.iterator.collect {
      case (key, value) if key.startsWith(prefix) && key != prefix + "impl" =>
        key.substring(prefix.length) -> value
    }.toMap

  private def isResolver(name: String): Boolean =
    name == classOf[ResolvingRapidsFileIO].getName ||
      name == classOf[ResolvingRapidsFileIO].getSimpleName

  private def constructor(name: String, key: String): Constructor[_ <: RapidsFileIO] = {
    try {
      TrampolineUtil.classForName[AnyRef](name, initialize = false)
        .asSubclass(classOf[RapidsFileIO]).getConstructor(classOf[JMap[_, _]])
    } catch {
      case e @ (_: ReflectiveOperationException | _: ClassCastException | _: LinkageError) =>
        throw new IllegalArgumentException(
          s"$key=$name must implement RapidsFileIO and have a public java.util.Map constructor", e)
    }
  }

  private[fileio] def instantiate(
      name: String,
      options: Map[String, String],
      hadoopConf: Configuration,
      key: String): RapidsFileIO = {
    if (name == classOf[HadoopFileIO].getName) {
      new HadoopFileIO(hadoopConf)
    } else if (isResolver(name)) {
      new ResolvingRapidsFileIO(options.asJava, hadoopConf)
    } else {
      val ctor = constructor(name, key)
      try {
        ctor.newInstance(options.asJava)
      } catch {
        case e: InvocationTargetException =>
          throw new IllegalArgumentException(s"Failed to initialize $key=$name", e.getCause)
        case e @ (_: ReflectiveOperationException | _: LinkageError) =>
          throw new IllegalArgumentException(s"Failed to instantiate $key=$name", e)
      }
    }
  }
}
