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

import java.util.{Locale, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.{RapidsFileIO, RapidsInputFile, RapidsOutputFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.util.SerializableConfiguration

/** Resolves each file by URI scheme. Unregistered schemes use the job's Hadoop filesystem. */
final class ResolvingRapidsFileIO private[fileio] (
    properties: JMap[String, String],
    conf: Configuration) extends RapidsFileIO {

  def this(properties: JMap[String, String]) = this(properties, new Configuration())

  private val options = properties.asScala.toMap
  private val hadoopConf = new SerializableConfiguration(conf)
  private val defaultUri = FileSystem.getDefaultUri(conf)
  @transient private lazy val fallback = new HadoopFileIO(hadoopConf.value)
  @transient private lazy val delegates = new ConcurrentHashMap[String, RapidsFileIO]()

  private[fileio] def delegateFor(path: Path): RapidsFileIO = {
    val scheme = Option(path.toUri.getScheme).getOrElse(defaultUri.getScheme)
      .toLowerCase(Locale.ROOT)
    val key = scheme + ".impl"
    options.get(key) match {
      case Some(name) => delegates.computeIfAbsent(scheme, _ =>
        FileIOConfig.instantiate(name, FileIOConfig.scoped(options, scheme + "."),
          hadoopConf.value, FileIOConfig.PREFIX + key))
      case None => fallback
    }
  }

  private def qualify(path: Path): Path = {
    if (path.toUri.getScheme != null) path
    else if (path.isAbsolute) path.makeQualified(defaultUri, path)
    else FileSystem.get(hadoopConf.value).makeQualified(path)
  }

  override def newInputFile(path: Path): RapidsInputFile =
    delegateFor(path).newInputFile(qualify(path))

  override def newInputFile(path: String): RapidsInputFile = {
    val location = new Path(path)
    if (location.toUri.getScheme == null) newInputFile(location)
    else delegateFor(location).newInputFile(path)
  }

  override def newOutputFile(path: Path): RapidsOutputFile =
    delegateFor(path).newOutputFile(qualify(path))

  override def newOutputFile(path: String): RapidsOutputFile = {
    val location = new Path(path)
    if (location.toUri.getScheme == null) newOutputFile(location)
    else delegateFor(location).newOutputFile(path)
  }
}
