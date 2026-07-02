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
{"spark": "400db173"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.runtime.MetadataLogFileIndex
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
import org.apache.spark.sql.types.StructType

object FileStreamSinkShims {
  def hasMetadata(
      paths: Seq[String],
      hadoopConf: Configuration,
      sqlConf: org.apache.spark.sql.internal.SQLConf): Boolean = {
    FileStreamSink.hasMetadata(paths, hadoopConf, sqlConf)
  }

  def newMetadataLogFileIndex(
      sparkSession: SparkSession,
      basePath: org.apache.hadoop.fs.Path,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType]): MetadataLogFileIndex = {
    new MetadataLogFileIndex(sparkSession, basePath, options, userSpecifiedSchema)
  }
}
