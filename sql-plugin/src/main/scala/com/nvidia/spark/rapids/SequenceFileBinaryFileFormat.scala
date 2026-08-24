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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

/**
 * GPU-only internal format exposing serialized keys and values from trusted, bounded BLOCK files.
 */
class SequenceFileBinaryFileFormat extends FileFormat with Serializable {
  import SequenceFileBinaryFileFormat._

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(DATA_SCHEMA)

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    validateDataSchema(dataSchema)
    validateRequiredSchema(requiredSchema)
    require(partitionSchema.isEmpty, s"$FORMAT_NAME does not support partition columns")
    val sqlConf = sparkSession.sessionState.conf
    require(!booleanOption(options, "ignoreMissingFiles", sqlConf.ignoreMissingFiles) &&
      !booleanOption(options, "ignoreCorruptFiles", sqlConf.ignoreCorruptFiles),
      s"$FORMAT_NAME does not support ignoreMissingFiles or ignoreCorruptFiles")
    _ => throw new UnsupportedOperationException(
      s"$FORMAT_NAME is an internal GPU-only data source and requires the RAPIDS Accelerator")
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(s"$FORMAT_NAME does not support writing")
  }
}

object SequenceFileBinaryFileFormat {
  val FORMAT_NAME: String = "SequenceFile binary"
  val KEY_FIELD: String = "key"
  val VALUE_FIELD: String = "value"

  val DATA_SCHEMA: StructType = StructType(Seq(
    StructField(KEY_FIELD, BinaryType, nullable = true),
    StructField(VALUE_FIELD, BinaryType, nullable = true)))

  private[rapids] def booleanOption(
      options: Map[String, String],
      name: String,
      default: Boolean): Boolean = {
    options.collectFirst {
      case (key, value) if key.equalsIgnoreCase(name) => value.toBoolean
    }.getOrElse(default)
  }

  private[rapids] def validateDataSchema(schema: StructType): Unit = {
    require(schema == DATA_SCHEMA,
      s"$FORMAT_NAME requires schema ${DATA_SCHEMA.simpleString}, found ${schema.simpleString}")
  }

  private[rapids] def validateRequiredSchema(schema: StructType): Unit = {
    val names = schema.fieldNames.toSeq
    require(names.distinct.size == names.size,
      s"$FORMAT_NAME projection contains duplicate columns: ${names.mkString(", ")}")
    schema.fields.foreach { field =>
      require(DATA_SCHEMA.exists(expected =>
        expected.name == field.name && expected.dataType == field.dataType),
        s"$FORMAT_NAME cannot read ${field.name}:${field.dataType.simpleString}")
    }
  }
}
