/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._

object FilePartitionShims {

  def getPartitions(selectedPartitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    selectedPartitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values)
      }
    }
  }

  def getFiles(p: FilePartition): Array[PartitionedFile] = p.files

  def copyWithFiles(p: FilePartition, newFiles: Array[PartitionedFile]): FilePartition = {
    p.copy(files = newFiles)
  }

  def splitFiles(sparkSession: SparkSession,
      hadoopConf: Configuration,
      selectedPartitions: Array[PartitionDirectory],
      maxSplitBytes: Long): Seq[PartitionedFile] = {

    def canBeSplit(filePath: Path, hadoopConf: Configuration): Boolean = {
      // Checks if file at path `filePath` can be split.
      // Uncompressed Hive Text files may be split. GZIP compressed files are not.
      // Note: This method works on a Path, and cannot take a `FileStatus`.
      //       partition.files is an Array[FileStatus] on vanilla Apache Spark,
      //       but an Array[SerializableFileStatus] on Databricks.
      val codec = new CompressionCodecFactory(hadoopConf).getCodec(filePath)
      codec == null || codec.isInstanceOf[SplittableCompressionCodec]
    }

    selectedPartitions.flatMap { partition =>
      partition.files.flatMap { f =>
        PartitionedFileUtil.splitFiles(
          sparkSession,
          f,
          f.getPath,
          isSplitable = canBeSplit(f.getPath, hadoopConf),
          maxSplitBytes,
          partition.values
        )
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
  }

  def splitFiles(
      selectedPartitions: Array[PartitionDirectory],
      relation: HadoopFsRelation,
      maxSplitBytes: Long): Array[PartitionedFile] = {

    selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        // getPath() is very expensive so we only want to call it once in this block:
        val filePath = file.getPath
        val isSplitable = relation.fileFormat.isSplitable(
          relation.sparkSession, relation.options, filePath)
        PartitionedFileUtil.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          filePath = file.getPath,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
  }

  def getFilePartitions(
      relation: HadoopFsRelation,
      splitFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
  }

}
