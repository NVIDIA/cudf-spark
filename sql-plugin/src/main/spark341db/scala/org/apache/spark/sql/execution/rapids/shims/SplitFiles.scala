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
{"spark": "341db"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory, PartitionedFile}

trait SplitFiles {
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
        PartitionedFileUtilsShim.splitFiles(
          sparkSession,
          f,
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
        PartitionedFileUtilsShim.splitFiles(
          sparkSession = relation.sparkSession,
          file = file,
          isSplitable = isSplitable,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partition.values
        )
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
  }
}
