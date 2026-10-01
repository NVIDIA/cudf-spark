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

package com.nvidia.spark.rapids.parquet

import java.nio.file.{Files, Path => NioPath, StandardCopyOption}

import com.nvidia.spark.rapids.{GpuMetric, NoopMetric, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class GpuParquetFileFilterHandlerSuite extends AnyFunSuite {
  private val readSchema = StructType(Seq(
    StructField("c2_string", StringType),
    StructField("c3_long", LongType),
    StructField("c1_int", IntegerType)))

  private class TrackingHadoopFileIO(conf: Configuration) extends HadoopFileIO(conf) {
    var knownLengthInputFiles: Int = 0
    var freshLengthInputFiles: Int = 0

    override def newInputFile(path: Path, knownLength: Long): RapidsInputFile = {
      knownLengthInputFiles += 1
      super.newInputFile(path, knownLength)
    }

    override def newInputFile(path: Path): RapidsInputFile = {
      freshLengthInputFiles += 1
      super.newInputFile(path)
    }
  }

  private def copyResource(resource: String, target: NioPath): Unit = {
    withResource(getClass.getResourceAsStream(resource)) { input =>
      Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def filterFooter(file: NioPath, plannedSize: Long): TrackingHadoopFileIO = {
    val conf = new Configuration()
    val fileIO = new TrackingHadoopFileIO(conf)
    val handler = GpuParquetFileFilterHandler(
      new SQLConf(), Map[String, GpuMetric]().withDefaultValue(NoopMetric))
    val partitionedFile = PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow.empty, file.toUri.toString, 0, plannedSize)

    handler.filterBlocks(fileIO, RapidsConf.ParquetFooterReaderType.JAVA,
      partitionedFile, conf, Array.empty, readSchema)
    fileIO
  }

  test("footer filtering uses the planned file size") {
    val file = Files.createTempFile("parquet-known-length", ".parquet")
    try {
      copyResource("/disorder-read-schema.parquet", file)

      val fileIO = filterFooter(file, Files.size(file))

      assert(fileIO.knownLengthInputFiles > 0)
      assertResult(0)(fileIO.freshLengthInputFiles)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("footer filtering retries when a file is replaced with a shorter valid file") {
    val file = Files.createTempFile("parquet-stale-length", ".parquet")
    try {
      copyResource("/file-splits.parquet", file)
      val plannedSize = Files.size(file)
      copyResource("/disorder-read-schema.parquet", file)
      assert(Files.size(file) < plannedSize)

      val fileIO = filterFooter(file, plannedSize)

      assert(fileIO.knownLengthInputFiles > 0)
      assert(fileIO.freshLengthInputFiles > 0)
    } finally {
      Files.deleteIfExists(file)
    }
  }
}
