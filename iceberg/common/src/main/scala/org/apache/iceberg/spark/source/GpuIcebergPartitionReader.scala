/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.MapUtil.toMapStrict
import com.nvidia.spark.rapids.fileio.iceberg.{IcebergFileIO, IcebergInputFile}
import com.nvidia.spark.rapids.iceberg.ShimUtils
import com.nvidia.spark.rapids.iceberg.ShimUtils.locationOf
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter
import com.nvidia.spark.rapids.iceberg.parquet._
import org.apache.iceberg._
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.mapping.NameMappingParser

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch



class GpuIcebergPartitionReader(private val task: GpuSparkInputPartition,
    private val threadConf: ThreadConf,
    private val metrics: Map[String, GpuMetric],
) extends PartitionReader[ColumnarBatch] {
  private var inited = false
  
  private lazy val table = GpuSparkScanAccess.table(task.cpuPartition)
  private lazy val fileIO = table.io()
  private lazy val rapidsFileIO = new IcebergFileIO(fileIO)
  private lazy val conf = newConf()
  private lazy val (inputFiles, tasks) = collectFiles()
  private lazy val gpuDeleteFiterMap: Map[IcebergPartitionedFile, Option[GpuDeleteFilter]] =
    tasks.map {
      case (file, task) =>
        val filter = if (task.deletes().asScala.nonEmpty) {
          Some(new GpuDeleteFilter(rapidsFileIO, table.schema(),
            inputFiles, conf, task.deletes().asScala.toSeq))
        } else {
          None
        }
        file -> filter
    }
  private lazy val reader: GpuIcebergParquetReader = createDataFileParquetReader()


  override def close(): Unit = {
    if (inited) {
      reader.close()
    }
  }

  override def next: Boolean = {
    reader.hasNext
  }

  override def get(): ColumnarBatch = {
    reader.next()
  }

  private def createDataFileParquetReader() = {
    if (tasks.values.exists(_.file().format() != FileFormat.PARQUET)) {
      throw new UnsupportedOperationException("Only parquet files are supported")
    }

    val files = tasks.keys.toSeq

    inited = true

    threadConf match {
      case SingleFile =>
        new GpuSingleThreadIcebergParquetReader(rapidsFileIO, files, constantsMap,
          gpuDeleteFiterMap, conf)
      case _: MultiThread =>
        new GpuMultiThreadIcebergParquetReader(rapidsFileIO, files, constantsMap,
          gpuDeleteFiterMap, conf)
      case _: MultiFile =>
        new GpuCoalescingIcebergParquetReader(rapidsFileIO, files, constantsMap, conf)
    }
  }

  private def collectFiles() = {
    val tasks: Seq[FileScanTask] = GpuSparkScanAccess
      .taskGroup(task.cpuPartition)
      .tasks()
      .asScala
      .map(t => t.asFileScanTask())
      .toSeq

    val encryptedFiles = tasks.flatMap(t => Seq(t.file()) ++ t.deletes().asScala)
      .map(f => EncryptedFiles.encryptedInput(
        fileIO.newInputFile(locationOf(f)),
        f.keyMetadata()))

    val inputFiles = table.encryption()
      .decrypt(encryptedFiles.asJava)
      .asScala
      .map(f => f.location() -> new IcebergInputFile(f))
      .toMap

    val taskMap = toMapStrict(tasks.map(t => {
      val file = inputFiles(locationOf(t.file()))
      val icebergFile = IcebergPartitionedFile(file,
        Some((t.start(), t.length())),
        Some(t.residual()))

      icebergFile -> t
    }))

    (inputFiles, taskMap)
  }

  private def newConf(): GpuIcebergParquetReaderConf = {
    val nameMapping = Option(table.properties()
      .get(TableProperties.DEFAULT_NAME_MAPPING))
      .map(nm => NameMappingParser.fromJson(nm))

    GpuIcebergParquetReaderConf(
      GpuSparkScanAccess.isCaseSensitive(task.cpuPartition),
      task.hadoopConf.value.value,
      task.maxReadBatchSizeRows,
      task.maxReadBatchSizeBytes,
      task.gpuTargetBatchSizeBytes,
      task.maxGpuColumnSizeBytes,
      task.chunkedReaderEnabled,
      task.maxChunkedReaderMemoryUsageSizeBytes,
      task.parquetDebugDumpPrefix,
      task.parquetDebugDumpAlways,
      metrics,
      threadConf,
      task.expectedSchema,
      nameMapping)
  }

  private def constantsMap(icebergFile: IcebergPartitionedFile): java.util.Map[Integer, _] = {
    val task = tasks(icebergFile)
    val filter = gpuDeleteFiterMap(icebergFile)
    val requiredSchema = filter.map(_.requiredSchema).getOrElse(conf.expectedSchema)
    ShimUtils.constantsMap(task, requiredSchema, table)
  }
}
