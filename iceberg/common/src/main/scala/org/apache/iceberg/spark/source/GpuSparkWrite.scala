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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableSeq
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.GpuIcebergSpecPartitioner
import com.nvidia.spark.rapids.shims.parquet.ParquetFieldIdShims
import org.apache.hadoop.mapreduce.Job
import org.apache.iceberg._
import org.apache.iceberg.io._
import org.apache.iceberg.spark.{GpuTypeToSparkType, Spark3Util, SparkSchemaUtil}
import org.apache.iceberg.spark.functions.{GpuFieldTransform, GpuTransform}
import org.apache.iceberg.spark.source.GpuWriteContext.positionDeleteSparkType

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{DataWriter, _}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration



class GpuSparkWrite(cpu: Write) extends GpuWrite with RequiresDistributionAndOrdering  {
  private val writeRequirements = cpu.asInstanceOf[RequiresDistributionAndOrdering]

  private[source] val table: Table = GpuSparkWriteAccess.table(cpu)
  private[source] val format: FileFormat = GpuSparkWriteAccess.format(cpu)

  override def toBatch: BatchWrite = {
    // Call the CPU version's toBatch to get the appropriate BatchWrite implementation
    // Iceberg's SparkWrite returns different implementations based on write mode:
    // - BatchAppend for append operations
    // - DynamicOverwrite for dynamic partition overwrite
    // - BatchRewrite for copy-on-write operations (DELETE)
    // Since these are private classes, we check the class name to determine which GPU version
    // to use
    val cpuBatch = cpu.toBatch
    val cpuBatchClassName = cpuBatch.getClass.getSimpleName

    cpuBatchClassName match {
      case "BatchAppend" => new GpuBatchAppend(this, cpuBatch)
      case "DynamicOverwrite" => new GpuDynamicOverwrite(this, cpuBatch)
      case "OverwriteByFilter" => new GpuOverwriteByFilter(this, cpuBatch)
      case "CopyOnWriteOperation" => new GpuCopyOnWriteOperation(this, cpuBatch)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported Iceberg batch write type: $cpuBatchClassName")
    }
  }

  override def toStreaming: StreamingWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support streaming write")

  override def toString: String = s"GpuIcebergWrite(table=$table, format=$format)"

  override def distributionStrictlyRequired(): Boolean =
    writeRequirements.distributionStrictlyRequired()

  override def requiredNumPartitions(): Int = writeRequirements.requiredNumPartitions()

  override def advisoryPartitionSizeInBytes(): Long =
    writeRequirements.advisoryPartitionSizeInBytes()

  override def requiredDistribution(): Distribution = writeRequirements.requiredDistribution()

  override def requiredOrdering(): Array[SortOrder] = writeRequirements.requiredOrdering()

  private[source] def createDataWriterFactory: DataWriterFactory = {
    val sparkContext: JavaSparkContext = GpuSparkWriteAccess.sparkContext(cpu)
    val tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table))
    val queryId = GpuSparkWriteAccess.queryId(cpu)
    val outputSpecId = GpuSparkWriteAccess.outputSpecId(cpu)
    val targetFileSize = GpuSparkWriteAccess.targetFileSize(cpu)
    val writeSchema = GpuSparkWriteAccess.writeSchema(cpu)
    // Convert writeSchema to Spark StructType with Iceberg field IDs (PARQUET:field_id).
    // The CPU path uses Iceberg's own Parquet writer which natively embeds field IDs, but
    // the GPU path uses Spark's Parquet infrastructure which requires field IDs in the
    // StructType metadata. Without them, Iceberg's ParquetMetrics cannot extract file-level
    // statistics, causing StrictMetricsEvaluator to fail during overwrite validation.
    val dsSchema = GpuTypeToSparkType.toSparkType(writeSchema)
    val useFanout = GpuSparkWriteAccess.useFanoutWriter(cpu)
    val writeProps = GpuSparkWriteAccess.writeProperties(cpu)

    if (!format.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but got: $format")
    }

    val hadoopConf = sparkContext.hadoopConfiguration

    val job = {
      val tmpJob  = Job.getInstance(hadoopConf)
      tmpJob.setOutputKeyClass(classOf[Void])
      tmpJob.setOutputValueClass(classOf[InternalRow])
      tmpJob
    }

    val outputWriterFactory = new GpuParquetFileFormat().prepareWrite(
      SparkSession.active,
      job,
      GpuSparkWrite.translateIcebergWriteProperties(writeProps.asScala.toMap),
      dsSchema
    )

    val serializedHadoopConf = new SerializableConfiguration(job.getConfiguration)
    val statsTracker = new GpuWriteJobStatsTracker(serializedHadoopConf,
      GpuWriteJobStatsTracker.basicMetrics,
      GpuWriteJobStatsTracker.taskMetrics)

    new GpuWriterFactory(
      tableBroadcast,
      queryId,
      format,
      outputSpecId,
      targetFileSize,
      writeSchema,
      dsSchema,
      useFanout,
      writeProps.asScala.toMap,
      outputWriterFactory,
      statsTracker,
      serializedHadoopConf)
  }
}

object GpuSparkWrite {
  def supports(cpuClass: Class[_ <: Write]): Boolean = {
    GpuSparkWriteAccess.supports(cpuClass)
  }

  // Iceberg already resolved the Parquet codec via its own precedence chain (write option
  // > spark.sql.iceberg.compression-codec > table property > zstd) and stored the result
  // under `write.parquet.compression-codec`. Spark's ParquetOptions only recognizes
  // `compression` / `parquet.compression`, so without translation it falls through to
  // `spark.sql.parquet.compression.codec` and silently overrides Iceberg's choice.
  private[source] def translateIcebergWriteProperties(
      writeProps: Map[String, String]): Map[String, String] = {
    writeProps.get(TableProperties.PARQUET_COMPRESSION) match {
      case Some(codec) if !writeProps.contains("compression") =>
        writeProps + ("compression" -> codec)
      case _ => writeProps
    }
  }

  /**
   * Fall back to the CPU when the Iceberg-resolved Parquet codec cannot be reproduced on
   * the GPU. cuDF's Parquet writer only supports a subset of Iceberg's codecs (uncompressed,
   * snappy, zstd). Before [[translateIcebergWriteProperties]] forwarded the codec, an
   * unsupported value was silently masked by `spark.sql.parquet.compression.codec`; now that
   * the resolved codec reaches `prepareWrite`, an unsupported value would fail at execution
   * instead of falling back, so it must be rejected at planning time.
   *
   * A single `ColumnarOutputWriterFactory` is shared by the data and position-delete writers,
   * so the GPU path can only honor one codec. When Iceberg resolves a delete codec
   * (`write.delete.parquet.compression-codec`) that differs from the data codec, fall back
   * rather than silently writing delete files with the data codec.
   */
  private[source] def tagParquetCompressionForGpu(
      writeProps: java.util.Map[String, String],
      hasDeleteFiles: Boolean,
      meta: SparkPlanMeta[_]): Unit = {
    def gpuSupports(codec: String): Boolean =
      GpuParquetFileFormat.parseCompressionType(codec.toUpperCase(Locale.ROOT)).isDefined

    Option(writeProps.get(TableProperties.PARQUET_COMPRESSION)).foreach { dataCodec =>
      if (!gpuSupports(dataCodec)) {
        meta.willNotWorkOnGpu(s"Iceberg Parquet compression codec '$dataCodec' is not " +
          "supported by the GPU writer")
      }
      if (hasDeleteFiles) {
        val deleteCodec = Option(writeProps.get(TableProperties.DELETE_PARQUET_COMPRESSION))
          .getOrElse(dataCodec)
        if (!deleteCodec.equalsIgnoreCase(dataCodec)) {
          meta.willNotWorkOnGpu(s"Iceberg delete-file compression codec '$deleteCodec' differs " +
            s"from the data-file codec '$dataCodec'; the GPU writer uses a single codec for both")
        }
      }
    }
  }

  /**
   * Tag for GPU support for Iceberg write operations.
   * This method checks:
   * 1. File format is supported (only Parquet)
   * 2. Partition transforms are supported
   */
  private[iceberg] def tagForGpuWrite(
      dataFormat: Option[FileFormat],
      dataSparkType: Option[StructType],
      dataSchema: Option[Schema],
      deleteFormat: Option[FileFormat],
      partitionSpec: PartitionSpec,
      meta: SparkPlanMeta[_]): Unit = {

    // Iceberg requires Parquet field IDs for correct file-level metrics. Without them,
    // StrictMetricsEvaluator fails during overwrite validation.
    val spark = SparkSessionUtils.sessionFromPlan(meta.wrapped.asInstanceOf[SparkPlan])
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val sqlConf = spark.sessionState.conf
    if (!ParquetFieldIdShims.getParquetIdWriteEnabled(hadoopConf, sqlConf)) {
      meta.willNotWorkOnGpu("Iceberg requires Parquet field IDs to be written for correct " +
        "file-level metrics. Set spark.sql.parquet.fieldId.write.enabled=true")
    }

    // Check file format support
    if (dataFormat.exists(!_.equals(FileFormat.PARQUET))) {
      meta.willNotWorkOnGpu(s"GpuSparkWrite only supports Parquet, but got: ${dataFormat.get}")
    }

    if (deleteFormat.exists(!_.equals(FileFormat.PARQUET))) {
      meta.willNotWorkOnGpu(s"GpuSparkWrite only supports Parquet, but got: ${deleteFormat.get}")
    }

    // Check partition transform support
    if (partitionSpec.isPartitioned && dataSchema.isDefined) {
      for (partitionField <- partitionSpec.fields().asScala) {
        val transform = partitionField.transform()
        GpuTransform.tryFrom(transform) match {
          case Success(t) =>
            val fieldTransform = GpuFieldTransform(partitionField.sourceId(), t)
            if (!fieldTransform.supports(dataSparkType.get, dataSchema.get)) {
              meta.willNotWorkOnGpu(
                s"Iceberg partition transform $transform is not supported on GPU")
            }
          case Failure(_) => meta.willNotWorkOnGpu(
            s"Iceberg partition transform $transform is not supported on GPU")
        }
      }
    }
  }

  def tagForGpu(cpuWrite: Write, meta: SparkPlanMeta[_]): Unit = {
    if (!supports(cpuWrite.getClass)) {
      meta.willNotWorkOnGpu(s"GpuSparkWrite only supports " +
        s"${GpuSparkWriteAccess.sparkWriteClassName()}, " +
        s"but got: ${cpuWrite.getClass.getName}")
      return
    }

    val dataFileFormat: FileFormat = GpuSparkWriteAccess.format(cpuWrite)

    val table: Table = GpuSparkWriteAccess.table(cpuWrite)
    val partitionSpec = table.spec()

    val dsSchema = GpuSparkWriteAccess.dsSchema(cpuWrite)
    val writeSchema = GpuSparkWriteAccess.writeSchema(cpuWrite)

    tagForGpuWrite(Some(dataFileFormat), Some(dsSchema), Some(writeSchema),
      None, partitionSpec, meta)

    // Append / copy-on-write only writes data files, so no delete codec to consider.
    tagParquetCompressionForGpu(GpuSparkWriteAccess.writeProperties(cpuWrite),
      hasDeleteFiles = false, meta)
  }

  /**
   * Tag for GPU support for Iceberg CTAS operations.
   * This method checks file format and partitioning support for CREATE TABLE AS SELECT.
   *
   * @param cpuExec The CPU AtomicCreateTableAsSelectExec
   * @param meta The metadata for tagging
   */
  def tagForGpuCtas(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: SparkPlanMeta[_]): Unit = {
    val properties: Map[String, String] = Spark3Util
      .rebuildCreateProperties(cpuExec.tableSpec.properties.asJava)
      .asScala.toMap
    val fileFormatStr = properties.getOrElse(TableProperties.DEFAULT_FILE_FORMAT,
      TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)

    val fileFormat = FileFormat.fromString(fileFormatStr)

    // Convert Spark schema to Iceberg schema
    val querySchema = cpuExec.query.schema
    val icebergSchema = SparkSchemaUtil.convert(querySchema)

    // Convert Spark connector transforms to Iceberg PartitionSpec
    val partitionSpec = Spark3Util.toPartitionSpec(icebergSchema, cpuExec.partitioning.toArray)

    // Reuse tagForGpuWrite for validation
    tagForGpuWrite(Option(fileFormat), Option(querySchema), Option(icebergSchema),
      None, partitionSpec, meta)
  }

  /**
   * Tag for GPU support for Iceberg RTAS operations.
   * This method checks file format and partitioning support for REPLACE TABLE AS SELECT.
   *
   * @param cpuExec The CPU AtomicReplaceTableAsSelectExec
   * @param meta The metadata for tagging
   */
  def tagForGpuRtas(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: SparkPlanMeta[_]): Unit = {
    val properties: Map[String, String] = Spark3Util
      .rebuildCreateProperties(cpuExec.tableSpec.properties.asJava)
      .asScala.toMap
    val fileFormatStr = properties.getOrElse(TableProperties.DEFAULT_FILE_FORMAT,
      TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)

    val fileFormat = FileFormat.fromString(fileFormatStr)

    // Convert Spark schema to Iceberg schema
    val querySchema = cpuExec.query.schema
    val icebergSchema = SparkSchemaUtil.convert(querySchema)

    // Convert Spark connector transforms to Iceberg PartitionSpec
    val partitionSpec = Spark3Util.toPartitionSpec(icebergSchema, cpuExec.partitioning.toArray)

    // Reuse tagForGpuWrite for validation
    tagForGpuWrite(Some(fileFormat), Some(querySchema), Some(icebergSchema),
      None, partitionSpec, meta)
  }

  def convert(cpuWrite: Write): GpuSparkWrite = {
    new GpuSparkWrite(cpuWrite)
  }

}

class GpuWriterFactory(val tableBroadcast: Broadcast[Table],
  val queryId: String,
  val format: FileFormat,
  val outputSpecId: Int,
  val targetFileSize: Long,
  val writeSchema: Schema,
  val dsSchema: StructType,
  val useFanout: Boolean,
  val ignore: Map[String, String],
  val outputWriterFactory: ColumnarOutputWriterFactory,
  val statsTracker: GpuWriteJobStatsTracker,
  val hadoopConf: SerializableConfiguration
) extends DataWriterFactory {

  private lazy val fileIO: IcebergFileIO = new IcebergFileIO(tableBroadcast.value.io())

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val table = tableBroadcast.value
    val spec = table.specs().get(outputSpecId)
    val io = table.io()
    val operationId = s"$queryId-0"
    val outputFileFactory = OutputFileFactory
      .builderFor(table, partitionId, taskId)
      .format(format)
      .operationId(operationId)
      .build()

    val writerFactory = new GpuSparkFileWriterFactory(
      table,
      format,
      dsSchema,
      table.sortOrder(),
      format,
      positionDeleteSparkType,
      outputWriterFactory,
      statsTracker.newTaskInstance(),
      hadoopConf,
      fileIO)

    if (spec.isUnpartitioned) {
      new GpuUnpartitionedDataWriter(writerFactory, outputFileFactory, io, spec, targetFileSize)
        .asInstanceOf[DataWriter[InternalRow]]
    } else {
      new GpuPartitionedDataWriter(writerFactory, outputFileFactory, io, spec, writeSchema,
        dsSchema, targetFileSize, useFanout)
        .asInstanceOf[DataWriter[InternalRow]]
    }
  }
}

class GpuUnpartitionedDataWriter(
  val fileWriterFactory: GpuSparkFileWriterFactory,
  val fileFactory: OutputFileFactory,
  val io: FileIO,
  val spec: PartitionSpec,
  val targetFileSize: Long)
  extends DataWriter[ColumnarBatch] {
  private val delegate = new GpuRollingDataWriter(
    fileWriterFactory,
    fileFactory,
    io,
    targetFileSize,
    spec,
    null)


  override def write(t: ColumnarBatch): Unit = {
    val scb = closeOnExcept(t) { _ =>
      SpillableColumnarBatch(t, ACTIVE_ON_DECK_PRIORITY)
    }
    delegate.write(scb)
  }

  override def commit(): WriterCommitMessage = {
    close()

    val result = delegate.result()
    GpuSparkWriteAccess.taskCommit(result.dataFiles().toArray(new Array[DataFile](0)))
  }

  override def abort(): Unit = {
    close()

    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles())
  }

  override def close(): Unit = {
    delegate.close()
  }
}

class GpuPartitionedDataWriter(
  val writerFactory: GpuSparkFileWriterFactory,
  val fileFactory: OutputFileFactory,
  val io: FileIO,
  val spec: PartitionSpec,
  val dataSchema: Schema,
  val dataSparkType: StructType,
  val targetFileSize: Long,
  val fanoutEnabled: Boolean,
) extends DataWriter[ColumnarBatch] {

  private val delegate: PartitioningWriter[SpillableColumnarBatch, DataWriteResult] =
    if (fanoutEnabled) {
      new GpuFanoutDataWriter(writerFactory, fileFactory, io,
        targetFileSize)
    } else {
      new GpuClusteredDataWriter(writerFactory, fileFactory, io,
        targetFileSize)
    }

  private val partitioner = new GpuIcebergSpecPartitioner(spec, dataSchema.asStruct())

  override def write(record: ColumnarBatch): Unit = {
    partitioner.partition(record)
      .safeConsume { part =>
        delegate.write(part.batch, spec, part.partition)
      }
  }

  override def commit(): WriterCommitMessage = {
    close()

    val result = delegate.result()
    GpuSparkWriteAccess.taskCommit(result.dataFiles().toArray(new Array[DataFile](0)))
  }

  override def abort(): Unit = {
    close()

    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles())
  }

  override def close(): Unit = {
    delegate.close()
  }
}
