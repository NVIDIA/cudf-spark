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
import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar, Table => CudfTable}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableProducingArray, AutoCloseableSeq}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.{ColumnarBatchWithPartition, GpuIcebergPartitioner, GpuIcebergSpecPartitioner}
import com.nvidia.spark.rapids.iceberg.utils.GpuStructProjection
import org.apache.hadoop.mapreduce.Job
import org.apache.iceberg._
import org.apache.iceberg.deletes.DeleteGranularity
import org.apache.iceberg.io._
import org.apache.iceberg.io.DeleteSchemaUtil
import org.apache.iceberg.spark.GpuTypeToSparkType
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.spark.source.GpuSparkWrite.supports
import org.apache.iceberg.spark.source.GpuWriteContext.{emptyPartitionData, positionDeleteDataTypes, positionDeleteSparkType}
import org.apache.iceberg.types.{Types => IcebergTypes}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration



/**
 * GPU version of Iceberg's SparkPositionDeltaWrite.
 * This class handles merge-on-read DELETE operations that write position delete files.
 */
class GpuSparkPositionDeltaWrite(cpu: DeltaWrite)
  extends GpuDeltaWrite with RequiresDistributionAndOrdering {
  private val writeRequirements = cpu.asInstanceOf[RequiresDistributionAndOrdering]

  private[source] val table = GpuSparkWriteAccess.table(cpu)

  override def toBatch: DeltaBatchWrite = {
    // Call the CPU version's toBatch to get PositionDeltaBatchWrite
    val cpuBatch = cpu.toBatch
    // Wrap it in GPU version
    new GpuPositionDeltaBatchWrite(this, cpuBatch)
  }

  override def toStreaming: StreamingWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support streaming write")

  override def toString: String = s"GpuSparkPositionDeltaWrite(table=$table)"
  
  override def requiredDistribution(): Distribution = writeRequirements.requiredDistribution()

  override def requiredOrdering(): Array[SortOrder] = writeRequirements.requiredOrdering()

  override def advisoryPartitionSizeInBytes(): Long =
    writeRequirements.advisoryPartitionSizeInBytes()

  private[source] def createDeltaWriterFactory: DeltaWriterFactory = {
    val sparkContext: JavaSparkContext = GpuSparkWriteAccess.sparkContext(cpu)
    val tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table))
    val command = GpuSparkWriteAccess.command(cpu)
    val context = GpuWriteContext(GpuSparkWriteAccess.context(cpu))
    val writeProps = GpuSparkWriteAccess.writeProperties(cpu)
      .asScala
      .toMap

    if (!context.dataFileFormat.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but data format got: ${context.dataFileFormat}")
    }

    if (!context.deleteFileFormat.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but delete format got: ${context.deleteFileFormat}")
    }

    val hadoopConf = sparkContext.hadoopConfiguration
    val job = {
      val tmpJob  = Job.getInstance(hadoopConf)
      tmpJob.setOutputKeyClass(classOf[Void])
      tmpJob.setOutputValueClass(classOf[InternalRow])
      tmpJob
    }

    val dataSparkTypeWithFieldIds = GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(
      command, context)

    val outputWriterFactory = new GpuParquetFileFormat().prepareWrite(
      SparkSession.active,
      job,
      GpuSparkWrite.translateIcebergWriteProperties(writeProps),
      dataSparkTypeWithFieldIds
    )

    val serializedHadoopConf = new SerializableConfiguration(job.getConfiguration)
    val statsTracker = new GpuWriteJobStatsTracker(serializedHadoopConf,
      GpuWriteJobStatsTracker.basicMetrics,
      GpuWriteJobStatsTracker.taskMetrics)

    new GpuPositionDeltaWriterFactory(
      tableBroadcast,
      command,
      context,
      writeProps,
      outputWriterFactory,
      statsTracker,
      serializedHadoopConf)
  }
}



object GpuSparkPositionDeltaWrite {
  private[source] def dataWriterSchemaFor(command: Command, context: GpuWriteContext): Schema = {
    if (command == Command.DELETE) {
      DeleteSchemaUtil.pathPosSchema()
    } else {
      Option(context.dataSchema).getOrElse(
        throw new IllegalStateException(s"Missing data schema for delta command: $command"))
    }
  }

  private[source] def dataWriterSparkTypeFor(
      command: Command,
      context: GpuWriteContext): StructType = {
    toSparkType(dataWriterSchemaFor(command, context))
  }

  def tableOf(deltaWrite: DeltaWrite): Table = {
    GpuSparkWriteAccess.table(deltaWrite)
  }

  def tagForGpu(deltaWrite: DeltaWrite, meta: SparkPlanMeta[_]): Unit = {
    if (!supports(deltaWrite.getClass)) {
      meta.willNotWorkOnGpu(s"GpuSparkWrite only supports " +
        s"${GpuSparkWriteAccess.sparkWriteClassName()}, " +
        s"but got: ${deltaWrite.getClass.getName}")
      return
    }
    val context = GpuWriteContext(GpuSparkWriteAccess.context(deltaWrite))

    val table: Table = tableOf(deltaWrite)
    val partitionSpec = table.spec()

    // Iceberg's delta write is similar to normal write, but will write position deletes
    // additionally. Position deletes have only two data types: string + int. So it's
    // safe to use normal write tag method
    GpuSparkWrite.tagForGpuWrite(
      Option(context.dataFileFormat),
      Option(toSparkType(table.schema())),
      Option(table.schema()),
      Option(context.deleteFileFormat),
      partitionSpec,
      meta)

    // Merge-on-read writes both data files and position-delete files, so the resolved
    // delete codec matters too.
    GpuSparkWrite.tagParquetCompressionForGpu(GpuSparkWriteAccess.writeProperties(deltaWrite),
      hasDeleteFiles = true, meta)
  }

  def convert(deltaWrite: DeltaWrite): GpuSparkPositionDeltaWrite = {
    new GpuSparkPositionDeltaWrite(deltaWrite)
  }
}

class GpuPositionDeltaWriterFactory(
  val tableSer: Broadcast[Table],
  val command: Command,
  val context: GpuWriteContext,
  val writeProps: Map[String, String],
  val outputWriterFactory: ColumnarOutputWriterFactory,
  val statsTracker: GpuWriteJobStatsTracker,
  val hadoopConf: SerializableConfiguration) extends DeltaWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    val table = tableSer.value

    val deleteFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
      .format(context.deleteFileFormat)
      .operationId(context.queryId)
      .suffix("deletes")
      .build()

    val dataFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
      .format(context.dataFileFormat)
      .operationId(context.queryId)
      .build()

    val writerFactory = new GpuSparkFileWriterFactory(
      table,
      context.dataFileFormat,
      GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(command, context),
      table.sortOrder(),
      context.deleteFileFormat,
      positionDeleteSparkType,
      outputWriterFactory,
      statsTracker.newTaskInstance(),
      hadoopConf,
      new IcebergFileIO(table.io()))

    if (command == Command.DELETE) {
      new GpuDeleteOnlyDeltaWriter(table, writerFactory, deleteFileFactory, context)
        .asInstanceOf[DeltaWriter[InternalRow]]
    } else {
      if (table.spec().isUnpartitioned) {
        new GpuUnpartitionedDeltaWriter(table, writerFactory, dataFileFactory,
          deleteFileFactory, context)
          .asInstanceOf[DeltaWriter[InternalRow]]
      } else {
        new GpuPartitionedDeltaWriter(table, writerFactory, dataFileFactory,
          deleteFileFactory, context)
          .asInstanceOf[DeltaWriter[InternalRow]]
      }
    }
  }
}


trait GpuDeltaWriter extends DeltaWriter[ColumnarBatch] {

  def context: GpuWriteContext

  protected def buildPartitionProjections(
    partitionType: IcebergTypes.StructType,
    specs: collection.Map[Integer, PartitionSpec]): Map[Int, GpuStructProjection] = {
    specs.map(e => (e._1.toInt, GpuStructProjection(partitionType, e._2.partitionType())))
      .toMap
  }

  protected def newDeleteWriter(table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    outputFileFactory: OutputFileFactory,
    context: GpuWriteContext): PartitioningWriter[SpillableColumnarBatch, DeleteWriteResult] = {

    val io = table.io()
    val inputOrdered = context.inputOrdered
    val targetFileSize = context.targetDeleteFileSize

    if (inputOrdered) {
      new GpuClusteredPositionDeleteWriter(writerFactory, outputFileFactory, io, targetFileSize)
    } else {
      new GpuFanoutPositionDeleteWriter(writerFactory, outputFileFactory, io, targetFileSize)
    }
  }

  protected def newDataWriter(table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    outputFileFactory: OutputFileFactory,
    context: GpuWriteContext): PartitioningWriter[SpillableColumnarBatch, DataWriteResult] = {

    val io = table.io()
    val inputOrdered = context.inputOrdered
    val targetFileSize = context.targetDataFileSize

    if (inputOrdered) {
      new GpuClusteredDataWriter(writerFactory, outputFileFactory, io, targetFileSize)
    } else {
      new GpuFanoutDataWriter(writerFactory, outputFileFactory, io, targetFileSize)
    }
  }

  protected def newPositionDeltaWriter(table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    dataFileFactory: OutputFileFactory,
    deleteFileFactory: OutputFileFactory,
    context: GpuWriteContext): GpuBasePositionDeltaWriter = {

    val dataWriter = newDataWriter(table, writerFactory, dataFileFactory, context)
    val deleteWriter = newDeleteWriter(table, writerFactory, deleteFileFactory, context)
    new GpuBasePositionDeltaWriter(dataWriter, deleteWriter)
  }

  protected def uniqueSpecIds(batch: ColumnarBatch, specIdOrdinal: Int): RapidsHostColumnVector = {
    val specIdCol = batch.column(specIdOrdinal)
      .asInstanceOf[GpuColumnVector]
      .getBase
    withResource(new CudfTable(specIdCol)) { specIdTable =>
      val uniqueSpecIdTable = specIdTable
        .dropDuplicates(Array(0), DuplicateKeepOption.KEEP_ANY, false)
      withResource(uniqueSpecIdTable) { _ =>
        new RapidsHostColumnVector(IntegerType, uniqueSpecIdTable.getColumn(0).copyToHost())
      }
    }
  }

  protected def extractToStruct(batch: ColumnarBatch, ordinal: Int): ColumnarBatch = {
    require(batch.column(ordinal).dataType().isInstanceOf[StructType],
      "Can't extract from non struct type")


    val structType = batch.column(ordinal)
      .dataType()
      .asInstanceOf[StructType]

    val innerCols = batch.column(ordinal)
      .asInstanceOf[GpuColumnVector]
      .getBase
      .getChildColumnViews
      .zip(structType.fields)
      .safeMap(p => GpuColumnVector.from(p._1.copyToColumnVector(), p._2.dataType))
      .map(_.asInstanceOf[ColumnVector])

    new ColumnarBatch(innerCols, batch.numRows())
  }

  protected def extractPositionDeletes(batch: ColumnarBatch,
                                       filePathOrdinal: Int,
                                       posOrdinal: Int): ColumnarBatch = {
    val cols = Array(filePathOrdinal, posOrdinal)
      .map(batch.column(_).asInstanceOf[GpuColumnVector])
      .safeMap(_.incRefCount().asInstanceOf[ColumnVector])

    new ColumnarBatch(cols, batch.numRows())
  }

  protected def newDeleteWriteContext(metadata: ColumnarBatch, rowId: ColumnarBatch)
  : DeleteWriteContext = {
    withResource(Seq(metadata, rowId)) { _ =>
      var ret = DeleteWriteContext(spillPartValues = SpillableColumnarBatch(
        extractToStruct(metadata, context.partitionOrdinal()),
        ACTIVE_ON_DECK_PRIORITY))

      ret = closeOnExcept(ret) { _ =>
        ret.copy(spillPosDeletes = SpillableColumnarBatch(
          extractPositionDeletes(rowId, context.fileOrdinal(), context.positionOrdinal()),
          ACTIVE_ON_DECK_PRIORITY))
      }

      ret = closeOnExcept(ret) { _ =>
        ret.copy(uniqueSpecIdCol = uniqueSpecIds(metadata, context.specIdOrdinal()))
      }

      closeOnExcept(ret) { _ =>
        ret.copy(specIdCol = metadata.column(context.specIdOrdinal())
          .asInstanceOf[GpuColumnVector]
          .getBase
          .incRefCount())
      }
    }
  }
}

case class DeleteWriteContext(
  spillPartValues: SpillableColumnarBatch = null,
  spillPosDeletes: SpillableColumnarBatch = null,
  uniqueSpecIdCol: RapidsHostColumnVector = null,
  specIdCol: CudfColumnVector = null) extends AutoCloseable {

  override def close(): Unit = {
    productIterator
      .map(_.asInstanceOf[AutoCloseable])
      .toSeq
      .safeClose()
  }
}

/**
 * GPU version of Iceberg's BasePositionDeltaWriter.
 * Combines both data writing and delete writing for merge-on-read operations.
 */
class GpuBasePositionDeltaWriter(
    private val dataWriter: PartitioningWriter[SpillableColumnarBatch, DataWriteResult],
    private val deleteWriter: PartitioningWriter[SpillableColumnarBatch, DeleteWriteResult]) {

  def writeDelete(batch: SpillableColumnarBatch, spec: PartitionSpec,
                  partition: StructLike): Unit = {
    deleteWriter.write(batch, spec, partition)
  }

  def insert(row: SpillableColumnarBatch, spec: PartitionSpec,
             partition: StructLike): Unit = {
    dataWriter.write(row, spec, partition)
  }

  def close(): Unit = {
    Seq(dataWriter, deleteWriter).safeClose()
  }

  def result(): WriteResult = {
    val dataResult = dataWriter.result()
    val deleteResult = deleteWriter.result()
    WriteResult.builder()
      .addDataFiles(dataResult.dataFiles())
      .addDeleteFiles(deleteResult.deleteFiles())
      .addReferencedDataFiles(deleteResult.referencedDataFiles())
      .build()
  }
}

/**
 * Base trait for delta writers that handle both deletes and data writes.
 * This is the GPU equivalent of Java's DeleteAndDataDeltaWriter.
 */
trait GpuDeleteAndDataDeltaWriter extends GpuDeltaWriter {
  protected val table: Table
  protected val delegate: GpuBasePositionDeltaWriter
  protected val io: FileIO
  protected val specs: mutable.Map[Integer, PartitionSpec]

  // Ordinals for extracting fields from delete records
  protected val specIdOrdinal: Int
  protected val partitionOrdinal: Int
  protected val fileOrdinal: Int
  protected val positionOrdinal: Int

  // Partition handling
  protected val tablePartitionType: IcebergTypes.StructType
  protected val tablePartitionSparkType: StructType
  protected val tablePartitionDataTypes: Array[DataType]
  protected val deletePartitionProjections: Map[Int, GpuStructProjection]

  private val partitioners: mutable.Map[Int, GpuIcebergPartitioner] = mutable.Map()

  private var closed: Boolean = false

  override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
    require(metadata != null, "Metadata batch must be non null")

    withRetryNoSplit(newDeleteWriteContext(metadata, rowId)) { deleteWriteContext =>
      withResource(deleteWriteContext.spillPartValues.getColumnarBatch()) { partValues =>
        withResource(deleteWriteContext.spillPosDeletes.getColumnarBatch()) { posDeletes =>
          val specIdCol = deleteWriteContext.uniqueSpecIdCol
          for (rowIdx <- 0 until specIdCol.getRowCount.toInt) {
            val specIdHost = specIdCol.getInt(rowIdx)
            withResource(Scalar.fromInt(specIdHost)) { specId =>
              val spec = specs(specIdHost)
              val partitioner = partitioners.getOrElseUpdate(spec.specId(),
                new GpuIcebergPartitioner(spec.partitionType(),
                  DeleteSchemaUtil.pathPosSchema().asStruct()))

              val specIdFilter = deleteWriteContext.specIdCol.equalTo(specId)

              withResource(specIdFilter) { _ =>
                val filteredPartitionValues = GpuColumnVector.filter(partValues,
                  tablePartitionDataTypes,
                  specIdFilter)

                val specProjection = withResource(filteredPartitionValues) { _ =>
                  deletePartitionProjections(specIdHost).project(filteredPartitionValues)
                }

                val partitions = withResource(specProjection) { _ =>
                  val filteredPositionDeletes = GpuColumnVector.filter(posDeletes,
                    positionDeleteDataTypes, specIdFilter)

                  if (specProjection.numCols() > 0) {
                    withResource(filteredPositionDeletes) { _ =>
                      partitioner.partition(specProjection, filteredPositionDeletes)
                    }
                  } else {
                    // Unpartitioned spec
                    Seq(ColumnarBatchWithPartition(
                      SpillableColumnarBatch(filteredPositionDeletes,
                        SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
                      emptyPartitionData
                    ))
                  }
                }

                partitions.safeConsume(p => writeDelete(p.batch, spec, p.partition))
              }
            }
          }
        }
      }
    }
  }

  protected def writeDelete(batch: SpillableColumnarBatch, spec: PartitionSpec,
                           partition: StructLike): Unit

  override def update(metadata: ColumnarBatch, rowId: ColumnarBatch,
                     row: ColumnarBatch): Unit = {
    throw new UnsupportedOperationException("Update must be represented as delete and insert")
  }

  override def commit(): WriterCommitMessage = {
    close()
    val result = delegate.result()
    GpuSparkWriteAccess.deltaTaskCommit(result)
  }

  override def abort(): Unit = {
    close()
    val result = delegate.result()
    val files = mutable.ListBuffer[ContentFile[_]]()
    files ++= result.dataFiles().map(_.asInstanceOf[ContentFile[_]])
    files ++= result.deleteFiles().map(_.asInstanceOf[ContentFile[_]])
    SparkCleanupUtil.deleteTaskFiles(io, files.asJava)
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }
}

/**
 * GPU version of DeleteOnlyDeltaWriter.
 * 
 * This implements position delete writes for merge-on-read DELETE operations.
 * Based on the CPU implementation:
 */
class GpuDeleteOnlyDeltaWriter(
    table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    deleteFileFactory: OutputFileFactory,
    override val context: GpuWriteContext) extends GpuDeltaWriter {

  private val io: FileIO = table.io()
  private val specs: mutable.Map[Integer, PartitionSpec] = table.specs().asScala
  private val partitioners: mutable.Map[Int, GpuIcebergPartitioner] = mutable.Map()

  // Ordinals for extracting fields from delete records
  private val tablePartitionType: IcebergTypes.StructType = Partitioning.partitionType(table)
  private val tablePartitionSparkType: StructType = GpuTypeToSparkType
    .toSparkType(tablePartitionType)
  private val tablePartitionDataTypes: Array[DataType] = tablePartitionSparkType
    .fields
    .map(_.dataType)

  // Delegate writer based on whether the table uses fanout or clustered writing
  // GPU writers work with SpillableColumnarBatch instead of PositionDelete objects
  private val delegate: PartitioningWriter[SpillableColumnarBatch, DeleteWriteResult] =
    newDeleteWriter(table, writerFactory, deleteFileFactory, context)
  

  // Partition projections for each spec
  private val partitionProjections: Map[Int, GpuStructProjection] =
    buildPartitionProjections(tablePartitionType, specs)
  
  private var closed: Boolean = false

  override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
    require(metadata != null, "Metadata batch must be non null for delete-only writer")

    withRetryNoSplit(newDeleteWriteContext(metadata, rowId)) { deleteWriteContext =>
      withResource(deleteWriteContext.spillPartValues.getColumnarBatch()) { partValues =>
        withResource(deleteWriteContext.spillPosDeletes.getColumnarBatch()) { posDeletes =>
          val uniqueSpecIdCol = deleteWriteContext.uniqueSpecIdCol
          for (rowIdx <- 0 until uniqueSpecIdCol.getRowCount.toInt) {
            val specIdHost = uniqueSpecIdCol.getInt(rowIdx)
            withResource(Scalar.fromInt(specIdHost)) { specId =>
              val spec = table.specs().get(specIdHost)
              val partitioner = partitioners.getOrElseUpdate(spec.specId(),
                  new GpuIcebergPartitioner(spec.partitionType(),
                    DeleteSchemaUtil.pathPosSchema().asStruct()))

              val specIdFilter = deleteWriteContext.specIdCol
                .equalTo(specId)

              withResource(specIdFilter) { _ =>
                val filteredPartitionValues = GpuColumnVector.filter(partValues,
                  tablePartitionDataTypes,
                  specIdFilter)

                val specProjection = withResource(filteredPartitionValues) { _ =>
                  partitionProjections(specIdHost).project(filteredPartitionValues)
                }

                val partitions = withResource(specProjection) { _ =>
                  val filteredPositionDeletes = GpuColumnVector.filter(posDeletes,
                    positionDeleteDataTypes, specIdFilter)

                  if (specProjection.numCols() > 0) {
                    withResource(filteredPositionDeletes) { _ =>
                      partitioner.partition(specProjection, filteredPositionDeletes)
                    }
                  } else {
                    // Unpartitioned spec
                    Seq(ColumnarBatchWithPartition(
                      SpillableColumnarBatch(filteredPositionDeletes,
                        SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
                      emptyPartitionData
                    ))
                  }
                }

                partitions.safeConsume(p => delegate.write(p.batch, spec, p.partition))
              }
            }
          }
        }
      }
    }
  }

  override def update(
      metadata: ColumnarBatch,
      rowId: ColumnarBatch, 
      row: ColumnarBatch): Unit = {
    throw new UnsupportedOperationException("Delete-only writer does not support updates")
  }

  override def insert(row: ColumnarBatch): Unit = {
    throw new UnsupportedOperationException("Delete-only writer does not support inserts")
  }

  override def commit(): WriterCommitMessage = {
    close()
    val result = delegate.result()
    GpuSparkWriteAccess.deltaTaskCommit(result)
  }

  override def abort(): Unit = {
    close()
    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.deleteFiles())
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }
}

/**
 * GPU version of UnpartitionedDeltaWriter for merge-on-read UPDATE/MERGE operations.
 * Handles both position deletes and data writes for unpartitioned tables.
 */
class GpuUnpartitionedDeltaWriter(
    protected val table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    dataFileFactory: OutputFileFactory,
    deleteFileFactory: OutputFileFactory,
    override val context: GpuWriteContext) extends GpuDeleteAndDataDeltaWriter {

  protected val io: FileIO = table.io()
  protected val specs: mutable.Map[Integer, PartitionSpec] = table.specs().asScala

  // The data spec for writing new rows
  private val dataSpec: PartitionSpec = table.spec()

  // Ordinals for extracting fields from delete records
  protected val specIdOrdinal: Int = context.specIdOrdinal()
  protected val partitionOrdinal: Int = context.partitionOrdinal()
  protected val fileOrdinal: Int = context.fileOrdinal()
  protected val positionOrdinal: Int = context.positionOrdinal()

  // Partition handling
  protected val tablePartitionType: IcebergTypes.StructType = Partitioning.partitionType(table)
  protected val tablePartitionSparkType: StructType = GpuTypeToSparkType
    .toSparkType(tablePartitionType)
  protected val tablePartitionDataTypes: Array[DataType] = tablePartitionSparkType
    .fields
    .map(_.dataType)
  protected val deletePartitionProjections: Map[Int, GpuStructProjection] =
    buildPartitionProjections(tablePartitionType, specs)

  // Create the combined position delta writer
  protected val delegate: GpuBasePositionDeltaWriter =
    newPositionDeltaWriter(table, writerFactory, dataFileFactory, deleteFileFactory, context)

  protected def writeDelete(batch: SpillableColumnarBatch, spec: PartitionSpec,
                           partition: StructLike): Unit = {
    delegate.writeDelete(batch, spec, partition)
  }

  override def insert(row: ColumnarBatch): Unit = {
    val spillBatch = closeOnExcept(row) { _ =>
      SpillableColumnarBatch(row, ACTIVE_ON_DECK_PRIORITY)
    }
    delegate.insert(spillBatch, dataSpec, null)
  }
}

/**
 * GPU version of PartitionedDeltaWriter for merge-on-read UPDATE/MERGE operations.
 * Handles both position deletes and data writes for partitioned tables.
 */
class GpuPartitionedDeltaWriter(
    protected val table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    dataFileFactory: OutputFileFactory,
    deleteFileFactory: OutputFileFactory,
    override val context: GpuWriteContext) extends GpuDeleteAndDataDeltaWriter {

  protected val io: FileIO = table.io()
  protected val specs: mutable.Map[Integer, PartitionSpec] = table.specs().asScala

  // The data spec for writing new rows
  private val dataSpec: PartitionSpec = table.spec()
  private val dataPartitioner: GpuIcebergSpecPartitioner =
    new GpuIcebergSpecPartitioner(dataSpec, table.schema().asStruct())

  // Ordinals for extracting fields from delete records
  protected val specIdOrdinal: Int = context.specIdOrdinal()
  protected val partitionOrdinal: Int = context.partitionOrdinal()
  protected val fileOrdinal: Int = context.fileOrdinal()
  protected val positionOrdinal: Int = context.positionOrdinal()

  // Partition handling
  protected val tablePartitionType: IcebergTypes.StructType = Partitioning.partitionType(table)
  protected val tablePartitionSparkType: StructType = GpuTypeToSparkType
    .toSparkType(tablePartitionType)
  protected val tablePartitionDataTypes: Array[DataType] = tablePartitionSparkType
    .fields
    .map(_.dataType)
  protected val deletePartitionProjections: Map[Int, GpuStructProjection] =
    buildPartitionProjections(tablePartitionType, specs)

  // Create the combined position delta writer
  protected val delegate: GpuBasePositionDeltaWriter =
    newPositionDeltaWriter(table, writerFactory, dataFileFactory, deleteFileFactory, context)

  protected def writeDelete(batch: SpillableColumnarBatch, spec: PartitionSpec,
                           partition: StructLike): Unit = {
    delegate.writeDelete(batch, spec, partition)
  }

  override def insert(row: ColumnarBatch): Unit = {
    // Partition the data and write each partition
    dataPartitioner.partition(row)
      .safeConsume { part =>
        delegate.insert(part.batch, dataSpec, part.partition)
      }
  }
}


case class GpuWriteContext(
  dataSchema: Schema,
  dataSparkType: StructType,
  dataFileFormat: FileFormat,
  targetDataFileSize: Long,
  deleteSparkType: StructType,
  metadataSparkType: StructType,
  deleteFileFormat: FileFormat,
  targetDeleteFileSize: Long,
  deleteGranularity: DeleteGranularity,
  queryId: String,
  useFanoutWriter: Boolean,
  inputOrdered: Boolean) {

  /**
   * Returns the ordinal of the spec ID column in the delete schema.
   * This is used to identify which partition spec the delete belongs to.
   */
  def specIdOrdinal(): Int = {
    metadataSparkType.fieldIndex(MetadataColumns.SPEC_ID.name())
  }

  /**
   * Returns the ordinal of the partition column in the delete schema.
   * Contains the partition values for the deleted rows.
   */
  def partitionOrdinal(): Int = {
    metadataSparkType.fieldIndex(MetadataColumns.PARTITION_COLUMN_NAME)
  }

  /**
   * Returns the ordinal of the file path column in the delete schema.
   * Identifies which data file the deletes apply to.
   */
  def fileOrdinal(): Int = {
    deleteSparkType.fieldIndex(MetadataColumns.FILE_PATH.name())
  }

  /**
   * Returns the ordinal of the position column in the delete schema.
   * The row position within the data file to delete.
   */
  def positionOrdinal(): Int = {
    deleteSparkType.fieldIndex(MetadataColumns.ROW_POSITION.name())
  }
}

object GpuWriteContext {
  val positionDeleteSparkType: StructType = toSparkType(DeleteSchemaUtil.pathPosSchema())

  private[iceberg] val positionDeleteDataTypes = positionDeleteSparkType.fields.map(_.dataType)
  private[iceberg] val emptyPartitionData: PartitionData =
    new PartitionData(IcebergTypes.StructType.of()) {
    override def copy(): PartitionData = {
      this
    }
  }

  /**
   * Creates a GpuWriteContext from a CPU Context object through the root-layout helper.
   */
  private[iceberg] def apply(cpu: AnyRef): GpuWriteContext = {
    val dataSchema = GpuSparkWriteAccess.contextDataSchema(cpu)
    val dataSparkType = GpuSparkWriteAccess.contextDataSparkType(cpu)
    val dataFileFormat = GpuSparkWriteAccess.contextDataFileFormat(cpu)
    val targetDataFileSize = GpuSparkWriteAccess.contextTargetDataFileSize(cpu)
    val deleteSparkType = GpuSparkWriteAccess.contextDeleteSparkType(cpu)
    val metadataSparkType = GpuSparkWriteAccess.contextMetadataSparkType(cpu)
    val deleteFileFormat = GpuSparkWriteAccess.contextDeleteFileFormat(cpu)
    val targetDeleteFileSize = GpuSparkWriteAccess.contextTargetDeleteFileSize(cpu)
    val deleteGranularity = GpuSparkWriteAccess.contextDeleteGranularity(cpu)
    val queryId = GpuSparkWriteAccess.contextQueryId(cpu)
    val useFanoutWriter = GpuSparkWriteAccess.contextUseFanoutWriter(cpu)
    val inputOrdered = GpuSparkWriteAccess.contextInputOrdered(cpu)
    
    GpuWriteContext(
      dataSchema,
      dataSparkType,
      dataFileFormat,
      targetDataFileSize,
      deleteSparkType,
      metadataSparkType,
      deleteFileFormat,
      targetDeleteFileSize,
      deleteGranularity,
      queryId,
      useFanoutWriter,
      inputOrdered)
  }
}
