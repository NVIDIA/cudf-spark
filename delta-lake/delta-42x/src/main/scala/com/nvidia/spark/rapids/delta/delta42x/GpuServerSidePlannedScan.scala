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

package com.nvidia.spark.rapids.delta.delta42x

import java.lang.reflect.InvocationTargetException

import scala.util.control.NonFatal

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.parquet.{GpuParquetMultiFilePartitionReaderFactory,
  GpuParquetPartitionReaderFactory, GpuParquetScan}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.delta.serverSidePlanning.{ScanPlan, ServerSidePlannedFileInputPartition,
  ServerSidePlannedScan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/** Delta 4.2/4.3 have no public accessors for the reader schema or the cached plan. */
private[delta42x] class ServerSidePlannedScanAccess(scan: ServerSidePlannedScan) {
  private def field(name: String): java.lang.reflect.Field = {
    val result = classOf[ServerSidePlannedScan].getDeclaredField(name)
    result.setAccessible(true)
    result
  }

  val spark: SparkSession = field("spark").get(scan).asInstanceOf[SparkSession]
  val tableSchema: StructType = field("tableSchema").get(scan).asInstanceOf[StructType]
  private val planMethod = classOf[ServerSidePlannedScan].getDeclaredMethod("scanPlan")
  planMethod.setAccessible(true)

  // Invoke the lazy accessor, never its backing field: planning must run exactly once,
  // including when the reader factory is requested before the input partitions.
  def plan: ScanPlan = {
    try {
      planMethod.invoke(scan).asInstanceOf[ScanPlan]
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }
}

class ServerSidePlannedScanMeta(
    scan: ServerSidePlannedScan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ScanMeta[ServerSidePlannedScan](scan, conf, parent, rule) {

  private var access: Option[ServerSidePlannedScanAccess] = None

  private def hasRequiredField(dataType: DataType): Boolean = dataType match {
    case schema: StructType =>
      schema.fields.exists(f => !f.nullable || hasRequiredField(f.dataType))
    case ArrayType(elementType, _) => hasRequiredField(elementType)
    case MapType(keyType, valueType, _) => hasRequiredField(keyType) || hasRequiredField(valueType)
    case _ => false
  }

  override def tagSelfForGpu(): Unit = {
    access = try {
      Some(new ServerSidePlannedScanAccess(scan))
    } catch {
      case NonFatal(_) =>
        willNotWorkOnGpu("Delta server-side planned scan reader state is not accessible")
        None
    }
    access.foreach { a =>
      GpuParquetScan.tagSupport(a.spark, scan.readSchema(), this)
      // Delta reads the authoritative manifest without Spark's file-skipping wrapper.
      if (a.spark.sessionState.conf.ignoreMissingFiles ||
          a.spark.sessionState.conf.ignoreCorruptFiles) {
        willNotWorkOnGpu("Delta server-side planned reads must fail on missing or corrupt files")
      }
    }
    // GPU readers synthesize nulls for absent fields, whereas Delta's CPU reader fails
    // for required fields. Conservatively fall back without reading file footers here.
    if (hasRequiredField(scan.readSchema())) {
      willNotWorkOnGpu("Delta server-side planned reads with required fields are not supported")
    }
    // Delta's reader does not set InputFileBlockHolder. GPU file readers do, so using
    // them below input_file_* expressions would change the query's results.
    var ancestor = parent
    while (ancestor.isDefined) {
      val current = ancestor.get
      current.wrapped match {
        case _: ShuffleExchangeLike | _: BroadcastExchangeLike => ancestor = None
        case plan: SparkPlan =>
          if (InputFileBlockRule.hasInputFileExpression(plan)) {
            willNotWorkOnGpu("Delta server-side planned reads do not populate input-file metadata")
          }
          ancestor = current.parent
        case _ => ancestor = current.parent
      }
    }
  }

  override def convertToGpu(): GpuScan =
    GpuServerSidePlannedScan(scan, access.get, conf)
}

/** Preserve Delta's planning and partition boundaries; substitute only the Parquet reader. */
private[delta42x] case class GpuServerSidePlannedScan(
    cpuScan: ServerSidePlannedScan,
    access: ServerSidePlannedScanAccess,
    rapidsConf: RapidsConf) extends GpuScan with Batch {

  override def readSchema(): StructType = cpuScan.readSchema()
  override def description(): String = cpuScan.description()
  override def toBatch: Batch = this
  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED
  override def planInputPartitions(): Array[InputPartition] = cpuScan.planInputPartitions()
  override def withInputFile(): GpuScan = this

  override def equals(other: Any): Boolean = other match {
    case that: GpuServerSidePlannedScan => cpuScan == that.cpuScan
    case _ => false
  }

  override def hashCode(): Int = cpuScan.hashCode()

  override def createReaderFactory(): PartitionReaderFactory = {
    val spark = access.spark
    val plan = access.plan
    val hadoopConf = spark.sessionState.newHadoopConf()
    plan.credentials.foreach(_.configure(hadoopConf))
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val reader = if (rapidsConf.isParquetPerFileReadEnabled) {
      GpuParquetPartitionReaderFactory(spark.sessionState.conf, broadcastConf,
        access.tableSchema, readSchema(), StructType(Nil), Array.empty, rapidsConf, metrics,
        Map.empty)
    } else {
      GpuParquetMultiFilePartitionReaderFactory(spark.sessionState.conf, broadcastConf,
        access.tableSchema, readSchema(), StructType(Nil), Array.empty, rapidsConf,
        ThreadPoolConfBuilder(rapidsConf), metrics, queryUsesInputFile = false)
    }
    new ServerSidePlannedGpuReaderFactory(reader)
  }
}

private[delta42x] class ServerSidePlannedGpuReaderFactory(val reader: PartitionReaderFactory)
  extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new IllegalStateException("GPU Parquet scan cannot read rows")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val file = partition.asInstanceOf[ServerSidePlannedFileInputPartition]
    if (!file.fileFormat.equalsIgnoreCase("parquet")) {
      throw new UnsupportedOperationException(
        s"File format '${file.fileFormat}' is not supported. Only Parquet is supported.")
    }
    val partitionedFile = PartitionedFile(
      partitionValues = InternalRow.empty,
      filePath = SparkPath.fromPathString(file.filePath),
      start = 0,
      length = file.fileSizeInBytes)
    reader.createColumnarReader(FilePartition(0, Array(partitionedFile)))
  }
}
