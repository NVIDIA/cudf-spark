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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import _root_.io.delta.kernel.internal.SnapshotImpl
import _root_.io.delta.spark.internal.v2.read.{ProtocolMetadataAdapterV2, SparkReaderFactory, SparkScan}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.delta.common.GpuDeltaParquetFileFormatBase2
import com.nvidia.spark.rapids.parquet.GpuParquetScan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.delta.actions.{Format, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration

/** Kept in the 4.2 module: older Delta releases need not link the optional V2 connector. */
object DeltaV2ScanRule {
  private def createDeltaV2ScanMeta(
    scan: Scan,
    conf: com.nvidia.spark.rapids.RapidsConf,
    parent: Option[com.nvidia.spark.rapids.RapidsMeta[_, _, _]],
    r: com.nvidia.spark.rapids.DataFromReplacementRule) =
    new DeltaV2ScanMeta(scan, conf, parent, r)

  def rules: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val scanClass: Option[Class[_ <: Scan]] = try {
      Some(ShimReflectionUtils.loadClass(
        "io.delta.spark.internal.v2.read.SparkScan").asSubclass(classOf[Scan]))
    } catch {
      case _: ClassNotFoundException => None
      case _: NoClassDefFoundError => None
    }
    scanClass.map { cls =>
      val rule = new ScanRule[Scan](
        createDeltaV2ScanMeta,
        "Delta DataSource V2 batch scan", ClassTag(cls))
      cls -> rule
    }.toMap
  }
}

class DeltaV2ScanMeta(
    scan: Scan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends ScanMeta[Scan](scan, conf, parent, rule) {
  // Extract only immutable analysis state. Never call toBatch, estimateStatistics or plan files
  // while tagging: those calls can perform I/O and change when CPU planning errors are raised.
  private lazy val state = Try(DeltaV2ScanState(scan.asInstanceOf[SparkScan]))

  override def supportsRuntimeFilters: Boolean = true

  override def tagSelfForGpu(): Unit = {
    val session = SparkSession.active
    GpuParquetScan.tagSupport(session, scan.readSchema(), this)
    val sqlConf = session.sessionState.conf
    if (sqlConf.ignoreMissingFiles || sqlConf.ignoreCorruptFiles) {
      // Delta's SparkPartitionReader does not implement FileScanRDD's file-error suppression.
      willNotWorkOnGpu("Delta V2 file-error handling differs from the GPU multi-file reader")
    }
    val cpuOptions = scan.asInstanceOf[SparkScan].getOptions
    val unsupportedReadOptions = Seq(
      "datetimeRebaseMode", "int96RebaseMode", "ignoreMissingFiles", "ignoreCorruptFiles")
    if (unsupportedReadOptions.exists(cpuOptions.containsKey)) {
      willNotWorkOnGpu("Delta V2 per-read Parquet overrides are not supported on GPU")
    }
    var ancestor = parent
    while (ancestor.isDefined) {
      ancestor.get.wrapped match {
        case plan: SparkPlan if plan.expressions.exists(_.exists {
            case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
            case _ => false
          }) =>
          // Unlike FileScanRDD, Delta's SparkPartitionReader does not set InputFileBlockHolder.
          willNotWorkOnGpu("Delta V2 input-file expressions require the original CPU reader")
        case _ =>
      }
      ancestor = ancestor.get.parent
    }
    state match {
      case Failure(e) => willNotWorkOnGpu(s"Unsupported Delta V2 scan state: ${e.getMessage}")
      case Success(s) =>
        val supportedReaderFeatures = Set("columnMapping", "deletionVectors", "timestampNtz")
        val unsupported = s.protocol.readerFeatures.getOrElse(Set.empty) -- supportedReaderFeatures
        if (unsupported.nonEmpty) {
          willNotWorkOnGpu(s"Unsupported Delta V2 reader features: ${unsupported.toSeq.sorted}")
        }
        if (!s.metadata.format.provider.equalsIgnoreCase("parquet")) {
          willNotWorkOnGpu("Delta V2 GPU scans require Parquet files")
        }
        val cpu = scan.asInstanceOf[SparkScan]
        if (cpu.getReadDataSchema.fields.exists { field =>
            !cpu.getDataSchema.fieldNames.contains(field.name) ||
              field.name.startsWith("__delta_internal_") ||
              field.metadata.contains("__metadata_col")
          }) {
          willNotWorkOnGpu("Delta V2 generated metadata columns are not supported on GPU")
        }
        Try(s.newGpuFormat()) match {
          case Failure(e) =>
            willNotWorkOnGpu(s"Unsupported Delta V2 Parquet reader: ${e.getMessage}")
          case Success(_) =>
        }
        if (s.hasDeletionVectors &&
            (!conf.isDeltaDeletionVectorPredicatePushdownEnabled ||
              !SparkSession.active.sessionState.conf.getConf(
                DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX))) {
          willNotWorkOnGpu("Delta V2 deletion vectors require the native GPU Delta reader")
        }
    }
  }

  override def convertToGpu(): GpuScan =
    GpuDeltaV2Scan(scan.asInstanceOf[SparkScan], state.get, conf)
}

/** Version-specific bridge for fields which Delta 4.2/4.3 do not expose via accessors. */
private[delta42x] case class DeltaV2ScanState(
    protocol: Protocol,
    metadata: Metadata,
    tablePath: String,
    hasDeletionVectors: Boolean,
    dataFilters: Array[Filter],
    outputOrdinals: Array[Int]) {
  def newGpuFormat(): GpuDeltaParquetFileFormatBase2 =
    new GpuDeltaParquetFileFormatBase2(protocol, metadata,
      tablePath = if (hasDeletionVectors) Some(tablePath) else None)
}

private[delta42x] object DeltaV2ScanState {
  def apply(scan: SparkScan): DeltaV2ScanState = {
    require(scan.getClass == classOf[SparkScan], "Unexpected Delta SparkScan implementation")
    def field[T](name: String): T = {
      val f = classOf[SparkScan].getDeclaredField(name)
      f.setAccessible(true)
      f.get(scan).asInstanceOf[T]
    }
    val snapshot = field[SnapshotImpl]("initialSnapshot")
    val kp = snapshot.getProtocol
    val km = snapshot.getMetadata
    val adapter = new ProtocolMetadataAdapterV2(kp, km)
    val protocol = Protocol(kp.getMinReaderVersion, kp.getMinWriterVersion,
      if (kp.supportsReaderFeatures) Some(kp.getReaderFeatures.asScala.toSet) else None,
      if (kp.supportsWriterFeatures) Some(kp.getWriterFeatures.asScala.toSet) else None)
    // Use Delta's own schema adapter, including column-mapping and nested-field metadata.
    val parts = km.getPartitionColumns
    val metadata = Metadata(
      id = km.getId,
      name = km.getName.orElse(null),
      description = km.getDescription.orElse(null),
      format = Format(km.getFormat.getProvider, km.getFormat.getOptions.asScala.toMap),
      schemaString = adapter.getReferenceSchema.json,
      partitionColumns = (0 until parts.getSize).map(parts.getElements.getString),
      configuration = km.getConfiguration.asScala.toMap,
      createdTime = if (km.getCreatedTime.isPresent) {
        Some(km.getCreatedTime.get.longValue)
      } else {
        None
      })
    val readerFields = scan.getReadDataSchema.fields ++ scan.getPartitionSchema.fields
    val outputFields = scan.readSchema().fields
    val ordinals = outputFields.map { output =>
      readerFields.indexWhere(f => f.name == output.name && f.dataType == output.dataType)
    }
    require(readerFields.length == outputFields.length &&
      ordinals.forall(_ >= 0) && ordinals.distinct.length == ordinals.length,
      "Delta V2 output is not a permutation of the Parquet reader schema")
    DeltaV2ScanState(protocol, metadata, snapshot.getDataPath.toString,
      adapter.isDeletionVectorReadable, field[Array[Filter]]("dataFilters").clone(), ordinals)
  }
}

/** Delta retains ownership of the snapshot, Kernel scan, planning and runtime partition filters. */
private[delta42x] case class GpuDeltaV2Scan(
    cpu: SparkScan,
    state: DeltaV2ScanState,
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
  extends GpuScan with SupportsRuntimeV2Filtering with SupportsReportStatistics
    with SupportsReportPartitioning {

  override def readSchema(): StructType = cpu.readSchema()
  override def description(): String = cpu.description()
  override def estimateStatistics(): Statistics = cpu.estimateStatistics()
  override def outputPartitioning(): Partitioning = cpu match {
    case partitioned: SupportsReportPartitioning => partitioned.outputPartitioning()
    // Delta 4.3 reports no partitioning. Its ordinary FilePartition objects remain unchanged.
    case _ => new UnknownPartitioning(0)
  }
  override def filterAttributes(): Array[NamedReference] = cpu.filterAttributes()
  override def filter(predicates: Array[Predicate]): Unit = cpu.filter(predicates)
  override def supportedCustomMetrics(): Array[CustomMetric] = cpu.supportedCustomMetrics()
  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpu.reportDriverMetrics()
  override def columnarSupportMode(): Scan.ColumnarSupportMode = Scan.ColumnarSupportMode.SUPPORTED
  override def withInputFile(): GpuScan = copy(queryUsesInputFile = true)

  override def equals(other: Any): Boolean = other match {
    case that: GpuDeltaV2Scan => cpu == that.cpu && queryUsesInputFile == that.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = 31 * cpu.hashCode() + queryUsesInputFile.hashCode()

  override def toBatch(): Batch = new DeltaV2Batch(cpu.toBatch())

  private class DeltaV2Batch(val cpuBatch: Batch) extends Batch {
    val inputFileSensitive: Boolean = queryUsesInputFile

    override def equals(other: Any): Boolean = other match {
      case that: GpuDeltaV2Scan#DeltaV2Batch =>
        cpuBatch == that.cpuBatch && inputFileSensitive == that.inputFileSensitive
      case _ => false
    }

    override def hashCode(): Int = 31 * cpuBatch.hashCode() + inputFileSensitive.hashCode()

    // Keep the original partition objects (including HasPartitionKey), splits and ordering.
    override def planInputPartitions(): Array[InputPartition] = cpuBatch.planInputPartitions()

    override def createReaderFactory(): PartitionReaderFactory = {
      val spark = SparkSession.active
      val format = state.newGpuFormat()
      val filters = if (state.hasDeletionVectors) Array.empty[Filter] else state.dataFilters
      val options = cpu.getOptions.asCaseSensitiveMap.asScala.toMap
      val factory = if (rapidsConf.isParquetPerFileReadEnabled) {
        // The native reader removes deleted rows without adding internal output columns.
        new SparkReaderFactory(format.buildReaderWithPartitionValuesAndMetrics(
          spark, cpu.getDataSchema, cpu.getPartitionSchema, cpu.getReadDataSchema,
          filters.toSeq, options, cpu.getConfiguration, metrics), true)
      } else {
        val broadcastConf = spark.sparkContext.broadcast(
          new SerializableConfiguration(cpu.getConfiguration))
        format.createV2ReaderFactory(spark.sessionState.conf, broadcastConf,
          cpu.getDataSchema, cpu.getReadDataSchema, cpu.getPartitionSchema,
          filters, rapidsConf, metrics, queryUsesInputFile)
      }
      new DeltaV2PartitionReaderFactory(factory, state.outputOrdinals)
    }
  }
}

/** Unwrap only when reading; Spark must still see Delta's HasPartitionKey during planning. */
private[delta42x] class DeltaV2PartitionReaderFactory(
    delegate: PartitionReaderFactory,
    outputOrdinals: Array[Int])
  extends PartitionReaderFactory {
  private def filePartition(partition: InputPartition): FilePartition = partition match {
    case file: FilePartition => file
    // DeltaInputPartition exists in 4.2 only. Do not link it when running with Delta 4.3.
    case delta if delta.getClass.getName == "io.delta.spark.internal.v2.read.DeltaInputPartition" =>
      delta.getClass.getMethod("getFilePartition").invoke(delta).asInstanceOf[FilePartition]
    case other => throw new IllegalArgumentException(
      s"Unexpected Delta input partition: ${other.getClass.getName}")
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new IllegalStateException("GPU Delta reader cannot produce CPU rows")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val reader = delegate.createColumnarReader(filePartition(partition))
    if (outputOrdinals.indices.forall(i => outputOrdinals(i) == i)) {
      reader
    } else {
      // Like Delta 4.3's ColumnReorderReadFunction, this only permutes column references.
      // No device allocation or copy is needed, and identity layouts have no wrapper overhead.
      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = reader.next()
        override def close(): Unit = reader.close()
        override def get(): ColumnarBatch = withResource(reader.get()) { batch =>
          val columns = outputOrdinals.safeMap { i =>
            batch.column(i).asInstanceOf[GpuColumnVector].incRefCount()
          }
          closeOnExcept(columns) { retained =>
            new ColumnarBatch(retained.map(c => c: ColumnVector), batch.numRows())
          }
        }
      }
    }
  }
}
