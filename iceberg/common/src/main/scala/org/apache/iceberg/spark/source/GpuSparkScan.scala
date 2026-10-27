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
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.iceberg.{IcebergEncryptionSupport, IcebergFormatVersionSupport, ShimUtils}
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderUtils
import com.nvidia.spark.rapids.parquet.GpuParquetScan
import org.apache.iceberg.{FileFormat, FileScanTask, ScanTaskGroup, TableProperties}
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.mapping.NameMappingParser
import org.apache.iceberg.parquet.GpuParquetIO
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.spark.GpuSparkReadConf
import org.apache.iceberg.types.Types

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.StructType


abstract class GpuSparkScan(val cpuScan: Scan,
    val rapidsConf: RapidsConf,
    val queryUsesInputFile: Boolean,
) extends GpuScan with SupportsReportStatistics {
  private val readConf: GpuSparkReadConf = new GpuSparkReadConf(
    GpuSparkScanAccess.readConf(cpuScan))

  def readTimestampWithoutZone: Boolean = readConf.handleTimestampWithoutZone()

  override def readSchema(): StructType = cpuScan.readSchema()

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  override def toBatch: Batch = {
    new GpuSparkBatch(GpuSparkScanAccess.toBatch(cpuScan), this)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "GpuSparkScan does not support toMicroBatchStream()")

  override def description(): String = cpuScan.description()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpuScan.reportDriverMetrics()

  override def supportedCustomMetrics(): Array[CustomMetric] = cpuScan.supportedCustomMetrics()

  protected def groupingKeyType(): Types.StructType

  protected def taskGroups(): Seq[_ <: ScanTaskGroup[_]]

  // Iceberg can read shredded Variant independently of Spark's Parquet setting. Check actual
  // files: current write properties do not describe files already present in the snapshot.
  // Cache this per scan and inspect each file only once, even when it has multiple splits.
  private lazy val variantLayoutSupport: Try[Unit] = Try {
    val table = GpuSparkScanAccess.table(cpuScan)
    val requiredSchema = GpuSparkScanAccess.expectedSchema(cpuScan)
    val nameMapping = Option(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING))
      .map(NameMappingParser.fromJson)
    val sqlConf = SQLConf.get
    val converter = new ParquetToSparkSchemaConverter(
      sqlConf.isParquetBinaryAsString,
      sqlConf.isParquetINT96AsTimestamp,
      GpuSparkScanAccess.caseSensitive(cpuScan),
      sqlConf.parquetInferTimestampNTZEnabled,
      sqlConf.legacyParquetNanosAsLong)
    val checkedFiles = scala.collection.mutable.HashSet.empty[String]
    taskGroups().iterator.flatMap(_.tasks().asScala).foreach { task =>
      val file = task match {
        case fileTask: FileScanTask => fileTask.file()
        case _ => throw new UnsupportedOperationException("Variant input requires a file scan task")
      }
      val location = ShimUtils.locationOf(file)
      if (checkedFiles.add(location)) {
        require(file.format() == FileFormat.PARQUET, "Variant input must be Parquet")
        val input = table.encryption().decrypt(EncryptedFiles.encryptedInput(
          table.io().newInputFile(location, file.fileSizeInBytes()), file.keyMetadata()))
        val options = GpuIcebergParquetReaderUtils.buildReaderOptions(input, None)
        withResource(ParquetFileReader.open(GpuParquetIO.file(input), options)) { reader =>
          val (_, projectedSchema) = GpuParquetIO.projectSchema(
            reader.getFileMetaData.getSchema, requiredSchema, nameMapping)
          // Use the execution reader's strict conversion: it rejects shredded Variant layouts
          // before any GPU task runs. Fail closed if a footer cannot be checked.
          converter.convert(ShimUtils.getParquetUnshader().unshade(projectedSchema))
        }
      }
    }
  }

  def hasNestedType: Boolean = {
    GpuSparkScanAccess.expectedSchema(cpuScan)
      .asStruct()
      .fields()
      .asScala
      .exists { field => field.`type`().isNestedType }
  }
}


object GpuSparkScan {
  def isMetadataScan(scan: Scan): Boolean = {
    GpuSparkScanAccess.isMetadataScan(scan)
  }

  def tryConvert(cpuScan: Scan, rapidsConf: RapidsConf): Try[GpuSparkScan] = {
    Try {
      if (GpuSparkScanAccess.isBatchQueryScan(cpuScan)) {
        new GpuSparkBatchQueryScan(cpuScan, rapidsConf, false)
      } else if (GpuSparkScanAccess.isCopyOnWriteScan(cpuScan)) {
        ShimUtils.newCopyOnWriteScan(cpuScan, rapidsConf, false)
      } else {
        throw new IllegalArgumentException(
          s"Currently iceberg support only supports batch query scan and copy-on-write scan, " +
            s"but got ${cpuScan.getClass.getName}")
      }
    }
  }

  def tagForGpu(meta: ScanMeta[Scan], gpuScan: Try[GpuSparkScan]): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergReadEnabled) {
      meta.willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
    }

    Try {
      IcebergFormatVersionSupport.tagForFormatVersion(
        GpuSparkScanAccess.table(meta.wrapped), meta)
    } match {
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining Iceberg table version: $e")
      case _ =>
    }

    val readSchema = meta.wrapped.readSchema()
    GpuParquetScan.tagVariantSupport(readSchema, meta, SQLConf.get)
    FileFormatChecks.tag(meta, readSchema, IcebergFormatType, ReadFileOp)

    Try {
      GpuSparkScan.isMetadataScan(meta.wrapped)
    } match {
      case Success(true) => meta.willNotWorkOnGpu("scan is a metadata scan")
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
      case _ =>
    }

    gpuScan match {
      case Success(scan) =>
        IcebergEncryptionSupport.tagForScan(meta.wrapped, meta)
        if (meta.canThisBeReplaced && readSchema.exists { field =>
          TrampolineUtil.dataTypeExistsRecursively(field.dataType, GpuColumnVector.isVariantType)
        }) {
          scan.variantLayoutSupport match {
            case Failure(e) =>
              GpuParquetScan.tagVariantScanPrefixForCpu(meta,
                s"GPU Iceberg reader requires unshredded Variant input: ${e.getMessage}")
            case _ =>
          }
        }
      case Failure(e) => meta.willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
    }
  }
}
