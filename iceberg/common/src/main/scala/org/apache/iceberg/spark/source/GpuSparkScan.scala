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
import com.nvidia.spark.rapids.iceberg.{IcebergEncryptionSupport, IcebergFormatVersionSupport, ShimUtils}
import com.nvidia.spark.rapids.parquet.GpuParquetScan
import com.nvidia.spark.rapids.perf.{ExecutionEndCallbacks, PlanKey, ScanContext, ScanHistoryKey,
  ScanSplitHeuristic, SplitTotals}
import org.apache.iceberg.{MetadataColumns, ScanTaskGroup}
import org.apache.iceberg.spark.GpuSparkReadConf
import org.apache.iceberg.types.Types

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


abstract class GpuSparkScan(val cpuScan: Scan,
    val rapidsConf: RapidsConf,
    val queryUsesInputFile: Boolean,
) extends GpuScan with SupportsReportStatistics {
  private val readConf: GpuSparkReadConf = new GpuSparkReadConf(
    GpuSparkScanAccess.readConf(cpuScan))

  def readTimestampWithoutZone: Boolean = readConf.handleTimestampWithoutZone()

  override def readSchema(): StructType = cpuScan.readSchema()

  // Spark input-file expressions use the current split's path and offsets for the whole batch.
  override lazy val coalesceAfter: Boolean = !queryUsesInputFile && projectsMetadataOrIdentityColumn

  private def projectsMetadataOrIdentityColumn: Boolean = {
    val schema = GpuSparkScanAccess.expectedSchema(cpuScan)
    val columns = schema.columns().asScala
    if (columns.isEmpty) {
      false
    } else if (columns.exists(field => MetadataColumns.isMetadataColumn(field.fieldId()))) {
      true
    } else {
      // Older files can still supply an identity value after the table's partition spec changes.
      val specs = GpuSparkScanAccess.table(cpuScan).specs().values().asScala
      specs.exists { spec =>
        spec.fields().asScala.exists { field =>
          field.transform().isIdentity() && schema.findField(field.sourceId()) != null
        }
      }
    }
  }

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  override def toBatch: Batch = {
    new GpuSparkBatch(GpuSparkScanAccess.toBatch(cpuScan), this)
  }

  /** History key at this planning (runtime filters can change it); None when history is off. */
  def historyKey(): Option[PlanKey] = ScanSplitHeuristic.historyKey {
    // Only batch query scans hold runtime filters as expressions.
    val runtimeFilters =
      if (GpuSparkScanAccess.isBatchQueryScan(cpuScan)) {
        Option(GpuSparkScanAccess.runtimeFilterExpressions(cpuScan))
          .map(_.asScala.toSeq).getOrElse(Seq.empty)
      } else {
        Seq.empty
      }
    IcebergPlanKey.of(GpuSparkScanAccess.table(cpuScan).name(), readSchema(),
      GpuSparkScanAccess.filterExpressions(cpuScan).asScala.toSeq, runtimeFilters)
  }

  /** Bytes of the splits read; None when history is off. */
  @transient lazy val splitTotals: Option[SplitTotals] =
    if (ScanSplitHeuristic.instance.isEnabled) {
      Some(SplitTotals.create(SparkContext.getOrCreate()))
    } else {
      None
    }

  /** Only the newest planning is recorded. */
  @transient @volatile private var latestPlanning = 0

  /** Queues this planning for recording when its query ends. */
  def registerPlanning(planned: Int, listedBytes: => Long, key: Option[PlanKey]): Unit =
    for (totals <- splitTotals; scanKey <- key; sized <- ScanHistoryKey.of(scanKey, listedBytes)) {
      val planning = synchronized {
        latestPlanning += 1
        latestPlanning
      }
      ScanSplitHeuristic.instance.register(
        ExecutionEndCallbacks.currentExecutionId(SparkContext.getOrCreate()),
        ScanContext.forObservation(sized, planned, () => totals.drain())
          .copy(isCurrent = () => planning == latestPlanning))
    }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "GpuSparkScan does not support toMicroBatchStream()")

  override def description(): String = cpuScan.description()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpuScan.reportDriverMetrics()

  override def supportedCustomMetrics(): Array[CustomMetric] = cpuScan.supportedCustomMetrics()

  protected def groupingKeyType(): Types.StructType

  protected def taskGroups(): Seq[_ <: ScanTaskGroup[_]]

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
      IcebergFormatVersionSupport.tagForScan(
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
      case Success(_) =>
        IcebergEncryptionSupport.tagForScan(meta.wrapped, meta)
      case Failure(e) => meta.willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
    }
  }
}
