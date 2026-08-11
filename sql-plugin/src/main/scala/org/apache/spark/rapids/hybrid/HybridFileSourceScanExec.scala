/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.hybrid

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuExec, GpuMetric, RapidsConf, TargetSize}
import com.nvidia.spark.rapids.hybrid.NativeBackendApis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.rapids.GpuDataSourceScanExec
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The SparkPlan for HybridParquetScan which does the same job as GpuFileSourceScanExec but in a
 * different approach. Therefore, this class is similar to GpuFileSourceScanExec in a lot of way.
 */
case class HybridFileSourceScanExec(originPlan: FileSourceScanExec
                                   )(@transient val rapidsConf: RapidsConf)
  extends GpuDataSourceScanExec with GpuExec {
  import GpuMetric._

  require(originPlan.relation.fileFormat.getClass == classOf[ParquetFileFormat],
    "HybridScan only supports ParquetFormat")

  logInfo(s"using HybridScan over HadoopFsRelation: $originPlan")

  override val nodeName: String = {
    s"HybridScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def relation: BaseRelation = originPlan.relation

  override def tableIdentifier: Option[TableIdentifier] = originPlan.tableIdentifier

  // All expressions are on the CPU.
  override def gpuExpressions: Seq[Expression] = Nil

  @transient private lazy val nativePlan: SparkPlan = {
    NativeBackendApis.overrideFileSourceScanExec(originPlan)
  }

  private val coalesceSizeGoal = rapidsConf.gpuTargetBatchSizeBytes

  override def output: Seq[Attribute] = nativePlan.output

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")

    val location = originPlan.relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        GpuDataSourceScanExec.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    Map(
      "HybridScan" -> "enabled",
      "Format" -> originPlan.relation.fileFormat.toString,
      "ReadSchema" -> originPlan.requiredSchema.catalogString,
      "Batched" -> supportsColumnar.toString,
      "PartitionFilters" -> seqToString(originPlan.partitionFilters),
      "DataFilters" -> seqToString(originPlan.dataFilters),
      "Location" -> locationDesc,
    )
  }

  lazy val inputRDD: RDD[InternalRow] = {
    // execute the embedded CPU Native SparkPlan
    val cpuScanRDD = NativeBackendApis.executeNativePlan(nativePlan)
    logInfo(s"Using HybridScan to read parquet: ${metadata.toString()}")

    // Narrow down all used metrics on the Executor side, in case transferring unnecessary metrics
    val embeddedMetrics = {
      val mapBuilder = mutable.Map.empty[String, GpuMetric]
      hybridCommonMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
      nativeMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
      preloadMetrics.keys.foreach(key => mapBuilder += key -> allMetrics(key))
      mapBuilder.toMap
    }

    new HybridParquetScanRDD(cpuScanRDD,
      output,
      originPlan.requiredSchema,
      TargetSize(coalesceSizeGoal),
      rapidsConf.hybridParquetPreloadBatches,
      embeddedMetrics
    )
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  /**
   * Note: Databricks SparkPlan has a lazy value `commonMetrics`, here use another name
   */
  private val hybridCommonMetrics: Map[String, () => GpuMetric] = Map[String, () => GpuMetric](
    "HybridScanTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "HybridScanTime")),
    "GpuAcquireTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "GpuAcquireTime")),
  )
  private val nativeMetrics: Map[String, () => GpuMetric] = Map[String, () => GpuMetric](
    "CoalescedBatches" -> (() => createMetric(MODERATE_LEVEL, "CoalescedBatches")),
    "CpuReaderBatches" -> (() => createMetric(MODERATE_LEVEL, "CpuReaderBatches")),
    "C2COutputSize" -> (() => createSizeMetric(MODERATE_LEVEL, "C2COutputSize")),
    "C2CTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "C2CTime")),
    "PageableH2DTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "PageableH2DTime")),
    "PinnedH2DTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "PinnedH2DTime")),
    "PageableH2DSize" -> (() => createSizeMetric(MODERATE_LEVEL, "PageableH2DSize")),
    "PinnedH2DSize" -> (() => createSizeMetric(MODERATE_LEVEL, "PinnedH2DSize")),
  )
  private val preloadMetrics: Map[String, () => GpuMetric] = {
    if (rapidsConf.hybridParquetPreloadBatches > 0) {
      Map("preloadWaitTime" -> (() => createNanoTimingMetric(MODERATE_LEVEL, "preloadWaitTime")))
    } else {
      Map.empty
    }
  }

  override lazy val allMetrics: Map[String, GpuMetric] = {
    val mapBuilder = Map.newBuilder[String, GpuMetric]
    mapBuilder += OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)
    mapBuilder += SCAN_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, "TotalTime")
    // Add common embedded metrics
    hybridCommonMetrics.foreach { case (key, generator) =>
      mapBuilder += key -> generator()
    }
    // NativeConverter metrics
    nativeMetrics.foreach { case (key, generator) =>
      mapBuilder += key -> generator()
    }
    // Preloading related metrics
    preloadMetrics.foreach { case (key, generator) =>
      mapBuilder += key -> generator()
    }
    // Expose all metrics of the underlying CpuNativeScanExec
    nativePlan.metrics.foreach { case (key, metric) =>
      mapBuilder += s"Hybrid_$key" -> GpuMetric.wrap(metric)
    }
    mapBuilder.result()
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val scanTime = gpuLongMetric(SCAN_TIME)
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          scanTime.ns {
            val hasNext = batches.hasNext
            hasNext
          }
        }

        override def next(): ColumnarBatch = {
          val startNs = System.nanoTime()
          val batch = batches.next()
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          batch
        }
      }
    }
  }

  override def doCanonicalize(): HybridFileSourceScanExec = {
    HybridFileSourceScanExec(
      originPlan.canonicalized.asInstanceOf[FileSourceScanExec])(rapidsConf)
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)
}
