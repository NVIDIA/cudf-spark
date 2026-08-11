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

import java.util.Objects

import com.nvidia.spark.rapids.RapidsConf
import org.apache.iceberg.FileScanTask

import org.apache.spark.sql.connector.read.{Scan, Statistics}

/**
 * Version-agnostic base for the GPU copy-on-write scan. Iceberg 1.6.x, 1.9.x,
 * and 1.10.x have {@code SparkCopyOnWriteScan} implementing
 * {@code SupportsRuntimeFiltering} with {@code filter(Filter[])}; Iceberg 1.11.x
 * switched to {@code SupportsRuntimeV2Filtering} with {@code filter(Predicate[])}.
 * The per-Iceberg-version concrete subclass lives in {@code iceberg-1-6-x} /
 * {@code iceberg-1-9-x} / {@code iceberg-1-10-x} (and {@code iceberg-1-11-x}
 * once it lands) and mixes in the matching Spark runtime-filter trait + delegates
 * {@code filter} to the matching Iceberg API.
 */
abstract class GpuSparkCopyOnWriteScanBase(
    override val cpuScan: Scan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean) extends
  GpuSparkPartitioningAwareScan[FileScanTask](cpuScan, rapidsConf, queryUsesInputFile) {

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkCopyOnWriteScanBase =>
        this.cpuScan == that.cpuScan &&
          this.queryUsesInputFile == that.queryUsesInputFile
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuScan, Boolean.box(queryUsesInputFile))
  }

  override def toString: String = {
    s"GpuSparkCopyOnWriteScan(table=${GpuSparkScanAccess.table(cpuScan)}, " +
      s"branch=${GpuSparkScanAccess.branch(cpuScan)}, " +
      s"type=${GpuSparkScanAccess.expectedSchema(cpuScan).asStruct()}, " +
      s"filters=${GpuSparkScanAccess.filterExpressions(cpuScan)}, " +
      s"caseSensitive=${GpuSparkScanAccess.caseSensitive(cpuScan)}, " +
      s"queryUseInputFile=$queryUsesInputFile)"
  }
}
