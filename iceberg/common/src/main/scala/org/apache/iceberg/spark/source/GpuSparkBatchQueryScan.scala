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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.iceberg.PartitionScanTask
import org.apache.iceberg.expressions.Expression

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsRuntimeV2Filtering}

class GpuSparkBatchQueryScan(
    override val cpuScan: Scan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean) extends
  GpuSparkPartitioningAwareScan[PartitionScanTask](cpuScan, rapidsConf, queryUsesInputFile)
  with SupportsRuntimeV2Filtering {

  private val runtimeFilterExpressions: List[Expression] =
    GpuSparkScanAccess.runtimeFilterExpressions(cpuScan)
    .asScala
    .toList

  private def runtimeFilterScan: SupportsRuntimeV2Filtering =
    cpuScan.asInstanceOf[SupportsRuntimeV2Filtering]

  override def filterAttributes(): Array[NamedReference] = runtimeFilterScan.filterAttributes()

  override def filter(predicates: Array[Predicate]): Unit = runtimeFilterScan.filter(predicates)

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkBatchQueryScan =>
        this.cpuScan == that.cpuScan &&
          this.queryUsesInputFile == that.queryUsesInputFile
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuScan, Boolean.box(queryUsesInputFile))
  }

  override def toString: String = {
    s"GpuSparkBatchQueryScan(table=${GpuSparkScanAccess.table(cpuScan)}, )" +
      s"branch=${GpuSparkScanAccess.branch(cpuScan)}, " +
      s"type=${GpuSparkScanAccess.expectedSchema(cpuScan).asStruct()}, " +
      s"filters=${GpuSparkScanAccess.filterExpressions(cpuScan)}, " +
      s"runtimeFilters=$runtimeFilterExpressions, " +
      s"caseSensitive=${GpuSparkScanAccess.caseSensitive(cpuScan)}, " +
      s"queryUseInputFile=$queryUsesInputFile"
  }

  /** Create a version of this scan with input file name support */
  override def withInputFile(): GpuScan = {
    new GpuSparkBatchQueryScan(cpuScan, rapidsConf, true)
  }
}
