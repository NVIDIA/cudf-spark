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
/*** spark-rapids-shim-json-lines
{"spark": "411"}
spark-rapids-shim-json-lines ***/

package org.apache.iceberg.spark.source

import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.iceberg.expressions.Expression

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsRuntimeV2Filtering}

/**
 * GPU wrapper for Iceberg 1.11's {@code SparkIncrementalAppendScan} (the
 * `.option("start-snapshot-id", ...)` / `.option("end-snapshot-id", ...)` read
 * path). This class was introduced in Iceberg 1.11 — before 1.11 the same
 * incremental-read path went through {@code SparkBatchQueryScan} (accelerated by
 * {@link GpuSparkBatchQueryScan}). It mirrors {@link GpuSparkBatchQueryScan}
 * because both CPU scans extend {@code SparkRuntimeFilterableScan} (a
 * {@code SparkPartitioningAwareScan<PartitionScanTask>}).
 *
 * <p>Takes the public {@code Scan} type and reaches Iceberg internals only through
 * the root-loadable {@link GpuSparkScanAccess} bridge and public Spark interfaces
 * ({@code SupportsRuntimeV2Filtering}). It must NOT reference the package-private
 * {@code SparkIncrementalAppendScan} directly: under {@code extraClassPath}
 * (system-classpath) the Iceberg classes load in the app classloader while this
 * shimmed class loads in Spark's MutableURLClassLoader, so any same-package access
 * would fail with {@code IllegalAccessError} (see issue #14959).
 */
class GpuSparkIncrementalAppendScan(
    override val cpuScan: Scan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean) extends
  GpuSparkPartitioningAwareScan[org.apache.iceberg.PartitionScanTask](
    cpuScan, rapidsConf, queryUsesInputFile)
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

  override def equals(obj: Any): Boolean = obj match {
    case that: GpuSparkIncrementalAppendScan =>
      this.cpuScan == that.cpuScan &&
        this.queryUsesInputFile == that.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(cpuScan, Boolean.box(queryUsesInputFile))

  override def toString: String =
    s"GpuSparkIncrementalAppendScan(table=${GpuSparkScanAccess.table(cpuScan)}, " +
      s"branch=${GpuSparkScanAccess.branch(cpuScan)}, " +
      s"type=${GpuSparkScanAccess.expectedSchema(cpuScan).asStruct()}, " +
      s"filters=${GpuSparkScanAccess.filterExpressions(cpuScan)}, " +
      s"runtimeFilters=$runtimeFilterExpressions, " +
      s"caseSensitive=${GpuSparkScanAccess.caseSensitive(cpuScan)}, " +
      s"queryUseInputFile=$queryUsesInputFile)"

  override def withInputFile(): GpuScan =
    new GpuSparkIncrementalAppendScan(cpuScan, rapidsConf, true)
}

object GpuSparkIncrementalAppendScan {
  /** Java-callable factory used by {@code IcebergProviderImpl}. Takes the public
   *  {@code Scan} type — never the package-private {@code SparkIncrementalAppendScan}. */
  def create(cpuScan: Scan, rapidsConf: RapidsConf, queryUsesInputFile: Boolean): GpuSparkScan =
    new GpuSparkIncrementalAppendScan(cpuScan, rapidsConf, queryUsesInputFile)
}
