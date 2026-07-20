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

package com.nvidia.spark.rapids.iceberg.iceberg111x

import scala.reflect.ClassTag
import scala.util.Try

import com.nvidia.spark.rapids.{GpuScan, ScanMeta, ScanRule, ShimReflectionUtils}
import com.nvidia.spark.rapids.iceberg.IcebergProviderBase
import org.apache.iceberg.spark.source.{GpuSparkIncrementalAppendScan, GpuSparkScan}

import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeV2Filtering}

class IcebergProviderImpl extends IcebergProviderBase {

  /**
   * Adds a {@code SparkIncrementalAppendScan} rule on top of the base provider's two rules
   * ({@code SparkBatchQueryScan}, {@code SparkCopyOnWriteScan}). The incremental-append scan
   * is a 1.11-only class — before 1.11 the same query path went through
   * {@code SparkBatchQueryScan} and was matched by the base rule. The CPU class is loaded
   * by string here because it is package-private and not directly referenceable from
   * outside {@code org.apache.iceberg.spark.source}.
   */
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val cpuIncrementalAppendScanClass = ShimReflectionUtils.loadClass(
      "org.apache.iceberg.spark.source.SparkIncrementalAppendScan")

    val incrementalRule = new ScanRule[Scan](
      (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
        private lazy val convertedScan: Try[GpuSparkScan] = Try(
          GpuSparkIncrementalAppendScan.create(a, this.conf, false))

        override def supportsRuntimeFilters: Boolean =
          a.isInstanceOf[SupportsRuntimeV2Filtering]

        override def tagSelfForGpu(): Unit = {
          GpuSparkScan.tagForGpu(this, convertedScan)
        }

        override def convertToGpu(): GpuScan = convertedScan.get
      },
      "Iceberg incremental append scan",
      ClassTag(cpuIncrementalAppendScanClass)
    )

    super.getScans + (
      cpuIncrementalAppendScanClass.asSubclass(classOf[Scan]) -> incrementalRule)
  }
}
