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

package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeV2Filtering}

/**
 * Iceberg 1.11.x copy-on-write scan: {@code SupportsRuntimeV2Filtering} with
 * {@code filter(Array[Predicate])}.
 */
class GpuSparkCopyOnWriteScan(
    cpuScanArg: Scan,
    rapidsConfArg: RapidsConf,
    queryUsesInputFileArg: Boolean)
  extends GpuSparkCopyOnWriteScanBase(cpuScanArg, rapidsConfArg, queryUsesInputFileArg)
  with SupportsRuntimeV2Filtering {

  private def runtimeFilterScan: SupportsRuntimeV2Filtering =
    cpuScan.asInstanceOf[SupportsRuntimeV2Filtering]

  override def filterAttributes(): Array[NamedReference] = runtimeFilterScan.filterAttributes()

  override def filter(predicates: Array[Predicate]): Unit = runtimeFilterScan.filter(predicates)

  override def withInputFile(): GpuScan =
    new GpuSparkCopyOnWriteScan(cpuScan, rapidsConf, true)
}

object GpuSparkCopyOnWriteScan {
  /** Java-callable factory used by {@code ShimUtilsImpl.newCopyOnWriteScan}. */
  def create(cpuScan: Scan, rapidsConf: RapidsConf, queryUsesInputFile: Boolean)
      : GpuSparkScan =
    new GpuSparkCopyOnWriteScan(cpuScan, rapidsConf, queryUsesInputFile)
}
