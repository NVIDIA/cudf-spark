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
import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.sources.Filter

/**
 * Copy-on-write scan for the Iceberg versions that expose the V1 runtime-filter
 * contract: {@code SupportsRuntimeFiltering} with {@code filter(Array[Filter])}.
 * This covers Iceberg 1.6.x, 1.9.x, and 1.10.x. Iceberg 1.11.x switched to
 * {@code SupportsRuntimeV2Filtering} with {@code filter(Array[Predicate])} and
 * therefore ships its own per-version subclass.
 *
 * <p>Because this class depends only on the public {@code Scan} +
 * {@code SupportsRuntimeFiltering} types (Iceberg internals are reached through
 * the root-loadable {@link GpuSparkScanAccess} bridge in the base class), the V1
 * path lives once in {@code iceberg/common} and is instantiated by every
 * V1-version {@code ShimUtilsImpl} via {@link #create}, rather than being copied
 * per module.
 */
class GpuSparkCopyOnWriteV1Scan(
    cpuScanArg: Scan,
    rapidsConfArg: RapidsConf,
    queryUsesInputFileArg: Boolean)
  extends GpuSparkCopyOnWriteScanBase(cpuScanArg, rapidsConfArg, queryUsesInputFileArg)
  with SupportsRuntimeFiltering {

  private def runtimeFilterScan: SupportsRuntimeFiltering =
    cpuScan.asInstanceOf[SupportsRuntimeFiltering]

  override def filterAttributes(): Array[NamedReference] = runtimeFilterScan.filterAttributes()

  override def filter(filters: Array[Filter]): Unit = runtimeFilterScan.filter(filters)

  override def withInputFile(): GpuScan =
    new GpuSparkCopyOnWriteV1Scan(cpuScan, rapidsConf, true)
}

object GpuSparkCopyOnWriteV1Scan {
  /** Java-callable factory used by the 1.6.x / 1.9.x / 1.10.x {@code ShimUtilsImpl}. */
  def create(cpuScan: Scan, rapidsConf: RapidsConf, queryUsesInputFile: Boolean)
      : GpuSparkScan =
    new GpuSparkCopyOnWriteV1Scan(cpuScan, rapidsConf, queryUsesInputFile)
}
