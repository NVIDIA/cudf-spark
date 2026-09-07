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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.iceberg.ScanTaskGroup
import org.apache.iceberg.types.Types

import org.apache.spark.sql.connector.read.Scan

/** GPU scan for file groups staged by Iceberg's rewrite_data_files action. */
class GpuSparkStagedScan(
    override val cpuScan: Scan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean)
  extends GpuSparkScan(cpuScan, rapidsConf, queryUsesInputFile) {

  override def groupingKeyType(): Types.StructType =
    GpuSparkScanAccess.groupingKeyType(cpuScan)

  override def taskGroups(): Seq[_ <: ScanTaskGroup[_]] =
    GpuSparkScanAccess.taskGroups(cpuScan).asScala.toSeq

  override def withInputFile(): GpuScan =
    new GpuSparkStagedScan(cpuScan, rapidsConf, true)

  override def toString: String =
    s"GpuSparkStagedScan(table=${GpuSparkScanAccess.table(cpuScan)}, " +
      s"type=${GpuSparkScanAccess.expectedSchema(cpuScan).asStruct()}, " +
      s"queryUseInputFile=$queryUsesInputFile)"
}
