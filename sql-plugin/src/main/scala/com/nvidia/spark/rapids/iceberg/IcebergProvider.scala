/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.{GpuExec, GpuExpression, ScanRule, ShimLoaderTemp, SparkPlanMeta, StaticInvokeMeta}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan

/** Interfaces to avoid accessing the optional Apache Iceberg jars directly in common code. */
trait IcebergProvider {
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]

  def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit
  def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression

  def isSupportedWrite(write: Class[_ <: Write]): Boolean
  def isSupportedCatalog(catalogClass: Class[_]): Boolean

  def tagForGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): Unit
  def convertToGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): GpuExec
}

/**
 * Probe to detect the Iceberg version at runtime.
 * Loaded via ShimReflectionUtils with a fixed class name (independent of shimPackage).
 */
trait IcebergProbe {
  def isSupportedSparkVersion(): Boolean
  def getDetectedVersion: String
  def shimPackage: String
  def getProvider: IcebergProvider
}

object IcebergProvider {
  val cpuBatchQueryScanClassName: String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
  val cpuCopyOnWriteScanClassName: String = "org.apache.iceberg.spark.source.SparkCopyOnWriteScan"

  private lazy val probe: IcebergProbe =
    ShimLoaderTemp.newIcebergProbe()

  def apply(): IcebergProvider = probe.getProvider

  /**
   * Returns the exact Iceberg version string (e.g. "1.6.1", "1.9.2", "1.10.1")
   * detected from the runtime jar.
   */
  lazy val detectedVersion: String = probe.getDetectedVersion

  /**
   * Returns the version-specific sub-package for the current Spark + Iceberg combination.
   * Used by [[ShimUtils]] to load the correct shim classes.
   */
  lazy val shimPackage: String = probe.shimPackage

  def isSupportedSparkVersion(): Boolean = probe.isSupportedSparkVersion()
}

object NoIcebergProvider extends IcebergProvider {
  private def unsupported: Nothing =
    throw new UnsupportedOperationException("Iceberg is not supported in this configuration")

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Map.empty
  override def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit = unsupported
  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = unsupported
  override def isSupportedWrite(write: Class[_ <: Write]): Boolean = false
  override def isSupportedCatalog(catalogClass: Class[_]): Boolean = false
  override def tagForGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](
      cpuExec: P, meta: M): Unit = unsupported
  override def convertToGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](
      cpuExec: P, meta: M): GpuExec = unsupported
}
