/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.{GpuAlias, GpuColumnarToRowExec, GpuExec, GpuProjectExec, GpuRowToColumnarExec, RapidsConf, TargetSize}

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, NamedExpression}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.constraints.{Constraint, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Clock

/** Common type from which all open-source Delta Lake implementations derive. */
abstract class AbstractGpuOptimisticTransactionBase(
    deltaLog: DeltaLog,
    catalog: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)
  (implicit clock: Clock)
  extends ShimOptimisticTransaction(deltaLog, catalog, snapshot)(clock)
    with DeltaLogging {

  def this(deltaLog: DeltaLog, catalog: Option[CatalogTable], snapshot: Option[Snapshot],
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    // It is better to pass just the Option[Snapshot] as it is, so that it can be computed
    // in the constructor of OptimisticTransaction. However, the constructor that takes
    // an Option[Snapshot] is reletively new and not available in old versions of Delta.
    // To avoid breaking compatibility, we compute the snapshot here if it is not provided.
    this(deltaLog, catalog, snapshot.getOrElse(deltaLog.update()), rapidsConf)(clock)
  }

  /**
   * Adds checking of constraints on the table
   *
   * @param plan        Plan to generate the table to check against constraints
   * @param constraints Constraints to check on the table
   * @return GPU columnar plan to execute
   */
  protected def addInvariantChecks(plan: SparkPlan, constraints: Seq[Constraint]): SparkPlan = {
    val cpuInvariants =
      DeltaInvariantCheckerExec.buildInvariantChecks(plan.output, constraints, plan.session)
    GpuCheckDeltaInvariant.maybeConvertToGpu(cpuInvariants, rapidsConf) match {
      case Some(gpuInvariants) =>
        val gpuPlan = convertToGpu(plan)
        GpuDeltaInvariantCheckerExec(gpuPlan, gpuInvariants)
      case None =>
        val cpuPlan = convertToCpu(plan)
        // Delegate to per-version shim because the CPU exec constructor differs across versions
        ShimDeltaInvariantCheckerExec(cpuPlan, constraints, cpuInvariants)
    }
  }

  /** GPU version of convertEmptyToNullIfNeeded */
  private def gpuConvertEmptyToNullIfNeeded(
      plan: GpuExec,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (!spark.conf.get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        GpuAlias(GpuEmpty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) GpuProjectExec(projectList.toList, plan) else plan
  }

  /**
   * If there is any string partition column and there are constraints defined, add a projection to
   * convert empty string to null for that column. The empty strings will be converted to null
   * eventually even without this convert, but we want to do this earlier before check constraints
   * so that empty strings are correctly rejected. Note that this should not cause the downstream
   * logic in `FileFormatWriter` to add duplicate conversions because the logic there checks the
   * partition column using the original plan's output. When the plan is modified with additional
   * projections, the partition column check won't match and will not add more conversion.
   *
   * @param plan        The original SparkPlan.
   * @param partCols    The partition columns.
   * @param constraints The defined constraints.
   * @return A SparkPlan potentially modified with an additional projection on top of `plan`
   */
  override def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    // Reuse the CPU implementation if the plan ends up on the CPU, otherwise do the
    // equivalent on the GPU.
    plan match {
      case g: GpuExec => gpuConvertEmptyToNullIfNeeded(g, partCols, constraints)
      case _ => super.convertEmptyToNullIfNeeded(plan, partCols, constraints)
    }
  }

  protected def convertToCpu(plan: SparkPlan): SparkPlan = plan match {
    case GpuRowToColumnarExec(p, _) => p
    case p: GpuExec => GpuColumnarToRowExec(p)
    case p => p
  }

  protected def convertToGpu(plan: SparkPlan): SparkPlan = plan match {
    case GpuColumnarToRowExec(p, _) => p
    case p: GpuExec => p
    case p => GpuRowToColumnarExec(p, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
  }
}
