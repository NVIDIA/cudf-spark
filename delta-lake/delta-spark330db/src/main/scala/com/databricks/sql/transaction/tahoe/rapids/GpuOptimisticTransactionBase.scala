/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
 *
 * This file was derived from OptimisticTransaction.scala and TransactionalWrite.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.constraints.{Constraint, DeltaInvariantCheckerExec}
import com.databricks.sql.transaction.tahoe.files.TahoeBatchFileIndex
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.DeltaWriteUtils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null
import org.apache.spark.sql.rapids.delta.{DeltaShufflePartitionsUtil, GpuOptimizeWriteExchangeExec, OptimizeWriteExchangeExec}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Clock

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the DeltaLog, MUST go through this instance rather
 * than directly to the DeltaLog otherwise they will not be check for logical conflicts
 * with concurrent updates.
 *
 * This class is not thread-safe.
 *
 * @param deltaLog The Delta Log for the table this transaction is modifying.
 * @param snapshot The snapshot that this transaction is reading at.
 * @param rapidsConf RAPIDS Accelerator config settings.
 */
abstract class GpuOptimisticTransactionBase
    (deltaLog: DeltaLog, snapshot: Snapshot, val rapidsConf: RapidsConf)
    (implicit clock: Clock)
  extends OptimisticTransaction(deltaLog, snapshot)(clock)
  with DeltaLogging {

  /**
   * Adds checking of constraints on the table
   * @param plan Plan to generate the table to check against constraints
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
        DeltaInvariantCheckerExec(cpuPlan, constraints)
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
   * @param plan The original SparkPlan.
   * @param partCols The partition columns.
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

  override def writeFiles(
      inputData: Dataset[_],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    writeFiles(inputData, None, additionalConstraints)
  }

  protected def applyOptimizeWriteIfNeeded(
      spark: SparkSession,
      physicalPlan: SparkPlan,
      partitionSchema: StructType,
      isOptimize: Boolean,
      writeOptions: Option[DeltaOptions]): SparkPlan = {
    val optimizeWriteEnabled = !isOptimize &&
      DeltaWriteUtils.shouldOptimizeWrite(writeOptions, spark.sessionState.conf, metadata)
    if (optimizeWriteEnabled) {
      val planWithoutTopRepartition =
        DeltaShufflePartitionsUtil.removeTopRepartition(physicalPlan)
      val partitioning = DeltaShufflePartitionsUtil.partitioningForRebalance(
        physicalPlan.output, partitionSchema, spark.sessionState.conf.numShufflePartitions)
      planWithoutTopRepartition match {
        case p: GpuExec =>
          val partMeta = GpuOverrides.wrapPart(partitioning, rapidsConf, None)
          partMeta.tagForGpu()
          if (partMeta.canThisBeReplaced) {
            val plan = GpuOptimizeWriteExchangeExec(partMeta.convertToGpu(), p)
            if (GpuShuffleEnv.useGPUShuffle(rapidsConf)) {
              GpuCoalesceBatches(plan, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
            } else {
              GpuShuffleCoalesceExec(plan, rapidsConf.gpuTargetBatchSizeBytes)
            }
          } else {
            GpuColumnarToRowExec(OptimizeWriteExchangeExec(partitioning, p))
          }
        case p =>
          OptimizeWriteExchangeExec(partitioning, p)
      }
    } else {
      physicalPlan
    }
  }

  protected def isOptimizeCommand(plan: LogicalPlan): Boolean = {
    val leaves = plan.collectLeaves()
    leaves.size == 1 && leaves.head.collect {
      case LogicalRelation(HadoopFsRelation(
      index: TahoeBatchFileIndex, _, _, _, _, _), _, _, _) =>
        index.actionType.equals("Optimize")
    }.headOption.getOrElse(false)
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
