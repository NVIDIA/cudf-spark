/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuExec
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of Delta Lake's DeltaInvariantCheckerExec.
 *
 * A physical operator that validates records, before they are written into Delta. Each row
 * is left unchanged after validations.
 */
case class GpuDeltaInvariantCheckerExec(
    child: SparkPlan,
    checks: Seq[GpuCheckDeltaInvariant]) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    if (checks.isEmpty) return child.executeColumnar()
    val boundRefs = checks.map(_.withBoundReferences(child.output, allMetrics))

    child.executeColumnar().mapPartitionsInternal { batches =>
      batches.map { batch =>
        closeOnExcept(batch) { _ =>
          boundRefs.foreach(_.columnarEval(batch))
        }
        batch
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}
