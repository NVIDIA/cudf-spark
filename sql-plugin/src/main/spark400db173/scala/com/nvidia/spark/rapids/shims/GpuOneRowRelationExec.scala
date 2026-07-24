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
{"spark": "400db173"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, RapidsConf,
  RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.GpuMetric.NUM_OUTPUT_ROWS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, OneRowRelationExec}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of OneRowRelationExec.
 * OneRowRelationExec is used for queries like "SELECT 1" that have no FROM clause.
 * It produces a single row with no columns.
 * 
 * This GPU version produces a single ColumnarBatch with one row and zero columns,
 * which uses no GPU memory since there are no actual columns.
 */
case class GpuOneRowRelationExec() extends LeafExecNode with GpuExec {

  override val nodeName: String = "GpuScan OneRowRelation"

  override val output: Seq[Attribute] = Nil

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    sparkContext.parallelize(Seq(null), 1).mapPartitions { _ =>
      // Create a ColumnarBatch with 1 row and 0 columns
      val batch = new ColumnarBatch(Array.empty, 1)
      numOutputRows += 1
      Iterator.single(batch)
    }
  }

  // Row-based execution fallback
  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
  }

  // Override makeCopy to handle Spark's TreeNode reflection issue with no-arg case classes
  override def makeCopy(newArgs: Array[AnyRef]): GpuOneRowRelationExec = {
    GpuOneRowRelationExec()
  }

  // Override doCanonicalize to avoid reflection issues during plan canonicalization
  override protected def doCanonicalize(): GpuOneRowRelationExec = {
    GpuOneRowRelationExec()
  }
}

/**
 * Meta class for OneRowRelationExec to convert it to GPU.
 */
class GpuOneRowRelationExecMeta(
    exec: OneRowRelationExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[OneRowRelationExec](exec, conf, parent, rule) {

  override def convertToGpu(): GpuExec = GpuOneRowRelationExec()
}
