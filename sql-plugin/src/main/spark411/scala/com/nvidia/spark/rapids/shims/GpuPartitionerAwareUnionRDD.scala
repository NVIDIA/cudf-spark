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
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Groups partitions at corresponding indices instead of concatenating.
 * This matches Spark 4.1's SQLPartitioningAwareUnionRDD behavior .
 */
private[shims] class GpuPartitionerAwareUnionRDD(
    sc: SparkContext,
    rdds: Seq[RDD[ColumnarBatch]],
    numPartitions: Int,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric)
  extends RDD[ColumnarBatch](sc, rdds.map(rdd => new OneToOneDependency(rdd))) {

  require(rdds.nonEmpty, "RDDs cannot be empty")

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { i =>
      new GpuPartitionerAwareUnionPartition(i, rdds.map(_.partitions(i)))
    }.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val parentPartitions = partition.asInstanceOf[
        GpuPartitionerAwareUnionPartition].parentPartitions
    rdds.iterator.zip(parentPartitions.iterator).flatMap { case (rdd, parentPartition) =>
      rdd.iterator(parentPartition, context).map { batch =>
        numOutputBatches += 1
        numOutputRows += batch.numRows
        batch
      }
    }
  }
}

private[shims] class GpuPartitionerAwareUnionPartition(
    override val index: Int,
    val parentPartitions: Seq[Partition]) extends Partition {

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)
}
