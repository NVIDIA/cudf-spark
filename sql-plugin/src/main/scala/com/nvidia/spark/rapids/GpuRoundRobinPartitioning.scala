/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.Random

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Represents a partitioning where incoming columnar batched rows are distributed evenly
 * across output partitions by starting from a zero-th partition number and distributing rows
 * in a round-robin fashion. This partitioning is used when implementing the
 * DataFrame.repartition() operator.
 */
case class GpuRoundRobinPartitioning(numPartitions: Int)
  extends GpuExpression with ShimExpression with GpuPartitioning {
  override def children: Seq[GpuExpression] = Nil

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  private def partitionInternalWithClose(
      batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    val sparkTypes = GpuColumnVector.extractTypes(batch)
    if (1 == numPartitions) {
      // Skip retry since partition number = 1
      withResource(batch) { batch =>
        withResource(GpuColumnVector.from(batch)) { table =>
          val columns = (0 until table.getNumberOfColumns).zip(sparkTypes).map {
            case (idx, sparkType) =>
              GpuColumnVector
                .from(table.getColumn(idx).incRefCount(), sparkType)
          }.toArray
          (Array(0), columns)
        }
      }
    } else {
      withResource(batch) { b =>
        withResource(GpuColumnVector.from(b)) { table =>
          withResource(table.
            roundRobinPartition(numPartitions, getStartPartition)) { partedTable =>
            val parts = partedTable.getPartitions
            val columns = (0 until partedTable.getNumberOfColumns.toInt).zip(sparkTypes).map {
              case (idx, sparkType) =>
                GpuColumnVector
                  .from(partedTable.getColumn(idx).incRefCount(), sparkType)
            }.toArray
            (parts, columns)
          }
        }
      }
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    if (batch.numCols() <= 0) {
      return Array(batch).zipWithIndex
    }
    NvtxRegistry.ROUND_ROBIN_PARTITION.push()
    try {
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        NvtxRegistry.ROUND_ROBIN_PARTITION_SLICE.push()
        try {
          partitionInternalWithClose(batch)
        } finally {
          NvtxRegistry.ROUND_ROBIN_PARTITION_SLICE.pop()
        }
      }
      sliceInternalGpuOrCpuAndClose(numRows, partitionIndexes, partitionColumns)
    } finally {
      NvtxRegistry.ROUND_ROBIN_PARTITION.pop()
    }
  }

  private def getStartPartition: Int = {
    // Follow Spark's way of getting the randomized position to pass to the round robin,
    // which starts at randomNumber + 1 and we do the same here to maintain compatibility.
    // See ShuffleExchangeExec.getPartitionKeyExtractor for reference.
    val random = if (null != TaskContext.get()) {
      new Random(TaskContext.get().partitionId())
    } else {
      // For unit test purpose where task context does not exist
      new Random
    }
    (random.nextInt(numPartitions) + 1).hashCode % numPartitions
  }
}
