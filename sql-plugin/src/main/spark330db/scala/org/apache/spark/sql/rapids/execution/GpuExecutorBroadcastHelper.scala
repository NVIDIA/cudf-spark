/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{ConcatAndConsumeAll, GpuCoalesceIterator, GpuColumnVector, GpuMetric, NoopMetric, RequireSingleBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.CoalesceReadOption
import com.nvidia.spark.rapids.GpuShuffleCoalesceUtils

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Helper object to handle [[EXECUTOR_BROADCAST]] from Databricks. Logically, an executor broadcast 
 * involves the executor "broadcasting" its data to the other executors, and then each of the 
 * executors build their own version of the broadcast relation. We accomplish a similar version 
 * of this by reading the shuffle data in each executor and passing the columnar batch data to the
 * GPU versions of BroadcastHashJoin and BroadcastNestedLoopJoin, which by necessity must build 
 * these relations on the executor because they do the work of the join on the GPU device itself,
 * which means they require a format of the data that can be used on the GPU.
 */
object GpuExecutorBroadcastHelper {
  import GpuMetric._

  // This reads the shuffle data that we have retrieved using `getShuffleRDD` from the shuffle
  // exchange. WARNING: Do not use this method outside of this context. This method can only be
  // used in the context of the executor broadcast and is NOT safe for distributed compute.
  private def shuffleDataIterator(shuffleData: RDD[ColumnarBatch]): Iterator[ColumnarBatch] = {
    shuffleData.partitions.map { part =>
      shuffleData.iterator(part, TaskContext.get())
    }.reduceLeft(_ ++ _)
  }

  // Because of the nature of the executor-side broadcast, every executor needs to read the 
  // shuffle data directly from the executor that produces the broadcast data. 
  // Note that we can't call mapPartitions here because we're on an executor, and we don't 
  // have access to a SparkContext.
  private def shuffleCoalesceIterator(
    shuffleData: RDD[ColumnarBatch],
    buildSchema: StructType,
    metricsMap: Map[String, GpuMetric],
    targetSize: Long): Iterator[ColumnarBatch] = {

    val dataTypes = GpuColumnVector.extractTypes(buildSchema)

    // Use the GPU Shuffle Coalesce iterator to concatenate and load batches onto the 
    // host as needed. Since we don't have GpuShuffleCoalesceExec in the plan for the 
    // executor broadcast scenario, we have to use that logic here to efficiently 
    // grab and release the semaphore while doing I/O. We wrap this with GpuCoalesceIterator
    // to ensure this always a single batch for the following step.
    val shuffleMetrics = Map(
      CONCAT_TIME -> metricsMap(CONCAT_TIME),
      OP_TIME_LEGACY -> metricsMap(OP_TIME_LEGACY),
    ).withDefaultValue(NoopMetric)

    val iter = shuffleDataIterator(shuffleData)
    new GpuCoalesceIterator(
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(iter, targetSize,
        dataTypes,
        CoalesceReadOption(SQLConf.get),
        shuffleMetrics),
      dataTypes,
      RequireSingleBatch,
      NoopMetric, // numInputRows
      NoopMetric, // numInputBatches
      NoopMetric, // numOutputRows
      NoopMetric, // numOutputBatches
      NoopMetric, // collectTime
      metricsMap(CONCAT_TIME), // concatTime
      metricsMap(OP_TIME_LEGACY), // opTime
      "GpuBroadcastHashJoinExec").asInstanceOf[Iterator[ColumnarBatch]]
  }

  /**
   * Given an RDD of ColumnarBatch containing broadcast data from a shuffle, get the 
   * fully coalesced ColumnarBatch loaded on to the GPU.
   *
   * This method uses getSingleBatchWithVerification to handle the empty relation case 
   * and the unexpected scenario of receiving more than a single batch
   *
   * @param shuffleData - the shuffle data from the executor for the "executor broadcast"
   * @param buildSchema - the schema expected for the output
   * @param buildOutput - the output attributes expected in case we receieve an empty RDD
   * @param metricsMap - metrics to populate while the shuffle coalesce iterator loads 
   *                     data from I/O to the device
   * @param targetSize - the target batch size in bytes for loading data on to the GPU 
   */
  def getExecutorBroadcastBatch(
      shuffleData: RDD[ColumnarBatch], 
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      metricsMap: Map[String, GpuMetric],
      targetSize: Long): ColumnarBatch = {

    // TODO: Ensure we're not reading the shuffle data multiple times. 
    // See https://github.com/NVIDIA/spark-rapids/issues/7599
    val it = shuffleCoalesceIterator(shuffleData, buildSchema, metricsMap, targetSize)
    // In a broadcast hash join, right now the GPU expects a single batch
    ConcatAndConsumeAll.getSingleBatchWithVerification(it, buildOutput)
  }

  /**
   * Given an RDD of ColumnarBatch containing broadcast data from a shuffle, get the 
   * number of rows that the received batch contains
   *
   * The method uses shuffleDataIterator here to get the number of rows here without
   * loading on to the GPU. The batch here needs to be closed.
   *
   */
  def getExecutorBroadcastBatchNumRows(shuffleData: RDD[ColumnarBatch]): Int = {
    // Ideally we cache this data so we're not reading the shuffle multiple times. 
    // This requires caching the data and making it spillable/etc.
    // See https://github.com/NVIDIA/spark-rapids/issues/7599
    val it = shuffleDataIterator(shuffleData)
    if (it.hasNext) {
      var numRows = 0
      while (it.hasNext) {
        withResource(it.next) { batch =>
          numRows += batch.numRows
        }
      }
      numRows
    } else {
      0
    }
  }

}