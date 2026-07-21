/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import com.nvidia.spark.rapids.GpuPartitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBaseWithMetrics, ShuffledBatchRDD}

case class GpuShuffleExchangeExec(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin,
    advisoryPartitionSize: Option[Long] = None)(
    cpuOutputPartitioning: Partitioning)
    extends GpuShuffleExchangeExecBaseWithMetrics(gpuOutputPartitioning, child)
        with ShuffleExchangeLike {
  def shuffleId: Int = shuffleDependencyColumnar.shuffleId

  override def otherCopyArgs: Seq[AnyRef] = cpuOutputPartitioning :: Nil

  override val outputPartitioning: Partitioning = cpuOutputPartitioning

  override def numMappers: Int = shuffleDependencyColumnar.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependencyColumnar.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] = {
    new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    // note that Spark will only use the sizeInBytes statistic but making the rowCount
    // available here means that we can more easily reference it in GpuOverrides when
    // planning future query stages when AQE is on
    Statistics(
      sizeInBytes = metrics("dataSize").value,
      rowCount = Some(metrics("numOutputRows").value)
    )
  }
}
