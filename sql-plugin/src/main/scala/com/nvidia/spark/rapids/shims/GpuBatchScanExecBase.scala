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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuBatchScanExecMetrics, GpuScan}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuBatchScanExecBase(
    @transient scan: GpuScan,
    runtimeFilters: Seq[Expression] = Seq.empty)
  extends DataSourceV2ScanExecBase with GpuBatchScanExecMetrics with FilteredPartitions {

  // All expressions are filter expressions used on the CPU.
  override def gpuExpressions: Seq[Expression] = Nil

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  @transient lazy val batch: Batch = scan.toBatch

  override lazy val inputRDD: RDD[InternalRow] = {
    scan.metrics = allMetrics
    if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      new GpuDataSourceRDD(sparkContext, filteredPartitions, readerFactory)
    }
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}