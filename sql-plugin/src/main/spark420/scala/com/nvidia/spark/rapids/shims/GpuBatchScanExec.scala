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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.collection.immutable.ArraySeq

import com.google.common.base.Objects
import com.nvidia.spark.rapids.{GpuBatchScanExecMetrics, GpuMetric, GpuScan}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression,
  Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceV2ScanExecBase,
  PushDownUtils}
import org.apache.spark.sql.execution.metric.SQLLastAttemptMetrics
import org.apache.spark.sql.rapids.shims.RowLevelOperationTableShims
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: GpuScan,
    runtimeFilters: Seq[Expression] = Seq.empty,
    ordering: Option[Seq[SortOrder]] = None,
    @transient table: Table,
    keyGroupedPartitioning: Option[Seq[Expression]] = None)
    extends DataSourceV2ScanExecBase with GpuBatchScanExecMetrics {

  // All expressions are filter expressions used on the CPU.
  override def gpuExpressions: Seq[Expression] = Nil

  @transient lazy val batch: Batch = if (scan == null) null else scan.toBatch

  import GpuMetric._

  override lazy val allMetrics: Map[String, GpuMetric] = {
    // Spark derives DELETE row counts from scan output rows. Keep this in GPU allMetrics so
    // GpuExec.metrics exposes the last-attempt metric instead of the default cumulative metric.
    val outputRows = if (isDeleteRowLevelOperationTable(table)) {
      GpuMetric.wrap(SQLLastAttemptMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS))
    } else {
      createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS)
    }
    Map(
      NUM_OUTPUT_ROWS -> outputRows,
      NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
      OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)) ++
      additionalMetrics
  }

  private def isDeleteRowLevelOperationTable(table: Table): Boolean = {
    RowLevelOperationTableShims.isDeleteRowLevelOperationTable(table)
  }

  override def equals(other: Any): Boolean = other match {
    case other: GpuBatchScanExec =>
      this.batch != null && this.batch == other.batch &&
        this.runtimeFilters == other.runtimeFilters &&
        this.keyGroupedPartitioning == other.keyGroupedPartitioning
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters, keyGroupedPartitioning)

  @transient override lazy val inputPartitions: Seq[InputPartition] =
    ArraySeq.unsafeWrapArray(batch.planInputPartitions())

  @transient protected lazy val filteredPartitions: Seq[Option[InputPartition]] = {
    val originalPartitioning = outputPartitioning

    val filtered = PushDownUtils.pushRuntimeFilters(scan, runtimeFilters, table, output)
    if (filtered) {
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case k: KeyedPartitioning =>
          // Spark 4.2 preserves keyed partitioning through runtime filtering by keeping each
          // original key position, padding filtered-out partitions with None.
          if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
            throw new SparkException("Data source must have preserved the original partitioning " +
              "during runtime filtering: not all partitions implement HasPartitionKey after " +
              "filtering")
          }

          val inputMap = k.partitionKeys.groupBy(identity).view.mapValues(_.size)
          val comparableKeyWrapperFactory = InternalRowComparableWrapper
            .getInternalRowComparableWrapperFactory(k.expressionDataTypes)
          val filteredMap = newPartitions.groupBy(
            p => comparableKeyWrapperFactory(p.asInstanceOf[HasPartitionKey].partitionKey()))

          if (!filteredMap.keySet.subsetOf(inputMap.keySet)) {
            throw new SparkException("During runtime filtering, data source must not report new " +
              "partition keys that are not present in the original partitioning.")
          }

          inputMap.toSeq
            .sortBy(_._1)(k.keyOrdering)
            .flatMap { case (key, size) =>
              val fps = filteredMap.getOrElse(key, Array.empty[InputPartition])

              if (fps.size > size) {
                throw new SparkException("During runtime filtering, data source must not report " +
                  s"new partitions for a given key. Before: $size partitions. " +
                  s"After: ${fps.size} partitions")
              }

              fps.map(p => Option.apply(p)).padTo(size, Option.empty[InputPartition])
            }

        case _ =>
          newPartitions.toSeq.map(p => Option.apply(p))
      }
    } else {
      (originalPartitioning match {
        case k: KeyedPartitioning =>
          // Match Spark's canonical key order even when runtime filters are not pushed.
          inputPartitions.sortBy(_.asInstanceOf[HasPartitionKey].partitionKey())(k.keyRowOrdering)
        case _ => inputPartitions
      }).map(p => Option.apply(p))
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    scan.metrics = allMetrics
    val rdd = if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      sparkContext.parallelize(Seq.empty[InternalRow], 1)
    } else {
      new DataSourceRDD(sparkContext, filteredPartitions, readerFactory, supportsColumnar,
        customMetrics)
    }
    postDriverMetrics(scan.reportDriverMetrics())
    rdd
  }

  override def doCanonicalize(): GpuBatchScanExec = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output),
      keyGroupedPartitioning = keyGroupedPartitioning.map(QueryPlan.normalizePredicates(_, output)))
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

  override def nodeName: String = s"GpuBatchScan ${table.name()}".trim
}
