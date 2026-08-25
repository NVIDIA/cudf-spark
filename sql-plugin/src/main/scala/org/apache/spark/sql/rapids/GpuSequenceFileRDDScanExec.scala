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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.shims.{GpuDataSourceRDD, PartitionedFileUtilsShim, ShimLeafExecNode}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.rdd.{NewHadoopPartition, NewHadoopRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class GpuSequenceFileRDDScanExec(
    outputAttrs: Seq[Attribute],
    sourceColumns: Seq[SequenceFileRddReadProof.SourceColumn],
    @transient sourceRdd: NewHadoopRDD[Any, Any])(
    @transient val rapidsConf: RapidsConf)
  extends ShimLeafExecNode with GpuExec {

  override def output: Seq[Attribute] = outputAttrs
  override def outputPartitioning: Partitioning = UnknownPartitioning(filePartitions.size)
  override def outputOrdering: Seq[SortOrder] = Nil
  override def outputBatching: CoalesceGoal = TargetSize(rapidsConf.gpuTargetBatchSizeBytes)
  override def otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)
  override val nodeName: String = "GpuSequenceFileRDDScan"

  override protected val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override protected val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    SCHEDULE_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME),
    BUFFER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_BUBBLE),
    FILTER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME_BUBBLE),
    SCHEDULE_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME_BUBBLE),
    COPY_BUFFER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_COPY_BUFFER_TIME),
    SCAN_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_SCAN_TIME),
    "numPartedFiles" -> createMetric(DEBUG_LEVEL, "number of PartitionedFiles"))

  private lazy val readDataSchema = StructType(sourceColumns.map {
    case SequenceFileRddReadProof.Key =>
      SequenceFileBinaryFileFormat.DATA_SCHEMA(SequenceFileBinaryFileFormat.KEY_FIELD)
    case SequenceFileRddReadProof.Value =>
      SequenceFileBinaryFileFormat.DATA_SCHEMA(SequenceFileBinaryFileFormat.VALUE_FIELD)
  })

  private def toPartitionedFile(partition: org.apache.spark.Partition): PartitionedFile = {
    val hadoopPartition = partition match {
      case p: NewHadoopPartition => p
      case other => throw new IllegalStateException(
        s"Expected NewHadoopPartition, found ${other.getClass.getName}")
    }
    val split = hadoopPartition.serializableHadoopSplit.value match {
      case fileSplit: FileSplit if fileSplit.getClass == classOf[FileSplit] => fileSplit
      case other => throw new IllegalStateException(
        s"Expected FileSplit, found ${other.getClass.getName}")
    }
    val file = PartitionedFileUtilsShim.newPartitionedFile(
      InternalRow.empty,
      split.getPath.toUri.toString,
      split.getStart,
      split.getLength)
    PartitionedFileUtilsShim.withNewLocations(file, sourceRdd.preferredLocations(partition))
  }

  // Only the proven source's selected splits are regrouped; the paths are never listed again.
  @transient private lazy val filePartitions: Seq[FilePartition] = {
    val selectedFiles = sourceRdd.partitions.map(toPartitionedFile).toSeq
    FilePartition.getFilePartitions(
      sparkSession,
      selectedFiles,
      sparkSession.sessionState.conf.filesMaxPartitionBytes)
  }

  @transient private lazy val readerFactory = {
    val broadcastedConf = sparkContext.broadcast(
      new SerializableConfiguration(sourceRdd.getConf))
    GpuReadSequenceFileBinaryFormat.createRddReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      readDataSchema,
      rapidsConf,
      allMetrics)
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric(SCAN_TIME)
    GpuDataSourceRDD(sparkContext, filePartitions, readerFactory)
      .asInstanceOf[RDD[ColumnarBatch]]
      .mapPartitionsInternal { batches =>
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = scanTime.ns(batches.hasNext)

          override def next(): ColumnarBatch = {
            val batch = batches.next()
            numOutputRows += batch.numRows()
            batch
          }
        }
      }
  }
}
