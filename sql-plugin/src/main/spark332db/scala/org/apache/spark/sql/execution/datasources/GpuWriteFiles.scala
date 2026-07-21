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
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources

import java.util.Date

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, GpuMetric, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuFileFormatWriter, GpuWriteJobDescription}
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The write files spec holds all information of [[V1WriteCommand]] if its provider is
 * [[FileFormat]].
 */
case class GpuWriteFilesSpec(
    description: GpuWriteJobDescription,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpecFunc: SparkPlan => Option[GpuConcurrentOutputWriterSpec])

class GpuWriteFilesMeta(
    writeFilesExec: WriteFilesExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[WriteFilesExec](writeFilesExec, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    // WriteFilesExec only has one child
    val child = childPlans.head.convertIfNeeded()
    GpuWriteFilesExec(
      child,
      writeFilesExec.fileFormat,
      writeFilesExec.partitionColumns,
      writeFilesExec.bucketSpec,
      writeFilesExec.options,
      writeFilesExec.staticPartitions,
      conf.outputDebugDumpPrefix
    )
  }
}

/**
 * Responsible for writing files.
 */
case class GpuWriteFilesExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec,
    baseOutputDebugPath: Option[String]) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = Seq.empty

  /**
   * Cpu version for SparkPlan.executeWrite, just throw an exception
   */
  override protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  /**
   * Gpu version for SparkPlan.executeWrite
   */
  def executeColumnarWrite(
      writeFilesSpec: GpuWriteFilesSpec): RDD[WriterCommitMessage] = executeQuery {
    // Copied from SparkPlan.executeWrite
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteColumnarWrite(writeFilesSpec)
  }

  /**
   * Gpu version for SparkPlan.doExecuteWrite
   *
   * @param writeFilesSpec
   * @return
   */
  private def doExecuteColumnarWrite(
      writeFilesSpec: GpuWriteFilesSpec): RDD[WriterCommitMessage] = {
    val rdd = child.executeColumnar()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      session.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
    } else {
      rdd
    }

    val concurrentOutputWriterSpec = writeFilesSpec.concurrentOutputWriterSpecFunc(child)
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val localBaseOutputDebugPath = baseOutputDebugPath
    // The op_time metrics of this node and every descendant. They are forwarded
    // to GpuFileFormatWriter.executeTask so that the parent Insert command's
    // `.ns(excludeMetrics)` wrap subtracts child op_time deltas. The
    // pre-WriteFilesExec path in GpuFileFormatWriter.write computes the same
    // list via a `plan match { case gpuExec: GpuExec => ... }` block; without
    // this argument, the wrap defaults to Seq.empty and Insert's op_time
    // double-counts the entire upstream pipeline.
    val excludeMetrics: Seq[GpuMetric] =
      getOpTimeNewMetric.toSeq ++ getDescendantOpTimeMetrics

    rddWithNonEmptyPartitions.mapPartitionsInternal { iterator =>
      val sparkStageId = TaskContext.get().stageId()
      val sparkPartitionId = TaskContext.get().partitionId()
      val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
      val ret = GpuFileFormatWriter.executeTask(
        description,
        jobTrackerID,
        sparkStageId,
        sparkPartitionId,
        sparkAttemptNumber,
        committer,
        iterator,
        concurrentOutputWriterSpec,
        localBaseOutputDebugPath,
        excludeMetrics)

      Iterator(ret)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw SparkException.internalError(s"$nodeName does not support doExecute")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }

  override def stringArgs: Iterator[Any] = Iterator(child)
}

object GpuWriteFiles {

  def createConcurrentOutputWriterSpec(
      sparkSession: SparkSession,
      sortColumns: Seq[Attribute],
      output: Seq[Attribute],
      batchSize: Long,
      sortOrder: Seq[SortOrder]): Option[GpuConcurrentOutputWriterSpec] = {
    val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
    val concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
    if (concurrentWritersEnabled) {
      Some(GpuConcurrentOutputWriterSpec(maxWriters, output, batchSize, sortOrder))
    } else {
      None
    }
  }

  /**
   * Find the first `GpuWriteFilesExec`
   */
  def getWriteFilesOpt(p: SparkPlan): Option[GpuWriteFilesExec] = {
    p.collectFirst {
      case w: GpuWriteFilesExec => w
    }
  }

  /**
   * Create hadoop task attempt context from current spark task context and given hadoop conf.
   * <br/>
   *
   * Note that the given hadoop conf will be modified to set necessary configs.
   * @param hadoopConf Hadoop configuration
   * @return Hadoop task attempt context
   */
  def calcHadoopTaskAttemptContext(hadoopConf: Configuration): TaskAttemptContext = {
    val tc = TaskContext.get()
    if (tc == null) {
      throw new IllegalStateException("TaskContext is not available")
    }

    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, tc.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, tc.partitionId())
    val taskAttemptId = new TaskAttemptID(taskId, tc.attemptNumber())

    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id",  taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }
}
