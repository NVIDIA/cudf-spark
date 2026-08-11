/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuMetric, GpuMetricFactory, MetricsLevel, RapidsConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Simple metrics collected during an instance of [[GpuFileFormatDataWriter]].
 * These were first introduced in https://github.com/apache/spark/pull/18159 (SPARK-20703).
 */
case class BasicColumnarWriteTaskStats(
    partitions: Seq[InternalRow],
    numFiles: Int,
    numWriters: Int,
    numBytes: Long,
    numRows: Long)
    extends WriteTaskStats


/**
 * Simple metrics collected during an instance of [[GpuFileFormatDataWriter]].
 * This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker`.
 */
class BasicColumnarWriteTaskStatsTracker(
    hadoopConf: Configuration,
    taskCommitTimeMetric: Option[GpuMetric])
    extends ColumnarWriteTaskStatsTracker with Logging {
  private[this] val partitions: mutable.ArrayBuffer[InternalRow] = mutable.ArrayBuffer.empty
  private[this] var numFiles: Int = 0
  private[this] var numSubmittedFiles: Int = 0
  private[this] var numBytes: Long = 0L
  private[this] var numRows: Long = 0L
  private[this] var maxNumWriters: Int = 0

  private[this] val submittedFiles = mutable.HashSet[String]()

  /**
   * Get the size of the file expected to have been written by a worker.
   * @param filePath path to the file
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(filePath: String): Option[Long] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    getFileSize(fs, path)
  }

  /**
   * Get the size of the file expected to have been written by a worker.
   * This supports the XAttr in HADOOP-17414 when the "magic committer" adds
   * a custom HTTP header to the a zero byte marker.
   * If the output file as returned by getFileStatus > 0 then the length if
   * returned. For zero-byte files, the (optional) Hadoop FS API getXAttr() is
   * invoked. If a parseable, non-negative length can be retrieved, this
   * is returned instead of the length.
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(fs: FileSystem, path: Path): Option[Long] = {
    // the normal file status probe.
    try {
      val len = fs.getFileStatus(path).getLen
      if (len > 0) {
        return Some(len)
      }
    } catch {
      case e: FileNotFoundException =>
        // may arise against eventually consistent object stores.
        logDebug(s"File $path is not yet visible", e)
        return None
    }

    // Output File Size is 0. Look to see if it has an attribute
    // declaring a future-file-length.
    // Failure of API call, parsing, invalid value all return the
    // 0 byte length.

    var len = 0L
    try {
      val attr = fs.getXAttr(path, BasicColumnarWriteJobStatsTracker.FILE_LENGTH_XATTR)
      if (attr != null && attr.nonEmpty) {
        val str = new String(attr, StandardCharsets.UTF_8)
        logDebug(s"File Length statistics for $path retrieved from XAttr: $str")
        // a non-empty header was found. parse to a long via the java class
        val l = java.lang.Long.parseLong(str)
        if (l > 0) {
          len = l
        } else {
          logDebug("Ignoring negative value in XAttr file length")
        }
      }
    } catch {
      case e: NumberFormatException =>
        // warn but don't dump the whole stack
        logInfo(s"Failed to parse" +
            s" ${BasicColumnarWriteJobStatsTracker.FILE_LENGTH_XATTR}:$e;" +
            s" bytes written may be under-reported");
      case e: UnsupportedOperationException =>
        // this is not unusual; ignore
        logDebug(s"XAttr not supported on path $path", e);
      case e: Exception =>
        // Something else. Log at debug and continue.
        logDebug(s"XAttr processing failure on $path", e);
    }
    Some(len)
  }

  override def newPartition(partitionValues: InternalRow): Unit = {
    partitions.append(partitionValues)
  }

  override def newFile(filePath: String): Unit = {
    submittedFiles += filePath
    numSubmittedFiles += 1
  }

  override def closeFile(filePath: String): Unit = {
    updateFileStats(filePath)
    submittedFiles.remove(filePath)
  }

  private def updateFileStats(filePath: String): Unit = {
    getFileSize(filePath).foreach { len =>
      numBytes += len
      numFiles += 1
    }
  }

  override def newBatch(filePath: String, batch: ColumnarBatch): Unit = {
    numRows += batch.numRows()
  }

  /**
   * Update the number of open writers.
   * It will be a noop if the given number is not larger than the current one. Since
   * only the max value is needed. This is designed for the concurrent writers case.
   */
  override def writersNumber(numWriters: Int): Unit = {
    if (numWriters > maxNumWriters) {
      maxNumWriters = numWriters
    }
  }

  override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(numBytes)
      outputMetrics.setRecordsWritten(numRows)
    }

    if (numSubmittedFiles != numFiles) {
      logWarning(s"Expected $numSubmittedFiles files, but only saw $numFiles. " +
        "This could be due to the output format not writing empty files, " +
        "or files being not immediately visible in the filesystem.")
    }
    taskCommitTimeMetric.foreach(_ += taskCommitTime)
    BasicColumnarWriteTaskStats(partitions.toSeq, numFiles, maxNumWriters, numBytes, numRows)
  }
}


/**
 * Simple [[ColumnarWriteJobStatsTracker]] implementation that's serializable,
 * capable of instantiating [[BasicColumnarWriteTaskStatsTracker]] on executors and processing the
 * `BasicColumnarWriteTaskStats` they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class BasicColumnarWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val driverSideMetrics: Map[String, GpuMetric],
    taskCommitTimeMetric: GpuMetric)
  extends ColumnarWriteJobStatsTracker {

  def this(
      serializableHadoopConf: SerializableConfiguration,
      metrics: Map[String, GpuMetric]) = {
    this(serializableHadoopConf, metrics - TASK_COMMIT_TIME, metrics(TASK_COMMIT_TIME))
  }

  override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
    new BasicColumnarWriteTaskStatsTracker(serializableHadoopConf.value, Some(taskCommitTimeMetric))
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    val sparkContext = SparkContext.getActive.get
    val partitionsSet: mutable.Set[InternalRow] = mutable.HashSet.empty
    var numFiles: Long = 0L
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L
    var maxNumWriters: Long = 0L

    val basicStats = stats.map(_.asInstanceOf[BasicColumnarWriteTaskStats])

    basicStats.foreach { summary =>
      partitionsSet ++= summary.partitions
      numFiles += summary.numFiles
      totalNumBytes += summary.numBytes
      totalNumOutput += summary.numRows
      if (summary.numWriters > maxNumWriters) {
        maxNumWriters = summary.numWriters
      }
    }

    driverSideMetrics(BasicColumnarWriteJobStatsTracker.JOB_COMMIT_TIME).add(jobCommitTime)
    driverSideMetrics(BasicColumnarWriteJobStatsTracker.NUM_FILES_KEY).add(numFiles)
    driverSideMetrics(BasicColumnarWriteJobStatsTracker.NUM_OUTPUT_BYTES_KEY).add(totalNumBytes)
    driverSideMetrics(BasicColumnarWriteJobStatsTracker.NUM_OUTPUT_ROWS_KEY).add(totalNumOutput)
    driverSideMetrics(BasicColumnarWriteJobStatsTracker.NUM_PARTS_KEY).add(partitionsSet.size)
    driverSideMetrics.get(BasicColumnarWriteJobStatsTracker.MAX_WRITERS_NUM_KEY)
      .foreach(_.add(maxNumWriters))

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      GpuMetric.unwrap(driverSideMetrics).values.toList)
  }
}

object BasicColumnarWriteJobStatsTracker {
  private val NUM_FILES_KEY = "numFiles"
  private val NUM_OUTPUT_BYTES_KEY = "numOutputBytes"
  private val NUM_OUTPUT_ROWS_KEY = "numOutputRows"
  private val NUM_PARTS_KEY = "numParts"
  private val MAX_WRITERS_NUM_KEY = "maxWritersNumber"
  val TASK_COMMIT_TIME = "taskCommitTime"
  val JOB_COMMIT_TIME = "jobCommitTime"
  /** XAttr key of the data length header added in HADOOP-17414. */
  val FILE_LENGTH_XATTR = "header.x-hadoop-s3a-magic-data-length"

  def metrics: Map[String, GpuMetric] = {
    val sparkContext = SparkContext.getActive.get
    val metricsConf = MetricsLevel(sparkContext.conf.get(RapidsConf.METRICS_LEVEL.key,
      RapidsConf.METRICS_LEVEL.defaultValue))
    val metricFactory = new GpuMetricFactory(metricsConf, sparkContext)
    Map(
      NUM_FILES_KEY -> metricFactory.create(GpuMetric.ESSENTIAL_LEVEL,
        "number of written files"),
      NUM_OUTPUT_BYTES_KEY -> metricFactory.createSize(GpuMetric.ESSENTIAL_LEVEL,
        "written output"),
      NUM_OUTPUT_ROWS_KEY -> metricFactory.create(GpuMetric.ESSENTIAL_LEVEL,
        "number of output rows"),
      NUM_PARTS_KEY -> metricFactory.create(GpuMetric.ESSENTIAL_LEVEL,
        "number of dynamic part"),
      TASK_COMMIT_TIME -> metricFactory.createTiming(GpuMetric.ESSENTIAL_LEVEL,
        "task commit time"),
      JOB_COMMIT_TIME -> metricFactory.createTiming(GpuMetric.ESSENTIAL_LEVEL,
        "job commit time"),
      MAX_WRITERS_NUM_KEY -> metricFactory.create(GpuMetric.DEBUG_LEVEL,
        "max writers number")
    )
  }
}
