/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.hashing.{MurmurHash3 => ScalaMurmur3Hash}

import ai.rapids.cudf.{OrderByArg, Table}
import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.shims.GpuFileFormatDataWriterShim
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Cast, Concat, Expression, HiveHash, Literal, Murmur3Hash, NullsFirst, ScalaUDF, UnsafeProjection}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.{BucketingUtils, PartitioningUtils, WriteTaskResult}
import org.apache.spark.sql.rapids.GpuFileFormatDataWriter._
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.rapids.shims.FileCommitProtocolShims
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuFileFormatDataWriter {
  private def ceilingDiv(num: Long, divisor: Long) = {
    ((num + divisor - 1) / divisor).toInt
  }

  def shouldSplitToFitMaxRecordsPerFile(
      maxRecordsPerFile: Long, recordsInFile: Long, numRowsInBatch: Long): Boolean = {
    maxRecordsPerFile > 0 && (recordsInFile + numRowsInBatch) > maxRecordsPerFile
  }

  private def getSplitIndexes(maxRecordsPerFile: Long, recordsInFile: Long, numRows: Long) = {
    (maxRecordsPerFile > 0, recordsInFile < maxRecordsPerFile) match {
      case (false, _) => IndexedSeq.empty
      case (true, false) =>
        (1 until ceilingDiv(numRows, maxRecordsPerFile))
            .map(i => (i * maxRecordsPerFile).toInt)
      case (true, true) => {
        val filledUp = maxRecordsPerFile - recordsInFile
        val remain = numRows - filledUp
        (0 until ceilingDiv(remain, maxRecordsPerFile))
            .map(i => (filledUp + i * maxRecordsPerFile).toInt)
      }
    }
  }

  /**
   * Split a table into parts if recordsInFile + batch row count would go above
   * maxRecordsPerFile and make the splits spillable.
   *
   * The logic to find out what the splits should be is delegated to getSplitIndexes.
   *
   * The input batch is closed in case of error or in case we have to split it.
   * It is not closed if it wasn't split.
   *
   * @param scb Spillable ColumnarBatch to split (and close)
   * @param maxRecordsPerFile max rowcount per file
   * @param recordsInFile row count in the file so far
   * @return array of SpillableColumnarBatch splits
   */
  def splitToFitMaxRecordsAndCloseWithRetry(
      scb: SpillableColumnarBatch,
      maxRecordsPerFile: Long,
      recordsInFile: Long): Array[SpillableColumnarBatch] = {
    val splitIndexes = closeOnExcept(scb) { _ =>
      getSplitIndexes(maxRecordsPerFile, recordsInFile, scb.numRows())
    }
    if (splitIndexes.isEmpty) {
      // this should never happen, as `splitToFitMaxRecordsAndCloseWithRetry` is called when
      // splits should already happen, but making it more efficient in that case
      Array(scb)
    } else {
      // actually split it
      withRetryNoSplit(scb) { _ =>
        val tbl = withResource(scb.getColumnarBatch()) { batch =>
          GpuColumnVector.from(batch)
        }
        val cts = withResource(tbl) { _ =>
          tbl.contiguousSplit(splitIndexes: _*)
        }
        withResource(cts) { _ =>
          cts.safeMap(ct =>
            SpillableColumnarBatch(ct, scb.dataTypes, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }
      }
    }
  }
}

/**
 * Abstract class for writing out data in a single Spark task using the GPU.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.FileFormatDataWriter`.
 */
abstract class GpuFileFormatDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol) extends DataWriter[ColumnarBatch] {

  protected class WriterAndStatus {
    var writer: ColumnarOutputWriter = _

    /** Number of records in current file. */
    var recordsInFile: Long = 0

    /**
     * File counter for writing current partition or bucket. For same partition or bucket,
     * we may have more than one file, due to number of records limit per file.
     */
    var fileCounter: Int = 0

    final def release(): Unit = {
      if (writer != null) {
        try {
          writer.close()
          statsTrackers.foreach(_.closeFile(writer.path()))
        } finally {
          writer = null
        }
      }
    }
  }

  /**
   * Max number of files a single task writes out due to file size. In most cases the number of
   * files written should be very small. This is just a safe guard to protect some really bad
   * settings, e.g. maxRecordsPerFile = 1.
   */
  protected val MAX_FILE_COUNTER: Int = 1000 * 1000
  protected val updatedPartitions: mutable.Set[String] = mutable.Set[String]()
  protected var currentWriterStatus: WriterAndStatus = new WriterAndStatus()

  /** Trackers for computing various statistics on the data as it's being written out. */
  protected val statsTrackers: Seq[ColumnarWriteTaskStatsTracker] =
    description.statsTrackers.map(_.newTaskInstance())

  /** Release resources of a WriterStatus. */
  protected final def releaseOutWriter(status: WriterAndStatus): Unit = {
    status.release()
  }

  protected final def writeUpdateMetricsAndClose(scb: SpillableColumnarBatch,
      writerStatus: WriterAndStatus): Unit = {
    writerStatus.recordsInFile += writerStatus.writer.writeSpillableAndClose(scb)
  }

  /** Release all resources. Public for testing */
  def releaseResources(): Unit = {
    // Release current writer by default, as this is the only resource to be released.
    releaseOutWriter(currentWriterStatus)
  }

  /** Write an iterator of column batch. */
  def writeWithIterator(iterator: Iterator[ColumnarBatch]): Unit = {
    while (iterator.hasNext) {
      write(iterator.next())
    }
  }

  /** Writes a columnar batch of records */
  def write(batch: ColumnarBatch): Unit

  /** Get the operator time metric for timing collection */
  def operatorTimeMetric: GpuMetric = {
    statsTrackers.find(_.isInstanceOf[GpuWriteTaskStatsTracker]) match {
      case Some(tracker: GpuWriteTaskStatsTracker) =>
        tracker.opTimeNew
      case _ => NoopMetric
    }
  }

  protected val reportSingleWriter = true

  private def updateWritersNumber(): Unit = {
    if (currentWriterStatus.writer != null && reportSingleWriter) {
      // By default, only one writer is opened for write.
      // And concurrent writer will do this by itself.
      statsTrackers.foreach(_.writersNumber(1))
    }
  }

  /**
   * Returns the summary of relative information which
   * includes the list of partition strings written out. The list of partitions is sent back
   * to the driver and used to update the catalog. Other information will be sent back to the
   * driver too and used to e.g. update the metrics in UI.
   */
  override def commit(): WriteTaskResult = {
    updateWritersNumber()
    releaseResources()
    val (taskCommitMessage, taskCommitTime) = TimingUtils.timeTakenMs {
      committer.commitTask(taskAttemptContext)
    }
    val summary = GpuFileFormatDataWriterShim.createWriteSummary(
      updatedPartitions = updatedPartitions.toSet,
      stats = statsTrackers.map(_.getFinalStats(taskCommitTime))
    )
    WriteTaskResult(taskCommitMessage, summary)
  }

  override def abort(): Unit = {
    try {
      updateWritersNumber()
      releaseResources()
    } finally {
      committer.abortTask(taskAttemptContext)
    }
  }

  override def close(): Unit = {}
}

/** GPU data writer for empty partitions */
class GpuEmptyDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol
) extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  override def write(batch: ColumnarBatch): Unit = {
    batch.close()
  }
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class GpuSingleDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    debugOutputBasePath: Option[String])
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  // Initialize currentWriter and statsTrackers
  newOutputWriter()

  private def newOutputWriter(): Unit = {
    currentWriterStatus.recordsInFile = 0
    val fileCounter = currentWriterStatus.fileCounter
    releaseResources()

    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = FileCommitProtocolShims.newTaskTempFile(
      committer,
      taskAttemptContext,
      None,
      f"-c$fileCounter%03d" + ext)

    val debugOutputPath = debugOutputBasePath.map { base =>
      base + s"/DEBUG_${taskAttemptContext.getTaskAttemptID}" +
        f"_c$fileCounter%03d_${System.nanoTime()}.debug"
    }

    currentWriterStatus.writer = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext,
      statsTrackers = statsTrackers,
      debugOutputPath = debugOutputPath,
      fileIO = description.fileIO)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(batch: ColumnarBatch): Unit = {
    val maxRecordsPerFile = description.maxRecordsPerFile
    val recordsInFile = currentWriterStatus.recordsInFile
    val scb = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    if (!shouldSplitToFitMaxRecordsPerFile(
        maxRecordsPerFile, recordsInFile, scb.numRows())) {
      writeUpdateMetricsAndClose(scb, currentWriterStatus)
    } else {
      val partBatches = splitToFitMaxRecordsAndCloseWithRetry(scb, maxRecordsPerFile,
        recordsInFile)
      val needNewWriterForFirstPart = recordsInFile >= maxRecordsPerFile
      closeOnExcept(partBatches) { _ =>
        partBatches.zipWithIndex.foreach { case (partBatch, partIx) =>
          if (partIx > 0 || needNewWriterForFirstPart) {
            currentWriterStatus.fileCounter += 1
            val fileCounter = currentWriterStatus.fileCounter
            assert(fileCounter <= MAX_FILE_COUNTER,
              s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")
            newOutputWriter()
          }
          // null out the entry so that we don't double close
          partBatches(partIx) = null
          writeUpdateMetricsAndClose(partBatch, currentWriterStatus)
        }
      }
    }
  }
}

/**
 * Dynamic partition writer with single writer, meaning only one writer is opened at any time
 * for writing, meaning this single function can write to multiple directories (partitions)
 * or files (bucketing). The data to be written are required to be sorted on partition and/or
 * bucket column(s) before writing.
 */
class GpuDynamicPartitionDataSingleWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    debugOutputBasePath: Option[String])
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  /** Wrapper class to index a unique concurrent output writer. */
  protected class WriterIndex(
      var partitionValues: Option[InternalRow],
      var bucketId: Option[Int]) extends Product2[Option[InternalRow], Option[Int]] {

    override def hashCode(): Int = ScalaMurmur3Hash.productHash(this)

    override def equals(obj: Any): Boolean = {
      if (canEqual(obj)) {
        val otherWI = obj.asInstanceOf[WriterIndex]
        partitionValues == otherWI.partitionValues && bucketId == otherWI.bucketId
      } else {
        false
      }
    }

    override def _1: Option[InternalRow] = partitionValues
    override def _2: Option[Int] = bucketId
    override def canEqual(that: Any): Boolean = that.isInstanceOf[WriterIndex]
  }

  /**
   * A case class to hold the batch, the optional partition values and the optional bucket
   * ID for a split group. All the rows in the batch belong to the group defined by the
   * partition values and the bucket ID.
   */
  private case class SplitPack(split: SpillableColumnarBatch, partValues: Option[InternalRow],
      bucketId: Option[Int]) extends AutoCloseable {
    override def close(): Unit = {
      split.safeClose()
    }
  }
  /**
   * The index for current writer. Intentionally make the index mutable and reusable.
   * Avoid JVM GC issue when many short-living `WriterIndex` objects are created
   * if switching between concurrent writers frequently.
   */
  private val currentWriterId: WriterIndex = new WriterIndex(None, None)

  /** Flag saying whether or not the data to be written out is partitioned. */
  protected val isPartitioned: Boolean = description.partitionColumns.nonEmpty

  /** Flag saying whether or not the data to be written out is bucketed. */
  protected val isBucketed: Boolean = description.bucketSpec.isDefined

  assert(isPartitioned || isBucketed,
    s"""GpuDynamicPartitionWriteTask should be used for writing out data that's either
       |partitioned or bucketed. In this case neither is true.
       |GpuWriteJobDescription: $description
     """.stripMargin)

  // All data is sorted ascending with default null ordering
  private val nullsSmallest = Ascending.defaultNullOrdering == NullsFirst

  /** Extracts the partition values out of an input batch. */
  private lazy val getPartitionColumnsAsBatch: ColumnarBatch => ColumnarBatch = {
    // Using Internal method: this is a simple column projection in data writer context
    // where metrics are not available (runs on executors after serialization).
    val expressions = GpuBindReferences.bindGpuReferencesNoMetrics(
      description.partitionColumns,
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  private lazy val getBucketIdColumnAsBatch: ColumnarBatch => ColumnarBatch = {
    // Using Internal method: this is a simple projection in data writer context
    // where metrics are not available (runs on executors after serialization).
    val expressions = GpuBindReferences.bindGpuReferencesNoMetrics(
      Seq(description.bucketSpec.get.bucketIdExpression),
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  /**
   * Expression that given partition columns builds a path string like: col1=val/col2=val/...
   * This is used after we pull the unique partition values back to the host.
   */
  private lazy val partitionPathExpression: Expression = Concat(
    description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  /** Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
   * the partition string.
   */
  private lazy val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression), description.partitionColumns)
    row => proj(row).getString(0)
  }

  /** Extracts the output values of an input batch. */
  protected lazy val getDataColumnsAsBatch: ColumnarBatch => ColumnarBatch = {
    // Using Internal method: this is a simple column projection in data writer context
    // where metrics are not available (runs on executors after serialization).
    val expressions = GpuBindReferences.bindGpuReferencesNoMetrics(
      description.dataColumns,
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  protected def getKeysBatch(cb: ColumnarBatch): ColumnarBatch = {
    val keysBatch = withResource(getPartitionColumnsAsBatch(cb)) { partCb =>
      if (isBucketed) {
        withResource(getBucketIdColumnAsBatch(cb)) { bucketIdCb =>
          GpuColumnVector.combineColumns(partCb, bucketIdCb)
        }
      } else {
        GpuColumnVector.incRefCounts(partCb)
      }
    }
    require(keysBatch.numCols() > 0, "No sort key is specified")
    keysBatch
  }

  protected def genGetBucketIdFunc(keyHostCb: ColumnarBatch): Int => Option[Int] = {
    if (isBucketed) {
      // The last column is the bucket id column
      val bucketIdCol = keyHostCb.column(keyHostCb.numCols() - 1)
      i => Some(bucketIdCol.getInt(i))
    } else {
      _ => None
    }
  }

  protected def genGetPartValuesFunc(keyHostCb: ColumnarBatch): Int => Option[InternalRow] = {
    if (isPartitioned) {
      // Use the existing code to convert each row into a path. It would be nice to do this
      // on the GPU, but the data should be small and there are things we cannot easily
      // support on the GPU right now
      import scala.collection.JavaConverters._
      val partCols = description.partitionColumns.indices.map(keyHostCb.column)
      val iter = new ColumnarBatch(partCols.toArray, keyHostCb.numRows()).rowIterator()
        .asScala.map(_.copy())
      _ => Some(iter.next)
    } else {
      _ => None
    }
  }

  // distinct value sorted the same way the input data is sorted.
  private def distinctAndSort(t: Table): Table = {
    val columnIds = 0 until t.getNumberOfColumns
    withResource(t.groupBy(columnIds: _*).aggregate()) { distinct =>
      distinct.orderBy(columnIds.map(OrderByArg.asc(_, nullsSmallest)): _*)
    }
  }

  // Get the split indexes for t given the keys we want to split on
  private def splitIndexes(t: Table, keys: Table): Array[Int] = {
    val nullsSmallestArray = Array.fill[Boolean](t.getNumberOfColumns)(nullsSmallest)
    val desc = Array.fill[Boolean](t.getNumberOfColumns)(false)
    withResource(t.upperBound(nullsSmallestArray, keys, desc)) { cv =>
      GpuColumnVector.toIntArray(cv)
    }
  }

  // Convert a table to a ColumnarBatch on the host, so we can iterate through it.
  protected def copyToHostAsBatch(input: Table, colTypes: Array[DataType]): ColumnarBatch = {
    withResource(GpuColumnVector.from(input, colTypes)) { tmp =>
      new ColumnarBatch(GpuColumnVector.extractColumns(tmp).safeMap(_.copyToHost()), tmp.numRows())
    }
  }

  /**
   * Split a batch according to the sorted keys (partitions + bucket ids).
   * Returns a tuple with an array of the splits as `ContiguousTable`'s, an array of
   * paths and bucket ids to use to write each partition and(or) bucket file.
   */
  private def splitBatchByKeyAndClose(batch: ColumnarBatch): Array[SplitPack] = {
    val (keysCb, dataCb) = withResource(batch) { _ =>
      closeOnExcept(getDataColumnsAsBatch(batch)) { data =>
        (getKeysBatch(batch), data)
      }
    }
    val (keyHostCb, splitIds) = closeOnExcept(dataCb) { _ =>
      val (splitIds, distinctKeysTbl, keysCbTypes) = withResource(keysCb) { _ =>
        val keysCbTypes = GpuColumnVector.extractTypes(keysCb)
        withResource(GpuColumnVector.from(keysCb)) { keysTable =>
          closeOnExcept(distinctAndSort(keysTable)) { distinctKeysTbl =>
            (splitIndexes(keysTable, distinctKeysTbl), distinctKeysTbl, keysCbTypes)
          }
        }
      }
      withResource(distinctKeysTbl) { _ =>
        (copyToHostAsBatch(distinctKeysTbl, keysCbTypes), splitIds)
      }
    }
    val splits = closeOnExcept(keyHostCb) { _ =>
      val scbOutput = closeOnExcept(dataCb)( _ =>
        SpillableColumnarBatch(dataCb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      withRetryNoSplit(scbOutput) { scb =>
        withResource(scb.getColumnarBatch()) { outCb =>
          withResource(GpuColumnVector.from(outCb)) { outputColumnsTbl =>
            withResource(outputColumnsTbl) { _ =>
              outputColumnsTbl.contiguousSplit(splitIds: _*)
            }
          }
        }
      }
    }
    // Build the split result
    withResource(splits) { _ =>
      withResource(keyHostCb) { _ =>
        val getBucketId = genGetBucketIdFunc(keyHostCb)
        val getNextPartValues = genGetPartValuesFunc(keyHostCb)
        val outDataTypes = description.dataColumns.map(_.dataType).toArray
        (0 until keyHostCb.numRows()).safeMap { idx =>
          val split = splits(idx)
          splits(idx) = null
          closeOnExcept(split) { _ =>
            SplitPack(
              SpillableColumnarBatch(split, outDataTypes,
                SpillPriorities.ACTIVE_BATCHING_PRIORITY),
              getNextPartValues(idx), getBucketId(idx))
          }
        }.toArray
      }
    }
  }

  /**
   * Create a new writer according to the given writer id, and update the given
   * writer status. It also closes the old writer in the writer status by default.
   */
  protected final def renewOutWriter(newWriterId: WriterIndex, curWriterStatus: WriterAndStatus,
      closeOldWriter: Boolean = true): Unit = {
    if (closeOldWriter) {
      releaseOutWriter(curWriterStatus)
    }
    curWriterStatus.recordsInFile = 0
    curWriterStatus.writer = newWriter(newWriterId.partitionValues, newWriterId.bucketId,
      curWriterStatus.fileCounter)
  }

  /**
   * Set up a writer to the given writer status for the given writer id.
   * It will create a new one if needed. This is used when seeing a new partition
   * and(or) a new bucket id.
   */
  protected def setupCurrentWriter(newWriterId: WriterIndex, curWriterStatus: WriterAndStatus,
      closeOldWriter: Boolean = true): Unit = {
    renewOutWriter(newWriterId, curWriterStatus, closeOldWriter)
  }

  /**
   * Opens a new OutputWriter given a partition key and/or a bucket id.
   * If bucket id is specified, we will append it to the end of the file name, but before the
   * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
   *
   * @param partValues  the partition values
   * @param bucketId    the bucket which all tuples being written by this OutputWriter belong to,
   *                    currently does not support `bucketId`, it's always None
   * @param fileCounter integer indicating the number of files to be written to `partDir`
   */
  def newWriter(partValues: Option[InternalRow], bucketId: Option[Int],
      fileCounter: Int): ColumnarOutputWriter = {
    val partDir = partValues.map(getPartitionPath(_))
    partDir.foreach(updatedPartitions.add)
    // Currently will be empty
    val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

    // This must be in a form that matches our bucketing format. See BucketingUtils.
    val ext = f"$bucketIdStr.c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskAttemptContext)

    val customPath = partDir.flatMap { dir =>
      description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
    }

    val currentPath = if (customPath.isDefined) {
      FileCommitProtocolShims.newTaskTempFileAbsPath(
        committer, taskAttemptContext, customPath.get, ext)
    } else {
      FileCommitProtocolShims.newTaskTempFile(committer, taskAttemptContext, partDir, ext)
    }

    val debugOutputPath = debugOutputBasePath.map { base =>
      if (customPath.isDefined) {
        val hash = customPath.get.hashCode
        base + s"/DEBUG_CUSTOM_${hash}_${taskAttemptContext.getTaskAttemptID}" +
          f"_c$fileCounter%03d_${System.nanoTime()}.debug"
      }  else {
        base + s"/${partDir.mkString("/")}/DEBUG_${taskAttemptContext.getTaskAttemptID}" +
          f"_c$fileCounter%03d_${System.nanoTime()}.debug"
      }
    }

    val outWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext,
      statsTrackers = statsTrackers,
      debugOutputPath = debugOutputPath,
      description.fileIO)

    statsTrackers.foreach(_.newFile(currentPath))
    outWriter
  }

  protected final def writeBatchPerMaxRecordsAndClose(scb: SpillableColumnarBatch,
      writerId: WriterIndex, writerStatus: WriterAndStatus): Unit = {
    val maxRecordsPerFile = description.maxRecordsPerFile
    val recordsInFile = writerStatus.recordsInFile

    if (!shouldSplitToFitMaxRecordsPerFile(maxRecordsPerFile, recordsInFile, scb.numRows())) {
      writeUpdateMetricsAndClose(scb, writerStatus)
    } else {
      val splits = splitToFitMaxRecordsAndCloseWithRetry(scb, maxRecordsPerFile, recordsInFile)
      withResource(splits) { _ =>
        val needNewWriterForFirstPart = recordsInFile >= maxRecordsPerFile
        splits.zipWithIndex.foreach { case (part, partIx) =>
          if (partIx > 0 || needNewWriterForFirstPart) {
            writerStatus.fileCounter += 1
            assert(writerStatus.fileCounter <= MAX_FILE_COUNTER,
              s"File counter ${writerStatus.fileCounter} is beyond max value $MAX_FILE_COUNTER")
            // will create a new file, so close the old writer
            renewOutWriter(writerId, writerStatus)
          }
          splits(partIx) = null
          writeUpdateMetricsAndClose(part, writerStatus)
        }
      }
    }
  }

  /**
   * Called just before updating the current writer status when seeing a new partition
   * or a bucket.
   *
   * @param curWriterId the current writer index
   */
  protected def preUpdateCurrentWriterStatus(curWriterId: WriterIndex): Unit ={}

  override def write(batch: ColumnarBatch): Unit = {
    // The input batch that is entirely sorted, so split it up by partitions and (or)
    // bucket ids, and write the split batches one by one.
    withResource(splitBatchByKeyAndClose(batch)) { splitPacks =>
      splitPacks.zipWithIndex.foreach { case (SplitPack(sp, partVals, bucketId), i) =>
        val hasDiffPart = partVals != currentWriterId.partitionValues
        val hasDiffBucket = bucketId != currentWriterId.bucketId
        if (hasDiffPart || hasDiffBucket) {
          preUpdateCurrentWriterStatus(currentWriterId)
          if (hasDiffPart) {
            currentWriterId.partitionValues = partVals
            statsTrackers.foreach(_.newPartition(partVals.get))
          }
          if (hasDiffBucket) {
            currentWriterId.bucketId = bucketId
          }
          currentWriterStatus.fileCounter = 0
          setupCurrentWriter(currentWriterId, currentWriterStatus)
        }
        splitPacks(i) = null
        writeBatchPerMaxRecordsAndClose(sp, currentWriterId, currentWriterStatus)
      }
    }
  }
}

/**
 * Dynamic partition writer with concurrent writers, meaning multiple concurrent
 * writers are opened for writing.
 *
 * The process has the following steps:
 *  - Step 1: Maintain a map of output writers per each partition columns. Keep all
 *    writers opened; Cache the inputted batches by splitting them into sub-groups and
 *    each partition holds a list of spillable sub-groups; Find and write the max pending
 *    partition data if the total caches exceed the limitation.
 *  - Step 2: If number of concurrent writers exceeds limit, fall back to sort-based write
 *    (`GpuDynamicPartitionDataSingleWriter`), sort rest of batches on partition.
 *    Write batch by batch, and eagerly close the writer when finishing
 *    Caller is expected to call `writeWithIterator()` instead of `write()` to write records.
 *    Note: when fall back to `GpuDynamicPartitionDataSingleWriter`, the single writer should
 *    restore un-closed writers and should handle un-flushed spillable caches.
 */
class GpuDynamicPartitionDataConcurrentWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    spec: GpuConcurrentOutputWriterSpec,
    debugOutputBasePath: Option[String])
  extends GpuDynamicPartitionDataSingleWriter(description, taskAttemptContext,
    committer, debugOutputBasePath)
  with Logging {

  /** Wrapper class for status and caches of a unique concurrent output writer. */
  private class WriterStatusWithBatches extends WriterAndStatus with AutoCloseable {
    // caches for this partition or writer
    val tableCaches: ListBuffer[SpillableColumnarBatch] = ListBuffer()

    // current device bytes for the above caches
    var deviceBytes: Long = 0

    override def close(): Unit = try {
      releaseOutWriter(this)
    } finally {
      tableCaches.safeClose()
      tableCaches.clear()
    }
  }

  override protected val reportSingleWriter: Boolean = false

  // Keep all the unclosed writers, key is a partition path and(or) bucket id.
  // Note: if fall back to sort-based mode, also use the opened writers in the map.
  private val concurrentWriters = mutable.HashMap[WriterIndex, WriterStatusWithBatches]()

  private val partitionFlushSize =
    if (description.concurrentWriterPartitionFlushSize <= 0) {
      // if the property is equal or less than 0, use default value given by the
      // writer factory
      description.outputWriterFactory.partitionFlushSize(taskAttemptContext)
    } else {
      // if the property is greater than 0, use the property value
      description.concurrentWriterPartitionFlushSize
    }

  // Pending split batches that are not cached for the concurrent write because
  // there are too many open writers, and it is going to fall back to the sorted
  // sequential write.
  private val pendingBatches: mutable.Queue[SpillableColumnarBatch] = mutable.Queue.empty

  override def writeWithIterator(iterator: Iterator[ColumnarBatch]): Unit = {
    // 1: try concurrent writer
    while (iterator.hasNext && pendingBatches.isEmpty) {
      // concurrent write and update the `concurrentWriters` map.
      this.write(iterator.next())
    }

    // 2: fall back to single write if the input is not all consumed.
    if (pendingBatches.nonEmpty || iterator.hasNext) {
      // sort the all the pending batches and ones in `iterator`
      val pendingCbsIter = new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = pendingBatches.nonEmpty

        override def next(): ColumnarBatch = {
          if (!hasNext) {
            throw new NoSuchElementException()
          }
          withResource(pendingBatches.dequeue())(_.getColumnarBatch())
        }
      }
      val (sortMetric, sortOpTime) =
        statsTrackers.find(_.isInstanceOf[GpuWriteTaskStatsTracker]).map { tc =>
          val tt = tc.asInstanceOf[GpuWriteTaskStatsTracker]
          (tt.sortTime, tt.sortOpTime)
        }.getOrElse((NoopMetric, NoopMetric))

      val sortIter = GpuOutOfCoreSortIterator(pendingCbsIter ++ iterator,
        new GpuSorter(spec.sortOrder, spec.output, Map.empty[String, GpuMetric]),
        GpuSortExec.targetSize(spec.batchSize),
        sortOpTime, sortMetric, NoopMetric, NoopMetric)
      while (sortIter.hasNext) {
        // write with sort-based sequential writer
        super.write(sortIter.next())
      }
    }
  }

  /** This is for the fallback case, used to clean the writers map. */
  override def preUpdateCurrentWriterStatus(curWriterId: WriterIndex): Unit = {
    concurrentWriters.remove(curWriterId)
  }

  /** This is for the fallback case, try to find the writer from cache first. */
  override def setupCurrentWriter(newWriterId: WriterIndex, writerStatus: WriterAndStatus,
      closeOldWriter: Boolean): Unit = {
    if (closeOldWriter) {
      releaseOutWriter(writerStatus)
    }
    val oOpenStatus = concurrentWriters.get(newWriterId)
    if (oOpenStatus.isDefined) {
      val openStatus = oOpenStatus.get
      writerStatus.writer = openStatus.writer
      writerStatus.recordsInFile = openStatus.recordsInFile
      writerStatus.fileCounter = openStatus.fileCounter
    } else {
      super.setupCurrentWriter(newWriterId, writerStatus, closeOldWriter = false)
    }
  }

  /**
   * The write path of concurrent writers
   *
   * @param cb the columnar batch to be written
   */
  override def write(cb: ColumnarBatch): Unit = {
    if (cb.numRows() == 0) {
      // TODO https://github.com/NVIDIA/spark-rapids/issues/6453
      // To solve above issue, I assume that an empty batch will be wrote for saving metadata.
      // If the assumption it's true, this concurrent writer should write the metadata here,
      // and should not run into below splitting and caching logic
      cb.close()
      return
    }

    // Split the batch and cache the result, along with opening the writers.
    splitBatchToCacheAndClose(cb)
    // Update the maxWriterNumber metric since the writers are already opened and cached.
    statsTrackers.foreach(_.writersNumber(concurrentWriters.size))
    // Write the cached batches
    val writeFunc: (WriterIndex, WriterStatusWithBatches) => Unit =
      if (pendingBatches.nonEmpty) {
        // Flush all non-empty caches before going into sorted sequential write. Some writers may
        // have been flushed previously, but are kept open for possible reuse during fallback.
        writeOneCacheIfNeededAndClose
      } else {
        // Still the concurrent write, so write out only partitions that size > threshold.
        (wi, ws) =>
          if (ws.deviceBytes > partitionFlushSize) {
            writeOneCacheAndClose(wi, ws)
          }
      }
    concurrentWriters.foreach { case (writerIdx, writerStatus) =>
      writeFunc(writerIdx, writerStatus)
    }
  }

  private def writeOneCacheIfNeededAndClose(writerId: WriterIndex,
      status: WriterStatusWithBatches): Unit = {
    if (status.tableCaches.nonEmpty) {
      writeOneCacheAndClose(writerId, status)
    }
  }

  private def writeOneCacheAndClose(writerId: WriterIndex,
      status: WriterStatusWithBatches): Unit = {
    assert(status.tableCaches.nonEmpty)
    // Concat tables if needed
    val scbToWrite = GpuBatchUtils.concatSpillBatchesAndClose(status.tableCaches.toSeq).get
    status.tableCaches.clear()
    status.deviceBytes = 0
    writeBatchPerMaxRecordsAndClose(scbToWrite, writerId, status)
  }

  private def splitBatchToCacheAndClose(batch: ColumnarBatch): Unit = {
    // Split batch to groups by sort columns, [partition and(or) bucket id column].
    val (keysAndGroups, keyTypes) = withResource(batch) { _ =>
      val (opBatch, keyTypes) = withResource(getKeysBatch(batch)) { keysBatch =>
        val combinedCb = GpuColumnVector.combineColumns(keysBatch, batch)
        (combinedCb, GpuColumnVector.extractTypes(keysBatch))
      }
      withResource(opBatch) { _ =>
        withResource(GpuColumnVector.from(opBatch)) { opTable =>
          (opTable.groupBy(keyTypes.indices: _*).contiguousSplitGroupsAndGenUniqKeys(),
            keyTypes)
        }
      }
    }
    // Copy keys table to host and make group batches spillable
    val (keyHostCb, groups) = withResource(keysAndGroups) { _ =>
      // groups number should equal to uniq keys number
      assert(keysAndGroups.getGroups.length == keysAndGroups.getUniqKeyTable.getRowCount)
      closeOnExcept(copyToHostAsBatch(keysAndGroups.getUniqKeyTable, keyTypes)) { keyHostCb =>
        keysAndGroups.closeUniqKeyTable()
        val allTypes = description.allColumns.map(_.dataType).toArray
        val allColsIds = allTypes.indices.map(_ + keyTypes.length)
        val gps = keysAndGroups.getGroups.safeMap { gp =>
          withResource(gp.getTable) { gpTable =>
            withResource(new Table(allColsIds.map(gpTable.getColumn): _*)) { allTable =>
              SpillableColumnarBatch(GpuColumnVector.from(allTable, allTypes),
                SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            }
          }
        }
        (keyHostCb, gps)
      }
    }
    // Cache the result to either the map or the pending queue.
    withResource(groups) { _ =>
      withResource(keyHostCb) { _ =>
        val getBucketId = genGetBucketIdFunc(keyHostCb)
        val getNextPartValues = genGetPartValuesFunc(keyHostCb)
        var idx = 0
        var tempParVals: Option[InternalRow] = None
        while (idx < groups.length && concurrentWriters.size < spec.maxWriters) {
          val writerId = new WriterIndex(getNextPartValues(idx), getBucketId(idx))
          val writerStatus =
            concurrentWriters.getOrElseUpdate(writerId, new WriterStatusWithBatches)
          if (writerStatus.writer == null) {
            // a new partition or bucket, so create a writer
            renewOutWriter(writerId, writerStatus, closeOldWriter = false)
            // if due to different part, update the tracker
            if (tempParVals != writerId.partitionValues) {
              tempParVals = writerId.partitionValues
              statsTrackers.foreach(_.newPartition(tempParVals.get))
            }
          }
          withResource(groups(idx)) { gp =>
            groups(idx) = null
            withResource(gp.getColumnarBatch()) { cb =>
              val dataScb = SpillableColumnarBatch(getDataColumnsAsBatch(cb),
                SpillPriorities.ACTIVE_BATCHING_PRIORITY)
              writerStatus.tableCaches.append(dataScb)
              writerStatus.deviceBytes += dataScb.sizeInBytes
            }
          }
          idx += 1
        }
        if (idx < groups.length) {
          // The open writers number reaches the limit, and still some partitions are
          // not cached. Append to the queue for the coming fallback to the sorted
          // sequential write.
          groups.drop(idx).foreach(g => pendingBatches.enqueue(g))
          // Set to null to avoid double close
          (idx until groups.length).foreach(groups(_) = null)
          logInfo(s"Number of concurrent writers ${concurrentWriters.size} reaches " +
            "the threshold. Fall back from concurrent writers to sort-based sequential" +
            " writer.")
        }
      }
    }
  }

  /** Release all resources. */
  override def releaseResources(): Unit = {
    pendingBatches.safeClose()
    pendingBatches.clear()

    // write all caches
    concurrentWriters.foreach { case (wi, ws) =>
      if (ws.tableCaches.nonEmpty) {
        writeOneCacheAndClose(wi, ws)
      }
    }

    // close all resources
    concurrentWriters.values.toSeq.safeClose()
    concurrentWriters.clear()
    super.releaseResources()
  }
}

/**
 * Bucketing specification for all the write tasks.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.WriterBucketSpec`
 * @param bucketIdExpression Expression to calculate bucket id based on bucket column(s).
 * @param bucketFileNamePrefix Prefix of output file name based on bucket id.
 */
case class GpuWriterBucketSpec(
  bucketIdExpression: GpuExpression,
  bucketFileNamePrefix: Int => String)

/**
 * A shared job description for all the GPU write tasks.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.WriteJobDescription`.
 */
class GpuWriteJobDescription(
    val uuid: String, // prevent collision between different (appending) write jobs
    val serializableHadoopConf: SerializableConfiguration,
    val outputWriterFactory: ColumnarOutputWriterFactory,
    val allColumns: Seq[Attribute],
    val dataColumns: Seq[Attribute],
    val partitionColumns: Seq[Attribute],
    val bucketSpec: Option[GpuWriterBucketSpec],
    val path: String,
    val customPartitionLocations: Map[TablePartitionSpec, String],
    val maxRecordsPerFile: Long,
    val timeZoneId: String,
    val statsTrackers: Seq[ColumnarWriteJobStatsTracker],
    val concurrentWriterPartitionFlushSize: Long)
  extends Serializable {

  lazy val fileIO: HadoopFileIO = new HadoopFileIO(serializableHadoopConf.value)

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}

object BucketIdMetaUtils {
  // Tag for the bucketing write using Spark Murmur3Hash
  def tagForBucketingWrite(meta: RapidsMeta[_, _, _], bucketSpec: Option[BucketSpec],
      outputColumns: Seq[Attribute]): Unit = {
    bucketSpec.foreach { bSpec =>
      // Create a Murmur3Hash expression to leverage the overriding types check.
      val expr = Murmur3Hash(
        bSpec.bucketColumnNames.map(n => outputColumns.find(_.name == n).get),
        GpuHashPartitioningBase.DEFAULT_HASH_SEED)
      val hashMeta = GpuOverrides.wrapExpr(expr, meta.conf, None)
      hashMeta.tagForGpu()
      if(!hashMeta.canThisBeReplaced) {
        meta.willNotWorkOnGpu(s"Murmur3 hashing for generating bucket IDs can not run" +
          s" on GPU. Details: ${hashMeta.explain(all=false)}")
      }
    }
  }

  def tagForBucketingHiveWrite(meta: RapidsMeta[_, _, _], bucketSpec: Option[BucketSpec],
      outputColumns: Seq[Attribute]): Unit = {
    bucketSpec.foreach { bSpec =>
      // Create a HiveHash expression to leverage the overriding types check.
      val expr = HiveHash(bSpec.bucketColumnNames.map(n => outputColumns.find(_.name == n).get))
      val hashMeta = GpuOverrides.wrapExpr(expr, meta.conf, None)
      hashMeta.tagForGpu()
      if (!hashMeta.canThisBeReplaced) {
        meta.willNotWorkOnGpu(s"Hive hashing for generating bucket IDs can not run" +
          s" on GPU. Details: ${hashMeta.explain(all = false)}")
      }
    }
  }

  def getWriteBucketSpecForHive(
      bucketCols: Seq[Attribute],
      numBuckets: Int): GpuWriterBucketSpec = {
    // Hive bucketed table: use "HiveHash" and bitwise-and as bucket id expression.
    // "bitwise-and" is used to handle the case of a wrong bucket id when
    // the hash value is negative.
    val hashId = GpuBitwiseAnd(GpuHiveHash(bucketCols), GpuLiteral(Int.MaxValue))
    val bucketIdExpression = GpuPmod(hashId, GpuLiteral(numBuckets))

    // The bucket file name prefix is following Hive, Presto and Trino conversion, then
    // Hive bucketed tables written by Plugin can be read by other SQL engines.
    val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
    GpuWriterBucketSpec(bucketIdExpression, fileNamePrefix)
  }
}
