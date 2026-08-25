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

package com.nvidia.spark.rapids

import java.util
import java.util.concurrent.{Future, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer, JCudfSerialization,
  MemoryBuffer, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.FileUtils.createTempFile
import com.nvidia.spark.rapids.GpuMetric.{ASYNC_READ_TIME, SYNC_READ_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitTargetSizeInHalfGpu, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.io.async.{ThrottlingExecutor, TrafficController}
import com.nvidia.spark.rapids.jni.kudo.{DumpOption, KudoGpuSerializer,
  KudoHostMergeResultWrapper, KudoSerializer, MergeOptions}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A pair of buffers used for kudo serialization format.
 * The offsets buffer contains a list of byte offsets that index into the data buffer,
 * which are used by the kudo deserialization algorithm.
 */
case class KudoBuffers[T <: MemoryBuffer](data: T, offsets: T)
  extends AutoCloseable {
  override def close(): Unit = {
    Seq(data, offsets).safeClose()
  }
}

/**
 * Coalesces serialized tables on the host up to the target batch size before transferring
 * the coalesced result to the GPU. This reduces the overhead of copying data to the GPU
 * and also helps avoid holding onto the GPU semaphore while shuffle I/O is being performed.
 *
 * @note This should ALWAYS appear in the plan after a GPU shuffle when RAPIDS shuffle is
 *       not being used.
 */
case class GpuShuffleCoalesceExec(child: SparkPlan, targetBatchByteSize: Long)
  extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._
  import GpuShuffleCoalesceUtils._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    SYNC_READ_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SYNC_READ_TIME),
    ASYNC_READ_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_ASYNC_READ_TIME),
    READ_THROTTLING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_READ_THROTTLING_TIME),
  )

  override protected val outputBatchesLevel = MODERATE_LEVEL

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val metricsMap = allMetrics
    val targetSize = targetBatchByteSize
    val dataTypes = GpuColumnVector.extractTypes(schema)
    val readOption = CoalesceReadOption(conf)

    child.executeColumnar().mapPartitions { iter =>
      getGpuShuffleCoalesceIterator(iter, targetSize, dataTypes,
        readOption, metricsMap)
    }
  }
}

/** A case class to pack some options. */
case class CoalesceReadOption private(
    kudoEnabled: Boolean, kudoMode: RapidsConf.ShuffleKudoMode.Value, kudoDebugMode: DumpOption,
    kudoDebugDumpPrefix: Option[String], useAsync: Boolean)

object CoalesceReadOption extends Logging {

  private def resolveUseAsync(kudoMode: RapidsConf.ShuffleKudoMode.Value,
      useAsync: Boolean): Boolean = {
    if (useAsync && kudoMode == RapidsConf.ShuffleKudoMode.GPU) {
      logWarning("Both shuffle async read and kudo GPU mode are enabled. These configurations " +
        "should not be set together. Giving precedence to GPU kudo mode and disabling async read.")
      false
    } else {
      useAsync
    }
  }

  def apply(conf: SQLConf): CoalesceReadOption = {
    val dumpOption = RapidsConf.SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE.get(conf) match {
      case "NEVER" => DumpOption.Never
      case "ALWAYS" => DumpOption.Always
      case "ONFAILURE" => DumpOption.OnFailure
    }
    val kudoEnabled = RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.get(conf)
    val kudoMode = RapidsConf.ShuffleKudoMode.withName(RapidsConf.SHUFFLE_KUDO_READ_MODE.get(conf))
    val useAsync = RapidsConf.SHUFFLE_ASYNC_READ_ENABLED.get(conf)

    val finalUseAsync = resolveUseAsync(kudoMode, useAsync)

    CoalesceReadOption(kudoEnabled, kudoMode, dumpOption,
      RapidsConf.SHUFFLE_KUDO_SERIALIZER_DEBUG_DUMP_PREFIX.get(conf), finalUseAsync)
  }

  def apply(conf: RapidsConf): CoalesceReadOption = {
    val finalUseAsync = resolveUseAsync(conf.shuffleKudoReadMode, conf.shuffleAsyncReadEnabled)
    CoalesceReadOption(conf.shuffleKudoSerializerEnabled, conf.shuffleKudoReadMode,
      conf.shuffleKudoSerializerDebugMode, conf.shuffleKudoSerializerDebugDumpPrefix, finalUseAsync)
  }
}

object GpuShuffleCoalesceUtils {
  /**
   * Creates a split policy that divides a sequence of tables based on target byte size.
   * splitTargetSizeInHalfGpu uses dataSize/2 when dataSize is known and smaller than
   * targetSize/2, ensuring the byte-size loop always finds a valid split point for 2+
   * tables. Throws GpuSplitAndRetryOOM if the sequence cannot be split further (single
   * table, or newTarget < minSize).
   */
  def createSplitPolicyByTargetSize[T <: AutoCloseable](
      tableOperator: SerializedTableOperator[T, _],
      minSize: Long): CloseableTableSeqWithTargetSize[T] =>
      Seq[CloseableTableSeqWithTargetSize[T]] = {
    (wrapper: CloseableTableSeqWithTargetSize[T]) => {
      val tables = wrapper
      if (tables.length <= 1) {
        throw new com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM(
          s"GPU OutOfMemory: a sequence of ${tables.length} tables cannot be split!")
      }

      val newTargetSize = splitTargetSizeInHalfGpu(wrapper.targetSize).head
      val targetByteSize = newTargetSize.targetSize

      var currentSize = 0L
      var splitIndex = 0
      var firstHalfSize = 0L
      for (i <- tables.indices) {
        val tableSize = tableOperator.getDataLen(tables(i))
        if (currentSize + tableSize > targetByteSize && i > 0) {
          splitIndex = i
          firstHalfSize = currentSize
          currentSize = Long.MaxValue  // stop iterating
        } else {
          currentSize += tableSize
        }
      }

      val firstHalfTables = tables.take(splitIndex)
      val secondHalfTables = tables.drop(splitIndex)
      val secondHalfSize = newTargetSize.dataSize - firstHalfSize

      Seq(
        CloseableTableSeqWithTargetSize(firstHalfTables,
          AutoCloseableTargetSize(targetByteSize, newTargetSize.minSize, firstHalfSize)),
        CloseableTableSeqWithTargetSize(secondHalfTables,
          AutoCloseableTargetSize(targetByteSize, newTargetSize.minSize, secondHalfSize))
      )
    }
  }

  /**
   * Return an iterator that will pull in batches from the input iterator,
   * concatenate them up to the "targetSize" and move the concatenated result
   * to the GPU for each output batch.
   * The input iterator is expected to contain only serialized host batches just
   * returned from the Shuffle deserializer. Otherwise, it will blow up.
   *
   * @param iter               the input iterator containing only serialized host batches
   * @param targetSize         the target batch size for coalescing
   * @param dataTypes          the schema of the input batches
   * @param readOption         the coalesce read option
   * @param metricsMap         metrics map
   * @param prefetchFirstBatch whether prefetching the first bundle of serialized
   *                           batches with the total size up to the "targetSize". The
   *                           prefetched batches will be cached on host until the "next()"
   *                           is called. This is for some optimization cases in join.
   */
  def getGpuShuffleCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      targetSize: Long,
      dataTypes: Array[DataType],
      readOption: CoalesceReadOption,
      metricsMap: Map[String, GpuMetric],
      prefetchFirstBatch: Boolean = false,
      minSplitSize: Long = 10L * 1024 * 1024): Iterator[ColumnarBatch] = {
    val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
    val inBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
    val inRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
    val outBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
    val outRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
    val opTimeMetric = metricsMap(GpuMetric.OP_TIME_LEGACY)
    val readThrottlingMetric = metricsMap(GpuMetric.READ_THROTTLING_TIME)
    val hostIter: Iterator[_ <: AutoCloseable] = if (readOption.kudoEnabled) {
      if (readOption.kudoMode == RapidsConf.ShuffleKudoMode.GPU) {
        new KudoGpuShuffleCoalesceIterator(iter, targetSize, dataTypes, concatTimeMetric,
          inBatchesMetric, inRowsMetric, readThrottlingMetric, readOption, minSplitSize)
      } else {
        new KudoHostShuffleCoalesceIterator(iter, targetSize, dataTypes, concatTimeMetric,
          inBatchesMetric, inRowsMetric, readThrottlingMetric, readOption, minSplitSize)
      }
    } else {
      new HostShuffleCoalesceIterator(iter, targetSize, concatTimeMetric, inBatchesMetric,
        inRowsMetric, readThrottlingMetric)
    }
    val maybeBufferedIter: Iterator[_ <: AutoCloseable] = if (prefetchFirstBatch) {
      val bufferedIter = new CloseableBufferedIterator(hostIter)
      NvtxRegistry.SHUFFLE_FETCH_FIRST_BATCH {
        // Force a coalesce of the first batch before we grab the GPU semaphore
        bufferedIter.headOption
      }
      bufferedIter
    } else {
      hostIter
    }
    if (readOption.useAsync) {
      new GpuShuffleAsyncCoalesceIterator(
        maybeBufferedIter.asInstanceOf[Iterator[CoalescedHostResult]], dataTypes, targetSize,
        outBatchesMetric, outRowsMetric, metricsMap(ASYNC_READ_TIME), opTimeMetric,
        readThrottlingMetric)
    } else {
      if (readOption.kudoEnabled && readOption.kudoMode == RapidsConf.ShuffleKudoMode.GPU) {
        new GpuColumnarBatchMetricIterator(
          maybeBufferedIter.asInstanceOf[Iterator[ColumnarBatch]],
          outBatchesMetric, outRowsMetric, metricsMap(SYNC_READ_TIME), opTimeMetric)
      } else {
        new GpuShuffleCoalesceIterator(
          maybeBufferedIter.asInstanceOf[Iterator[CoalescedHostResult]],
          dataTypes, outBatchesMetric, outRowsMetric, metricsMap(SYNC_READ_TIME), opTimeMetric)
      }
    }
  }

  /** Get the buffer size of a serialized batch just returned by the Shuffle deserializer */
  def getSerializedBufferSize(cb: ColumnarBatch): Long = {
    assert(cb.numCols() == 1)
    cb.column(0) match {
      case col: KudoSerializedTableColumn => col.spillableKudoTable.length
      case serCol: SerializedTableColumn => {
        val hmb = serCol.hostBuffer
        if (hmb != null) hmb.getLength else 0L
      }
      case o => throw new IllegalStateException(s"unsupported type: ${o.getClass}")
    }
  }
}

/**
 * A trait representing the shuffle coalesced result by the Shuffle coalesce iterator.
 */
trait CoalescedHostResult extends AutoCloseable {
  /** Convert itself to a GPU batch */
  def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch

  /** Get the data size */
  def getDataSize: Long
}

/**
 * A trait defining some operations on the table T for concatenation.
 * This is used by CoalesceIteratorBase to separate the table operations from
 * the shuffle read process.
 */
sealed trait SerializedTableOperator[T <: AutoCloseable, R] {
  def getDataLen(table: T): Long

  def getNumRows(table: T): Int

  def concat(tables: Array[T]): R
}

class JCudfCoalescedHostResult(hostConcatResult: HostConcatResult) extends CoalescedHostResult {
  assert(hostConcatResult != null, "hostConcatResult should not be null")

  override def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch =
    cudf_utils.HostConcatResultUtil.getColumnarBatch(hostConcatResult, dataTypes)

  override def close(): Unit = hostConcatResult.close()

  override def getDataSize: Long = hostConcatResult.getTableHeader.getDataLen
}

class JCudfTableOperator
  extends SerializedTableOperator[SerializedTableColumn, CoalescedHostResult] {
  override def getDataLen(table: SerializedTableColumn): Long = table.header.getDataLen

  override def getNumRows(table: SerializedTableColumn): Int = table.header.getNumRows

  override def concat(tables: Array[SerializedTableColumn]): CoalescedHostResult = {
    assert(tables.nonEmpty, "no tables to be concatenated")
    val numCols = tables.head.header.getNumColumns
    val ret = if (numCols == 0) {
      val totalRowsNum = tables.map(getNumRows).sum
      cudf_utils.HostConcatResultUtil.rowsOnlyHostConcatResult(totalRowsNum)
    } else {
      val (headers, buffers) = tables.map(t => (t.header, t.hostBuffer)).unzip
      JCudfSerialization.concatToHostBuffer(headers, buffers)
    }
    new JCudfCoalescedHostResult(ret)
  }
}

case class RowCountOnlyMergeResult(rowCount: Int) extends CoalescedHostResult {
  override def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch = {
    new ColumnarBatch(Array.empty, rowCount)
  }

  override def getDataSize: Long = 0

  override def close(): Unit = {}
}

class KudoTableOperator(kudo: Option[KudoSerializer], readOption: CoalesceReadOption,
    taskIdentifier: String)
  extends SerializedTableOperator[KudoSerializedTableColumn, CoalescedHostResult] {
  require(kudo != null, "kudo serializer should not be null")

  override def getDataLen(column: KudoSerializedTableColumn): Long =
    column.spillableKudoTable.header
    .getTotalDataLen

  override def getNumRows(column: KudoSerializedTableColumn): Int =
    column.spillableKudoTable.header
    .getNumRows

  private def buildMergeOptions(): MergeOptions = {
    val dumpOption = readOption.kudoDebugMode
    val dumpPrefix = readOption.kudoDebugDumpPrefix
    if (dumpOption != DumpOption.Never && dumpPrefix.isDefined) {
      val updatedPrefix = s"${dumpPrefix.get}_${taskIdentifier}"
      lazy val (out, path) = createTempFile(new Configuration(), updatedPrefix, ".bin")
      new MergeOptions(dumpOption, () => out, path.toString)
    } else {
      new MergeOptions(dumpOption, null, null)
    }
  }

  override def concat(columns: Array[KudoSerializedTableColumn]): CoalescedHostResult = {
    require(columns.nonEmpty, "no tables to be concatenated")
    val numCols = columns.head.spillableKudoTable.header.getNumColumns
    if (numCols == 0) {
      val totalRowsNum = columns.map(getNumRows).sum
      RowCountOnlyMergeResult(totalRowsNum)
    } else {
      // "lock" all input tables in memory before merge
      withResource(columns.safeMap(_.spillableKudoTable.makeKudoTable)) { kudoTables =>
        val result = kudo.get.mergeOnHost(kudoTables, buildMergeOptions())
        KudoHostMergeResultWrapper(result)
      }
    }
  }
}


class KudoGpuTableOperator(dataTypes: Array[DataType])
  extends SerializedTableOperator[KudoSerializedTableColumn, ColumnarBatch] {

  override def getDataLen(column: KudoSerializedTableColumn): Long =
    column.spillableKudoTable.header
      .getTotalDataLen

  override def getNumRows(column: KudoSerializedTableColumn): Int =
    column.spillableKudoTable.header
      .getNumRows

  override def concat(columns: Array[KudoSerializedTableColumn]): ColumnarBatch = {
    require(columns.nonEmpty, "no tables to be concatenated")
    val numCols = columns.head.spillableKudoTable.header.getNumColumns
    if (numCols == 0) {
      val totalRowsNum = columns.map(getNumRows).sum
      new ColumnarBatch(Array.empty, totalRowsNum)
    } else {
      withResource(columns.safeMap(_.spillableKudoTable.makeKudoTable)) { kudoTables =>
        val dataBufSize = kudoTables.foldLeft(0L) { (acc, table) =>
          acc + table.getHeader.getTotalDataLen + table.getHeader.getSerializedSize
        }
        val offsetsBufSize = 8L * (kudoTables.length + 1)
        val dataHostBuf = HostMemoryBuffer.allocate(dataBufSize)
        val hostBuffers = closeOnExcept(dataHostBuf) { _ =>
          KudoBuffers(dataHostBuf, HostMemoryBuffer.allocate(offsetsBufSize))
        }
        withResource(hostBuffers) { case KudoBuffers(dataHost, offsetsHost) =>
          var currentOffset = 0L
          kudoTables.zipWithIndex.foreach { case (table, i) =>
            offsetsHost.setLong(i * 8L, currentOffset)
            table.getHeader.writeTo(dataHost, currentOffset)
            currentOffset += table.getHeader.getSerializedSize
            dataHost.copyFromHostBuffer(currentOffset, table.getBuffer, 0,
              table.getBuffer.getLength)

            currentOffset += table.getHeader.getTotalDataLen
          }
          offsetsHost.setLong(kudoTables.length * 8L, currentOffset)
          val dataDevBuf = DeviceMemoryBuffer.allocate(dataHost.getLength)
          val devBuffers = closeOnExcept(dataDevBuf) { _ =>
            KudoBuffers(dataDevBuf, DeviceMemoryBuffer.allocate(offsetsHost.getLength))
          }
          withResource(devBuffers) { case KudoBuffers(dataDev, offsetsDev) =>
            dataDev.copyFromHostBuffer(dataHost)
            offsetsDev.copyFromHostBuffer(offsetsHost)

            val schema = GpuColumnVector.from(dataTypes)
            withResource(KudoGpuSerializer.assembleFromDeviceRaw(
                schema, dataDev, offsetsDev)) { table =>
              GpuColumnVector.from(table, dataTypes)
            }
          }
        }
      }
    }
  }
}

/**
 * A wrapper for a sequence of tables with target size information that allows
 * splitting based on byte size when OOM occurs. Extends Seq[T] so it can be
 * used directly as a sequence.
 */
case class CloseableTableSeqWithTargetSize[T <: AutoCloseable](
    tables: Seq[T],
    targetSize: AutoCloseableTargetSize) extends Seq[T] with AutoCloseable {
  override def close(): Unit = {
    tables.foreach(_.safeClose())
    targetSize.close()
  }

  // Seq implementation
  override def length: Int = tables.length
  override def iterator: Iterator[T] = tables.iterator
  override def apply(idx: Int): T = tables.apply(idx)

  // Keep retry-OOM reporting bounded. SeqLike.toString would stringify every table.
  override def toString: String = {
    s"CloseableTableSeqWithTargetSize(numTables=$length, " +
      s"targetSize=${targetSize.targetSize}, minSize=${targetSize.minSize})"
  }
}


/**
 * Common base class for coalescing iterators that handles the core logic of
 * collecting serialized tables and managing batch sizes. Subclasses handle
 * the specific concatenation logic (host vs GPU) and async behavior.
 */
abstract class CoalesceIteratorBase[T <: AutoCloseable : ClassTag, R <: AutoCloseable](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    concatTimeMetric: GpuMetric,
    inputBatchesMetric: GpuMetric,
    inputRowsMetric: GpuMetric,
    readThrottlingMetric: GpuMetric,
    useAsync: Boolean = false,
    minSplitSize: Long = 10L * 1024 * 1024 // 10 MB default
) extends Iterator[R] with AutoCloseable {
  protected[this] val serializedTables = new util.ArrayDeque[T]
  @volatile protected[this] var numTablesInBatch: Int = 0
  @volatile protected[this] var numRowsInBatch: Int = 0
  @volatile protected[this] var batchByteSize: Long = 0L
  @volatile protected[this] var pendingResultIter: Option[Iterator[R]] = None
  @volatile protected[this] var inputIter:
    Option[Iterator[CloseableTableSeqWithTargetSize[T]]] = None

  protected val tableOperator: SerializedTableOperator[T, R]
  protected val splitPolicy: Option[CloseableTableSeqWithTargetSize[T] =>
      Seq[CloseableTableSeqWithTargetSize[T]]] = None
  protected val minSplitSizeForRetry: Long = minSplitSize

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  override def close(): Unit = {
    val allCloseables = new ArrayBuffer[AutoCloseable]()
    // Collect all serialized tables
    serializedTables.forEach(table => allCloseables += table)
    // Collect all AutoCloseable results from pending iterator
    inputIter.foreach(_.foreach {
      case ac: AutoCloseable => allCloseables += ac
      case _ => // non-closeable result
    })
    // Close all AutoCloseables together
    allCloseables.safeClose()

    serializedTables.clear()
    pendingResultIter = None
    inputIter = None
  }

  /**
   * Get the next result, checking for pending results from splits first.
   * Returns Some(result) if there's a result available, None if we need
   * to concatenate a new batch.
   */
  protected def getNextResult(): Option[R] = {
    pendingResultIter.flatMap { iter =>
      if (iter.hasNext) {
        Some(iter.next())
      } else {
        pendingResultIter = None
        None
      }
    }
  }

  protected def concatenateTables(tables: ArrayBuffer[T]): R = {
    withResource(new MetricRange(concatTimeMetric)) { _ =>
      val tablesSeq = tables.toSeq
      splitPolicy match {
        case Some(policy) =>
          val dataSize = tablesSeq.map(tableOperator.getDataLen).sum
          val targetSizeWrapper = AutoCloseableTargetSize(targetBatchByteSize,
            minSplitSizeForRetry, dataSize)
          val wrapper = CloseableTableSeqWithTargetSize(tablesSeq, targetSizeWrapper)
          val wrapperIter = Iterator(wrapper)
          inputIter = Some(wrapperIter)
          val resultIter = withRetry(wrapperIter, policy) { wrappedSeq =>
            tableOperator.concat(wrappedSeq.toArray)
          }
          // Store the iterator so we can yield results one by one if splits occurred
          if (resultIter.hasNext) {
            val firstResult = resultIter.next()
            closeOnExcept(firstResult) { _ =>
              // If there are more results, store the iterator for subsequent calls
              if (resultIter.hasNext) {
                pendingResultIter = Some(resultIter)
              }
            }
            firstResult
          } else {
            throw new IllegalStateException("withRetry returned empty iterator")
          }
        case None =>
          withRetryNoSplit(tablesSeq) { tablesSeq =>
            tableOperator.concat(tablesSeq.toArray)
          }
      }
    }
  }

  protected def bufferNextBatch(): Unit = {
    if (numTablesInBatch == serializedTables.size()) {
      var batchCanGrow = batchByteSize < targetBatchByteSize
      while (batchCanGrow && iter.hasNext) {
        closeOnExcept(iter.next()) { batch =>
          inputBatchesMetric += 1
          // don't bother tracking empty tables
          if (batch.numRows > 0) {
            inputRowsMetric += batch.numRows()
            val tableColumn = batch.column(0).asInstanceOf[T]
            batchCanGrow = canAddToBatch(tableColumn)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableOperator.getNumRows(tableColumn)
              batchByteSize += tableOperator.getDataLen(tableColumn)
            }
          } else {
            batch.close()
          }
        }
      }
    }
  }

  protected def canAddToBatch(nextTable: T): Boolean = {
    (batchByteSize + tableOperator.getDataLen(nextTable) <= targetBatchByteSize) &&
    (numRowsInBatch.toLong + tableOperator.getNumRows(nextTable) <= Integer.MAX_VALUE)
  }

  protected def updateBatchState(): Unit = {
    // Update the stats for the next batch in progress.
    // Note that the modification of these variables will happen before
    // the modifications in async bufferNextBatch(), we just need to ensure
    // their visibility to the next thread.
    numTablesInBatch = serializedTables.size
    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1,
        "should only track at most one buffer that is not in a batch")
      val firstTable = serializedTables.peekFirst()
      batchByteSize = tableOperator.getDataLen(firstTable)
      numRowsInBatch = tableOperator.getNumRows(firstTable)
    }
  }

  protected def extractAndUpdateBatch(): ArrayBuffer[T] = {
    val input = new ArrayBuffer[T]()
    for (_ <- 0 until numTablesInBatch)
      input += serializedTables.removeFirst()
    updateBatchState()
    input
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * serialized tables from shuffle. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
abstract class HostCoalesceIteratorBase[T <: AutoCloseable : ClassTag](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    concatTimeMetric: GpuMetric,
    inputBatchesMetric: GpuMetric,
    inputRowsMetric: GpuMetric,
    readThrottlingMetric: GpuMetric,
    useAsync: Boolean = false,
    minSplitSize: Long = 10L * 1024 * 1024 // 10 MB default
) extends CoalesceIteratorBase[T, CoalescedHostResult](
    iter, targetBatchByteSize, concatTimeMetric, inputBatchesMetric,
    inputRowsMetric, readThrottlingMetric, useAsync, minSplitSize) {

  private var executor: Option[ThrottlingExecutor] = None

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  private var bufferingFuture: Option[Future[_]] = None

  protected val tableOperator: SerializedTableOperator[T, CoalescedHostResult]

  override def close(): Unit = {
    super.close()
    executor.foreach { e =>
      e.shutdownNow(10, TimeUnit.SECONDS)
    }
  }

  private def concatenateTablesInHost(): CoalescedHostResult = {
    val input = extractAndUpdateBatch()

    if (useAsync && iter.hasNext) {
      if (executor.isEmpty) {
        executor =
          Some(new ThrottlingExecutor(
            TrampolineUtil.newDaemonCachedThreadPool(
              "async buffer thread for " + Thread.currentThread().getName, 1, 1),
            TrafficController.getReadInstance,
            stat => {
              readThrottlingMetric.add(stat.accumulatedThrottleTimeNs)
            }
          ))
      }
      bufferingFuture = Option(executor.get.submit(
        () => {
          NvtxRegistry.ASYNC_SHUFFLE_BUFFER {
            bufferNextBatch()
          }
        },
        targetBatchByteSize // This is just a estimation, may overestimate.
      ))
    }

    concatenateTables(input)
  }

  override def hasNext: Boolean = {
    // Check for pending results from splits first
    if (pendingResultIter.exists(_.hasNext)) {
      true
    } else {
      // Don't do any heavy things here to support the async read by
      // GpuShuffleAsyncCoalesceIterator.
      // Suppose "iter.hasNext" reads in only a header which should be small
      // enough to make this a very lightweight operation.
      bufferingFuture.isDefined || !serializedTables.isEmpty || iter.hasNext
    }
  }

  override def next(): CoalescedHostResult = {
    if (!hasNext) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }

    // Check for pending results from splits first
    getNextResult() match {
      case Some(result) => result
      case None =>
        // No pending results, proceed with normal concatenation
        bufferingFuture.map(f => {
          NvtxRegistry.ASYNC_SHUFFLE_BUFFER {
            f.get()
          }
        }).getOrElse({
          NvtxRegistry.ASYNC_SHUFFLE_BUFFER {
            bufferNextBatch()
          }
        }
        )
        bufferingFuture = None

        NvtxRegistry.SHUFFLE_CONCAT_CPU {
          concatenateTablesInHost()
        }
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * serialized tables from shuffle. The serialized tables within are collected up
 * to the target batch size and then concatenated on the gpu before handing
 * them to the caller on `.next()`
 */
abstract class GpuCoalesceIteratorBase[T <: AutoCloseable : ClassTag](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    concatTimeMetric: GpuMetric,
    inputBatchesMetric: GpuMetric,
    inputRowsMetric: GpuMetric,
    readThrottlingMetric: GpuMetric,
    useAsync: Boolean = false,
    minSplitSize: Long = 10L * 1024 * 1024 // 10 MB default
) extends CoalesceIteratorBase[T, ColumnarBatch](
    iter, targetBatchByteSize, concatTimeMetric, inputBatchesMetric,
    inputRowsMetric, readThrottlingMetric, useAsync, minSplitSize) {

  protected val tableOperator: SerializedTableOperator[T, ColumnarBatch]

  private def concatenateTablesInGpu(): ColumnarBatch = {
    val input = extractAndUpdateBatch()
    concatenateTables(input)
  }

  override def hasNext: Boolean = {
    // Check for pending results from splits first
    if (pendingResultIter.exists(_.hasNext)) {
      true
    } else {
      !serializedTables.isEmpty || iter.hasNext
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }

    // Check for pending results from splits first
    getNextResult() match {
      case Some(result) => result
      case None =>
        // No pending results, proceed with normal concatenation
        withResource(new NvtxRange("BufferNextBatch", NvtxColor.ORANGE)) { _ =>
          bufferNextBatch()
        }

        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withResource(new NvtxRange("concatTablesInGpu", NvtxColor.ORANGE)) { _ =>
          concatenateTablesInGpu()
        }
    }
  }
}

class HostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    concatTimeMetric: GpuMetric = NoopMetric,
    inputBatchesMetric: GpuMetric = NoopMetric,
    inputRowsMetric: GpuMetric = NoopMetric,
    readThrottlingMetric: GpuMetric = NoopMetric)
  extends HostCoalesceIteratorBase[SerializedTableColumn](iter, targetBatchSize,
    concatTimeMetric, inputBatchesMetric, inputRowsMetric, readThrottlingMetric) {
  override protected val tableOperator = new JCudfTableOperator
}

class KudoHostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    concatTimeMetric: GpuMetric = NoopMetric,
    inputBatchesMetric: GpuMetric = NoopMetric,
    inputRowsMetric: GpuMetric = NoopMetric,
    readThrottlingMetric: GpuMetric = NoopMetric,
    readOption: CoalesceReadOption,
    minSplitSize: Long = 10L * 1024 * 1024 // 10 MB default
    )
  extends HostCoalesceIteratorBase[KudoSerializedTableColumn](iter,
    targetBatchSize, concatTimeMetric, inputBatchesMetric, inputRowsMetric,
    readThrottlingMetric, readOption.useAsync, minSplitSize) {

  // Capture TaskContext info during RDD execution when it's available
  private val taskIdentifier = Option(TaskContext.get()) match {
    case Some(tc) => s"stage_${tc.stageId()}_task_${tc.taskAttemptId()}"
    case None => java.util.UUID.randomUUID().toString
  }

  override protected val tableOperator = {
    val kudoSer = if (dataTypes.nonEmpty) {
      Some(new KudoSerializer(GpuColumnVector.from(dataTypes)))
    } else {
      None
    }
    new KudoTableOperator(kudoSer, readOption, taskIdentifier)
  }

  // Create the byte-size-based split policy after tableOperator is initialized
  override protected val splitPolicy: Option[
      CloseableTableSeqWithTargetSize[KudoSerializedTableColumn] =>
      Seq[CloseableTableSeqWithTargetSize[KudoSerializedTableColumn]]] = {
    Some(GpuShuffleCoalesceUtils.createSplitPolicyByTargetSize(tableOperator, minSplitSizeForRetry))
  }
}

class KudoGpuShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    concatTimeMetric: GpuMetric = NoopMetric,
    inputBatchesMetric: GpuMetric = NoopMetric,
    inputRowsMetric: GpuMetric = NoopMetric,
    readThrottlingMetric: GpuMetric = NoopMetric,
    readOption: CoalesceReadOption,
    minSplitSize: Long = 10L * 1024 * 1024 // 10 MB default
    )
  extends GpuCoalesceIteratorBase[KudoSerializedTableColumn](iter,
    targetBatchSize, concatTimeMetric, inputBatchesMetric, inputRowsMetric,
    readThrottlingMetric, readOption.useAsync, minSplitSize) {

  override protected val tableOperator:
    SerializedTableOperator[KudoSerializedTableColumn, ColumnarBatch] =
    new KudoGpuTableOperator(dataTypes)

  // Create the byte-size-based split policy after tableOperator is initialized
  override protected val splitPolicy: Option[
      CloseableTableSeqWithTargetSize[KudoSerializedTableColumn] =>
      Seq[CloseableTableSeqWithTargetSize[KudoSerializedTableColumn]]] = {
    Some(GpuShuffleCoalesceUtils.createSplitPolicyByTargetSize(tableOperator, minSplitSizeForRetry))
  }
}


/**
 * Base class for GPU iterators that handle output metrics tracking.
 * Subclasses implement the specific batch conversion logic.
 */
abstract class GpuMetricIteratorBase[T](
    iter: Iterator[T],
    outputBatchesMetric: GpuMetric = NoopMetric,
    outputRowsMetric: GpuMetric = NoopMetric,
    readTimeMetric: GpuMetric = NoopMetric,
    opTimeMetric: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = GpuMetric.ns(readTimeMetric, opTimeMetric) {
    iter.hasNext
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more columnar batches")
    }
    val inputBatch = GpuMetric.ns(readTimeMetric, opTimeMetric) {
      iter.next()
    }
    GpuMetric.ns(opTimeMetric) {
      NvtxRegistry.SHUFFLE_CONCAT_LOAD_BATCH {
        val batch = convertToOutputBatch(inputBatch)
        outputBatchesMetric += 1
        outputRowsMetric += batch.numRows()
        batch
      }
    }
  }

  /** Convert the input batch type T to the final ColumnarBatch output */
  protected def convertToOutputBatch(inputBatch: T): ColumnarBatch
}

/**
 * GPU iterator for batches that are already in ColumnarBatch format.
 * No conversion needed, just passes through with metrics tracking.
 */
class GpuColumnarBatchMetricIterator(
    iter: Iterator[ColumnarBatch],
    outputBatchesMetric: GpuMetric = NoopMetric,
    outputRowsMetric: GpuMetric = NoopMetric,
    readTimeMetric: GpuMetric = NoopMetric,
    opTimeMetric: GpuMetric = NoopMetric)
  extends GpuMetricIteratorBase[ColumnarBatch](
    iter, outputBatchesMetric, outputRowsMetric, readTimeMetric, opTimeMetric) {

  override protected def convertToOutputBatch(batch: ColumnarBatch): ColumnarBatch = batch
}

/**
 * Iterator that expects only "CoalescedHostResult"s as the input, and transfers
 * them to GPU.
 */
class GpuShuffleCoalesceIterator(iter: Iterator[CoalescedHostResult],
    dataTypes: Array[DataType],
    outputBatchesMetric: GpuMetric = NoopMetric,
    outputRowsMetric: GpuMetric = NoopMetric,
    readTimeMetric: GpuMetric = NoopMetric,
    opTimeMetric: GpuMetric = NoopMetric) extends GpuMetricIteratorBase[CoalescedHostResult](
    iter, outputBatchesMetric, outputRowsMetric, readTimeMetric, opTimeMetric) {

  override protected def convertToOutputBatch(hostCoalescedResult: CoalescedHostResult):
    ColumnarBatch = {
    withResource(hostCoalescedResult) { _ =>
      // We acquire the GPU regardless of whether `hostConcatResult`
      // is an empty batch or not, because the downstream tasks expect
      // the `GpuShuffleCoalesceIterator` to acquire the semaphore and may
      // generate GPU data from batches that are empty.
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      hostCoalescedResult.toGpuBatch(dataTypes)
    }
  }
}

