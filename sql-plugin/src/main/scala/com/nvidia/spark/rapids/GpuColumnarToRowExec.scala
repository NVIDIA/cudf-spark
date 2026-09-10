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

package com.nvidia.spark.rapids

import scala.annotation.tailrec
import scala.collection.mutable.Queue

import ai.rapids.cudf.{Cuda, HostColumnVector, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.RowConversion
import com.nvidia.spark.rapids.shims.{CudfUnsafeRow, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuColumnToRowMapPartitionsRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An iterator that uses the GPU for columnar to row conversion of fixed width types.
 */
class AcceleratedColumnarToRowIterator(
    schema: Seq[Attribute],
    batches: Iterator[ColumnarBatch],
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric,
    streamTime: GpuMetric) extends Iterator[InternalRow] with Serializable {
  @transient private var pendingCvs: Queue[HostColumnVector] = Queue.empty
  // GPU batches read in must be closed by the receiver (us)
  @transient private var currentCv: Option[HostColumnVector] = None
  // The accelerated path now also accepts STRING (8-byte offset/length slot in the JCUDF row)
  // and DECIMAL128 (16-byte rep). Use the canonical C2R gate so this assertion stays in sync
  // with whatever CudfUnsafeRow can decode.
  assertInTests(schema.forall(attr => CudfRowTransitions.isC2RSupportedType(attr.dataType)))
  // We want to remap the rows to improve packing.  This means that they should be sorted by
  // the largest alignment to the smallest.

  // for packMap the nth entry is the index of the original input column that we want at
  // the nth entry.
  //
  // The JCUDF row layout stores a STRING/BINARY column as an 8-byte (uint32 offset, uint32
  // length) slot in the fixed-width region — not Spark's defaultSize-of-20 estimate — so
  // sorting by that real slot size keeps variable-width columns adjacent to LONG/DOUBLE and
  // avoids wasted padding between LONG and INT.
  private def packSizeOf(dt: DataType): Int = dt match {
    case StringType | BinaryType => 8
    case _                       => DecimalUtil.getDataTypeSize(dt)
  }

  private val packMap: Array[Int] = schema
    .zipWithIndex
    .sortWith { (x, y) => packSizeOf(x._1.dataType) > packSizeOf(y._1.dataType) }
    .map(_._2)
    .toArray
  // For unpackMap the nth entry is the index in the row that came back for the original
  private val unpackMap: Array[Int] = packMap
      .zipWithIndex
      .sortWith(_._1 < _._1)
      .map(_._2)

  private val outputRow = new CudfUnsafeRow(packMap.map(schema(_)), unpackMap)
  private var baseDataAddress: Long = -1
  private var at: Int = 0
  private var total: Int = 0

  // Pre-computed kernel-selection inputs. The fixed-width-optimized kernel only accepts
  // fixed-width columns (DType.getSizeInBytes > 0 — i.e. not STRING/BINARY/LIST) and only
  // tolerates per-row sizes up to ~1536 bytes; past that it throws "Row size is too large
  // to fit in shared memory". 96 DECIMAL128 columns (96 * 16 = 1536, plus validity) already
  // exceeds that limit, so the column-count check alone is not enough. Hoist both checks
  // out of the per-batch hot path.
  private val schemaAllFixedWidth: Boolean = schema.forall { a =>
    GpuColumnVector.getNonNestedRapidsType(a.dataType).getSizeInBytes > 0
  }
  private val schemaEstimatedRowBytes: Int =
    CudfUnsafeRow.getRowSizeEstimate(packMap.map(schema(_)))
  // Matches max_shared_size / 32 (smallest possible block_size that still divides by 32) in
  // calc_fixed_width_kernel_dims; see row_conversion.cu :: calc_fixed_width_kernel_dims.
  private val fixedWidthOptimizedRowLimitBytes = 1536

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeAllPendingBatches()
    }
  }

  private def setCurrentBatch(wip: HostColumnVector): Unit = {
    currentCv = Some(wip)
    at = 0
    total = wip.getRowCount().toInt
    val byteBuffer = currentCv.get.getChildColumnView(0).getData
    baseDataAddress = byteBuffer.getAddress
  }

  private def closeCurrentBatch(): Unit = {
    currentCv.foreach(_.close())
    currentCv = None
  }

  private def closeAllPendingBatches(): Unit = {
    closeCurrentBatch()
    pendingCvs.foreach(_.close())
    pendingCvs = Queue.empty
  }

  private def rearrangeRows(cb: ColumnarBatch): Table = {
    val columns = GpuColumnVector.extractBases(cb)
    val rearrangedColumns = packMap.map(columns(_))
    new Table(rearrangedColumns : _*)
  }

  private[this] def setupBatchAndClose(scb: SpillableColumnarBatch): Boolean = {
    numInputBatches += 1
    // In order to match the numOutputRows metric in the generated code we update
    // numOutputRows for each batch. This is less accurate than doing it at output
    // because it will over count the number of rows output in the case of a limit,
    // but it is more efficient.
    numOutputRows += scb.numRows()
    if (scb.numRows() > 0) {
      NvtxIdWithMetrics(NvtxRegistry.COLUMNAR_TO_ROW_BATCH, opTime) {
        val it = RmmRapidsRetryIterator.withRetry(scb, splitSpillableInHalfByRows) { attempt =>
          withResource(attempt.getColumnarBatch()) { attemptCb =>
            withResource(rearrangeRows(attemptCb)) { table =>
              // The fixed-width-optimized kernel beats the generic transpose for narrow
              // (< 100 cols) all-fixed-width schemas, but rejects STRING/BINARY and any row
              // wider than the per-block shmem budget (~1536 bytes; see the matching limit
              // in row_conversion.cu :: calc_fixed_width_kernel_dims). Fall back to the
              // generic convertToRows when either constraint is violated.
              val useOptimized =
                schema.length < 100 &&
                  schemaAllFixedWidth &&
                  schemaEstimatedRowBytes <= fixedWidthOptimizedRowLimitBytes
              if (useOptimized) {
                RowConversion.convertToRowsFixedWidthOptimized(table)
              } else {
                RowConversion.convertToRows(table)
              }
            }
          }
        }
        assertInTests(
          it.hasNext, "Got an unexpected empty iterator after setting up batch with retry")
        it.foreach { rowsCvList =>
          withResource(rowsCvList) { _ =>
            rowsCvList.foreach { rowsCv =>
              pendingCvs += rowsCv.copyToHost()
            }
          }
        }
        setCurrentBatch(pendingCvs.dequeue())
        true
      }
    } else { // scb.numRows() <= 0
      scb.close()
      false
    }
  }

  private[this] def loadNextBatch(): Unit = {
    closeCurrentBatch()
    if (pendingCvs.nonEmpty) {
      setCurrentBatch(pendingCvs.dequeue())
    } else {
      populateBatch()
    }
    GpuSemaphore.releaseIfNecessary(TaskContext.get())
  }

  @tailrec
  private def populateBatch(): Unit = {
    // keep fetching input batches until we have a non-empty batch ready
    val nextBatch = fetchNextBatch()
    if (nextBatch.isDefined) {
      if (!setupBatchAndClose(nextBatch.get)) {
        populateBatch()
      }
    }
  }

  private def fetchNextBatch(): Option[SpillableColumnarBatch] = {
    NvtxIdWithMetrics(NvtxRegistry.COLUMNAR_TO_ROW_FETCH, streamTime) {
      if (batches.hasNext) {
        // Make it spillable once getting a columnar batch.
        val spillBatch = closeOnExcept(batches.next()) { cb =>
          SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        Some(spillBatch)
      } else {
        None
      }
    }
  }

  override def hasNext: Boolean = {
    val itHasNext = at < total
    if (!itHasNext) {
      loadNextBatch()
      at < total
    } else {
      itHasNext
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    // Here we should do some code generation, but for now
    val startReadOffset = currentCv.get.getStartListOffset(at)
    val endReadOffset = currentCv.get.getEndListOffset(at)
    outputRow.pointTo(baseDataAddress + startReadOffset, (endReadOffset - startReadOffset).toInt)
    at += 1
    outputRow
  }
}

/**
 * ColumnarToRowIterator converts GPU ColumnarBatches to CPU InternalRows.
 *
 * @note releaseSemaphore = true (default) should only be used in cases where
 *       we are sure that no GPU memory is left unaccounted for (not spillable).
 *       One notable case where releaseSemaphore is false is when used in
 *       `GpuUserDefinedFunction`, which is evaluated as part of a projection, that
 *       may or may not include other GPU columns.
 */
class ColumnarToRowIterator(batches: Iterator[ColumnarBatch],
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric,
    streamTime: GpuMetric,
    nullSafe: Boolean = false,
    releaseSemaphore: Boolean = true) extends Iterator[InternalRow] with AutoCloseable {
  // GPU batches read in must be closed by the receiver (us)
  @transient private var cb: ColumnarBatch = null
  private var it: java.util.Iterator[InternalRow] = null

  private[this] lazy val toHost = if (nullSafe) {
    (gpuCV: GpuColumnVector) => gpuCV.copyToNullSafeHost()
  } else{
    (gpuCV: GpuColumnVector) => gpuCV.copyToHost()
  }

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeCurrentBatch()
    }
  }

  override def close(): Unit = closeCurrentBatch()

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  def loadNextBatch(): Unit = {
    closeCurrentBatch()
    it = null
    // devCb will be None if the parent iterator is empty
    val devCb = fetchNextBatch()
    // perform conversion
    try {
      devCb.foreach { devCb =>
        val sDevCb = SpillableColumnarBatch(devCb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        cb = withRetryNoSplit(sDevCb) { _ =>
          withResource(sDevCb.getColumnarBatch()) { devCb =>
            NvtxIdWithMetrics(NvtxRegistry.COLUMNAR_TO_ROW_BATCH, opTime) {
              new ColumnarBatch(GpuColumnVector.extractColumns(devCb).safeMap(toHost),
                devCb.numRows())
            }
          }
        }
        it = cb.rowIterator()
        // In order to match the numOutputRows metric in the generated code we update
        // numOutputRows for each batch. This is less accurate than doing it at output
        // because it will over count the number of rows output in the case of a limit,
        // but it is more efficient.
        numOutputRows += cb.numRows()
      }
    } finally {
      // Leaving the GPU for a while: if this iterator is configured to release
      // the semaphore, do it now.
      if (releaseSemaphore) {
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      }
    }
  }

  private def fetchNextBatch(): Option[ColumnarBatch] = {
    NvtxIdWithMetrics(NvtxRegistry.COLUMNAR_TO_ROW_FETCH, streamTime) {
      while (batches.hasNext) {
        numInputBatches += 1
        val devCb = batches.next()
        if (devCb.numRows() > 0) {
          return Some(devCb)
        } else {
          devCb.close()
        }
      }
      None
    }
  }

  override def hasNext: Boolean = {
    val itHasNext = it != null && it.hasNext
    if (!itHasNext) {
      loadNextBatch()
      it != null && it.hasNext
    } else {
      itHasNext
    }
  }

  override def next(): InternalRow = {
    if (it == null || !it.hasNext) {
      loadNextBatch()
    }
    if (it == null) {
      throw new NoSuchElementException()
    }
    it.next()
  }
}

object CudfRowTransitions {
  // Types whose encoded width on the R2C side is one of {1, 2, 4, 8} bytes, which is all the
  // GeneratedInternalRowToCudfRowIterator codegen knows how to emit. Widening this set requires
  // teaching that codegen to write 16-byte fields (DECIMAL128) and variable-width payloads
  // (STRING / BINARY) into the JCUDF row, which has not been done yet.
  def isR2CSupportedType(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BooleanType | DateType | TimestampType => true
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => true
    case _ => false
  }

  // Types that the C2R fast path can decode out of a JCUDF row. STRING is encoded in the row
  // as a (uint32 offset, uint32 length) slot pointing to the row-trailing variable-width
  // region; CudfUnsafeRow.getUTF8String reads it in place. DECIMAL128 (precision in (18, 38])
  // is supported after the spark-rapids-jni row_conversion fixes. BINARY would also work in
  // principle but cudf maps BINARY to LIST<INT8>, which the row_conversion path does not
  // handle today, so leave it out for now.
  def isC2RSupportedType(dataType: DataType): Boolean = dataType match {
    case dt: DecimalType if dt.precision <= DecimalType.MAX_PRECISION => true
    case StringType => true
    case other => isR2CSupportedType(other)
  }

  // Legacy alias kept for callers that pre-date the C2R/R2C split. Defaults to the strict
  // R2C set so older call sites do not accidentally pick up the wider C2R set.
  def isSupportedType(dataType: DataType): Boolean = isR2CSupportedType(dataType)

  def areAllR2CSupported(schema: Seq[Attribute]): Boolean =
    schema.forall(att => isR2CSupportedType(att.dataType))

  def areAllC2RSupported(schema: Seq[Attribute]): Boolean =
    schema.forall(att => isC2RSupportedType(att.dataType))

  def areAllSupported(schema: Seq[Attribute]): Boolean = areAllR2CSupported(schema)
}

case class GpuColumnarToRowExec(
    child: SparkPlan,
    exportColumnarRdd: Boolean = false)
    extends ShimUnaryExecNode with ColumnarToRowTransition with GpuExec {
  import GpuMetric._
  // We need to do this so the assertions don't fail
  override def supportsColumnar = false

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // Override the original metrics to remove NUM_OUTPUT_BATCHES, which makes no sense.
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)

    val f = GpuColumnarToRowExec.makeIteratorFunc(child.output, numOutputRows, numInputBatches,
      opTime, streamTime)

    val cdata = child.executeColumnar()
    val rdd = if (exportColumnarRdd) {
      // If we are exporting columnar rdd we need an easy way for the code that walks the
      // RDDs to know where the columnar to row transition is happening.
      GpuColumnToRowMapPartitionsRDD.mapPartitions(cdata, f)
    } else {
      cdata.mapPartitions(f)
    }

    rdd
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

object GpuColumnarToRowExec {
  /**
   * Helper to check if GPU accelerated row-column transpose is supported.
   * This is a workaround for [[https://github.com/rapidsai/cudf/issues/10569]],
   * where CUDF JNI column->row transposition works incorrectly on certain
   * GPU architectures.
   */
  // The accelerated columnar-to-row transpose kernel is correct on every compute capability
  // > Pascal after the spark-rapids-jni row_conversion bug fixes (off-by-one in
  // determine_tiles, tile-boundary race, etc.). Pre-Volta GPUs are kept on the slow path
  // because we have not validated the kernel there. Reference:
  // https://developer.nvidia.com/cuda-gpus
  private lazy val isAcceleratedTransposeSupported: Boolean =
    Cuda.getComputeCapabilityMajor > 6

  def makeIteratorFunc(
      output: Seq[Attribute],
      numOutputRows: GpuMetric,
      numInputBatches: GpuMetric,
      opTime: GpuMetric,
      streamTime: GpuMetric)
      : Iterator[ColumnarBatch] => Iterator[InternalRow] = {
    // Read the kill switch on the driver where this function is invoked (doExecute,
    // GpuRangePartitioner sampling, ...) so every caller honors it without having to
    // thread the conf through their own signatures.
    val acceleratedTransposeEnabled =
      new RapidsConf(SQLConf.get).isAcceleratedColumnarToRowEnabled
    if (CudfRowTransitions.areAllC2RSupported(output) &&
        // For a small number of columns it is still best to do it the original way
        output.length > 4 &&
        // We can support upto 2^31 bytes per row. That is ~250M columns of 64-bit fixed-width data.
        // This number includes the 1-bit validity per column, but doesn't include padding.
        // We are being conservative by only allowing 100M columns until we feel the need to
        // increase this number
        output.length <= 100000000) {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        // The fast path requires both a capable GPU and the conf-level kill switch
        // (spark.rapids.sql.acceleratedColumnarToRow.enabled) to be on. The conf exists so
        // users can A/B test or fall back without changing code.
        if (acceleratedTransposeEnabled && isAcceleratedTransposeSupported) {
          new AcceleratedColumnarToRowIterator(output, batches, numInputBatches, numOutputRows,
            opTime, streamTime).map(toUnsafe)
        } else {
          new ColumnarToRowIterator(batches,
            numInputBatches, numOutputRows, opTime, streamTime).map(toUnsafe)
        }
      }
    } else {
      (batches: Iterator[ColumnarBatch]) => {
        // UnsafeProjection is not serializable so do it on the executor side
        val toUnsafe = UnsafeProjection.create(output, output)
        new ColumnarToRowIterator(batches,
          numInputBatches, numOutputRows, opTime, streamTime).map(toUnsafe)
      }
    }
  }
}
