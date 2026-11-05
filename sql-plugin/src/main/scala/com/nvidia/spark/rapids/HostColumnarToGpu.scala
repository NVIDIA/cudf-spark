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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.function.LongFunction

import scala.collection.JavaConverters._

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{GpuTypeShims, ShimUnaryExecNode}
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BigIntVector,
  BitVector, DateDayVector, Float4Vector, Float8Vector, IntVector, SmallIntVector,
  TimeStampMicroTZVector, TinyIntVector, VarBinaryVector, VarCharVector}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

object HostColumnarToGpu extends Logging {

  /**
   * Copies an Arrow vector in bulk when cuDF shares its layout for the given type, into the
   * builder that `builderFor` returns for the bytes of the values or of their variable-width
   * data. Returns -1 without asking for a builder when the vector cannot be copied in bulk.
   */
  def arrowBulkCopy(
      cv: ArrowColumnVector,
      dataType: DataType,
      rows: Int,
      builderFor: LongFunction[RapidsHostColumnBuilder]): Long = {
    val vector = cv.getValueVector
    val nulls = vector.getNullCount.toLong
    lazy val validity: ByteBuffer = if (nulls == 0) {
      null
    } else {
      vector.getValidityBuffer.nioBuffer(0, (rows + 7) >>> 3)
    }
    if (rows == 0) {
      0L
    } else if (vector.getValueCount != rows) {
      // The null count covers the whole vector, not only the rows of the batch.
      -1L
    } else {
      (dataType, vector) match {
        case (BooleanType, v: BitVector) =>
          val bits = v.getDataBuffer.nioBuffer(0, (rows + 7) >>> 3)
          builderFor.apply(rows).appendBooleanBits(bits, validity, rows, nulls)
        case (ByteType, _: TinyIntVector) | (ShortType, _: SmallIntVector) |
             (IntegerType, _: IntVector) | (DateType, _: DateDayVector) |
             (LongType, _: BigIntVector) | (TimestampType, _: TimeStampMicroTZVector) |
             (FloatType, _: Float4Vector) | (DoubleType, _: Float8Vector) =>
          val bytes = rows.toLong * vector.asInstanceOf[BaseFixedWidthVector].getTypeWidth
          if (bytes > Int.MaxValue) {
            -1L
          } else {
            val values = vector.getDataBuffer.nioBuffer(0, bytes.toInt)
            builderFor.apply(bytes).appendFixedWidth(values, validity, rows, nulls)
          }
        case (StringType, v: VarCharVector) =>
          variableWidthCopy(v, validity, rows, nulls, builderFor, strings = true)
        case (BinaryType, v: VarBinaryVector) =>
          variableWidthCopy(v, validity, rows, nulls, builderFor, strings = false)
        case _ =>
          -1L
      }
    }
  }

  /** Copies the host pieces of a column to the GPU and concatenates them there. */
  def concatenateOnDevice(
      pieces: java.util.List[HostColumnVector]): ai.rapids.cudf.ColumnVector = {
    if (pieces.size() == 1) {
      pieces.get(0).copyToDevice()
    } else {
      withResource(pieces.asScala.toSeq.safeMap(_.copyToDevice())) { onDevice =>
        ai.rapids.cudf.ColumnVector.concatenate(onDevice: _*)
      }
    }
  }

  /** Closes every resource in the list, even if some fail to close, and empties it. */
  def closeAll[T <: AutoCloseable](resources: java.util.List[T]): Unit = {
    try {
      resources.asScala.safeClose()
    } finally {
      resources.clear()
    }
  }

  private def variableWidthCopy(
      v: BaseVariableWidthVector,
      validity: ByteBuffer,
      rows: Int,
      nulls: Long,
      builderFor: LongFunction[RapidsHostColumnBuilder],
      strings: Boolean): Long = {
    val offsets = v.getOffsetBuffer.nioBuffer(0, (rows + 1) * BaseVariableWidthVector.OFFSET_WIDTH)
    if (nulls > 0 && !nullsAreEmpty(validity, offsets, rows)) {
      -1L
    } else {
      val start = v.getStartOffset(0)
      val end = v.getStartOffset(rows)
      val data = v.getDataBuffer.nioBuffer(0, end)
      val b = builderFor.apply(end.toLong - start)
      if (strings) {
        b.appendStrings(offsets, data, validity, rows, nulls)
      } else {
        b.appendByteLists(offsets, data, validity, rows, nulls)
      }
    }
  }

  // cuDF expects null rows of variable-width columns to be empty, while Arrow does not.
  private def nullsAreEmpty(validity: ByteBuffer, offsets: ByteBuffer, rows: Int): Boolean = {
    val ends = offsets.duplicate().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    var empty = true
    var i = 0
    while (empty && i < rows) {
      val bits = validity.get(i >> 3)
      if (bits == -1 && (i & 7) == 0) {
        i += 8
      } else {
        if ((bits & (1 << (i & 7))) == 0) {
          empty = ends.get(i) == ends.get(i + 1)
        }
        i += 1
      }
    }
    empty
  }

  // Data type is passed explicitly to allow overriding the reported type from the column vector.
  // There are cases where the type reported by the column vector does not match the data.
  // See https://github.com/apache/iceberg/issues/6116.
  def columnarCopy(
      cv: ColumnVector,
      b: RapidsHostColumnBuilder,
      dataType: DataType,
      rows: Int): Long = {
    dataType match {
      case NullType =>
        ColumnarCopyHelper.nullCopy(b, rows)
      case BooleanType =>
        ColumnarCopyHelper.booleanCopy(cv, b, rows)
      case ByteType =>
        ColumnarCopyHelper.byteCopy(cv, b, rows)
      case ShortType =>
        ColumnarCopyHelper.shortCopy(cv, b, rows)
      case IntegerType | DateType =>
        ColumnarCopyHelper.intCopy(cv, b, rows)
      case LongType | TimestampType =>
        ColumnarCopyHelper.longCopy(cv, b, rows)
      case FloatType =>
        ColumnarCopyHelper.floatCopy(cv, b, rows)
      case DoubleType =>
        ColumnarCopyHelper.doubleCopy(cv, b, rows)
      case StringType =>
        ColumnarCopyHelper.stringCopy(cv, b, rows)
      case BinaryType =>
        ColumnarCopyHelper.binaryCopy(cv, b, rows)
      case dt: DecimalType =>
        cv match {
          case wcv: WritableColumnVector =>
            if (DecimalType.is32BitDecimalType(dt)) {
              ColumnarCopyHelper.decimal32Copy(wcv, b, rows)
            } else if (DecimalType.is64BitDecimalType(dt)) {
              ColumnarCopyHelper.decimal64Copy(wcv, b, rows)
            } else {
              ColumnarCopyHelper.decimal128Copy(wcv, b, rows)
            }
          case _ =>
            if (DecimalType.is32BitDecimalType(dt)) {
              ColumnarCopyHelper.decimal32Copy(cv, b, rows, dt.precision, dt.scale)
            } else if (DecimalType.is64BitDecimalType(dt)) {
              ColumnarCopyHelper.decimal64Copy(cv, b, rows, dt.precision, dt.scale)
            } else {
              ColumnarCopyHelper.decimal128Copy(cv, b, rows, dt.precision, dt.scale)
            }
        }
      case other if GpuTypeShims.isColumnarCopySupportedForType(other) =>
        GpuTypeShims.columnarCopy(cv, b, other, rows)
      case t =>
        throw new UnsupportedOperationException(
          s"Converting to GPU for $t is not currently supported")
    }
  }
}

/**
 * This iterator builds GPU batches from host batches. The host batches potentially use Spark's
 * UnsafeRow so it is not safe to cache these batches. Rows must be read and immediately written
 * to CuDF builders.
 */
class HostToGpuCoalesceIterator(iter: Iterator[ColumnarBatch],
    goal: CoalesceSizeGoal,
    schema: StructType,
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    streamTime: GpuMetric,
    concatTime: GpuMetric,
    copyBufTime: GpuMetric,
    opTime: GpuMetric,
    opName: String,
    useArrowCopyOpt: Boolean)
  extends AbstractGpuCoalesceIterator(iter,
    goal,
    numInputRows,
    numInputBatches,
    numOutputRows,
    numOutputBatches,
    streamTime,
    concatTime,
    opTime,
    opName) {

  // RequireSingleBatch goal is intentionally not supported in this iterator
  assert(!goal.isInstanceOf[RequireSingleBatchLike])

  var batchBuilder: GpuColumnVector.GpuColumnarBatchBuilderBase = _
  var totalRows = 0

  // Arrow vectors of these types are copied in bulk, see HostColumnarToGpu.arrowBulkCopy
  private def arrowTypesSupported(schema: StructType): Boolean = {
    val dataTypes = schema.fields.map(_.dataType)
    dataTypes.forall(GpuOverrides.isSupportedType(_, allowBinary = true))
  }

  /**
   * Initialize the builders using an estimated row count based on the schema and the desired
   * batch size defined by [[RapidsConf.GPU_BATCH_SIZE_BYTES]].
   */
  override def initNewBatch(batch: ColumnarBatch): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }

    // when reading host batches it is essential to read the data immediately and pass to a
    // builder and we need to determine how many rows to allocate in the builder based on the
    // schema and desired batch size
    batchRowLimit = if (batch.numCols() > 0) {
       GpuBatchUtils.estimateRowCount(goal.targetSizeBytes,
         GpuBatchUtils.estimateGpuMemory(schema, 512), 512)
    } else {
      // when there aren't any columns, it generally means user is doing a count() and we don't
      // need to limit batch size because there isn't any actual data
      Integer.MAX_VALUE
    }

    // if no columns then probably a count operation so doesn't matter which builder we use
    // as we won't actually copy any data and we can't tell what type of data it is without
    // having a column
    if (useArrowCopyOpt && batch.numCols() > 0 &&
      arrowTypesSupported(schema) &&
      batch.column(0).isInstanceOf[ArrowColumnVector]) {
      logDebug("Using GpuArrowColumnarBatchBuilder")
      batchBuilder = new GpuColumnVector.GpuArrowColumnarBatchBuilder(schema)
    } else {
      logDebug("Using GpuColumnarBatchBuilder")
      batchBuilder = new GpuColumnVector.GpuColumnarBatchBuilder(schema, batchRowLimit)
    }
    totalRows = 0
  }


  /**
   * addBatchToConcat for HostToGpuCoalesceIterator does not need to close `batch`
   * because the batch is closed by the producer iterator.
   * See: https://github.com/NVIDIA/spark-rapids/issues/6995
   * @param batch the batch to add in.
   */
  override def addBatchToConcat(batch: ColumnarBatch): Unit = {
    withResource(new MetricRange(copyBufTime)) { _ =>
      val rows = batch.numRows()
      var bytesCopied = 0L
      for (i <- 0 until batch.numCols()) {
        bytesCopied += batchBuilder.copyColumnar(batch.column(i), i, rows)
      }
      totalRows += rows
      if (rows > 0) {
        batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes, bytesCopied, rows)
      }
    }
  }

  override def getBatchDataSize(batch: ColumnarBatch): Long = {
    schema.fields.indices.map(GpuBatchUtils.estimateGpuMemory(schema, _, batch.numRows())).sum
  }

  override def hasAnyToConcat: Boolean = totalRows > 0

  override def concatAllAndPutOnGPU(): ColumnarBatch = {
    // About to place data back on the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val ret = RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch]{
      batchBuilder.tryBuild(totalRows)
    }
    val maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(ret)

    // refine the estimate for number of rows based on this batch
    batchRowLimit = GpuBatchUtils.estimateRowCount(goal.targetSizeBytes, maxDeviceMemory,
      ret.numRows())
    ret
  }

  override val supportsRetryIterator: Boolean = false

  override def getCoalesceRetryIterator: Iterator[ColumnarBatch] = {
    throw new UnsupportedOperationException(
      "HostColumnarToGpu iterator does not support retry iterators")
  }

  override def cleanupConcatIsDone(): Unit = {
    if (batchBuilder != null) {
      batchBuilder.close()
      batchBuilder = null
    }
    totalRows = 0
  }

  private var onDeck: Option[ColumnarBatch] = None

  override protected def hasOnDeck: Boolean = onDeck.isDefined
  override protected def saveOnDeck(batch: ColumnarBatch): Unit = onDeck = Some(batch)
  override protected def clearOnDeck(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
  override protected def popOnDeck(): ColumnarBatch = {
    val ret = onDeck.get
    onDeck = None
    ret
  }
  override protected def cleanupInputBatch(batch: ColumnarBatch): Unit = {
    // Host batches are closed by the producer not the consumer, so nothing to do.
  }
}

/**
 * Put columnar formatted data on the GPU.
 */
case class HostColumnarToGpu(child: SparkPlan, goal: CoalesceSizeGoal)
  extends ShimUnaryExecNode
  with GpuExec {
  import GpuMetric._
  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    CONCAT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME),
    COPY_BUFFER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_COPY_BUFFER_TIME)
    )

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def outputBatching: CoalesceGoal = goal

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
   * Returns an RDD[ColumnarBatch] that when mapped over will produce GPU-side column vectors
   * that are expected to be closed by its caller, not [[HostColumnarToGpu]].
   *
   * The expectation is that the only valid instantiation of this node is
   * as a child of a GPU exec node.
   *
   * @return an RDD of `ColumnarBatch`
   */
  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {

    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val copyBufTime = gpuLongMetric(COPY_BUFFER_TIME)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)

    // cache in a local to avoid serializing the plan
    val outputSchema = schema

    val batches = child.executeColumnar()

    val confUseArrow = new RapidsConf(child.conf).useArrowCopyOptimization
    batches.mapPartitions { iter =>
      new HostToGpuCoalesceIterator(iter, goal, outputSchema,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches,
        streamTime, concatTime, copyBufTime, opTime,
        "HostColumnarToGpu", confUseArrow)
    }
  }
}
