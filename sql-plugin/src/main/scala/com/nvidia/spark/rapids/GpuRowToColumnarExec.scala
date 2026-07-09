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

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, CpuSplitAndRetryOOM, GpuRetryOOM, GpuSplitAndRetryOOM}
import com.nvidia.spark.rapids.shims.{CudfUnsafeRow, GpuTypeShims, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, SpecializedGetters, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

private class GpuRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  /**
   * Append row values to the column builders and return an approximation of the data bytes
   * written.  It is a Double because of validity can be less than a single byte.
   */
  final def convert(row: InternalRow, builders: GpuColumnarBatchBuilder): Double = {
    var bytes: Double = 0
    for (idx <- schema.fields.indices) {
      bytes += converters(idx).append(row, idx, builders.builder(idx))
    }
    bytes
  }

  /**
   * Convert an array of rows into a batch. Please note that this does not do bounds or size
   * checking so keep the size of the batch small.
   * @param rows the rows to convert.
   * @param schema the schema of the rows.
   * @return The batch on the GPU.
   */
  final def convertBatch(rows: Array[InternalRow], schema: StructType): ColumnarBatch = {
    val numRows = rows.length
    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { builders =>
      rows.foreach(convert(_, builders))
      builders.build(numRows)
    }
  }
}

object GpuRowToColumnConverter {
  // Sizes estimates for different things
  /*
   * size of an offset entry.  In general we have 1 more offset entry than rows, so
   * we might be off by one entry per column.
   */
  private[this] val OFFSET = Integer.BYTES
  private[this] val VALIDITY = 0.125 // 1/8th of a byte (1 bit)
  private[this] val VALIDITY_N_OFFSET = OFFSET + VALIDITY

  private def getConverterFor(field: StructField): TypeConverter =
    getConverterForType(field.dataType, field.nullable)

  def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    (dataType, nullable) match {
      case (BooleanType, true) => BooleanConverter
      case (BooleanType, false) => NotNullBooleanConverter
      case (ByteType, true) => ByteConverter
      case (ByteType, false) => NotNullByteConverter
      case (ShortType, true) => ShortConverter
      case (ShortType, false) => NotNullShortConverter
      case (IntegerType, true) => IntConverter
      case (IntegerType, false) => NotNullIntConverter
      case (FloatType, true) => FloatConverter
      case (FloatType, false) => NotNullFloatConverter
      case (LongType, true) => LongConverter
      case (LongType, false) => NotNullLongConverter
      case (DoubleType, true) => DoubleConverter
      case (DoubleType, false) => NotNullDoubleConverter
      case (DateType, true) => IntConverter
      case (DateType, false) => NotNullIntConverter
      case (TimestampType, true) => LongConverter
      case (TimestampType, false) => NotNullLongConverter
      case (StringType, true) => StringConverter
      case (StringType, false) => NotNullStringConverter
      case (BinaryType, true) => BinaryConverter
      case (BinaryType, false) => NotNullBinaryConverter
      // NOT SUPPORTED YET
      // case CalendarIntervalType => CalendarConverter
      case (at: ArrayType, true) =>
        ArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case (at: ArrayType, false) =>
        NotNullArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case (st: StructType, true) =>
        StructConverter(st.fields.map(getConverterFor))
      case (st: StructType, false) =>
        NotNullStructConverter(st.fields.map(getConverterFor))
      case (dt: DecimalType, true) =>
        new DecimalConverter(dt.precision, dt.scale)
      case (dt: DecimalType, false) =>
        new NotNullDecimalConverter(dt.precision, dt.scale)
      case (MapType(k, v, vcn), true) =>
        MapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (MapType(k, v, vcn), false) =>
        NotNullMapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (NullType, _) =>
        // nullable=false appears only as a synthetic child of empty nested
        // containers (e.g. MapType(NullType, NullType) from an empty map),
        // where no row is ever appended, so NullConverter is safe either way.
        NullConverter
      // check special Shims types, such as DayTimeIntervalType
      case (otherType, nullable) if GpuTypeShims.hasConverterForType(otherType) =>
        GpuTypeShims.getConverterForType(otherType, nullable)
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object NullConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      builder.appendNull()
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getByte(column))
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
      2 + VALIDITY
    }

    override def getNullSize: Double = 2 + VALIDITY
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getShort(column))
      2
    }

    override def getNullSize: Double = 2 + VALIDITY
  }

  private[rapids] object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private[rapids] object NotNullIntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getInt(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getFloat(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private[rapids] object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private[rapids] object NotNullLongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getLong(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getDouble(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullStringConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bytes = row.getUTF8String(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullBinaryConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object NotNullBinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      val bytes = row.getBinary(column)
      builder.appendByteList(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private[this] def mapConvert(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter,
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val m = row.getMap(column)
    val numElements = m.numElements()
    val srcKeys = m.keyArray()
    val srcValues = m.valueArray()
    val structBuilder = builder.getChild(0)
    val keyBuilder = structBuilder.getChild(0)
    val valueBuilder = structBuilder.getChild(1)
    for (i <- 0 until numElements) {
      ret += keyConverter.append(srcKeys, i, keyBuilder)
      ret += valueConverter.append(srcValues, i, valueBuilder)
      structBuilder.endStruct()
    }
    builder.endList()
    ret + OFFSET
  }

  private case class MapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        mapConvert(keyConverter, valueConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullMapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      mapConvert(keyConverter, valueConverter, row, column, builder)

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  //  private object CalendarConverter extends FixedWidthTypeConverter {
  //    override def append(row: SpecializedGetters,
  //    column: Int,
  //    builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        val c = row.getInterval(column)
  //        cv.appendStruct(false)
  //        cv.getChild(0).appendInt(c.months)
  //        cv.getChild(1).appendLong(c.microseconds)
  //      }
  //    }
  //  }
  //

  private[this] def arrayConvert(
      childConverter: TypeConverter,
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val values = row.getArray(column)
    val numElements = values.numElements()
    val child = builder.getChild(0)
    for (i <- 0 until numElements) {
      ret += childConverter.append(values, i, child)
    }
    builder.endList()
    ret + OFFSET
  }

  private case class ArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        arrayConvert(childConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      arrayConvert(childConverter, row, column, builder)
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private[this] def structConvert(
      childConverters: Array[TypeConverter],
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val struct = row.getStruct(column, childConverters.length)
    for (i <- childConverters.indices) {
      ret += childConverters(i).append(struct, i, builder.getChild(i))
    }
    builder.endStruct()
    ret
  }

  private case class StructConverter(
      childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        childConverters.map(_.getNullSize).sum + VALIDITY
        // each child has to insert a null too, which is dependent on the child
      } else {
        structConvert(childConverters, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private case class NotNullStructConverter(
      childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      structConvert(childConverters, row, column, builder)
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private class DecimalConverter(
    precision: Int, scale: Int) extends NotNullDecimalConverter(precision, scale) {
    private val appendedSize = if (precision <= Decimal.MAX_LONG_DIGITS) {
      8 + VALIDITY
    } else {
      16 + VALIDITY
    }

    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        super.append(row, column, builder)
      }
      appendedSize
    }
  }

  private class NotNullDecimalConverter(precision: Int, scale: Int) extends TypeConverter {
    private val appendedSize = if (precision <= Decimal.MAX_INT_DIGITS) {
      4
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      8
    } else {
      16
    }

    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bigDecimal = row.getDecimal(column, precision, scale).toJavaBigDecimal
      builder.append(bigDecimal)
      appendedSize
    }

    override def getNullSize: Double = {
      appendedSize + VALIDITY
    }
  }
}

class RowToColumnarIterator(
    rowIter: Iterator[InternalRow],
    localSchema: StructType,
    localGoal: CoalesceSizeGoal,
    batchSizeBytes: Long,
    converters: GpuRowToColumnConverter,
    enableRetry: Boolean = true,
    numInputRows: GpuMetric = NoopMetric,
    numOutputRows: GpuMetric = NoopMetric,
    numOutputBatches: GpuMetric = NoopMetric,
    streamTime: GpuMetric = NoopMetric,
    opTime: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {

  private val targetSizeBytes = localGoal.targetSizeBytes
  private var initialRows = 0
  private var targetRows = 0
  private var totalOutputBytes: Long = 0
  private var totalOutputRows: Long = 0
  private lazy val rowCopyProjection: UnsafeProjection = UnsafeProjection.create(localSchema)

  // Carries a failed row across batch boundaries when OOM interrupted conversion.
  private var pendingRow: InternalRow = _

  // For GPU OOM split retry: data types needed for HostColumnarBatchWithRowRange
  private val dataTypes: Array[DataType] = localSchema.fields.map(_.dataType)
  // Holds remaining split batches from a GPU OOM split retry during host-to-GPU transfer.
  private var pendingBatchIter: Iterator[ColumnarBatch] = Iterator.empty

  override def hasNext: Boolean = {
    if (pendingBatchIter.hasNext) {
      true
    } else if (pendingRow != null) {
      true
    } else {
      val start = System.nanoTime()
      val result = rowIter.hasNext
      streamTime += System.nanoTime() - start
      result
    }
  }

  override def next(): ColumnarBatch = {
    if (pendingBatchIter.hasNext) {
      return recordOutput(pendingBatchIter.next())
    }
    if (!hasNext) {
      throw new NoSuchElementException
    }
    recordOutput(buildBatch())
  }

  private def recordOutput(batch: ColumnarBatch): ColumnarBatch = {
    numOutputRows += batch.numRows()
    numOutputBatches += 1
    // Refine targetRows estimate based on actual output batches (including split outputs).
    totalOutputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(batch)
    totalOutputRows += batch.numRows()
    if (totalOutputRows > 0 && totalOutputBytes > 0) {
      targetRows =
        GpuBatchUtils.estimateRowCount(targetSizeBytes, totalOutputBytes, totalOutputRows)
    }
    batch
  }

  private def copyRow(row: InternalRow): InternalRow = row match {
    case unsafe: UnsafeRow => unsafe.copy()
    case other => rowCopyProjection.apply(other).copy()
  }

  /** Consume the pending row if available, otherwise advance the iterator. */
  private def nextRow(): InternalRow = {
    if (pendingRow != null) {
      val r = pendingRow
      pendingRow = null
      r
    } else {
      rowIter.next()
    }
  }

  /**
   * Core conversion loop. When retry is enabled, runs inside a single RMM retry block
   * so host allocations throw retryable OOMs instead of fatal OutOfMemoryError.
   *
   * OOM strategy (retry enabled):
   *  - Has rows to emit (and not RequireSingleBatch) → emit partial batch, save failed row
   *  - No rows yet + RetryOOM → block until memory freed, then while loop retries
   *  - No rows yet + SplitAndRetryOOM → propagate (can't split a single row)
   */
  private def convertRows(builders: GpuColumnarBatchBuilder): (Int, Double) = {
    var rowCount = 0
    var byteCount: Double = 0

    if (enableRetry) {
      var batchDone = false
      RmmRapidsRetryIterator.withRetryBlock {
        while (!batchDone && hasNext &&
            (rowCount == 0 || rowCount < targetRows && byteCount < targetSizeBytes)) {
          val snapshots = builders.captureState()
          var row: InternalRow = null
          try {
            row = nextRow()
            byteCount += converters.convert(row, builders)
            rowCount += 1
          } catch {
            case oom @ (_: CpuRetryOOM | _: CpuSplitAndRetryOOM |
                        _: GpuRetryOOM | _: GpuSplitAndRetryOOM) =>
              builders.restoreState(snapshots)
              if (rowCount > 0 && !localGoal.isInstanceOf[RequireSingleBatchLike]) {
                // Emit partial batch. This also handles SplitAndRetryOOM: emitting
                // a smaller batch IS the right response to memory pressure, and
                // the host-to-GPU transfer has its own split retry to handle GPU OOM.
                if (row != null) {
                  pendingRow = copyRow(row)
                }
                batchDone = true
              } else {
                // No rows to emit — must retry or fail.
                oom match {
                  case _: CpuSplitAndRetryOOM | _: GpuSplitAndRetryOOM => throw oom
                  case _ =>
                    if (row != null) {
                      pendingRow = copyRow(row)
                    }
                    RmmRapidsRetryIterator.blockUntilMemoryFreed()
                }
              }
          }
        }
      }
    } else {
      while (hasNext && (rowCount == 0 || rowCount < targetRows && byteCount < targetSizeBytes)) {
        val row = nextRow()
        byteCount += converters.convert(row, builders)
        rowCount += 1
      }
    }
    (rowCount, byteCount)
  }

  private def estimateInitialTargetRows(): Unit = {
    // estimate the size of the first batch based on the schema
    if (targetRows == 0) {
      if (localSchema.fields.isEmpty) {
        // if there are no columns then we just default to a small number
        // of rows for the first batch
        targetRows = 1024
        initialRows = targetRows
      } else {
        val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
        val sampleBytes = GpuBatchUtils.estimateGpuMemory(localSchema, sampleRows)
        targetRows = GpuBatchUtils.estimateRowCount(targetSizeBytes, sampleBytes, sampleRows)
        initialRows = GpuBatchUtils.estimateRowCount(batchSizeBytes, sampleBytes, sampleRows)
      }
    }
  }

  private def buildBatch(): ColumnarBatch = {
    NvtxRegistry.ROW_TO_COLUMNAR {
      val streamStart = System.nanoTime()
      estimateInitialTargetRows()

      withResource(new GpuColumnarBatchBuilder(localSchema, initialRows)) { builders =>
        val (rowCount, _) = convertRows(builders)

        // enforce RequireSingleBatch limit
        if ((pendingRow != null || rowIter.hasNext) &&
            localGoal.isInstanceOf[RequireSingleBatchLike]) {
          throw new IllegalStateException("A single batch is required for this operation." +
              " Please try increasing your partition count.")
        }

        streamTime += System.nanoTime() - streamStart

        // About to place data back on the GPU
        // note that TaskContext.get() can return null during unit testing so we wrap it in an
        // option here
        Option(TaskContext.get())
            .foreach(ctx => GpuSemaphore.acquireIfNecessary(ctx))

        // Build host columns and wrap in HostColumnarBatchWithRowRange for split retry.
        // On GPU OOM during transfer, the batch can be split in half and retried.
        val hostColumns = RmmRapidsRetryIterator.withRetryNoSplit {
          builders.buildHostColumns()
        }
        val hostBatch = withResource(hostColumns) { _ =>
          HostColumnarBatchWithRowRange(hostColumns, rowCount, dataTypes)
        }

        localGoal match {
          case _: RequireSingleBatchLike =>
            // Cannot split when a single batch is required
            pendingBatchIter = Iterator.empty
            val ret = RmmRapidsRetryIterator.withRetryNoSplit(hostBatch) { batch =>
              NvtxIdWithMetrics(NvtxRegistry.ROW_TO_COLUMNAR, opTime) {
                batch.copyToGpu()
              }
            }
            numInputRows += rowCount
            ret
          case _ =>
            val it = RmmRapidsRetryIterator.withRetry(
              hostBatch,
              splitHostBatchInHalf
            ) { batch =>
              NvtxIdWithMetrics(NvtxRegistry.ROW_TO_COLUMNAR, opTime) {
                batch.copyToGpu()
              }
            }
            // Return the first batch now and keep the iterator for subsequent output batches.
            // This ensures we only transfer one split at a time.
            closeOnExcept(it.next()) { first =>
              pendingBatchIter = it
              numInputRows += rowCount
              first
            }
        }
        // Output metrics and targetRows refinement are handled in recordOutput().
      }
    }
  }

  /**
   * Split policy for HostColumnarBatchWithRowRange.
   * When GPU OOM occurs during host-to-GPU transfer, split the batch in half by rows.
   */
  private val splitHostBatchInHalf
      : HostColumnarBatchWithRowRange => Seq[HostColumnarBatchWithRowRange] = {
    (batch: HostColumnarBatchWithRowRange) => HostColumnarBatchWithRowRange.splitInHalf(batch)
  }
}

object GeneratedInternalRowToCudfRowIterator extends Logging {
  def apply(input: Iterator[InternalRow],
      schema: Array[Attribute],
      goal: CoalesceSizeGoal,
      streamTime: GpuMetric,
      opTime: GpuMetric,
      numInputRows: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): InternalRowToColumnarBatchIterator = {
    val ctx = new CodegenContext
    // setup code generation context to use our custom row variable
    val internalRow = ctx.freshName("internalRow")
    ctx.currentVars = null
    ctx.INPUT_ROW = internalRow

    val generateUnsafeProj = GenerateUnsafeProjection.createCode(ctx,
      schema.zipWithIndex.map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) }
    )

    val iterRef = ctx.addReferenceObj("iter", input, classOf[Iterator[UnsafeRow]].getName)
    val schemaRef = ctx.addReferenceObj("schema", schema,
      classOf[Array[Attribute]].getCanonicalName)
    val goalRef = ctx.addReferenceObj("goal", goal, classOf[CoalesceSizeGoal].getName)
    val streamTimeRef = ctx.addReferenceObj("streamTime", streamTime, classOf[GpuMetric].getName)
    val opTimeRef = ctx.addReferenceObj("opTime", opTime, classOf[GpuMetric].getName)
    val numInputRowsRef = ctx.addReferenceObj("numInputRows", numInputRows,
      classOf[GpuMetric].getName)
    val numOutputRowsRef = ctx.addReferenceObj("numOutputRows", numOutputRows,
      classOf[GpuMetric].getName)
    val numOutputBatchesRef = ctx.addReferenceObj("numOutputBatches", numOutputBatches,
      classOf[GpuMetric].getName)

    val rowBaseObj = ctx.freshName("rowBaseObj")
    val rowBaseOffset = ctx.freshName("rowBaseOffset")

    val sparkValidityOffset = UnsafeRow.calculateBitSetWidthInBytes(schema.length)
    // This needs to match what is in cudf and CudfUnsafeRow
    var cudfOffset = 0
    // scalastyle:off line.size.limit
    // The map will execute here because schema is an array. Not sure if there is a better way to
    // do this or not
    val copyData = schema.zipWithIndex.map { pair =>
      val attr = pair._1
      val colIndex = pair._2
      // This only works on fixed width types
      val length = DecimalUtil.getDataTypeSize(attr.dataType)
      cudfOffset = CudfUnsafeRow.alignOffset(cudfOffset, length)
      val ret = length match {
        case 1 => s"Platform.putByte(null, startAddress + $cudfOffset, Platform.getByte($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 2 => s"Platform.putShort(null, startAddress + $cudfOffset, Platform.getShort($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 4 => s"Platform.putInt(null, startAddress + $cudfOffset, Platform.getInt($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 8 => s"Platform.putLong(null, startAddress + $cudfOffset, Platform.getLong($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
//        case 16 => s"Platform.setDecimal(null, startAddress + $cudfOffset, getDecimal($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case _ => throw new IllegalStateException(s"$length  NOT SUPPORTED YET")
      }
      cudfOffset += length
      ret
    }

    val sparkValidityTmp = ctx.freshName("unsafeRowTmp")

    val cudfValidityStart = cudfOffset
    val cudfBitSetWidthInBytes = CudfUnsafeRow.calculateBitSetWidthInBytes(schema.length)

    val copyValidity = (0 until cudfBitSetWidthInBytes).map { cudfValidityByteIndex =>
      var ret = ""
      val byteOffsetInSparkLong = cudfValidityByteIndex % 8
      if (byteOffsetInSparkLong == 0) {
        // We need to get the validity data out of the unsafe row.
        ret += s"$sparkValidityTmp = Platform.getLong($rowBaseObj, $rowBaseOffset + $cudfValidityByteIndex);\n"
      }
      // Now write out the sub-part of it we care about, but converted to validity instead of isNull
      ret += s"Platform.putByte(null, startAddress + ${cudfValidityStart + cudfValidityByteIndex}, (byte)(0xFF & ~($sparkValidityTmp >> ${byteOffsetInSparkLong * 8})));"
      ret
    }

    // Each row is 64-bit aligned
    val rowLength = CudfUnsafeRow.alignOffset(cudfOffset + cudfBitSetWidthInBytes, 8)

    // First copy the data, validity we will copy afterwards

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificInternalRowToColumnarBatchIterator(references);
         |}
         |
         |final class SpecificInternalRowToColumnarBatchIterator extends ${classOf[InternalRowToColumnarBatchIterator].getName} {
         |  private final org.apache.spark.sql.catalyst.expressions.UnsafeProjection unsafeProj;
         |
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificInternalRowToColumnarBatchIterator(Object[] references) {
         |    super(
         |      $iterRef,
         |      $schemaRef,
         |      $goalRef,
         |      $streamTimeRef,
         |      $opTimeRef,
         |      $numInputRowsRef,
         |      $numOutputRowsRef,
         |      $numOutputBatchesRef);
         |
         |      ${ctx.initMutableStates()}
         |  }
         |
         |  // Avoid virtual function calls by copying the data in a batch at a time instead
         |  // of a row at a time.
         |  @Override
         |  public int[] fillBatch(ai.rapids.cudf.HostMemoryBuffer dataBuffer,
         |      ai.rapids.cudf.HostMemoryBuffer offsetsBuffer,
         |      long dataLength, int numRows) {
         |    final long dataBaseAddress = dataBuffer.getAddress();
         |    final long endDataAddress = dataBaseAddress + dataLength;
         |
         |    int dataOffset = 0;
         |    int currentRow = 0;
         |    int offsetIndex = 0;
         |
         |    // If we are here we have at least one row to process, so don't bother checking yet
         |    boolean done = false;
         |    while (!done) {
         |      UnsafeRow row;
         |      if (pending != null) {
         |        row = pending;
         |        pending = null;
         |      } else {
         |        InternalRow $internalRow = (InternalRow) input.next();
         |        if ($internalRow instanceof UnsafeRow) {
         |          row = (UnsafeRow) $internalRow;
         |        } else {
         |          ${generateUnsafeProj.code}
         |          row = ${generateUnsafeProj.value};
         |        }
         |      }
         |      int numBytesUsedByRow = copyInto(row, dataBaseAddress + dataOffset, endDataAddress);
         |      offsetsBuffer.setInt(offsetIndex, dataOffset);
         |      offsetIndex += 4;
         |      if (numBytesUsedByRow < 0) {
         |        pending = row;
         |        done = true;
         |      } else {
         |        currentRow += 1;
         |        dataOffset += numBytesUsedByRow;
         |        done = !(currentRow < numRows &&
         |            dataOffset < dataLength &&
         |            input.hasNext());
         |      }
         |    }
         |    return new int[] {dataOffset, currentRow};
         |  }
         |
         |  private int copyInto(UnsafeRow input, long startAddress, long endAddress) {
         |    Object $rowBaseObj = input.getBaseObject();
         |    long $rowBaseOffset = input.getBaseOffset();
         |    ${copyData.mkString("\n")}
         |
         |    // validity
         |    long $sparkValidityTmp;
         |    ${copyValidity.mkString("\n")}
         |
         |    return $rowLength;
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin
    // scalastyle:on line.size.limit

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${schema.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[InternalRowToColumnarBatchIterator]
  }
}

/**
 * GPU version of row to columnar transition.
 */
case class GpuRowToColumnarExec(child: SparkPlan, goal: CoalesceSizeGoal)
  extends ShimUnaryExecNode with GpuExec {
  import GpuMetric._

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputBatching: CoalesceGoal = goal

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    TrampolineUtil.doExecuteBroadcast(child)
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS)
  )

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // use local variables instead of class global variables to prevent the entire
    // object from having to be serialized
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val localGoal = goal
    val rowBased = child.execute()

    // cache in a local to avoid serializing the plan
    val localSchema = schema

    // We can support upto 2^31 bytes per row. That is ~250M columns of 64-bit fixed-width data.
    // This number includes the 1-bit validity per column, but doesn't include padding.
    // We are being conservative by only allowing 100M columns until we feel the need to
    // increase this number. Spark by default limits codegen to 100 fields
    // "spark.sql.codegen.maxFields".
    if ((1 until 100000000).contains(output.length) &&
        CudfRowTransitions.areAllR2CSupported(output)) {
      val localOutput = output
      rowBased.mapPartitions(rowIter => GeneratedInternalRowToCudfRowIterator(
        rowIter, localOutput.toArray, localGoal, streamTime, opTime,
        numInputRows, numOutputRows, numOutputBatches))
    } else {
      val converters = new GpuRowToColumnConverter(localSchema)
      val conf = new RapidsConf(child.conf)
      val batchSizeBytes = conf.gpuTargetBatchSizeBytes
      val enableR2cRetry = conf.isR2cRetryEnabled
      rowBased.mapPartitions(rowIter => new RowToColumnarIterator(rowIter,
        localSchema, localGoal, batchSizeBytes, converters, enableR2cRetry,
        numInputRows, numOutputRows, numOutputBatches, streamTime, opTime))
    }
  }
}
