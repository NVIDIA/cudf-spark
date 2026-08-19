/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION. All rights reserved.
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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{DecimalUtils, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.SQLExecutionRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This code is based off of the code for [[com.nvidia.spark.rapids.GpuRowToColumnarExec]] but this
 * is for `Row` instead of `InternalRow`, so it is not an exact transition.
 */
private class GpuExternalRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuExternalRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: Row, builders: GpuColumnarBatchBuilder): Double = {
    var bytes: Double = 0
    for (idx <- 0 until row.length) {
      bytes += converters(idx).append(row, idx, builders.builder(idx))
    }
    bytes
  }
}

private object GpuExternalRowToColumnConverter {

  // Sizes estimates for different things
  /*
   * size of an offset entry.  In general we have 1 more offset entry than rows, so
   * we might be off by one entry per column.
   */
  private[this] val OFFSET = Integer.BYTES
  private[this] val VALIDITY = 0.125 // 1/8th of a byte (1 bit)
  private[this] val VALIDITY_N_OFFSET = OFFSET + VALIDITY

  private abstract class TypeConverter extends Serializable {
    /** Append row value to the column builder and return the number of data bytes written */
    def append(row: Row, column: Int, builder: RapidsHostColumnBuilder): Double

    /**
     * This is here for structs.  When you append a null to a struct the size is not known
     * ahead of time.  Also because structs push nulls down to the children this size should
     * assume a validity even if the schema says it cannot be null.
     */
    def getNullSize: Double
  }

  private def getConverterFor(field: StructField): TypeConverter =
    getConverterForType(field.dataType, field.nullable)


  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
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
      case (NullType, true) =>
        NullConverter
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }
  private object NullConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.appendNull()
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getByte(column))
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getShort(column))
      2
    }

    override def getNullSize: Double = 2 + VALIDITY
  }

  private object IntConverter extends TypeConverter {
    override def append(row: Row,
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

  private object NotNullIntConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getInt(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getFloat(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object LongConverter extends TypeConverter {
    override def append(row: Row,
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

  private object NotNullLongConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getLong(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getDouble(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object StringConverter extends TypeConverter {
    override def append(row: Row,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullStringConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bytes = row.getString(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }
//
  private object BinaryConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullBinaryConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private object NotNullBinaryConverter extends TypeConverter {
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val child = builder.getChild(0)
      val bytes = row.asInstanceOf[GenericRow].getSeq[Byte](column)
      bytes.foreach(child.append)
      builder.endList()
      bytes.length + OFFSET
    }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private[this] def mapConvert(
    keyConverter: TypeConverter,
    valueConverter: TypeConverter,
    row: Row,
    column: Int,
    builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val m = row.getMap[Any, Any](column)
    val numElements = m.size
    val srcKeys = m.keys.toArray
    val srcValues = m.values.toArray
    val structBuilder = builder.getChild(0)
    val keyBuilder = structBuilder.getChild(0)
    val valueBuilder = structBuilder.getChild(1)
    for (i <- 0 until numElements) {
      ret += keyConverter.append(Row(srcKeys: _*), i, keyBuilder)
      ret += valueConverter.append(Row(srcValues: _*), i, valueBuilder)
      structBuilder.endStruct()
    }
    builder.endList()
    ret + OFFSET
  }

  private case class MapConverter(
    keyConverter: TypeConverter,
    valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: Row,
      column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        mapConvert(keyConverter, valueConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private case class NotNullMapConverter(
    keyConverter: TypeConverter,
    valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: Row,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      mapConvert(keyConverter, valueConverter, row, column, builder)

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  //
  //  private object CalendarConverter extends FixedWidthTypeConverter {
  //    override def append(
  //        row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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

  private[this] def arrayConvert(
    childConverter: TypeConverter,
    row: Row,
    column: Int,
    builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val values = row.getSeq(column)
    val numElements = values.size
    val child = builder.getChild(0)
    for (i <- 0 until numElements) {
      ret += childConverter.append(Row(values: _*), i, child)
    }
    builder.endList()
    ret + OFFSET
  }

  private case class ArrayConverter(childConverter: TypeConverter)
    extends TypeConverter {
    override def append(row: Row,
      column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        arrayConvert(childConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private case class NotNullArrayConverter(childConverter: TypeConverter)
    extends TypeConverter {
    override def append(row: Row,
      column: Int, builder: RapidsHostColumnBuilder): Double = {
      arrayConvert(childConverter, row, column, builder)
    }

    override def getNullSize: Double = VALIDITY_N_OFFSET
  }

  private[this] def structConvert(
    childConverters: Array[TypeConverter],
    row: Row,
    column: Int,
    builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val struct = row.getStruct(column)
    for (i <- childConverters.indices) {
      ret += childConverters(i).append(struct, i, builder.getChild(i))
    }
    builder.endStruct()
    ret
  }

  private case class StructConverter(
    childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: Row,
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
    override def append(row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      structConvert(childConverters, row, column, builder)
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private class DecimalConverter(
    precision: Int, scale: Int) extends NotNullDecimalConverter(precision, scale) {
    private val appendedSize = DecimalUtils.createDecimalType(precision, scale).getSizeInBytes +
        VALIDITY

    override def append(
      row: Row,
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
    private val appendedSize = DecimalUtils.createDecimalType(precision, scale).getSizeInBytes +
        VALIDITY

    override def append(
      row: Row,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bigDecimal = row.getDecimal(column)
      builder.append(bigDecimal)
      appendedSize
    }

    override def getNullSize: Double = {
      appendedSize + VALIDITY
    }
  }
}

private class ExternalRowToColumnarIterator(
    rowIter: Iterator[Row],
    localSchema: StructType,
    localGoal: CoalesceSizeGoal,
    converters: GpuExternalRowToColumnConverter) extends Iterator[ColumnarBatch] {

  private val targetSizeBytes = localGoal.targetSizeBytes
  private var targetRows = 0
  private var totalOutputBytes: Long = 0
  private var totalOutputRows: Long = 0

  override def hasNext: Boolean = rowIter.hasNext

  override def next(): ColumnarBatch = {
    if (!rowIter.hasNext) {
      throw new NoSuchElementException
    }
    buildBatch()
  }

  private def buildBatch(): ColumnarBatch = {

    // estimate the size of the first batch based on the schema
    if (targetRows == 0) {
      if (localSchema.fields.isEmpty) {
        targetRows = Integer.MAX_VALUE
      } else {
        val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
        val sampleBytes = GpuBatchUtils.estimateGpuMemory(localSchema, sampleRows)
        targetRows = GpuBatchUtils.estimateRowCount(targetSizeBytes, sampleBytes, sampleRows)
      }
    }

    Arm.withResource(new GpuColumnarBatchBuilder(localSchema, targetRows)) { builders =>
      var rowCount = 0
      // Double because validity can be < 1 byte, and this is just an estimate anyways
      var byteCount: Double = 0
      while (rowCount < targetRows && byteCount < targetSizeBytes  && rowIter.hasNext) {
        val row = rowIter.next()
        byteCount += converters.convert(row, builders)
        rowCount += 1
      }

      // enforce single batch limit when appropriate
      if (rowIter.hasNext && localGoal.isInstanceOf[RequireSingleBatchLike]) {
        throw new IllegalStateException("A single batch is required for this operation." +
          " Please try increasing your partition count.")
      }

      // About to place data back on the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val ret = builders.build(rowCount)

      // refine the targetRows estimate based on the average of all batches processed so far
      totalOutputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
      totalOutputRows += rowCount
      if (totalOutputRows > 0 && totalOutputBytes > 0) {
        targetRows =
          GpuBatchUtils.estimateRowCount(targetSizeBytes, totalOutputBytes, totalOutputRows)
      }

      // The returned batch will be closed by the consumer of it
      ret
    }
  }
}

/**
 * Please don't use this class directly use [[com.nvidia.spark.rapids.ColumnarRdd]] instead. We had
 * to place the implementation in a spark specific package to poke at the internals of spark more
 * than anyone should know about.
 *
 * This provides a way to get back out GPU Columnar data RDD[Table]. Each Table will have the same
 * schema as the dataframe passed in.  If the schema of the dataframe is something that Rapids does
 * not currently support an `IllegalArgumentException` will be thrown.
 *
 * The size of each table will be determined by what is producing that table but typically will be
 * about the number of bytes set by `RapidsConf.GPU_BATCH_SIZE_BYTES`.
 *
 * Table is not a typical thing in an RDD so special care needs to be taken when working with it.
 * By default it is not serializable so repartitioning the RDD or any other operator that involves
 * a shuffle will not work. This is because it is very expensive to serialize and deserialize a GPU
 * Table using a conventional spark shuffle. Also most of the memory associated with the Table is
 * on the GPU itself, so each table must be closed when it is no longer needed to avoid running out
 * of GPU memory.  By convention it is the responsibility of the one consuming the data to close it
 * when they no longer need it.
 */
object InternalColumnarRddConverter extends Logging {
  def apply(df: DataFrame): RDD[Table] = {
    convert(df)
  }

  // Extract RDD[ColumnarBatch] directly
  def extractRDDColumnarBatch(df: DataFrame): (Option[RDD[ColumnarBatch]], RDD[Row]) = {
    val schema = df.schema
    val unsupported = schema.map(_.dataType).filter( dt => !GpuOverrides.isSupportedType(dt,
      allowMaps = true, allowStringMaps = true, allowNull = true, allowStruct = true, allowArray
      = true, allowBinary = true, allowDecimal = true, allowNesting = true)).toSet
    if (unsupported.nonEmpty) {
      throw new IllegalArgumentException(s"Cannot convert $df to GPU columnar $unsupported are " +
        s"not currently supported data types for columnar.")
    }
    //This config lets the plugin tag the columnar transition we care about so we know what we got.
    df.sparkSession.sqlContext.setConf(RapidsConf.EXPORT_COLUMNAR_RDD.key, "true")
    val input = try {
      df.rdd
    } finally {
      df.sparkSession.sqlContext.setConf(RapidsConf.EXPORT_COLUMNAR_RDD.key, "false")
    }
    var batch: Option[RDD[ColumnarBatch]] = None
    // If we are exporting the data we will see
    // 1) MapPartitionsRDD - which is converting an InternalRow[ObjectType[Row]] to just a Row
    // 2) SQLExecutionRDD - which is where the SQL execution starts, and tries to tie the metrics
    //        collection back into the SQL side of things.
    // 3) MapPartitionsRDD - which is converting an InternalRow[Whatever] to
    //        InternalRow[ObjectType[Row]]
    // 4) GpuColumnToRowMapPartitionsRDD - which is the special RDD we are looking for.
    //
    // The GpuColumnToRowMapPartitionsRDD will only ever be inserted if the plugin sees the correct
    // SQL operators so we don't need to worry too much about what the MapPartitionsRDDs are doing
    // internally.  But if we don't see this pattern exactly we need to fall back to doing it the
    // slow way using Row as the source, because we don't know if one of the MapPartitionsRDDs
    // contains codegen etc.
    input match {
      case in: MapPartitionsRDD[Row, _] =>
        in.prev match {
          case sqlExecRdd: SQLExecutionRDD =>
            sqlExecRdd.sqlRDD match {
              case rowConversionRdd: MapPartitionsRDD[InternalRow, _] =>
                rowConversionRdd.prev match {
                  case c2rRdd: GpuColumnToRowMapPartitionsRDD =>
                    batch = Some(c2rRdd.prev)
                  case opTimeRdd: GpuOpTimeTrackingRDD[_] =>
                    opTimeRdd.prev match {
                      case c2rRdd: GpuColumnToRowMapPartitionsRDD =>
                        batch = Some(c2rRdd.prev)
                      case rdd =>
                        logDebug("Cannot extract columnar RDD directly. " +
                          s"(column to row not found $rdd)")
                    }
                  case rdd =>
                    logDebug("Cannot extract columnar RDD directly. " +
                      s"(column to row not found $rdd)")
                }
              case rdd =>
                logDebug("Cannot extract columnar RDD directly. " +
                  s"(Internal row to row rdd not found $rdd)")
            }
          case rdd =>
            logDebug("Cannot extract columnar RDD directly. " +
              s"(SQLExecutionRDD not found $rdd)")
        }
      case rdd =>
        logDebug(s"Cannot extract columnar RDD directly. " +
          s"(First MapPartitionsRDD not found $rdd)")
    }
    (batch, input)
  }

  def convert(df: DataFrame): RDD[Table] = {
    val schema = df.schema
    val (batch, input) = extractRDDColumnarBatch(df)
    val b = batch.getOrElse({
      // We have to fall back to doing a slow transition.
      val converters = new GpuExternalRowToColumnConverter(schema)
      val conf = new RapidsConf(df.sparkSession.sessionState.conf)
      val goal = TargetSize(conf.gpuTargetBatchSizeBytes)
      input.mapPartitions { rowIter =>
        new ExternalRowToColumnarIterator(rowIter, schema, goal, converters)
      }
    })

    b.map(cb => try {
      GpuColumnVector.from(cb)
    } finally {
      cb.close()
    })
  }
}

object GpuColumnToRowMapPartitionsRDD {
  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions(
      orig: RDD[ColumnarBatch],
      f: Iterator[ColumnarBatch] => Iterator[InternalRow],
      preservesPartitioning: Boolean = false): RDD[InternalRow] = {
    val sc = orig.context
    val cleanedF = sc.clean(f)
    new GpuColumnToRowMapPartitionsRDD(orig,
      (_: TaskContext, _: Int, iter: Iterator[ColumnarBatch]) => cleanedF(iter),
      preservesPartitioning)
  }
}

class GpuColumnToRowMapPartitionsRDD(
    prev: RDD[ColumnarBatch],
    // (TaskContext, partition index, iterator)
    f: (TaskContext, Int, Iterator[ColumnarBatch]) => Iterator[InternalRow],
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends MapPartitionsRDD[InternalRow, ColumnarBatch](
    prev,
    f,
    preservesPartitioning,
    isFromBarrier,
    isOrderSensitive) {
}
