/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.util.Random

import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class GpuBatchUtilsSuite extends AnyFunSuite {

  val intSchema = new StructType(Array(
    StructField("c0", DataTypes.IntegerType, nullable = true),
    StructField("c1", DataTypes.IntegerType, nullable = false)
  ))

  val stringSchema = new StructType(Array(
    StructField("c0", DataTypes.StringType, nullable = true),
    StructField("c1", DataTypes.StringType, nullable = false)
  ))

  val binarySchema = new StructType(Array(
    StructField("c0", DataTypes.StringType, nullable = true),
    StructField("c0", DataTypes.StringType, nullable = false)
  ))

  val decimalSchema = new StructType(Array(
    StructField("c0", DataTypes.StringType, nullable = true),
    StructField("c0", DataTypes.StringType, nullable = false)
  ))

  /** Mix of data types and nullable and not nullable */
  val mixedSchema = new StructType(Array(
    StructField("c0", DataTypes.ByteType, nullable = false),
    StructField("c0_nullable", DataTypes.ByteType, nullable = true),
    StructField("c1", DataTypes.ShortType, nullable = false),
    StructField("c1_nullable", DataTypes.ShortType, nullable = true),
    StructField("c2", DataTypes.IntegerType, nullable = false),
    StructField("c2_nullable", DataTypes.IntegerType, nullable = true),
    StructField("c3", DataTypes.LongType, nullable = false),
    StructField("c3_nullable", DataTypes.LongType, nullable = true),
    StructField("c4", DataTypes.FloatType, nullable = false),
    StructField("c4_nullable", DataTypes.FloatType, nullable = true),
    StructField("c5", DataTypes.DoubleType, nullable = false),
    StructField("c5_nullable", DataTypes.DoubleType, nullable = true),
    StructField("c6", DataTypes.StringType, nullable = false),
    StructField("c6_nullable", DataTypes.StringType, nullable = true),
    StructField("c7", DataTypes.BooleanType, nullable = false),
    StructField("c7_nullable", DataTypes.BooleanType, nullable = true),
    StructField("c8", DataTypes.createDecimalType(15, 6), nullable = false),
    StructField("c8_nullable", DataTypes.createDecimalType(15, 6), nullable = true)
  ))

  test("Calculate GPU memory for batch of 64 rows with integers") {
    compareEstimateWithActual(intSchema, 64)
  }

  test("Calculate GPU memory for batch of 64 rows with strings") {
    compareEstimateWithActual(stringSchema, 64)
  }

  test("Calculate GPU memory for batch of 64 rows with decimals") {
    compareEstimateWithActual(decimalSchema, 64)
  }

  test("Calculate GPU memory for batch of 64 rows with mixed types") {
    compareEstimateWithActual(mixedSchema, 64)
  }

  test("Calculate GPU memory for batch of 124 rows with integers") {
    compareEstimateWithActual(intSchema, 124)
  }

  test("Calculate GPU memory for batch of 124 rows with strings") {
    compareEstimateWithActual(stringSchema, 124)
  }

  test("Calculate GPU memory for batch of 124 rows with decimals") {
    compareEstimateWithActual(decimalSchema, 124)
  }

  test("Calculate GPU memory for batch of 124 rows with mixed types") {
    compareEstimateWithActual(mixedSchema, 124)
  }

  test("Calculate GPU memory for batch of 1024 rows with integers") {
    compareEstimateWithActual(intSchema, 1024)
  }

  test("Calculate GPU memory for batch of 1024 rows with strings") {
    compareEstimateWithActual(stringSchema, 1024)
  }

  test("Calculate GPU memory for batch of 1024 rows with decimals") {
    compareEstimateWithActual(decimalSchema, 1024)
  }

  test("Calculate GPU memory for batch of 1024 rows with mixed types") {
    compareEstimateWithActual(mixedSchema, 1024)
  }

  test("validity buffer calculation") {
    assert(GpuBatchUtils.calculateValidityBufferSize(1) == 64)
    assert(GpuBatchUtils.calculateValidityBufferSize(512) == 64)
    assert(GpuBatchUtils.calculateValidityBufferSize(513) == 128)
    assert(GpuBatchUtils.calculateValidityBufferSize(514) == 128)
    assert(GpuBatchUtils.calculateValidityBufferSize(1023) == 128)
    assert(GpuBatchUtils.calculateValidityBufferSize(1024) == 128)
    assert(GpuBatchUtils.calculateValidityBufferSize(1025) == 192)
  }

  test("offset buffer calculation") {
    assert(GpuBatchUtils.calculateOffsetBufferSize(64) == 260)
    assert(GpuBatchUtils.calculateOffsetBufferSize(1024) == 4100)
  }

  test("Batch size calculations") {
    assert(GpuBatchUtils.estimateRowCount(200, 200, 10) == 10)
    assert(GpuBatchUtils.estimateRowCount(200, 100, 10) == 20)
    assert(GpuBatchUtils.estimateRowCount(200, 99, 10) == 20)
    assert(GpuBatchUtils.estimateRowCount(200, 1, 10) == 2000)
    assert(GpuBatchUtils.estimateRowCount(100, 200, 10) == 10)
  }

  test("Batch size calculations cannot exceed Integer.MAX_VALUE") {
    assert(GpuBatchUtils.estimateRowCount(Long.MaxValue, 1, 1) == Integer.MAX_VALUE)
  }

  test("Batch size calculations assertion row count") {
    val e = intercept[AssertionError] {
      GpuBatchUtils.estimateRowCount(200, 100, 0)
    }
    assert(e.getMessage == "assertion failed: batch must contain at least one row")
  }

  test("Batch size calculations with 0 data size") {
    assert(GpuBatchUtils.estimateRowCount(200, 0, 1) == 1)
  }

  private def compareEstimateWithActual(schema: StructType, rowCount: Int): Unit = {
    val rows = GpuBatchUtilsSuite.createRows(schema, rowCount)
    val estimate = GpuBatchUtils.estimateGpuMemory(schema, rows.length)
    val actual = calculateGpuMemory(schema, rows)
    assert(estimate == actual)
  }

  /**
   * Note that this method is just for use in this unit test and the plugin does not attempt to
   * use InternalRow for calculating memory usage. It is just used here as a convenient way to
   * verify that the memory calculations are consistent with the numbers reported by CuDF.
   */
  private def calculateGpuMemory(schema: StructType, rows: Array[InternalRow]): Long = {
    val builders = new GpuColumnarBatchBuilder(schema, rows.length)
    try {
      val converters = new GpuRowToColumnConverter(schema)
      rows.foreach(row => converters.convert(row, builders))
      val batch = builders.build(rows.length)
      try {
        GpuColumnVector.getTotalDeviceMemoryUsed(batch)
      } finally {
        batch.close()
      }
    } finally {
      builders.close()
    }
  }
}

object GpuBatchUtilsSuite {

  def createRows(schema: StructType, rowCount: Int): Array[InternalRow] = {
    val rows = new mutable.ArrayBuffer[InternalRow](rowCount)
    val r = new Random(0)
    for (i <- 0 until rowCount) {
      rows.append(new GenericInternalRow(createRowValues(i, r, schema.fields)))
    }
    rows.toArray
  }

  def createExternalRows(schema: StructType, rowCount: Int): Array[Row] = {
    val externalRows = new mutable.ArrayBuffer[Row](rowCount)
    val r = new Random(0)
    for (i <- 0 until rowCount) {
      externalRows.append(new GenericRow(createExternalRowValues(i, r, schema.fields)))
    }
    externalRows.toArray
  }

  private def createValueForType(
      i: Int,
      r: Random,
      dt: DataType,
      nullable: Boolean,
      isInternal: Boolean = true): Any = {
    dt match {
      case DataTypes.BooleanType => maybeNull(nullable, i, r.nextBoolean())
      case DataTypes.ByteType => maybeNull(nullable, i, r.nextInt().toByte)
      case DataTypes.ShortType => maybeNull(nullable, i, r.nextInt().toShort)
      case DataTypes.IntegerType => maybeNull(nullable, i, r.nextInt())
      case DataTypes.LongType => maybeNull(nullable, i, r.nextLong())
      case DataTypes.FloatType => maybeNull(nullable, i, r.nextFloat())
      case DataTypes.DoubleType => maybeNull(nullable, i, r.nextDouble())
      // Spark use Int to store a Date internally, so use nextInt to avoid
      // 1). create Date object 2). convert Date to EpochDays int value
      case DataTypes.DateType => maybeNull(nullable, i, r.nextInt())
      // Spark use Long to store a Timestamp internally, so use nextLong to avoid
      // 1). create Timestamp object 2). convert Timestamp to microsecond long value
      case DataTypes.TimestampType => maybeNull(nullable, i, r.nextLong())
      case dataType: DecimalType =>
        val upperBound = (0 until dataType.precision).foldLeft(1L)((x, _) => x * 10)
        val unScaledValue = r.nextLong() % upperBound
        val d = maybeNull(nullable, i, Decimal(unScaledValue, dataType.precision, dataType.scale))
        if (d != null && !isInternal) {
          d.asInstanceOf[Decimal].toJavaBigDecimal
        } else {
          d
        }
      case dataType@DataTypes.StringType =>
        val length = if (nullable) dataType.defaultSize * 2 else dataType.defaultSize
        val utf8String = createUTF8String(length)
        val string = if (!isInternal) utf8String.toString() else utf8String

        if (nullable) {
          // since we want a deterministic test that compares the estimate with actual
          // usage we need to make sure the average length of strings is `dataType.defaultSize`
          if (i % 2 == 0) {
            null
          } else {
            string
          }
        } else {
          string
        }
      case dataType@DataTypes.BinaryType =>
        val length = if (nullable) dataType.defaultSize * 2 else dataType.defaultSize
        val bytes = r.nextString(length).getBytes
        val binary = if (!isInternal) bytes.toSeq else bytes

        if (nullable) {
          // since we want a deterministic test that compares the estimate with actual usage we
          // need to make sure the average length of binary values is `dataType.defaultSize`
          if (i % 2 == 0) {
            null
          } else {
            binary
          }
        } else {
          binary
        }
      case ArrayType(elementType, containsNull) =>
        if (nullable && i % 2 == 0) {
          null
        } else {
          val arrayValues = new mutable.ArrayBuffer[Any]()
          for (_ <- 0 to r.nextInt(10)) {
            arrayValues.append(createValueForType(i, r, elementType, containsNull, isInternal))
          }
          val array = ArrayData.toArrayData(arrayValues)
          if (!isInternal && array != null) {
            array.toSeq(elementType)
          } else {
            array
          }
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        if (nullable && i % 2 == 0) {
          null
        } else {
          val map = mutable.Map[Any, Any]()
          for (j <- 0 until 10) {
            if (valueContainsNull && j % 2 == 0) {
              map += (createValueForType(i, r, keyType, nullable = false, isInternal) -> null)
            } else {
              map += (createValueForType(i, r, keyType, nullable = false, isInternal) ->
                createValueForType(i, r, valueType, nullable = false, isInternal))
            }
          }
          val mapData = ArrayBasedMapData(map)
          if (mapData != null && !isInternal) {
            ArrayBasedMapData.toScalaMap(mapData)
          } else {
            mapData
          }
        }
      case StructType(fields) =>
        if (!isInternal) {
          new GenericRow(fields.map(f =>
            createValueForType(i, r, f.dataType, nullable = f.nullable, isInternal = false)))
        } else {
          InternalRow(fields.map(f => createValueForType(i, r, f.dataType, nullable)): _*)
        }

      case unknown => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private def createRowValues(i: Int, r: Random, fields: Array[StructField]) = {
    val values: Array[Any] = fields.map(field => {
      createValueForType(i, r, field.dataType, field.nullable)
    })
    values
  }

  private def createExternalRowValues(i: Int, r: Random, fields: Array[StructField]): Array[Any] = {
    val values: Array[Any] = fields.map { field => 
        createValueForType(i, r, field.dataType, field.nullable, isInternal = false)
    }
    values
  }

  private def maybeNull(nullable: Boolean, i: Int, value: Any): Any = {
    if (nullable && i % 2 == 0) {
      null
    } else {
      value
    }
  }

  private def createUTF8String(size: Int): UTF8String = {
    // avoid multi byte characters to keep the test simple
    val str = (0 until size).map(_ => 'a').mkString
    UTF8String.fromString(str)
  }
}
