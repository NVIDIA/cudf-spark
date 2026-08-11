/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.{ColumnarRdd, ColumnarToRowIterator, GpuBatchUtilsSuite, GpuColumnVectorUtils, NoopMetric, RapidsHostColumnVector, RmmSparkRetrySuiteBase, SparkQueryCompareTestSuite, TestResourceFinder}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import org.scalatest.Assertion

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._

class InternalColumnarRDDConverterSuite extends RmmSparkRetrySuiteBase {

  def compareMapAndMapDate[K, V](map: collection.Map[K, V], mapData: MapData): Assertion = {
    assert(map.size == mapData.numElements())
    val outputMap = mutable.Map[Any, Any]()
    // Only String now, TODO: support other data types in Map
    mapData.foreach(StringType, StringType, f = (k, v) => outputMap += (k.toString -> v.toString))
    val diff = outputMap.toSet diff map.toSet
    assert(diff.toMap.isEmpty)
  }

  test("transform binary data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Binary", BinaryType),
      StructField("BinaryNotNull", BinaryType, nullable = false)))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      val columnarBatch = batchBuilder.build(numRows)
      withResource(new ColumnarToRowIterator(Iterator(columnarBatch),
        NoopMetric, NoopMetric, NoopMetric, NoopMetric)) { c2rIterator =>
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getSeq[Byte](0) sameElements output.getBinary(0))
          }
          assert(input.getSeq[Byte](1) sameElements output.getBinary(1))
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }

  test("transform boolean, byte, short, int, float, long, double, date, timestamp data" +
    " back and forth between Row and Columnar") {
    val schema = StructType(Seq(
      StructField("Boolean", BooleanType),
      StructField("BinaryNotNull", BooleanType, nullable = false),
      StructField("Byte", ByteType),
      StructField("ByteNotNull", ByteType, nullable = false),
      StructField("Short", ShortType),
      StructField("ShortNotNull", ShortType, nullable = false),
      StructField("Int", IntegerType),
      StructField("IntNotNull", IntegerType, nullable = false),
      StructField("Float", FloatType),
      StructField("FloatNotNull", FloatType, nullable = false),
      StructField("Long", LongType),
      StructField("LongNotNull", LongType, nullable = false),
      StructField("Double", DoubleType),
      StructField("DoubleNotNull", DoubleType, nullable = false),
      StructField("Date", DateType),
      StructField("DateNotNull", DateType, nullable = false),
      StructField("Timestamp", TimestampType),
      StructField("TimestampNotNull", TimestampType, nullable = false),
      StructField("Decimal", DecimalType(20, 10)),
      StructField("DecimalNotNull", DecimalType(20, 10), nullable = false)
    ))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getBoolean(0) == output.getBoolean(0))
          }
          assert(input.getBoolean(1) == output.getBoolean(1))

          for ((f, i) <- schema.fields.zipWithIndex) {
            if (f.nullable && input.isNullAt(i)) {
              assert(output.isNullAt(i))
            } else {
              if (f.dataType.isInstanceOf[DecimalType]) {
                assert(input.get(i) == output.get(i, f.dataType)
                  .asInstanceOf[Decimal].toJavaBigDecimal)
              } else {
                assert(input.get(i) == output.get(i, f.dataType))
              }
            }
          }
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }

  test("transform string data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("String", StringType),
      StructField("StringNotNull", StringType, nullable = false)))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getString(0) == output.getString(0))
          }
          assert(input.getString(1) == output.getString(1))
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }


  test("transform byte data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Byte", ByteType),
      StructField("ByteNotNull", ByteType, nullable = false)))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getByte(0) == output.getByte(0))
          }
          assert(input.getByte(1) == output.getByte(1))
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }

  test("transform array data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Array", ArrayType.apply(DoubleType)),
      StructField("ArrayNotNull", ArrayType.apply(DoubleType, false), nullable = false)))
    val numRows = 300
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getSeq(0) sameElements output.getArray(0).toDoubleArray())
          }
          assert(input.getSeq(1) sameElements output.getArray(1).toDoubleArray())
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }

  test("transform map data back and forth between Row and Columnar") {
    val schema = StructType(Seq(
      StructField("Map", DataTypes.createMapType(StringType, StringType)),
      StructField("MapNotNull", DataTypes.createMapType(StringType, StringType),
        nullable = false)))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            compareMapAndMapDate(input.getMap(0), output.getMap(0))
          }
          compareMapAndMapDate(input.getMap(1), output.getMap(1))
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }

  test("transform struct data back and forth between Row and Columnar") {
    val structFieldArray = Array(
      StructField("struct_int", IntegerType),
      StructField("struct_double", DoubleType),
      StructField("struct_array", DataTypes.createArrayType(DoubleType))
    )
    val schema = StructType(Seq(
      StructField("Struct", DataTypes.createStructType(structFieldArray)),
      StructField("StructNotNull", DataTypes.createStructType(structFieldArray),
        nullable = false)))
    val numRows = 100
    val rows = GpuBatchUtilsSuite.createExternalRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val extR2CConverter = new GpuExternalRowToColumnConverter(schema)
      rows.foreach(extR2CConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            val inputStructRow = input.getStruct(0)
            val outputStructRow = output.getStruct(0, 3)
            if (inputStructRow.isNullAt(0)) {
              assert(outputStructRow.isNullAt(0))
            } else {
              assert(inputStructRow.getInt(0) == outputStructRow.getInt(0))
            }
            if (inputStructRow.isNullAt(1)) {
              assert(outputStructRow.isNullAt(1))
            } else {
              assert(inputStructRow.getDouble(1) == outputStructRow.getDouble(1))
            }
            if (inputStructRow.isNullAt(2)) {
              assert(outputStructRow.isNullAt(2))
            } else {
              assert(inputStructRow.getSeq(2) sameElements outputStructRow.getArray(2)
                .toDoubleArray())
            }
          }
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }
}

// these tests are separated out because they are really just spark tests, and they should skip
// the initialization done in `RmmSparkRetrySuiteBase`
class InternalColumnarRDDConverterSparkSessionSuite extends SparkQueryCompareTestSuite {
  test("InternalColumnarRddConverter should extractRDDTable RDD[ColumnarBatch]") {
    withGpuSparkSession(spark => {
      val path = TestResourceFinder.getResourcePath("disorder-read-schema.parquet")
      val df = spark.read.parquet(path)
      val (optionRddColumnBatch, _) = InternalColumnarRddConverter.extractRDDColumnarBatch(df)

      assert(optionRddColumnBatch.isDefined, "Can't extract RDD[ColumnarBatch]")

    }, new SparkConf().set("spark.rapids.sql.test.allowedNonGpu", "DeserializeToObjectExec"))
  }

  test("Extract RapidsHostColumnVector from GpuColumnVectorUtils") {
    withGpuSparkSession(spark => {
      val rdd = ColumnarRdd(spark.range(10).toDF())
      val result = rdd.map(table => {
        val columns = GpuColumnVectorUtils.extractHostColumns(table, Array(LongType))
        val isRapidsHostColumnVector = columns(0) match {
          case _: RapidsHostColumnVector => true
          case _ => false
        }
        columns.foreach(_.close())
        table.close()
        isRapidsHostColumnVector
      }).collect()
      assert(result.forall(_ == true))
    }, new SparkConf().set("spark.rapids.sql.test.allowedNonGpu", "DeserializeToObjectExec"))
  }
}
