/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class PCBSNullSafeRowToColumnarSuite extends AnyFunSuite {
  private val schema = StructType(Seq(
    StructField("id", IntegerType, nullable = false), StructField("n", NullType)))

  test("empty input does not allocate a batch") {
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "empty", 0, Long.MaxValue)) { allocator =>
      withResource(new PCBSNullSafeArrowIterator(
        Iterator.empty, schema, "UTC", 2, allocator)) { it =>
        assert(!it.hasNext)
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("multiple batches preserve rows and release each batch before advancing") {
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "batches", 0, Long.MaxValue)) { allocator =>
      val rows = (0 until 5).iterator.map(index => InternalRow(index, null))
      withResource(new PCBSNullSafeArrowIterator(rows, schema, "UTC", 2, allocator)) { it =>
        var expected = 0
        val sizes = scala.collection.mutable.ArrayBuffer[Int]()
        while (it.hasNext) {
          val batch = it.next()
          sizes += batch.numRows()
          assert(allocator.getChildAllocators.size() == 1)
          assert(batch.column(1).dataType() == NullType)
          (0 until batch.numRows()).foreach { row =>
            assert(batch.column(0).getInt(row) == expected)
            assert(batch.column(1).isNullAt(row))
            expected += 1
          }
        }
        assert(expected == 5)
        assert(sizes.toSeq == Seq(2, 2, 1))
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("early termination and consumer close release a batch exactly once") {
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "early", 0, Long.MaxValue)) { allocator =>
      val it = new PCBSNullSafeArrowIterator(Iterator(InternalRow(1, null)), schema, "UTC", 2,
        allocator)
      withResource(it) { _ =>
        val batch = it.next()
        batch.close()
        batch.close()
        it.close()
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("input failure while building a batch releases its root and allocator") {
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "failure", 0, Long.MaxValue)) { allocator =>
      val failure = new IllegalStateException("input failure")
      val rows = Iterator(InternalRow(1, null)) ++ Iterator.continually(throw failure)
      withResource(new PCBSNullSafeArrowIterator(rows, schema, "UTC", 2, allocator)) { it =>
        assert(intercept[IllegalStateException](it.next()) eq failure)
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("nested NullType coexists with timestamp decimal and interval fields") {
    val mixedSchema = schema
      .add("s", StructType(Seq(StructField("n", NullType))))
      .add("a", ArrayType(NullType))
      .add("m", MapType(IntegerType, NullType))
      .add("ts", TimestampType).add("dec", DecimalType(12, 2))
      .add("ym", YearMonthIntervalType()).add("dt", DayTimeIntervalType())
      .add("emptyArray", ArrayType(NullType, containsNull = false))
      .add("emptyMap", MapType(NullType, NullType, valueContainsNull = false))
    val row = InternalRow(1, null, InternalRow(null), new GenericArrayData(Array[Any](null)),
      new ArrayBasedMapData(new GenericArrayData(Array(1)), new GenericArrayData(Array[Any](null))),
      -1L, Decimal(BigDecimal("123.45")), 14, 1234567L,
      new GenericArrayData(Array.empty[Any]),
      new ArrayBasedMapData(new GenericArrayData(Array.empty[Any]),
        new GenericArrayData(Array.empty[Any])))
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "mixed", 0, Long.MaxValue)) { allocator =>
      withResource(new PCBSNullSafeArrowIterator(Iterator(row), mixedSchema, "UTC", 2, allocator)) {
        it =>
          val batch = it.next()
          assert(batch.column(2).getStruct(0).isNullAt(0))
          assert(batch.column(3).getArray(0).isNullAt(0))
          assert(batch.column(4).getMap(0).valueArray().isNullAt(0))
          assert(batch.column(5).getLong(0) == -1L)
          assert(batch.column(6).getDecimal(0, 12, 2) == Decimal(BigDecimal("123.45")))
          assert(batch.column(7).getInt(0) == 14)
          assert(batch.column(8).getLong(0) == 1234567L)
          assert(batch.column(9).getArray(0).numElements() == 0)
          assert(batch.column(10).getMap(0).numElements() == 0)
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("duplicate nested field names preserve distinct values") {
    val duplicates = StructType(Seq(StructField("same", IntegerType),
      StructField("same", StringType), StructField("_pcbs_0", NullType)))
    val inputSchema = StructType(Seq(StructField("s", duplicates)))
    val row = InternalRow(InternalRow(7, UTF8String.fromString("second"), null))
    withResource(ArrowUtils.rootAllocator.newChildAllocator(
      "names", 0, Long.MaxValue)) { allocator =>
      withResource(new PCBSNullSafeArrowIterator(Iterator(row), inputSchema, "UTC", 2, allocator)) {
        it =>
          val value = it.next().column(0).getStruct(0)
          assert(value.getInt(0) == 7)
          assert(value.getUTF8String(1).toString == "second")
          assert(value.isNullAt(2))
      }
      assert(allocator.getAllocatedMemory == 0)
      assert(allocator.getChildAllocators.isEmpty)
    }
  }

  test("negative-scale and high-precision decimals preserve values alongside NullType") {
    val conf = new SQLConf
    conf.setConfString("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    SQLConf.withExistingConf(conf) {
      val decimals = Seq((DecimalType(8, -2), BigDecimal("12300")),
        (DecimalType(38, 18), BigDecimal("12345678901234567890.123456789012345678")))
      decimals.foreach { case (decimalType, value) =>
        val expected = Decimal(value, decimalType.precision, decimalType.scale)
        val inputSchema = schema.add("d", decimalType)
        withResource(ArrowUtils.rootAllocator.newChildAllocator(
          "decimal", 0, Long.MaxValue)) { allocator =>
          val rows = Iterator(InternalRow(1, null, expected))
          withResource(new PCBSNullSafeArrowIterator(
            rows, inputSchema, "UTC", 2, allocator)) { it =>
            assert(it.next().column(2).getDecimal(0, decimalType.precision, decimalType.scale) ==
              expected)
          }
          assert(allocator.getAllocatedMemory == 0)
          assert(allocator.getChildAllocators.isEmpty)
        }
      }
    }
  }
}
