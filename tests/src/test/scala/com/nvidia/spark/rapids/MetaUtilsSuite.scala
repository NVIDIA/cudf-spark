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

import java.math.RoundingMode
import java.util

import ai.rapids.cudf.{ContiguousTable, DeviceMemoryBuffer, DType, HostColumnVector, Table}
import ai.rapids.cudf.HostColumnVector.{BasicType, StructData}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.CodecType
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class MetaUtilsSuite extends AnyFunSuite {
  private val contiguousTableSparkTypes: Array[DataType] = Array(
    IntegerType,
    StringType,
    DoubleType,
    DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5),
    ArrayType(StringType),
    StructType(Array(
      StructField("ints", IntegerType),
      StructField("strarray", ArrayType(StringType)))
    )
  )

  private def buildContiguousTable(): ContiguousTable = {
    def struct(vals: Object*): StructData = new StructData(vals:_*)
    val structType = new HostColumnVector.StructType(true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.ListType(true, new BasicType(true, DType.STRING)))
    withResource(new Table.TestBuilder()
        .column(5.asInstanceOf[Integer], null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
        .column(Array("1", "2", "three"), Array("4"), null, Array("five"))
        .column(structType,
          struct(1.asInstanceOf[Integer], util.Arrays.asList("a", "b", "c")),
          struct(2.asInstanceOf[Integer], util.Arrays.asList("1", "2", null)),
          struct(3.asInstanceOf[Integer], null),
          struct(null, util.Arrays.asList("xyz")))
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  private def buildDegenerateTable(schema: StructType): ContiguousTable = {
    withResource(GpuColumnVector.emptyBatch(schema)) { batch =>
      withResource(GpuColumnVector.from(batch)) { table =>
        table.contiguousSplit().head
      }
    }
  }

  test("buildTableMeta") {
    withResource(buildContiguousTable()) { contigTable =>
      val buffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(7, contigTable)

      val bufferMeta = meta.bufferMeta
      assertResult(7)(bufferMeta.id)
      assertResult(buffer.getLength)(bufferMeta.size)
      assertResult(buffer.getLength)(bufferMeta.uncompressedSize)
      assertResult(0)(bufferMeta.codecBufferDescrsLength)
      assertResult(contigTable.getRowCount)(meta.rowCount)
      assertResult(contigTable.getMetadataDirectBuffer)(meta.packedMetaAsByteBuffer())
    }
  }

  test("buildTableMeta with codec") {
    withResource(buildContiguousTable()) { contigTable =>
      val tableId = 7
      val codecType = CodecType.COPY
      val compressedSize: Long = 123
      val buffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(Some(tableId), contigTable, codecType, compressedSize)

      val bufferMeta = meta.bufferMeta
      assertResult(tableId)(bufferMeta.id)
      assertResult(compressedSize)(bufferMeta.size)
      assertResult(contigTable.getRowCount)(meta.rowCount)
      assertResult(contigTable.getMetadataDirectBuffer)(meta.packedMetaAsByteBuffer())
      assertResult(1)(bufferMeta.codecBufferDescrsLength)
      val codecDescr = bufferMeta.codecBufferDescrs(0)
      assertResult(codecType)(codecDescr.codec)
      assertResult(compressedSize)(codecDescr.compressedSize)
      assertResult(0)(codecDescr.compressedOffset)
      assertResult(0)(codecDescr.uncompressedOffset)
      assertResult(buffer.getLength)(codecDescr.uncompressedSize)
    }
  }

  test("buildDegenerateTableMeta no columns") {
    val degenerateBatch = new ColumnarBatch(Array(), 127)
    val meta = MetaUtils.buildDegenerateTableMeta(degenerateBatch)
    assertResult(null)(meta.bufferMeta)
    assertResult(null)(meta.packedMetaAsByteBuffer())
    assertResult(127)(meta.rowCount)
  }

  test("buildDegenerateTableMeta no rows") {
    val schema = StructType.fromDDL("a INT, b STRING, c DOUBLE, d DECIMAL(15, 5)")
    withResource(buildDegenerateTable(schema)) { contigTable =>
      withResource(GpuPackedTableColumn.from(contigTable)) { packedBatch =>
        val meta = MetaUtils.buildDegenerateTableMeta(packedBatch)
        assertResult(null)(meta.bufferMeta)
        assertResult(0)(meta.rowCount)
        assertResult(contigTable.getMetadataDirectBuffer)(meta.packedMetaAsByteBuffer())
      }
    }
  }

  test("buildDegenerateTableMeta no rows compressed table") {
    val schema = StructType.fromDDL("a INT, b STRING, c DOUBLE, d DECIMAL(15, 5)")
    withResource(buildDegenerateTable(schema)) { contigTable =>
      withResource(GpuPackedTableColumn.from(contigTable)) { uncompressedBatch =>
        val uncompressedMeta = MetaUtils.buildDegenerateTableMeta(uncompressedBatch)
        withResource(DeviceMemoryBuffer.allocate(0)) { buffer =>
          val compressedTable = CompressedTable(0, uncompressedMeta, buffer)
          withResource(GpuCompressedColumnVector.from(compressedTable)) { batch =>
            val meta = MetaUtils.buildDegenerateTableMeta(batch)
            assertResult(null)(meta.bufferMeta)
            assertResult(0)(meta.rowCount)
            assertResult(uncompressedMeta.packedMetaAsByteBuffer())(meta.packedMetaAsByteBuffer())
          }
        }
      }
    }
  }

  test("getBatchFromMeta") {
    withResource(buildContiguousTable()) { contigTable =>
      val origBuffer = contigTable.getBuffer
      val meta = MetaUtils.buildTableMeta(10, contigTable)
      withResource(origBuffer.sliceWithCopy(0, origBuffer.getLength)) { buffer =>
        withResource(MetaUtils.getBatchFromMeta(buffer, meta,
          contiguousTableSparkTypes)) { batch =>
          assertResult(contigTable.getRowCount)(batch.numRows)
          val table = contigTable.getTable
          assertResult(table.getNumberOfColumns)(batch.numCols)
          (0 until table.getNumberOfColumns).foreach { i =>
            val batchColumn = batch.column(i)
            assert(batchColumn.isInstanceOf[GpuColumnVectorFromBuffer])
            TestUtils.compareColumns(table.getColumn(i),
              batchColumn.asInstanceOf[GpuColumnVector].getBase)
          }
        }
      }
    }
  }
}
