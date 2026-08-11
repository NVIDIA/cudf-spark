/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuColumnarToRowSuite extends RmmSparkRetrySuiteBase {
  test("iterate past empty input batches") {
    val batchIter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      private[this] var batchCount = 0

      override def hasNext: Boolean = batchCount < 10

      override def next(): ColumnarBatch = {
        batchCount += 1
        if (batchCount % 2 == 0) {
          new ColumnarBatch(Array.empty, 0)
        } else {
          val gcv = GpuColumnVector.from(ColumnVector.fromStrings(batchCount.toString), StringType)
          new ColumnarBatch(Array(gcv), 1)
        }
      }
    }

    val ctriter = new ColumnarToRowIterator(batchIter, NoopMetric, NoopMetric, NoopMetric,
      NoopMetric)
    assertResult(Seq("1", "3", "5", "7", "9"))(ctriter.map(_.getString(0)).toSeq)
  }

  test("transform binary data back and forth between Row and Columnar") {
    val schema = StructType(Seq(StructField("Binary", BinaryType),
      StructField("BinaryNotNull", BinaryType, nullable = false)))
    val numRows = 300
    val rows = GpuBatchUtilsSuite.createRows(schema, numRows)

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val r2cConverter = new GpuRowToColumnConverter(schema)
      rows.foreach(r2cConverter.convert(_, batchBuilder))
      closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
        val c2rIterator = new ColumnarToRowIterator(Iterator(columnarBatch),
          NoopMetric, NoopMetric, NoopMetric, NoopMetric)
        rows.foreach { input =>
          val output = c2rIterator.next()
          if (input.isNullAt(0)) {
            assert(output.isNullAt(0))
          } else {
            assert(input.getBinary(0) sameElements output.getBinary(0))
          }
          assert(input.getBinary(1) sameElements output.getBinary(1))
        }
        assert(!c2rIterator.hasNext)
      }
    }
  }
}
