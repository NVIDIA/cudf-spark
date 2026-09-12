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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRowLineageWriterSuite extends RmmSparkRetrySuiteBase {
  private val writeSchema = new StructType().add("id", LongType)
    .add("_row_id", LongType).add("_last_updated_sequence_number", LongType)
  private val metadataSchema = new StructType().add("_last_updated_sequence_number", LongType)
    .add("_spec_id", LongType).add("_row_id", LongType)

  private def values(batch: ColumnarBatch, ordinal: Int): Seq[Option[Long]] = {
    withResource(batch.column(ordinal).asInstanceOf[GpuColumnVector].copyToHost()) { column =>
      (0 until batch.numRows()).map { row =>
        if (column.isNullAt(row)) None else Some(column.getLong(row))
      }
    }
  }

  private def dataBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder().column(Long.box(1L), 2L).build()) { table =>
      GpuColumnVector.from(table, Array[DataType](LongType))
    }
  }

  test("reinsert preserves row IDs and sequence nulls using metadata column names") {
    withResource(dataBatch()) { record =>
      withResource(new Table.TestBuilder()
          .column(Long.box(7L), null.asInstanceOf[java.lang.Long])
          .column(Long.box(0L), 0L).column(Long.box(101L), 102L).build()) { table =>
        withResource(GpuColumnVector.from(table, Array.fill[DataType](3)(LongType))) { metadata =>
          withResource(GpuDataWriterWithRowLineage.appendLineage(
              record, metadata, writeSchema, metadataSchema)) { physical =>
            assert(physical.numCols() == 3)
            assert(values(physical, 0) == Seq(Some(1L), Some(2L)))
            assert(values(physical, 1) == Seq(Some(101L), Some(102L)))
            assert(values(physical, 2) == Seq(Some(7L), None))
          }
          // Appending lineage borrows its inputs; closing the result must not release them.
          assert(values(metadata, 2) == Seq(Some(101L), Some(102L)))
        }
      }
      assert(values(record, 0) == Seq(Some(1L), Some(2L)))
    }
  }

  test("mixed inserts and reinserts preserve order and inherit lineage only for inserts") {
    withResource(dataBatch()) { record =>
      withResource(new Table.TestBuilder()
          .column(Long.box(7L), 8L).column(Long.box(0L), 0L)
          .column(Long.box(101L), 102L).build()) { table =>
        withResource(GpuColumnVector.from(table, Array.fill[DataType](3)(LongType))) { metadata =>
          withResource(CudfColumnVector.fromBooleans(true, false)) { reinsertMask =>
            withResource(GpuDataWriterWithRowLineage.appendLineage(
                record, metadata, writeSchema, metadataSchema, reinsertMask)) { physical =>
              assert(values(physical, 0) == Seq(Some(1L), Some(2L)))
              assert(values(physical, 1) == Seq(Some(101L), None))
              assert(values(physical, 2) == Seq(Some(7L), None))
            }
          }
          assert(values(metadata, 2) == Seq(Some(101L), Some(102L)))
          assert(values(metadata, 0) == Seq(Some(7L), Some(8L)))
        }
      }
      assert(values(record, 0) == Seq(Some(1L), Some(2L)))
    }
  }

  test("insert appends inheritable null lineage") {
    withResource(dataBatch()) { record =>
      withResource(GpuDataWriterWithRowLineage.appendLineage(
          record, null, writeSchema, null)) { physical =>
        assert(values(physical, 0) == Seq(Some(1L), Some(2L)))
        assert(values(physical, 1) == Seq(None, None))
        assert(values(physical, 2) == Seq(None, None))
      }
    }
  }

  test("complete Spark 3 rows and v2 rows do not acquire extra columns") {
    withResource(new Table.TestBuilder()
        .column(Long.box(1L), 2L)
        .column(Long.box(101L), 102L)
        .column(Long.box(7L), 8L).build()) { table =>
      withResource(GpuColumnVector.from(table, Array.fill[DataType](3)(LongType))) { record =>
        withResource(GpuDataWriterWithRowLineage.appendLineage(
            record, null, writeSchema, null)) { physical =>
          assert(physical.numCols() == 3)
          assert(values(physical, 1) == Seq(Some(101L), Some(102L)))
        }
      }
    }
    withResource(dataBatch()) { record =>
      withResource(GpuDataWriterWithRowLineage.appendLineage(
          record, null, new StructType().add("id", LongType), null)) { physical =>
        assert(physical.numCols() == 1)
        assert(values(physical, 0) == Seq(Some(1L), Some(2L)))
      }
    }
  }

  test("mismatched metadata row counts fail without consuming the record") {
    withResource(dataBatch()) { record =>
      withResource(new ColumnarBatch(Array.empty, 1)) { metadata =>
        val error = intercept[IllegalArgumentException] {
          GpuDataWriterWithRowLineage.appendLineage(
            record, metadata, writeSchema, metadataSchema)
        }
        assert(error.getMessage.contains("Metadata row count"))
      }
      assert(values(record, 0) == Seq(Some(1L), Some(2L)))
    }
  }
}
