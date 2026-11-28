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
{"spark":["350"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.iceberg.{PartitionSpec, Schema, StructLike}
import org.apache.iceberg.io.{DataWriteResult, PartitioningWriter}
import org.apache.iceberg.spark.GpuTypeToSparkType
import org.apache.iceberg.types.Types

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuPartitionedDataWriterRetrySuite extends RmmSparkRetrySuiteBase {
  private val schema = new Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.optional(2, "p", Types.DoubleType.get()))
  private val spec = PartitionSpec.builderFor(schema).identity("p").build()
  private val sparkType = GpuTypeToSparkType.toSparkType(schema)

  private def buildBatch(rows: Seq[(Int, Option[Double])]): ColumnarBatch = {
    val ids = rows.map(_._1).toArray
    val keys = rows.map(_._2.map(Double.box).orNull).toArray
    val idCol = GpuColumnVector.from(CudfColumnVector.fromInts(ids: _*), IntegerType)
    closeOnExcept(idCol) { _ =>
      val keyCol = GpuColumnVector.from(CudfColumnVector.fromBoxedDoubles(keys: _*), DoubleType)
      closeOnExcept(keyCol) { _ =>
        new ColumnarBatch(Array[ColumnVector](idCol, keyCol), rows.length)
      }
    }
  }

  private def keyBits(value: Option[Double]): Option[Long] = {
    value.map(v => java.lang.Double.doubleToLongBits(if (v.isNaN) Double.NaN else v))
  }

  private class RecordingPartitioningWriter
    extends PartitioningWriter[SpillableColumnarBatch, DataWriteResult] {
    val written = ArrayBuffer.empty[(Option[Long], Seq[(Int, Option[Long])])]
    var retriesAtFirstHandoff: Option[Int] = None

    override def write(
        batch: SpillableColumnarBatch,
        spec: PartitionSpec,
        partition: StructLike): Unit = {
      if (retriesAtFirstHandoff.isEmpty) {
        retriesAtFirstHandoff = Some(RmmSpark.getAndResetNumRetryThrow(/* taskId = */ 1))
      }
      val partBits = Option(partition.get(0, classOf[java.lang.Double]))
        .map(v => java.lang.Double.doubleToLongBits(v.doubleValue()))
      val rows = withResource(batch) { _ =>
        withResource(batch.getColumnarBatch()) { cb =>
          val idCol = cb.column(0).asInstanceOf[GpuColumnVector]
          val keyCol = cb.column(1).asInstanceOf[GpuColumnVector]
          withResource(idCol.copyToHost()) { hostIds =>
            withResource(keyCol.copyToHost()) { hostKeys =>
              (0 until cb.numRows()).map { i =>
                val id = hostIds.getInt(i)
                val key = if (hostKeys.isNullAt(i)) {
                  None
                } else {
                  Some(java.lang.Double.doubleToLongBits(hostKeys.getDouble(i)))
                }
                (id, key)
              }
            }
          }
        }
      }
      written += ((partBits, rows))
    }

    override def result(): DataWriteResult =
      new DataWriteResult(java.util.Collections.emptyList[org.apache.iceberg.DataFile]())

    override def close(): Unit = {}
  }

  test("GpuPartitionedDataWriter retries partitioning OOM before handing groups to delegate") {
    val inputRows = Seq(
      (1, Some(-0.0d)),
      (2, Some(0.0d)),
      (3, Some(-0.0d)),
      (4, None),
      (5, Some(Double.NaN)),
      (6, Some(java.lang.Double.longBitsToDouble(0x7ff8000000000001L))),
      (7, Some(1.5d)))
    val expectedByPart = inputRows.groupBy(r => keyBits(r._2))
      .map { case (part, rows) => (part, rows.map(r => (r._1, keyBits(r._2))).sortBy(_._1)) }

    var skip = 0
    var coveredAllocations = 0
    var triggered = true
    while (triggered) {
      val recording = new RecordingPartitioningWriter
      val writer = new GpuPartitionedDataWriter(
        null, null, null, spec, schema, sparkType, 1024L, false, StructType(Seq.empty)) {
        override protected def createDelegate():
            PartitioningWriter[SpillableColumnarBatch, DataWriteResult] = recording
      }

      val batch = buildBatch(inputRows)
      RmmSpark.getAndResetNumRetryThrow(/* taskId = */ 1)
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, skip)
      try {
        writer.write(batch)
      } finally {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 0)
        writer.close()
      }

      val actualAllRows = recording.written.flatMap(_._2)
      assert(actualAllRows.length == inputRows.length,
        s"skip=$skip: expected each row to be written once")
      assert(actualAllRows.map(_._1).sorted == inputRows.map(_._1).sorted,
        s"skip=$skip: row IDs mismatch")
      val actualByPart = recording.written.groupBy(_._1)
        .map { case (part, groups) =>
          (part, groups.flatMap(_._2).map { case (id, bits) =>
            (id, bits.map(b =>
              if (java.lang.Double.isNaN(java.lang.Double.longBitsToDouble(b))) {
                java.lang.Double.doubleToLongBits(Double.NaN)
              } else {
                b
              }))
          }.sortBy(_._1))
        }
      assert(actualByPart == expectedByPart, s"skip=$skip: partition assignment mismatch")

      val retriesBeforeHandoff = recording.retriesAtFirstHandoff.getOrElse(0)
      if (retriesBeforeHandoff > 0) {
        assert(retriesBeforeHandoff == 1,
          s"skip=$skip: expected 1 retry before first handoff")
        coveredAllocations += 1
        skip += 1
      } else {
        triggered = false
      }
    }
    assert(coveredAllocations >= 3,
      "Expected OOM injection to cover encode, group, and decode allocations, " +
        s"got $coveredAllocations")
  }
}
