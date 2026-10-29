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
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{LongType, StructField, StructType, VariantType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

class CachedBatchWriterVariantSuite extends SparkQueryCompareTestSuite {
  import CachedBatchWriterVariantSuite._

  // ColumnarBatchRow.copy() does not take Variant on Spark 4.0 and 4.1.0-4.1.1, so the
  // columnar entry has to carry its row into the next batch without copying it.
  test("columnar entry carries a Variant row into the next batch with its own values") {
    // Each row is estimated at 2056 bytes, so a 16 KiB budget holds seven of them.
    val numRows = 30
    val attrs = Seq(AttributeReference("id", LongType, nullable = false)(),
      AttributeReference("v", VariantType, nullable = false)())
    // The CPU session sends the columnar entry to the CPU writer whatever the GPU supports.
    withCpuSparkSession(spark => {
      // Spark's parse_json makes the Variant values, as no builder API exists on every runtime.
      val sources = spark.range(0, numRows, 1, 1).selectExpr("id",
        """parse_json(format_string('{"id": %d, "s": "%04d"}', id, id))""")
        .queryExecution.executedPlan.executeCollect()
        .map(r => (r.getLong(0), r.getVariant(1).getValue, r.getVariant(1).getMetadata))
      val ser = new ParquetCachedBatchSerializer
      val conf = TrampolineUtil.getSparkConf(spark)
      // A ColumnarBatch is not serializable, so the task builds its own.
      val input = spark.sparkContext.parallelize(Seq(0), 1)
        .mapPartitions(_ => Iterator(variantBatch(sources)))
      val cached = ser.convertColumnarBatchToCachedBatch(input, attrs, MEMORY_ONLY, conf)
        .collect().toList
      assert(cached.length > 2, "the rows should span several batches")
      val decoded = ser.convertCachedBatchToInternalRow(
        spark.sparkContext.parallelize(cached, numSlices = 1), attrs, attrs, conf)
        .map(_.copy()).collect()
      assert(decoded.length == numRows)
      decoded.zip(sources).foreach { case (row, (id, value, metadata)) =>
        val actual = row.getVariant(1)
        assert((row.getLong(0), actual.getValue.toSeq, actual.getMetadata.toSeq) ==
          (id, value.toSeq, metadata.toSeq))
      }
    }, new SparkConf().set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "16k"))
  }
}

object CachedBatchWriterVariantSuite {
  private val schema = StructType(Seq(StructField("id", LongType, nullable = false),
    StructField("v", VariantType, nullable = false)))

  private def variantBatch(rows: Seq[(Long, Array[Byte], Array[Byte])]): ColumnarBatch = {
    val cols = OnHeapColumnVector.allocateColumns(rows.length, schema)
    rows.zipWithIndex.foreach { case ((id, value, metadata), i) =>
      cols(0).putLong(i, id)
      cols(1).getChild(0).putByteArray(i, value)
      cols(1).getChild(1).putByteArray(i, metadata)
    }
    new ColumnarBatch(Array[ColumnVector](cols: _*), rows.length)
  }
}
