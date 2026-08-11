/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

/**
 * Unit tests for utility methods in [[ BatchWithPartitionDataUtils ]]
 */
class BatchWithPartitionDataSuite extends RmmSparkRetrySuiteBase {

  test("test splitting partition data into groups") {
    val maxGpuColumnSizeBytes = 1000L
    val (_, partValues, partRows, schema) = getSamplePartitionData
    val partitionRowData = PartitionRowData.from(partValues, partRows)
    val resultPartitions = BatchWithPartitionDataUtils.splitPartitionDataIntoGroups(
      partitionRowData, schema, maxGpuColumnSizeBytes)
    val resultRowCounts = resultPartitions.flatMap(_.map(_.rowNum)).sum
    val expectedRowCounts = partitionRowData.map(_.rowNum).sum
    assert(resultRowCounts == expectedRowCounts)
  }

  test("test splitting partition data into halves") {
    val (_, partValues, partRows, _) = getSamplePartitionData
    val partitionRowData = PartitionRowData.from(partValues, partRows)
    val resultPartitions = BatchWithPartitionDataUtils.splitPartitionDataInHalf(partitionRowData)
    val resultRowCounts = resultPartitions.flatMap(_.map(_.rowNum)).sum
    val expectedRowCounts = partitionRowData.map(_.rowNum).sum
    assert(resultRowCounts == expectedRowCounts)
  }

  test("test adding partition values to batch with OOM split and retry - unhandled") {
    // This test uses single-row partition values that should throw a GpuSplitAndRetryOOM exception
    // when a retry is forced.
    val maxGpuColumnSizeBytes = 1000L
    val (_, partValues, _, partSchema) = getSamplePartitionData
    closeOnExcept(buildBatch(getSampleValueData)) { valueBatch =>
      val resultBatchIter = BatchWithPartitionDataUtils.addPartitionValuesToBatch(valueBatch,
        Array(1), partValues.take(1), partSchema, maxGpuColumnSizeBytes)
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      withResource(resultBatchIter) { _ =>
        assertThrows[GpuSplitAndRetryOOM] {
          resultBatchIter.next()
        }
      }
    }
  }

  test("test adding partition values to batch with OOM split and retry") {
    // This test should split the input batch and process them when a retry is forced.
    val maxGpuColumnSizeBytes = 1000L
    val (partCols, partValues, partRows, partSchema) = getSamplePartitionData
    withResource(buildBatch(getSampleValueData)) { valueBatch =>
      withResource(buildBatch(partCols)) { partBatch =>
        withResource(GpuColumnVector.combineColumns(valueBatch, partBatch)) { expectedBatch =>
          // we incRefCounts here because `addPartitionValuesToBatch` takes ownership of
          // `valueBatch`, but we are keeping it alive since its columns are part of
          // `expectedBatch`
          GpuColumnVector.incRefCounts(valueBatch)
          val resultBatchIter = BatchWithPartitionDataUtils.addPartitionValuesToBatch(valueBatch,
            partRows, partValues, partSchema, maxGpuColumnSizeBytes)
          withResource(resultBatchIter) { _ =>
            RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
              RmmSpark.OomInjectionType.GPU.ordinal, 0)
            // Assert that the final count of rows matches expected batch
            // We also need to close each batch coming from `resultBatchIter`.
            val rowCounts = resultBatchIter.map(withResource(_) {
              _.numRows()
            }).sum
            assert(rowCounts == expectedBatch.numRows())
          }
        }
      }
    }
  }

  private def getSamplePartitionData: (Array[Array[String]], Array[InternalRow], Array[Long],
      StructType) = {
    val schema = StructType(Array(
      StructField("k0", StringType),
      StructField("k1", StringType)
    ))
    val partCols = Array(
      Array("pk1", "pk1", "pk2", "pk3", "pk3", "pk3", null, null, "pk4", "pk4"),
      Array("sk1", "sk1", null, "sk2", "sk2", "sk2", "sk3", "sk3", "sk3", "sk3")
    )
    // Partition values with counts from above sample
    // Declaring it directly instead of calculating from above to avoid code complexity in tests
    val partValues = Array(
      Array("pk1", "sk1"),
      Array("pk2", null),
      Array("pk3", "sk2"),
      Array(null, "sk3"),
      Array("pk4", "sk3")
    )
    val partRowNums = Array(2L, 1L, 3L, 2L, 2L)
    (partCols, partValues.map(toInternalRow), partRowNums, schema)
  }

  private def getSampleValueData: Array[Array[String]] = {
    Array(
      Array("v00", "v01", "v02", "v03", "v04", "v05", "v06", "v07", "v08", "v09"),
      Array("v10", "v11", "v12", "v13", "v14", "v15", "v16", "v17", "v18", "v19")
    )
  }

  private def toInternalRow(values: Array[String]): InternalRow = {
    val utfStrings = values.map(v => UTF8String.fromString(v).asInstanceOf[Any])
    new GenericInternalRow(utfStrings)
  }

  private def buildBatch(data: Array[Array[String]]): ColumnarBatch = {
    val numRows = data.head.length
    val colVectors = data.map(v =>
      GpuColumnVector.from(ColumnVector.fromStrings(v: _*), StringType))
    new ColumnarBatch(colVectors.toArray, numRows)
  }
}