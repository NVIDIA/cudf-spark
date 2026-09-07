/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg

import java.lang.Math.toIntExact

import scala.collection.JavaConverters._

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.{GpuTypeToSparkType, SparkStructLike}
import org.apache.iceberg.spark.functions._
import org.apache.iceberg.types.Types

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A GPU based Iceberg partitioner that partitions columnar batches by key.
 * This class takes pre-computed keys and values as separate columnar batches.
 *
 * @param keyType the iceberg struct type of the partition keys
 * @param dataType the iceberg struct type of the input data
 */
class GpuIcebergPartitioner(
  val keyType: Types.StructType,
  val dataType: Types.StructType) {

  private val keySparkType: StructType = GpuTypeToSparkType.toSparkType(keyType)
  private val dataSparkType: StructType = GpuTypeToSparkType.toSparkType(dataType)
  private val valueSparkType: Array[DataType] = dataSparkType.fields.map(_.dataType)

  /**
   * Partition the columnar batches by the given keys.
   * This method partitions the `values` columnar batch based on the `keys` columnar batch.
   * The number of rows in both batches must match.
   * <br/>
   * This method does NOT take ownership of the input columnar batches.
   * The caller is responsible for managing their lifecycle.
   *
   * @param keys the partition keys columnar batch
   * @param values the data values columnar batch
   * @return a sequence of partitioned batches with their partition keys
   */
  def partition(keys: ColumnarBatch, values: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    require(keys.numRows() == values.numRows(),
      s"Keys row count ${keys.numRows()} not matching with values row count ${values.numRows()}")

    if (keys.numRows() == 0) {
      return Seq.empty
    }

    val keyColIndices = (0 until keys.numCols()).toArray
    val inputColIndices = (keys.numCols() until (keys.numCols() + values.numCols())).toArray

    // Combine keys and values into a single batch: [key columns, input columns]
    val keysAndInputBatch = GpuColumnVector.combineColumns(keys, values)
    
    withResource(keysAndInputBatch) { _ =>
      withResource(GpuColumnVector.from(keysAndInputBatch)) { keysAndInputTable =>
        // Split the input columns by the key columns using the efficient JNI API
        val splitRet = withResource(keysAndInputTable) { _ =>
          keysAndInputTable.groupBy(keyColIndices: _*)
            .contiguousSplitGroupsAndGenUniqKeys(inputColIndices)
        }

        // Generate results
        withResource(splitRet) { _ =>
          // Generate the partition keys on the host side
          val partitionKeys = GpuIcebergPartitioner.toPartitionKeys(keyType,
            keySparkType,
            splitRet.getUniqKeyTable)

          // Release unique table to save GPU memory
          splitRet.closeUniqKeyTable()

          // Get the partitions
          val partitions = splitRet.getGroups

          // Combine the partition keys and partitioned tables
          partitionKeys.zip(partitions).map { case (partKey, partition) =>
            ColumnarBatchWithPartition(SpillableColumnarBatch(partition,
              valueSparkType,
              SpillPriorities.ACTIVE_BATCHING_PRIORITY),
              partKey)
          }.toSeq
        }
      }
    }
  }
}

/**
 * A GPU based Iceberg partitioner that partitions the input columnar batch into multiple
 * columnar batches based on the given partition spec.
 * This class is built on top of GpuIcebergPartitioner.
 *
 * @param spec the iceberg partition spec
 * @param dataType the iceberg struct type of the input data
 */
class GpuIcebergSpecPartitioner(val spec: PartitionSpec,
  val dataType: Types.StructType) {
  require(spec.isPartitioned, "Should not create a partitioner for unpartitioned table")
  private val inputSchema: Schema = spec.schema()
  private val dataSparkType: StructType = GpuTypeToSparkType.toSparkType(dataType)

  private val partitionExprs: Seq[GpuExpression] = spec.fields().asScala.map(getPartitionExpr).toSeq

  // Create the underlying partitioner
  private val partitioner = new GpuIcebergPartitioner(spec.partitionType(), dataType)

  /**
   * Partition the `input` columnar batch using iceberg's partition spec.
   * <br/>
   * This method first computes the partition keys using the partition expressions,
   * then delegates to the underlying GpuIcebergPartitioner to perform the actual partitioning.
   * <br/>
   * This method takes the ownership of the input columnar batch, and it should not be used after
   * this call.
   */
  def partition(input: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    if (input.numRows() == 0) {
      return Seq.empty
    }

    withResource(input) { _ =>
      val keyBatch = {
        val keyCols = partitionExprs.safeMap(_.columnarEval(input))
        closeOnExcept(keyCols) { _ =>
          new ColumnarBatch(keyCols.toArray, input.numRows())
        }
      }

      withResource(keyBatch) { _ =>
        partitioner.partition(keyBatch, input)
      }
    }
  }

  private def getPartitionExpr(field: PartitionField)
  : GpuExpression = {
    val transform = field.transform()
    val inputIndex = fieldIndex(inputSchema, field.sourceId())
    val sparkField = dataSparkType.fields(inputIndex)
    val inputRefExpr = GpuBoundReference(inputIndex, sparkField.dataType,
      sparkField.nullable)(newExprId, s"input$inputIndex")

    GpuTransform(transform.toString) match {
      // bucket transform is like "bucket[16]"
      case GpuBucket(bucket) => GpuBucketExpression(GpuLiteral.create(bucket), inputRefExpr)
      case GpuYears => GpuYearsExpression(inputRefExpr)
      case GpuMonths => GpuMonthsExpression(inputRefExpr)
      case GpuDays => GpuDaysExpression(inputRefExpr)
      case GpuHours => GpuHoursExpression(inputRefExpr)
      case GpuTruncate(width) =>
        GpuTruncateExpression(GpuLiteral.create(width), inputRefExpr)
      case GpuIdentity => inputRefExpr
    }
  }
}

case class ColumnarBatchWithPartition(batch: SpillableColumnarBatch, partition: StructLike) extends
  AutoCloseable {
  override def close(): Unit = {
    batch.close()
  }
}

object GpuIcebergPartitioner {

  private[iceberg] def toPartitionKeys(icebergType: Types.StructType,
    sparkType: StructType,
    table: Table): Array[SparkStructLike] = {
    val numCols = table.getNumberOfColumns
    val numRows = toIntExact(table.getRowCount)

    val hostColsArray = closeOnExcept(new Array[ColumnVector](numCols)) { hostCols =>
      for (colIdx <- 0 until numCols) {
        hostCols(colIdx) = new RapidsHostColumnVector(sparkType.fields(colIdx).dataType,
          table.getColumn(colIdx).copyToHost())
      }
      hostCols
    }

    withResource(new ColumnarBatch(hostColsArray, numRows)) { hostBatch =>
        hostBatch.rowIterator()
          .asScala
          .map(internalRow => new GpuInternalRow(internalRow))
          .map(internalRow => {
            val row = new GenericRowWithSchema(internalRow.toSeq(sparkType).toArray, sparkType)
            new SparkStructLike(icebergType).wrap(row)
          }).toArray
    }
  }
}
