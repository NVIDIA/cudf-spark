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
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.jni.JSONUtils
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuJsonTuple(children: Seq[Expression]) extends GpuGenerator
  with ShimExpression {
  override def nullable: Boolean = false // a row is always returned
  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail

  override def elementSchema: StructType = StructType(fieldExpressions.zipWithIndex.map {
    case (_, idx) => StructField(s"c$idx", StringType, nullable = true)
  })

  override def prettyName: String = "json_tuple"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 2) {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName has wrong number of auguments: expected > 1, but found ${children.length}"
      )
    } else if (children.forall(child => child.dataType == StringType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports string type input")
    }
  }

  override def generate(
      inputBatches: Iterator[SpillableColumnarBatch],
      generatorOffset: Int,
      outer: Boolean): Iterator[ColumnarBatch] = {

    val conf = SQLConf.get
    val targetBatchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)

    withRetry(inputBatches, splitSpillableInHalfByRows) { attempt =>
      withResource(attempt.getColumnarBatch()) { inputBatch =>
        val json = inputBatch.column(generatorOffset).asInstanceOf[GpuColumnVector].getBase

        val fieldInstructions = fieldExpressions.map { field =>
          withResourceIfAllowed(field.columnarEvalAny(inputBatch)) {
            case fieldScalar: GpuScalar =>
              val fieldString = fieldScalar.getBase.getJavaString
              val named = new JSONUtils.PathInstructionJni(
                  JSONUtils.PathInstructionType.NAMED, fieldString, -1)
              Array(named)
            case _ => throw new UnsupportedOperationException(s"JSON field must be a scalar value")
          }
        }

        val validPaths: java.util.List[java.util.List[JSONUtils.PathInstructionJni]] =
          java.util.Arrays.asList(fieldInstructions.map { arr =>
            java.util.Arrays.asList(arr: _*)
          }: _*)

        var validPathsIndex = 0
        withResource(new Array[ColumnVector](fieldInstructions.length)) { validPathColumns =>
          // Last argument -1 indicates to use automatically calculated parallelism
          withResource(JSONUtils.getJsonObjectMultiplePaths(
              json, validPaths, 4 * targetBatchSize, -1)) { chunkedResult =>
              chunkedResult.foreach { cr =>
                validPathColumns(validPathsIndex) = cr.incRefCount()
                validPathsIndex += 1
              }
          }

          val generatorCols = validPathColumns.safeMap(_.incRefCount).safeMap {
            col => GpuColumnVector.from(col, StringType)
          }
          val nonGeneratorCols = (0 until generatorOffset).safeMap { i =>
            inputBatch.column(i).asInstanceOf[GpuColumnVector].incRefCount
          }
          new ColumnarBatch((nonGeneratorCols ++ generatorCols).toArray, inputBatch.numRows)
        }
      }
    }
  }

  override def inputSplitIndices(inputBatch: ColumnarBatch,
      generatorOffset: Int,
      outer: Boolean,
      targetSizeBytes: Long,
      maxRows: Int = Int.MaxValue): Array[Int] = {

    val inputRows = inputBatch.numRows
    // if the number of input rows is 1 or less, cannot split
    if (inputRows <= 1) return Array()

    val outputRows = inputRows
    val json = inputBatch.column(generatorOffset).asInstanceOf[GpuColumnVector].getBase

    // we know we are going to output at most this much
    val estimatedOutputSizeBytes = (json.getDeviceMemorySize * fieldExpressions.length).toDouble

    val numSplitsForTargetSize =
      math.min(inputRows, math.ceil(estimatedOutputSizeBytes / targetSizeBytes).toInt)
    val splitIndices =
      GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetSize).distinct

    // how many splits will we need to keep the output rows under max value
    val numSplitsForTargetRow = math.ceil(outputRows / maxRows).toInt

    // If the number of splits needed to keep the row limits for cuDF is higher than
    // the splits we found by size, we need to use the row-based splits.
    // Note, given skewed input, we could be left with batches split at bad places,
    // e.g. all of the non nulls are in a single split. So we may need to re-split
    // that row-based slice using the size approach.
    if (numSplitsForTargetRow > splitIndices.length) {
      GpuBatchUtils.generateSplitIndices(inputRows, numSplitsForTargetRow)
    } else {
      splitIndices
    }
  }
}
