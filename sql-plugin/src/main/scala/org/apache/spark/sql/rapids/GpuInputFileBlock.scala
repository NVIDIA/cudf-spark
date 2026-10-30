/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuLeafExpression}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Returns the name of the file being read, or empty string if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 *
 * @param outputDictionary when true, the result is a DICTIONARY32 column with one STRING key
 *                         (the path) and all-zero INT32 indices, instead of a STRING column
 *                         that repeats the path on every row and can exceed the column size
 *                         limit (https://github.com/NVIDIA/cudf-spark/issues/13918). Only
 *                         GpuTransitionOverrides sets it, when every consumer of the output
 *                         column can handle a dictionary.
 */
case class GpuInputFileName(outputDictionary: Boolean = false) extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def prettyName: String = "input_file_name"

  // The encoding of the result is not part of what the expression means, so keep it out of plan
  // strings.
  override protected def stringArgs: Iterator[Any] = Iterator.empty

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val path = InputFileBlockHolder.getInputFilePath.toString
    if (outputDictionary) {
      // Every row of a batch comes from the same file, so the path is the only key. The indices
      // are INT32 because libcudf joins reject dictionary keys whose index widths differ.
      val dictionary = withResource(ColumnVector.fromStrings(path)) { keys =>
        withResource(Scalar.fromInt(0)) { zero =>
          withResource(ColumnVector.fromScalar(zero, batch.numRows())) { indices =>
            ColumnVector.makeDictionary(keys, indices)
          }
        }
      }
      closeOnExcept(dictionary) { _ =>
        GpuColumnVector.from(dictionary, dataType)
      }
    } else {
      withResource(Scalar.fromString(path)) { scalar =>
        GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()), dataType)
      }
    }
  }
}

object InputFileUtils {
  def setInputFileBlock(filePath: String, start: Long, length: Long): Unit = {
    InputFileBlockHolder.set(filePath, start, length)
  }

  def getCurInputFilePath(): String = InputFileBlockHolder.getInputFilePath.toString
  def getCurInputFileStartOffset: Long = InputFileBlockHolder.getStartOffset
  def getCurInputFileLength: Long = InputFileBlockHolder.getLength
}

/**
 * Returns the start offset of the block being read, or -1 if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 */
case class GpuInputFileBlockStart() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_block_start"

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(Scalar.fromLong(InputFileBlockHolder.getStartOffset)) { scalar =>
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()), dataType)
    }
  }
}

/**
 * Returns the length of the block being read, or -1 if not available.
 * This is extra difficult because we cannot coalesce batches in between when this
 * is used and the input file or else we could run into problems with returning the wrong thing.
 */
case class GpuInputFileBlockLength() extends GpuLeafExpression {
  /**
   * We need to recompute this if something fails.
   */
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_block_length"

  /**
   * We need our input to be a single block per file.
   */
  override def disableCoalesceUntilInput(): Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(Scalar.fromLong(InputFileBlockHolder.getLength)) { scalar =>
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()), dataType)
    }
  }
}
