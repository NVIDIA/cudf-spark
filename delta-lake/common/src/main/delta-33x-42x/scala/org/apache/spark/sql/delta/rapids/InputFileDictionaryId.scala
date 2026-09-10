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

package org.apache.spark.sql.delta.rapids

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{Arm, ExprChecks, ExprRule, GpuColumnVector, GpuExpression,
  GpuOverrides, GpuUnaryExpression, TypeSig}

import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rapids.GpuInputFileName
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Resolves the current input file to a collision-free ID without expanding the file path into a
 * string value for every row. This is internal to Delta DML plans.
 */
case class InputFileDictionaryId(fileIdByPath: Map[String, Long])
    extends LeafExpression with Nondeterministic with CodegenFallback {
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_dictionary_id"

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  override protected def evalInternal(input: InternalRow): Any = currentFileId

  private def currentFileId: Long = {
    val path = InputFileBlockHolder.getInputFilePath.toString
    fileIdByPath.getOrElse(path,
      throw new IllegalStateException(s"No dictionary ID for input file $path"))
  }
}

case class GpuInputFileDictionaryId(fileIdByPath: Map[String, Long], child: Expression)
    extends GpuUnaryExpression {
  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "input_file_dictionary_id"
  override def disableCoalesceUntilInput(): Boolean = true

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    throw new UnsupportedOperationException("GpuInputFileDictionaryId evaluates per batch")

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    // Some scan paths emit an empty batch after clearing the input-file holder. The ID is
    // unobservable for that batch, so avoid looking it up while preserving strict validation for
    // every batch containing rows.
    val fileId = if (batch.numRows() == 0) {
      0L
    } else {
      val path = InputFileBlockHolder.getInputFilePath.toString
      fileIdByPath.getOrElse(path,
        throw new IllegalStateException(s"No dictionary ID for input file $path"))
    }
    Arm.withResource(Scalar.fromLong(fileId)) { scalar =>
      GpuColumnVector.from(ColumnVector.fromScalar(scalar, batch.numRows()), dataType)
    }
  }
}

object InputFileDictionaryId {
  val exprRule: ExprRule[InputFileDictionaryId] =
    GpuOverrides.expr[InputFileDictionaryId](
      "Resolve a Delta input file to a collision-free dictionary ID",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (expr, conf, parent, rule) => new com.nvidia.spark.rapids.ExprMeta[InputFileDictionaryId](
        expr, conf, parent, rule) {
        override def convertToGpuImpl(): GpuExpression =
          GpuInputFileDictionaryId(expr.fileIdByPath, GpuInputFileName())
      })
}
