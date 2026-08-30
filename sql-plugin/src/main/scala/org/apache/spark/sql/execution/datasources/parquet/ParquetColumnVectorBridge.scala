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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

/**
 * Root-loaded bridge for accessing Spark's package-private ParquetColumnVector.
 */
abstract class ParquetColumnVectorBridge {
  def newParquetColumnVector(
      column: ParquetColumn,
      vector: WritableColumnVector,
      capacity: Int,
      memoryMode: MemoryMode,
      missingColumns: java.util.Set[ParquetColumn],
      isTopLevel: Boolean,
      defaultValue: Any): AnyRef

  private def asParquetColumnVector(vector: AnyRef): ParquetColumnVector =
    vector.asInstanceOf[ParquetColumnVector]

  def getColumn(vector: AnyRef): ParquetColumn = asParquetColumnVector(vector).getColumn

  def getChildren(vector: AnyRef): java.util.List[AnyRef] =
    asParquetColumnVector(vector).getChildren.asInstanceOf[java.util.List[AnyRef]]

  def getLeaves(vector: AnyRef): java.util.List[AnyRef] =
    asParquetColumnVector(vector).getLeaves.asInstanceOf[java.util.List[AnyRef]]

  def reset(vector: AnyRef): Unit = asParquetColumnVector(vector).reset()

  def getColumnReader(vector: AnyRef): VectorizedColumnReader =
    asParquetColumnVector(vector).getColumnReader

  def setColumnReader(vector: AnyRef, reader: VectorizedColumnReader): Unit =
    asParquetColumnVector(vector).setColumnReader(reader)

  def getValueVector(vector: AnyRef): WritableColumnVector =
    asParquetColumnVector(vector).getValueVector

  def getRepetitionLevelVector(vector: AnyRef): WritableColumnVector =
    asParquetColumnVector(vector).getRepetitionLevelVector

  def getDefinitionLevelVector(vector: AnyRef): WritableColumnVector =
    asParquetColumnVector(vector).getDefinitionLevelVector

  def assemble(vector: AnyRef): Unit = asParquetColumnVector(vector).assemble()
}
