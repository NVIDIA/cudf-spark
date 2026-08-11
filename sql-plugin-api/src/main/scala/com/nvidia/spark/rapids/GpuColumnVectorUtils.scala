/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import java.lang.reflect.Method

import ai.rapids.cudf.Table

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

object GpuColumnVectorUtils {
  lazy val extractHostColumnsMethod: Method = ShimLoader.loadGpuColumnVector()
      .getDeclaredMethod("extractHostColumns", classOf[Table], classOf[Array[DataType]])

  /**
   * Extract the columns from a table and convert them to RapidsHostColumnVector.
   * @param table to be extracted
   * @param colType the column types
   * @return an array of ColumnVector
   */
  def extractHostColumns(table: Table, colType: Array[DataType]): Array[ColumnVector] = {
    val columnVectors = extractHostColumnsMethod.invoke(null, table, colType)
    columnVectors.asInstanceOf[Array[ColumnVector]]
  }
}
