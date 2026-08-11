/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object DataTypeUtils {
  def isNestedType(dataType: DataType): Boolean = dataType match {
    case _: ArrayType | _: MapType | _: StructType => true
    case _ => false
  }

  def hasOffset(dataType: DataType): Boolean = dataType match {
    case _: ArrayType | StringType | _: BinaryType => true
    case _ => false
  }

  def hasNestedTypes(schema: StructType): Boolean =
    schema.exists(f => isNestedType(f.dataType))

  /**
   * If `t` is date/timestamp type or its children have a date/timestamp type.
   *
   * @param t input date type.
   * @return if contains date type.
   */
  def hasDateOrTimestampType(t: DataType): Boolean = {
    TrampolineUtil.dataTypeExistsRecursively(t, e =>
      e.isInstanceOf[DateType] || e.isInstanceOf[TimestampType])
  }
}
