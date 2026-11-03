/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.types.DataType

/**
 * An unsigned, 32-bit integer type that maps to DType.UINT32 in cudf.
 * @note This type should NOT be used in Catalyst plan nodes that could be exposed to
 *       CPU expressions.
 */
class GpuUnsignedIntegerType private() extends DataType {
  // The companion object and this class are separated so the companion object also subclasses
  // this type. Otherwise the companion object would be of type "UnsignedIntegerType$" in
  // byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 4

  override def simpleString: String = "uint"

  override def asNullable: DataType = this
}

case object GpuUnsignedIntegerType extends GpuUnsignedIntegerType


/**
 * An unsigned, 64-bit integer type that maps to DType.UINT64 in cudf.
 * @note This type should NOT be used in Catalyst plan nodes that could be exposed to
 *       CPU expressions.
 */
class GpuUnsignedLongType private() extends DataType {
  // The companion object and this class are separated so the companion object also subclasses
  // this type. Otherwise the companion object would be of type "UnsignedIntegerType$" in
  // byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 8

  override def simpleString: String = "ulong"

  override def asNullable: DataType = this
}

case object GpuUnsignedLongType extends GpuUnsignedLongType
