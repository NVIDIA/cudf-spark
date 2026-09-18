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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.unsafe.types.{BinaryView, TimestampNanosVal}

final class CudfUnsafeRow(
   attributes: Array[Attribute],
   remapping: Array[Int]) extends CudfUnsafeRowBase(attributes, remapping) {
  // Like getVariant, BinaryView and timestamp nanos are not part of the fast path yet.
  def getBinaryView(ordinal: Int): BinaryView = {
    throw new UnsupportedOperationException("BinaryView is not supported")
  }

  def getTimestampLTZNanos(ordinal: Int): TimestampNanosVal = {
    throw new UnsupportedOperationException("TimestampLTZNanos is not supported")
  }

  def getTimestampNTZNanos(ordinal: Int): TimestampNanosVal = {
    throw new UnsupportedOperationException("TimestampNTZNanos is not supported")
  }
}

object CudfUnsafeRow extends CudfUnsafeRowTrait
