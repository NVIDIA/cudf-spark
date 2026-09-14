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

package com.nvidia.spark.rapids.delta

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{BinaryType, SQLUserDefinedType, UserDefinedType}

/**
 * Collects touched row indexes directly in Delta's bitmap representation. This lets the merge
 * serialize deletion vectors without an element-by-element conversion from `Roaring64Bitmap`.
 */
@SQLUserDefinedType(udt = classOf[Delta24xRoaringBitmapUDT])
case class Delta24xRoaringBitmapWrapper(inner: RoaringBitmapArray) {
  def serializeToBytes(): Array[Byte] =
    inner.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
}

object Delta24xRoaringBitmapWrapper {
  def deserializeFromBytes(bytes: Array[Byte]): Delta24xRoaringBitmapWrapper =
    Delta24xRoaringBitmapWrapper(RoaringBitmapArray.readFrom(bytes))
}

class Delta24xRoaringBitmapUDT extends UserDefinedType[Delta24xRoaringBitmapWrapper] {
  override def sqlType: BinaryType.type = BinaryType
  override def serialize(obj: Delta24xRoaringBitmapWrapper): Any = obj.serializeToBytes()
  override def deserialize(datum: Any): Delta24xRoaringBitmapWrapper = datum match {
    case bytes: Array[Byte] => Delta24xRoaringBitmapWrapper.deserializeFromBytes(bytes)
    case other => throw new IllegalArgumentException(s"Unexpected bitmap value: ${other.getClass}")
  }
  override def userClass: Class[Delta24xRoaringBitmapWrapper] =
    classOf[Delta24xRoaringBitmapWrapper]
  override def typeName: String = "Delta24xRoaringBitmap"
}

object Delta24xRoaringBitmapUDAF extends
    Aggregator[Long, Delta24xRoaringBitmapWrapper, Delta24xRoaringBitmapWrapper] {
  override def zero: Delta24xRoaringBitmapWrapper =
    Delta24xRoaringBitmapWrapper(new RoaringBitmapArray())

  override def reduce(
      bitmap: Delta24xRoaringBitmapWrapper,
      rowIndex: Long): Delta24xRoaringBitmapWrapper = {
    bitmap.inner.add(rowIndex)
    bitmap
  }

  override def merge(
      left: Delta24xRoaringBitmapWrapper,
      right: Delta24xRoaringBitmapWrapper): Delta24xRoaringBitmapWrapper = {
    val merged = left.inner.copy()
    merged.merge(right.inner)
    Delta24xRoaringBitmapWrapper(merged)
  }

  override def finish(reduction: Delta24xRoaringBitmapWrapper): Delta24xRoaringBitmapWrapper =
    reduction
  override def bufferEncoder: Encoder[Delta24xRoaringBitmapWrapper] = ExpressionEncoder()
  override def outputEncoder: Encoder[Delta24xRoaringBitmapWrapper] = ExpressionEncoder()
}
