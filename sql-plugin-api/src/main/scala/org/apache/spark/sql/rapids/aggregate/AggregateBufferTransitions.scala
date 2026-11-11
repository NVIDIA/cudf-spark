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

package org.apache.spark.sql.rapids.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.{BloomFilter => SparkBloomFilter}

trait CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition
}

trait GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition
}

trait CpuToGpuBufferTransition extends ShimUnaryExpression with CodegenFallback

trait GpuToCpuBufferTransition extends ShimUnaryExpression with CodegenFallback {
  override def dataType: DataType = BinaryType
}

case class CpuToGpuBloomFilterBufferConverter() extends CpuToGpuAggregateBufferConverter {
  override def createExpression(child: Expression): CpuToGpuBufferTransition =
    CpuToGpuBloomFilterBufferTransition(child)
}

case class CpuToGpuBloomFilterBufferTransition(override val child: Expression)
    extends CpuToGpuBufferTransition {
  override def dataType: DataType = BinaryType

  override protected def nullSafeEval(input: Any): Array[Byte] = input.asInstanceOf[Array[Byte]]
}

case class GpuToCpuBloomFilterBufferConverter(
    estimatedNumItems: Long,
    numBits: Long) extends GpuToCpuAggregateBufferConverter {
  override def createExpression(child: Expression): GpuToCpuBufferTransition =
    GpuToCpuBloomFilterBufferTransition(child, estimatedNumItems, numBits)
}

case class GpuToCpuBloomFilterBufferTransition(
    override val child: Expression,
    estimatedNumItems: Long,
    numBits: Long) extends GpuToCpuBufferTransition {

  private lazy val emptyBloomFilterBytes: Array[Byte] = {
    val bloomFilter = SparkBloomFilter.create(estimatedNumItems, numBits)
    val out = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(out)
    try {
      bloomFilter.writeTo(dataOut)
      dataOut.flush()
      out.toByteArray
    } finally {
      dataOut.close()
    }
  }

  override def eval(input: InternalRow): Any = {
    val buffer = child.eval(input)
    if (buffer == null) {
      emptyBloomFilterBytes.clone()
    } else {
      buffer.asInstanceOf[Array[Byte]]
    }
  }

  // ShimUnaryExpression still requires nullSafeEval even though eval handles the null-to-empty
  // bloom filter rewrite directly for this bridge expression.
  override protected def nullSafeEval(input: Any): Array[Byte] = input.asInstanceOf[Array[Byte]]
}

class CpuToGpuCollectBufferConverter(
    elementType: DataType,
    containsNull: Boolean = false) extends CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition = {
    CpuToGpuCollectBufferTransition(child, elementType, containsNull)
  }
}

case class CpuToGpuCollectBufferTransition(
    override val child: Expression,
    private val elementType: DataType,
    private val containsNull: Boolean) extends CpuToGpuBufferTransition {

  private lazy val row = new UnsafeRow(1)

  override def dataType: DataType = ArrayType(elementType, containsNull)

  override protected def nullSafeEval(input: Any): ArrayData = {
    // Converts binary buffer into UnSafeArrayData, according to the deserialize method of Collect.
    // The input binary buffer is the binary view of a UnsafeRow, which only contains single field
    // with ArrayType of elementType. Since array of elements exactly matches the GPU format, we
    // don't need to do any conversion in memory level. Instead, we simply bind the binary data to
    // a reused UnsafeRow. Then, fetch the only field as ArrayData.
    val bytes = input.asInstanceOf[Array[Byte]]
    row.pointTo(bytes, bytes.length)
    row.getArray(0).copy()
  }
}

class GpuToCpuCollectBufferConverter extends GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition = {
    GpuToCpuCollectBufferTransition(child)
  }
}

case class GpuToCpuCollectBufferTransition(
    override val child: Expression) extends GpuToCpuBufferTransition {

  private lazy val projection = UnsafeProjection.create(Array(child.dataType))

  override protected def nullSafeEval(input: Any): Array[Byte] = {
    // Converts UnSafeArrayData into binary buffer, according to the serialize method of Collect.
    // The binary buffer is the binary view of a UnsafeRow, which only contains single field
    // with ArrayType of elementType. As Collect.serialize, we create an UnsafeProjection to
    // transform ArrayData to binary view of the single field UnsafeRow. Unlike Collect.serialize,
    // we don't have to build ArrayData from on-heap array, since the input is already formatted
    // in ArrayData(UnsafeArrayData).
    val arrayData = input.asInstanceOf[ArrayData]
    projection.apply(InternalRow.apply(arrayData)).getBytes
  }
}

/**
 * Convert the incoming byte stream received from Spark CPU into internal histogram buffer format.
 */
case class CpuToGpuPercentileBufferConverter(elementType: DataType)
  extends CpuToGpuAggregateBufferConverter {
  override def createExpression(child: Expression): CpuToGpuBufferTransition =
    CpuToGpuPercentileBufferTransition(child, elementType)
}

case class CpuToGpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends CpuToGpuBufferTransition {
  override def dataType: DataType =
    ArrayType(StructType(Seq(StructField("value", elementType),
      StructField("frequency", LongType))),
      containsNull = false)

  // Deserialization from the input byte stream into the internal histogram buffer format.
  override protected def nullSafeEval(input: Any): ArrayData = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)

    // Store a column of STRUCT<element, count>
    val histogram = ArrayBuffer[InternalRow]()
    val row = new UnsafeRow(2)

    try {
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        row.pointTo(bs, sizeOfNextRow)
        val element = row.get(0, elementType)
        val count = row.get(1, LongType).asInstanceOf[Long]
        histogram.append(InternalRow.apply(element, count))
        sizeOfNextRow = ins.readInt()
      }
      ArrayData.toArrayData(histogram)
    } finally {
      ins.close()
      bis.close()
    }
  }
}

/**
 * Convert the internal histogram buffer into a byte stream that can be deserialized by Spark CPU.
 */
case class GpuToCpuPercentileBufferConverter(elementType: DataType)
  extends GpuToCpuAggregateBufferConverter {
  override def createExpression(child: Expression): GpuToCpuBufferTransition =
    GpuToCpuPercentileBufferTransition(child, elementType)
}

case class GpuToCpuPercentileBufferTransition(override val child: Expression, elementType: DataType)
  extends GpuToCpuBufferTransition {
  // Serialization the internal histogram buffer into a byte array.
  override protected def nullSafeEval(input: Any): Array[Byte] = {
    val buffer = new Array[Byte](4 << 10) // 4K
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)

    val histogram = input.asInstanceOf[UnsafeArrayData]
    val projection = UnsafeProjection.create(Array[DataType](elementType, LongType))

    try {
      (0 until histogram.numElements()).foreach { i =>
        val row = histogram.getStruct(i, 2)
        val element = row.get(0, elementType)
        // The internal histogram buffer may contain null elements.
        // We need to skip them as the Spark CPU does not process nulls after
        // the updateAggregates step.
        if (element != null) {
          val unsafeRow = projection.apply(row)
          out.writeInt(unsafeRow.getSizeInBytes)
          unsafeRow.writeToStream(out, buffer)
        }
      }
      // Need to write a negative integer to indicate the end of the stream.
      out.writeInt(-1)
      out.flush()
      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }
}
