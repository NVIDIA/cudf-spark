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

package com.nvidia.spark.rapids.delta

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import ai.rapids.cudf.{ColumnVector, Scalar, Table}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.Arm.withResource
import org.roaringbitmap.longlong.Roaring64Bitmap

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}
import org.apache.spark.util.AccumulatorV2

class GpuDeltaRecordTouchedFileNameUDF(accum: AccumulatorV2[String, java.util.Set[String]])
    extends Function[String, Int] with RapidsUDF with Serializable {

  override def apply(fileName: String): Int = {
    accum.add(fileName)
    1
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 1, s"Expected one argument, received ${args.length}")
    val input = args.head
    require(numRows == input.getRowCount, s"Expected $numRows rows, received ${input.getRowCount}")
    withResource(new Table(input)) { t =>
      val hostData = withResource(t.dropDuplicates(Array(0), DuplicateKeepOption.KEEP_ANY, true)) {
        _.getColumn(0).copyToHost()
      }
      withResource(hostData) { _ =>
        (0 until hostData.getRowCount.toInt).foreach { i =>
          val str = if (hostData.isNull(i)) {
            null
          } else {
            hostData.getJavaString(i)
          }
          accum.add(str)
        }
      }
      withResource(Scalar.fromInt(1)) { one =>
        ColumnVector.fromScalar(one, input.getRowCount.toInt)
      }
    }
  }
}

class GpuDeltaRecordTouchedFilesStringBoolUDF(accum: AccumulatorV2[String, java.util.Set[String]])
  extends Function2[String, Boolean, Int] with RapidsUDF with Serializable {

  override def apply(fileName: String, shouldRecord: Boolean = true): Int = {
    if (shouldRecord) {
      accum.add(fileName)
      1
    } else {
      0
    }
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 2, s"Expected two argument, received ${args.length}")
    val fileName = args(0)
    val shouldRecord = args(1)
    require(numRows == fileName.getRowCount,
      s"Expected $numRows rows, received ${fileName.getRowCount}")
    withResource(new Table(args: _*)) { t =>
      withResource(t.filter(shouldRecord)) { filteredTable =>
        val hostData =
          withResource(filteredTable.dropDuplicates(Array(0), DuplicateKeepOption.KEEP_ANY, true)) {
            _.getColumn(0).copyToHost()
          }
        withResource(hostData) { _ =>
          (0 until hostData.getRowCount.toInt).foreach { i =>
            val str = if (hostData.isNull(i)) {
              null
            } else {
              hostData.getJavaString(i)
            }
            accum.add(str)
          }
        }
        withResource(Scalar.fromInt(1)) { one =>
          ColumnVector.fromScalar(one, fileName.getRowCount.toInt)
        }
      }
    }
  }
}

class GpuDeltaMetricUpdateUDF(metric: SQLMetric)
    extends Function0[Boolean] with RapidsUDF with Serializable {

  override def apply(): Boolean = {
    metric += 1
    true
  }

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.isEmpty)
    metric += numRows
    withResource(Scalar.fromBool(true)) { s =>
      ColumnVector.fromScalar(s, numRows)
    }
  }
}

class GpuDeltaNoopUDF extends Function1[Boolean, Boolean] with RapidsUDF with Serializable {
  override def apply(v1: Boolean): Boolean = v1

  override def evaluateColumnar(numRows: Int, args: ColumnVector*): ColumnVector = {
    require(args.length == 1)
    args(0).incRefCount()
  }
}

@SQLUserDefinedType(udt = classOf[RoaringBitmapUDT])
case class RoaringBitmapWrapper(inner: Roaring64Bitmap) {
  def serializeToBytes(): Array[Byte] = {
    withResource(new ByteArrayOutputStream()) { bout =>
      withResource(new DataOutputStream(bout)) { dao =>
        inner.serialize(dao)
      }
      bout.toByteArray
    }
  }
}

object RoaringBitmapWrapper {
  def deserializeFromBytes(bytes: Array[Byte]): RoaringBitmapWrapper = {
    withResource(new ByteArrayInputStream(bytes)) { bin =>
      withResource(new DataInputStream(bin)) { din =>
        val ret = RoaringBitmapWrapper(new Roaring64Bitmap)
        ret.inner.deserialize(din)
        ret
      }
    }
  }
}

class RoaringBitmapUDT extends UserDefinedType[RoaringBitmapWrapper] {

  override def sqlType: DataType = BinaryType

  override def serialize(obj: RoaringBitmapWrapper): Any = {
    obj.serializeToBytes()
  }

  override def deserialize(datum: Any): RoaringBitmapWrapper = {
    datum match {
      case b: Array[Byte] => RoaringBitmapWrapper.deserializeFromBytes(b)
      case t => throw new IllegalArgumentException(s"t: ${t.getClass}")
    }
  }

  override def userClass: Class[RoaringBitmapWrapper] = classOf[RoaringBitmapWrapper]

  override def typeName: String = "RoaringBitmap"
}

object RoaringBitmapUDAF extends Aggregator[Long, RoaringBitmapWrapper, RoaringBitmapWrapper] {
  override def zero: RoaringBitmapWrapper = RoaringBitmapWrapper(new Roaring64Bitmap())

  override def reduce(b: RoaringBitmapWrapper, a: Long): RoaringBitmapWrapper = {
    b.inner.addLong(a)
    b
  }

  override def merge(b1: RoaringBitmapWrapper, b2: RoaringBitmapWrapper): RoaringBitmapWrapper = {
    val ret = b1.inner.clone()
    ret.or(b2.inner)
    RoaringBitmapWrapper(ret)
  }

  override def finish(reduction: RoaringBitmapWrapper): RoaringBitmapWrapper = reduction

  override def bufferEncoder: Encoder[RoaringBitmapWrapper] = ExpressionEncoder()

  override def outputEncoder: Encoder[RoaringBitmapWrapper] = ExpressionEncoder()
}
