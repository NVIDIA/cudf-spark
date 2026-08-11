/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * copied from spark-catalyst test project.
 * UserDefinedType is in private[spark] package for Spark31x.
 */
class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

  // type is: array(double)
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(features: MyDenseVector): ArrayData = {
    new GenericArrayData(features.data.map(_.asInstanceOf[Any]))
  }

  override def deserialize(datum: Any): MyDenseVector = {
    datum match {
      case data: ArrayData =>
        new MyDenseVector(data.toDoubleArray())
    }
  }

  override def userClass: Class[MyDenseVector] = classOf[MyDenseVector]

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[MyDenseVectorUDT]
}

/**
 * copied from spark-catalyst test project
 */
@SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
class MyDenseVector(val data: Array[Double]) extends Serializable {
  override def hashCode(): Int = java.util.Arrays.hashCode(data)

  override def equals(other: Any): Boolean = other match {
    case v: MyDenseVector => java.util.Arrays.equals(this.data, v.data)
    case _ => false
  }

  override def toString: String = data.mkString("(", ", ", ")")
}
