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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.rapids.TestingUDT._
import org.apache.spark.sql.types._

/**
 * copied from ParquetQuerySuite from Spark
 * UserDefinedType is in private[spark] package for Spark31x.
 */

class TestNestedStructUDT extends UserDefinedType[TestNestedStruct] {
  override def sqlType: DataType =
    new StructType()
      .add("a", IntegerType, nullable = true)
      .add("b", LongType, nullable = false)
      .add("c", DoubleType, nullable = false)

  override def serialize(n: TestNestedStruct): Any = {
    val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
    row.setInt(0, n.a)
    row.setLong(1, n.b)
    row.setDouble(2, n.c)
    row
  }

  override def userClass: Class[TestNestedStruct] = classOf[TestNestedStruct]

  override def deserialize(datum: Any): TestNestedStruct = {
    datum match {
      case row: InternalRow =>
        TestNestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
    }
  }
}

@SQLUserDefinedType(udt = classOf[TestArrayUDT])
case class TestArray(value: Seq[Long])

class TestArrayUDT extends UserDefinedType[TestArray] {
  override def sqlType: DataType = ArrayType(LongType)

  override def serialize(obj: TestArray): Any = ArrayData.toArrayData(obj.value.toArray)

  override def userClass: Class[TestArray] = classOf[TestArray]

  override def deserialize(datum: Any): TestArray = datum match {
    case value: ArrayData => TestArray(value.toLongArray.toSeq)
  }
}

@SQLUserDefinedType(udt = classOf[TestPrimitiveUDT])
case class TestPrimitive(value: Int)

class TestPrimitiveUDT extends UserDefinedType[TestPrimitive] {
  override def sqlType: DataType = IntegerType

  override def serialize(obj: TestPrimitive): Any = obj.value

  override def userClass: Class[TestPrimitive] = classOf[TestPrimitive]

  override def deserialize(datum: Any): TestPrimitive = datum match {
    case value: Int => TestPrimitive(value)
  }
}

object TestingUDT {
  case class SingleElement(element: Long)

  @SQLUserDefinedType(udt = classOf[TestNestedStructUDT])
  case class TestNestedStruct(a: Integer, b: Long, c: Double)

  class TestNestedStructUDT extends UserDefinedType[TestNestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(n: TestNestedStruct): Any = {
      val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      row.setInt(0, n.a)
      row.setLong(1, n.b)
      row.setDouble(2, n.c)
      row
    }

    override def userClass: Class[TestNestedStruct] = classOf[TestNestedStruct]

    override def deserialize(datum: Any): TestNestedStruct = {
      datum match {
        case row: InternalRow =>
          TestNestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }
}
