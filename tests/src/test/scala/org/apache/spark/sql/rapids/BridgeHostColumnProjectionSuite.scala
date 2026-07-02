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

package org.apache.spark.sql.rapids

import java.util.concurrent.ConcurrentLinkedQueue

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsHostColumnBuilder}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.{GpuScalarSubqueryShims, ShimExpression}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, ExprId, LeafExpression,
  Literal, NamedExpression, NamedLambdaVariable, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private case class IdentityTrackingExpression(seenIds: ConcurrentLinkedQueue[Int])
    extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def eval(input: InternalRow): Any = {
    seenIds.add(System.identityHashCode(this))
    7
  }
}

private object ScalaUDFCloneTracker {
  val seenIds = new ConcurrentLinkedQueue[Int]()
}

private class IdentityTrackingUDF extends (Any => Any) with Serializable {
  override def apply(value: Any): Any = {
    ScalaUDFCloneTracker.seenIds.add(System.identityHashCode(this))
    value.asInstanceOf[Long] + 1L
  }
}

private case class PreparedSubqueryExpression(
    override val exprId: ExprId,
    value: Int,
    override val plan: BaseSubqueryExec = null)
    extends ExecSubqueryExpression with LeafLike[Expression] with CodegenFallback
    with GpuScalarSubqueryShims {
  @volatile protected var updated = false

  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def updateResult(): Unit = updated = true

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    value
  }

  override def withNewPlan(query: BaseSubqueryExec): PreparedSubqueryExpression =
    copy(plan = query)

  override def toString: String = s"prepared-subquery#$exprId"
}

private case class ConstructorSubqueryExpression(subquery: ExecSubqueryExpression)
    extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def eval(input: InternalRow): Any = subquery.eval(input)
}

private case class LambdaVariableReferenceTrackingExpression(
    first: NamedLambdaVariable,
    second: NamedLambdaVariable,
    seenValueIds: ConcurrentLinkedQueue[Int])
    extends ShimExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = Seq(first, second)

  override def eval(input: InternalRow): Any = {
    seenValueIds.add(System.identityHashCode(first.value))
    seenValueIds.add(System.identityHashCode(second.value))
    11
  }
}

/**
 * Test suite for BridgeHostColumnProjection to verify it correctly writes
 * various data types directly to RapidsHostColumnBuilder.
 */
class BridgeHostColumnProjectionSuite extends AnyFunSuite {

  test("projection owns a cloned expression tree") {
    val seenIds = new ConcurrentLinkedQueue[Int]()
    val expr = IdentityTrackingExpression(seenIds)
    val originalId = System.identityHashCode(expr)
    val projection = BridgeHostColumnProjection.create(Seq(expr))

    val resultType = GpuColumnVector.convertFrom(IntegerType, false)
    withResource(new RapidsHostColumnBuilder(resultType, 1)) { builder =>
      projection.apply(Iterator(InternalRow.empty), Array(builder))
      withResource(builder.build()) { result =>
        assert(result.getInt(0) == 7)
      }
    }

    assert(seenIds.size() == 1)
    assert(seenIds.peek() != originalId)
  }

  test("projection refreshes lambda variable references") {
    val seenIds = new ConcurrentLinkedQueue[Int]()
    val variable = NamedLambdaVariable("x", IntegerType, nullable = false,
      exprId = NamedExpression.newExprId)
    val sameExprIdReference = variable.copy()
    val originalValueId = System.identityHashCode(variable.value)
    val expr = LambdaVariableReferenceTrackingExpression(variable, sameExprIdReference, seenIds)
    val projection = BridgeHostColumnProjection.create(Seq(expr))

    val resultType = GpuColumnVector.convertFrom(IntegerType, false)
    withResource(new RapidsHostColumnBuilder(resultType, 1)) { builder =>
      projection.apply(Iterator(InternalRow.empty), Array(builder))
      withResource(builder.build()) { result =>
        assert(result.getInt(0) == 11)
      }
    }

    assert(seenIds.size() == 2)
    val seen = seenIds.iterator()
    val firstValueId = seen.next()
    val secondValueId = seen.next()
    assert(firstValueId == secondValueId)
    assert(firstValueId != originalValueId)
  }

  test("projection preserves prepared subquery state") {
    val subquery = PreparedSubqueryExpression(ExprId(1), 9)
    subquery.updateResult()
    val projection = BridgeHostColumnProjection.create(Seq(subquery))

    val resultType = GpuColumnVector.convertFrom(IntegerType, false)
    withResource(new RapidsHostColumnBuilder(resultType, 1)) { builder =>
      projection.apply(Iterator(InternalRow.empty), Array(builder))
      withResource(builder.build()) { result =>
        assert(result.getInt(0) == 9)
      }
    }
  }

  test("projection preserves prepared subqueries in constructor fields") {
    val subquery = PreparedSubqueryExpression(ExprId(1), 9)
    subquery.updateResult()
    val projection = BridgeHostColumnProjection.create(Seq(ConstructorSubqueryExpression(subquery)))

    val resultType = GpuColumnVector.convertFrom(IntegerType, false)
    withResource(new RapidsHostColumnBuilder(resultType, 1)) { builder =>
      projection.apply(Iterator(InternalRow.empty), Array(builder))
      withResource(builder.build()) { result =>
        assert(result.getInt(0) == 9)
      }
    }
  }

  test("projection owns a cloned ScalaUDF function") {
    ScalaUDFCloneTracker.seenIds.clear()
    val function = new IdentityTrackingUDF()
    val originalId = System.identityHashCode(function)
    val expr = ScalaUDF(function, LongType, Seq(Literal(1L)))
    val projection = BridgeHostColumnProjection.create(Seq(expr))

    val resultType = GpuColumnVector.convertFrom(LongType, true)
    withResource(new RapidsHostColumnBuilder(resultType, 1)) { builder =>
      projection.apply(Iterator(InternalRow.empty), Array(builder))
      withResource(builder.build()) { result =>
        assert(result.getLong(0) == 2L)
      }
    }

    assert(ScalaUDFCloneTracker.seenIds.size() == 1)
    assert(ScalaUDFCloneTracker.seenIds.peek() != originalId)
  }

  /**
   * Helper function to create test data and verify projection results.
   */
  private def testProjection[T](
      dataType: DataType,
      testData: Seq[(T, Boolean)], // (value, isNull)
      buildExpression: => Expression,
      verifyFunction: (HostColumnVector, Int, T, Boolean) => Unit): Unit = {
    
    val expr = buildExpression
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    // Convert test data to InternalRow format
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    // Create builder
    val resultType = GpuColumnVector.convertFrom(dataType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      // Apply projection
      projection.apply(rows.iterator, Array(builder))
      
      // Build and verify results
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length, 
          s"Expected ${rows.length} rows, got ${result.getRowCount}")
        
        // Verify each row
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          verifyFunction(result, idx, expectedValue, expectedIsNull)
        }
      }
    }
  }

  test("boolean type - nullable") {
    val testData = Seq(
      (true, false),
      (false, false),
      (true, true),  // null value
      (false, true), // null value
      (true, false)
    )
    
    testProjection[Boolean](
      BooleanType,
      testData,
      BoundReference(0, BooleanType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          assert(result.getBoolean(idx) == expectedValue,
            s"Expected $expectedValue at index $idx, got ${result.getBoolean(idx)}")
        }
      }
    )
  }

  test("boolean type - non-nullable") {
    val testData = Seq(
      (true, false),
      (false, false),
      (true, false),
      (false, false)
    )
    
    testProjection[Boolean](
      BooleanType,
      testData,
      BoundReference(0, BooleanType, false),
      (result, idx, expectedValue, _) => {
        assert(!result.isNull(idx), s"Expected non-null at index $idx")
        assert(result.getBoolean(idx) == expectedValue,
          s"Expected $expectedValue at index $idx, got ${result.getBoolean(idx)}")
      }
    )
  }

  test("byte type") {
    val testData = Seq(
      (1.toByte, false),
      (Byte.MinValue, false),
      (Byte.MaxValue, false),
      (0.toByte, true), // null
      (42.toByte, false)
    )
    
    testProjection[Byte](
      ByteType,
      testData,
      BoundReference(0, ByteType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getByte(idx) == expectedValue)
        }
      }
    )
  }

  test("short type") {
    val testData = Seq(
      (1.toShort, false),
      (Short.MinValue, false),
      (Short.MaxValue, false),
      (0.toShort, true), // null
      (1234.toShort, false)
    )
    
    testProjection[Short](
      ShortType,
      testData,
      BoundReference(0, ShortType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getShort(idx) == expectedValue)
        }
      }
    )
  }

  test("int type") {
    val testData = Seq(
      (1, false),
      (Int.MinValue, false),
      (Int.MaxValue, false),
      (0, true), // null
      (123456, false)
    )
    
    testProjection[Int](
      IntegerType,
      testData,
      BoundReference(0, IntegerType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getInt(idx) == expectedValue)
        }
      }
    )
  }

  test("long type") {
    val testData = Seq(
      (1L, false),
      (Long.MinValue, false),
      (Long.MaxValue, false),
      (0L, true), // null
      (123456789L, false)
    )
    
    testProjection[Long](
      LongType,
      testData,
      BoundReference(0, LongType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getLong(idx) == expectedValue)
        }
      }
    )
  }

  test("float type") {
    val testData = Seq(
      (1.5f, false),
      (-1.5f, false),
      (Float.MinValue, false),
      (Float.MaxValue, false),
      (-Float.MaxValue, false), // large negative value
      (0.0f, true), // null
      (3.14159f, false),
      (-3.14159f, false),
      (Float.NaN, false),
      (java.lang.Float.intBitsToFloat(0x7fc00000), false), // NaN (pos quiet, upper)
      (java.lang.Float.intBitsToFloat(0xffc00000), false), // NaN (neg quiet, upper)
      (java.lang.Float.intBitsToFloat(0x7f800001), false), // NaN (pos signal, lower)
      (java.lang.Float.intBitsToFloat(0xff800001), false), // NaN (neg signal, lower)
      (Float.PositiveInfinity, false),
      (Float.NegativeInfinity, false)
    )
    
    testProjection[Float](
      FloatType,
      testData,
      BoundReference(0, FloatType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          val actual = result.getFloat(idx)
          if (expectedValue.isNaN) {
            assert(actual.isNaN, s"Expected NaN at index $idx, got $actual")
          } else {
            assert(actual == expectedValue,
              s"Expected $expectedValue at index $idx, got $actual")
          }
        }
      }
    )
  }

  test("double type") {
    val testData = Seq(
      (1.5, false),
      (-1.5, false),
      (Double.MinValue, false),
      (Double.MaxValue, false),
      (-Double.MaxValue, false), // large negative value
      (0.0, true), // null
      (3.141592653589793, false),
      (-3.141592653589793, false),
      (Double.NaN, false),
      (java.lang.Double.longBitsToDouble(0x7ff8000000000000L), false), // NaN (pos quiet, upper)
      (java.lang.Double.longBitsToDouble(0xfff8000000000000L), false), // NaN (neg quiet, upper)
      (java.lang.Double.longBitsToDouble(0x7ff0000000000001L), false), // NaN (pos signal, lower)
      (java.lang.Double.longBitsToDouble(0xfff0000000000001L), false), // NaN (neg signal, lower)
      (Double.PositiveInfinity, false),
      (Double.NegativeInfinity, false)
    )
    
    testProjection[Double](
      DoubleType,
      testData,
      BoundReference(0, DoubleType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          val actual = result.getDouble(idx)
          if (expectedValue.isNaN) {
            assert(actual.isNaN, s"Expected NaN at index $idx, got $actual")
          } else {
            assert(actual == expectedValue,
              s"Expected $expectedValue at index $idx, got $actual")
          }
        }
      }
    )
  }

  test("string type") {
    val testData = Seq(
      (UTF8String.fromString("hello"), false),
      (UTF8String.fromString(""), false),
      (UTF8String.fromString("world with spaces"), false),
      (UTF8String.fromString("special chars: @#$%"), false),
      (null.asInstanceOf[UTF8String], true), // null
      (UTF8String.fromString("after null"), false)
    )
    
    testProjection[UTF8String](
      StringType,
      testData,
      BoundReference(0, StringType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          val actual = result.getJavaString(idx)
          assert(actual == expectedValue.toString,
            s"Expected '$expectedValue' at index $idx, got '$actual'")
        }
      }
    )
  }

  test("binary type") {
    val testData = Seq(
      (Array[Byte](1, 2, 3), false),
      (Array[Byte](), false),
      (Array[Byte](0, 127, -128, 42), false),
      (null.asInstanceOf[Array[Byte]], true), // null
      (Array[Byte](10, 20, 30), false)
    )
    
    testProjection[Array[Byte]](
      BinaryType,
      testData,
      BoundReference(0, BinaryType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          // Binary is stored as list type in cudf
          val actualList = result.getList(idx)
          val actual = new Array[Byte](actualList.size())
          (0 until actualList.size()).foreach { i =>
            actual(i) = actualList.get(i).asInstanceOf[Byte]
          }
          assert(java.util.Arrays.equals(actual, expectedValue),
            s"Expected ${expectedValue.mkString("[", ",", "]")} at index $idx, " +
            s"got ${actual.mkString("[", ",", "]")}")
        }
      }
    )
  }

  test("date type") {
    val testData = Seq(
      (18628, false), // 2021-01-01
      (0, false),     // 1970-01-01
      (-365, false),  // 1969-01-01
      (0, true),      // null
      (19000, false)
    )
    
    testProjection[Int](
      DateType,
      testData,
      BoundReference(0, DateType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getInt(idx) == expectedValue)
        }
      }
    )
  }

  test("timestamp type") {
    val testData = Seq(
      (1609459200000000L, false), // 2021-01-01 00:00:00.000000 UTC
      (0L, false),                 // 1970-01-01 00:00:00.000000 UTC
      (-1000000L, false),          // Before epoch
      (0L, true),                  // null
      (1672531200000000L, false),  // 2023-01-01 00:00:00.000000 UTC
      // Test with hours/minutes/seconds precision
      (3600000000L, false),        // 1 hour: 1970-01-01 01:00:00 UTC
      (60000000L, false),          // 1 minute: 1970-01-01 00:01:00 UTC
      (1000000L, false),           // 1 second: 1970-01-01 00:00:01 UTC
      // Test with milliseconds/microseconds
      (1000L, false),              // 1 ms: 1970-01-01 00:00:00.001 UTC
      (1L, false),                 // 1 us: 1970-01-01 00:00:00.000001 UTC
      (123456789L, false),         // complex: 1970-01-01 00:02:03.456789 UTC
      // Test with full precision
      (1609459261123456L, false)   // full: 2021-01-01 00:01:01.123456 UTC
    )
    
    testProjection[Long](
      TimestampType,
      testData,
      BoundReference(0, TimestampType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getLong(idx) == expectedValue)
        }
      }
    )
  }

  test("day-time interval type") {
    val intervalType = DayTimeIntervalType()
    val testData = Seq(
      (0L, false),              // 0 duration
      (86400000000L, false),    // 1 day in microseconds
      (3600000000L, false),     // 1 hour in microseconds
      (60000000L, false),       // 1 minute in microseconds
      (1000000L, false),        // 1 second in microseconds
      (-86400000000L, false),   // negative 1 day
      (0L, true),               // null
      (90061000000L, false)     // 1 day, 1 hour, 1 minute, 1 second
    )
    
    testProjection[Long](
      intervalType,
      testData,
      BoundReference(0, intervalType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getLong(idx) == expectedValue)
        }
      }
    )
  }

  test("year-month interval type") {
    val intervalType = YearMonthIntervalType()
    val testData = Seq(
      (0, false),      // 0 duration
      (12, false),     // 1 year (12 months)
      (1, false),      // 1 month
      (-12, false),    // negative 1 year
      (0, true),       // null
      (25, false),     // 2 years, 1 month
      (120, false)     // 10 years
    )
    
    testProjection[Int](
      intervalType,
      testData,
      BoundReference(0, intervalType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getInt(idx) == expectedValue)
        }
      }
    )
  }

  test("multiple expressions") {
    // Test projecting multiple expressions at once
    val numRows = 5
    val intData = Seq(1, 2, 3, 4, 5)
    val stringData = Seq("a", "b", "c", "d", "e")
    
    val rows = intData.zip(stringData).map { case (i, s) =>
      InternalRow(i, UTF8String.fromString(s))
    }
    
    val exprs = Seq(
      BoundReference(0, IntegerType, false),
      BoundReference(1, StringType, false)
    )
    
    val projection = BridgeHostColumnProjection.create(exprs)
    
    val intType = GpuColumnVector.convertFrom(IntegerType, false)
    val stringType = GpuColumnVector.convertFrom(StringType, false)
    
    withResource(new RapidsHostColumnBuilder(intType, numRows)) { intBuilder =>
      withResource(new RapidsHostColumnBuilder(stringType, numRows)) { stringBuilder =>
        // Apply projection to both builders
        projection.apply(rows.iterator, Array(intBuilder, stringBuilder))
        
        // Verify int column
        withResource(intBuilder.build()) { intResult =>
          assert(intResult.getRowCount == numRows)
          intData.zipWithIndex.foreach { case (expected, idx) =>
            assert(intResult.getInt(idx) == expected)
          }
        }
        
        // Verify string column
        withResource(stringBuilder.build()) { stringResult =>
          assert(stringResult.getRowCount == numRows)
          stringData.zipWithIndex.foreach { case (expected, idx) =>
            assert(stringResult.getJavaString(idx) == expected)
          }
        }
      }
    }
  }

  test("large batch") {
    // Test with a larger number of rows to ensure code generation handles it well
    val numRows = 10000
    val testData = (0 until numRows).map { i =>
      (i, i % 10 == 0) // every 10th value is null
    }
    
    testProjection[Int](
      IntegerType,
      testData,
      BoundReference(0, IntegerType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getInt(idx) == expectedValue)
        }
      }
    )
  }

  test("mixed nullability") {
    // Test with a mix of null and non-null values throughout
    val testData = Seq(
      (100, false),
      (200, true),
      (300, false),
      (400, true),
      (500, false),
      (600, true),
      (700, false)
    )
    
    testProjection[Int](
      IntegerType,
      testData,
      BoundReference(0, IntegerType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        if (expectedIsNull) {
          assert(result.isNull(idx))
        } else {
          assert(result.getInt(idx) == expectedValue)
        }
      }
    )
  }

  test("literal expressions") {
    // Test projecting literal values
    val numRows = 5
    val rows = (0 until numRows).map(_ => InternalRow())
    
    val exprs = Seq(
      Literal(42, IntegerType),
      Literal(UTF8String.fromString("constant"), StringType)
    )
    
    val projection = BridgeHostColumnProjection.create(exprs)
    
    val intType = GpuColumnVector.convertFrom(IntegerType, false)
    val stringType = GpuColumnVector.convertFrom(StringType, false)
    
    withResource(new RapidsHostColumnBuilder(intType, numRows)) { intBuilder =>
      withResource(new RapidsHostColumnBuilder(stringType, numRows)) { stringBuilder =>
        projection.apply(rows.iterator, Array(intBuilder, stringBuilder))
        
        // All values should be the literal values
        withResource(intBuilder.build()) { intResult =>
          (0 until numRows).foreach { idx =>
            assert(intResult.getInt(idx) == 42)
          }
        }
        
        withResource(stringBuilder.build()) { stringResult =>
          (0 until numRows).foreach { idx =>
            assert(stringResult.getJavaString(idx) == "constant")
          }
        }
      }
    }
  }

  test("empty batch") {
    // Test with zero rows
    val rows = Seq.empty[InternalRow]
    val expr = BoundReference(0, IntegerType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val intType = GpuColumnVector.convertFrom(IntegerType, true)
    withResource(new RapidsHostColumnBuilder(intType, 0)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == 0)
      }
    }
  }

  test("null type") {
    // Test projecting null type - always returns null
    val numRows = 5
    val rows = (0 until numRows).map(_ => InternalRow(null))
    
    val expr = BoundReference(0, NullType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val nullType = GpuColumnVector.convertFrom(NullType, true)
    withResource(new RapidsHostColumnBuilder(nullType, numRows)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == numRows)
        (0 until numRows).foreach { idx =>
          assert(result.isNull(idx), s"Expected null at index $idx")
        }
      }
    }
  }

  test("array type - integers") {
    val testData = Seq(
      (Seq(1, 2, 3), false),
      (Seq(), false),
      (Seq(10, 20, 30, 40), false),
      (null.asInstanceOf[Seq[Int]], true), // null
      (Seq(100), false)
    )
    
    val arrayType = ArrayType(IntegerType, containsNull = false)
    
    // Convert Scala Seq to Spark ArrayData
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(new org.apache.spark.sql.catalyst.util.GenericArrayData(value.toArray))
      }
    }
    
    val expr = BoundReference(0, arrayType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(arrayType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            val actualList = result.getList(idx)
            assert(actualList.size() == expectedValue.length,
              s"Expected array size ${expectedValue.length}, " +
                s"got ${actualList.size()} at index $idx")
            expectedValue.zipWithIndex.foreach { case (expectedElem, elemIdx) =>
              assert(actualList.get(elemIdx).asInstanceOf[Int] == expectedElem,
                s"Expected $expectedElem at array index $elemIdx, row $idx")
            }
          }
        }
      }
    }
  }

  test("struct type") {
    val structType = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))
    
    val testData = Seq(
      (InternalRow(1, UTF8String.fromString("Alice")), false),
      (InternalRow(2, UTF8String.fromString("Bob")), false),
      (null.asInstanceOf[InternalRow], true), // null
      (InternalRow(3, UTF8String.fromString("Charlie")), false)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, structType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(structType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            // Verify struct has 2 children (id and name fields)
            assert(result.getNumChildren == 2, 
              s"Expected 2 children in struct, got ${result.getNumChildren}")
            
            // Verify id field - don't close child views, they're views into parent data
            val idChild = result.getChildColumnView(0)
            assert(idChild.getInt(idx) == expectedValue.getInt(0),
              s"Expected id ${expectedValue.getInt(0)} at index $idx")
            
            // Verify name field
            val nameChild = result.getChildColumnView(1)
            assert(nameChild.getJavaString(idx) == expectedValue.getUTF8String(1).toString,
              s"Expected name ${expectedValue.getUTF8String(1)} at index $idx")
          }
        }
      }
    }
  }

  test("map type") {    
    val mapType = MapType(IntegerType, StringType, valueContainsNull = false)
    
    val testData = Seq(
      (Map(1 -> "one", 2 -> "two"), false),
      (Map(), false),
      (Map(10 -> "ten"), false),
      (null.asInstanceOf[Map[Int, String]], true), // null
      (Map(3 -> "three", 4 -> "four"), false)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        val keys = value.keys.toArray
        val values = value.values.map(UTF8String.fromString).toArray
        val mapData = org.apache.spark.sql.catalyst.util.ArrayBasedMapData(keys, values)
        InternalRow(mapData)
      }
    }
    
    val expr = BoundReference(0, mapType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(mapType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            // Maps are represented as List<Struct<key, value>> in cuDF
            // The result should have 1 child (the struct)
            assert(result.getNumChildren == 1,
              s"Expected 1 child (struct), got ${result.getNumChildren}")
            
            // Get the struct child which has key and value columns
            // Don't close child views - they're views into parent data
            val structChild = result.getChildColumnView(0)
            assert(structChild.getNumChildren == 2,
              s"Expected struct with 2 children (key, value), got ${structChild.getNumChildren}")
            
            // Get key and value columns from the struct
            val keyColumn = structChild.getChildColumnView(0)
            val valueColumn = structChild.getChildColumnView(1)
            
            // Get the list for this row to know how many elements
            val structList = result.getList(idx)
            val mapSize = structList.size()
            assert(mapSize == expectedValue.size,
              s"Expected map size ${expectedValue.size}, got $mapSize at index $idx")
            
            // Build map from key/value columns - order doesn't matter for map comparison
            // The list tells us which range of key/value elements belong to this row
            val actualMap = (0 until mapSize).map { i =>
              // For List types, elements are stored contiguously in the child columns
              // We need to find the offset for this row's elements
              val listOffset = {
                var offset = 0
                for (r <- 0 until idx) {
                  if (!result.isNull(r)) {
                    offset += result.getList(r).size()
                  }
                }
                offset
              }
              val key = keyColumn.getInt(listOffset + i)
              val value = valueColumn.getJavaString(listOffset + i)
              key -> value
            }.toMap
            
            // Compare maps - this is order-agnostic
            assert(actualMap.size == expectedValue.size,
              s"Map size mismatch at index $idx: expected " +
                s"${expectedValue.size}, got ${actualMap.size}")
            expectedValue.foreach { case (expectedKey, expectedVal) =>
              assert(actualMap.contains(expectedKey),
                s"Missing key $expectedKey in actual map at index $idx. Actual: $actualMap")
              assert(actualMap(expectedKey) == expectedVal,
                s"Value mismatch for key $expectedKey at index $idx: " +
                s"expected '$expectedVal', got '${actualMap(expectedKey)}'")
            }
          }
        }
      }
    }
  }

  test("array of nullable elements") {
    val arrayType = ArrayType(IntegerType, containsNull = true)
    
    val testData = Seq(
      (Seq(Some(1), None, Some(3)), false),
      (Seq(Some(10), Some(20)), false),
      (null.asInstanceOf[Seq[Option[Int]]], true)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        val arrayData = new org.apache.spark.sql.catalyst.util.GenericArrayData(
          value.map {
            case Some(v) => v.asInstanceOf[Any]
            case None => null
          }.toArray
        )
        InternalRow(arrayData)
      }
    }
    
    val expr = BoundReference(0, arrayType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(arrayType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            val actualList = result.getList(idx)
            assert(actualList.size() == expectedValue.length,
              s"Expected array size ${expectedValue.length}, got ${actualList.size()}")
            
            expectedValue.zipWithIndex.foreach { case (expectedElem, elemIdx) =>
              expectedElem match {
                case Some(v) =>
                  assert(actualList.get(elemIdx) != null,
                    s"Expected non-null at array index $elemIdx, row $idx")
                  assert(actualList.get(elemIdx).asInstanceOf[Int] == v,
                    s"Expected $v at array index $elemIdx, row $idx")
                case None =>
                  assert(actualList.get(elemIdx) == null,
                    s"Expected null at array index $elemIdx, row $idx")
              }
            }
          }
        }
      }
    }
  }

  test("nested struct") {
    val innerStructType = StructType(Seq(
      StructField("x", IntegerType, nullable = false),
      StructField("y", IntegerType, nullable = false)
    ))
    
    val outerStructType = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("point", innerStructType, nullable = false)
    ))
    
    val testData = Seq(
      (InternalRow(1, InternalRow(10, 20)), false),
      (InternalRow(2, InternalRow(30, 40)), false),
      (null.asInstanceOf[InternalRow], true)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, outerStructType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(outerStructType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            
            // Verify outer struct has 2 children
            assert(result.getNumChildren == 2)
            
            // Verify id field - don't close child views, they're views into parent data
            val idChild = result.getChildColumnView(0)
            assert(idChild.getInt(idx) == expectedValue.getInt(0))
            
            // Verify nested struct (point field)
            val pointChild = result.getChildColumnView(1)
            assert(pointChild.getNumChildren == 2)
            
            val innerRow = expectedValue.getStruct(1, 2)
            val xChild = pointChild.getChildColumnView(0)
            assert(xChild.getInt(idx) == innerRow.getInt(0),
              s"Expected x=${innerRow.getInt(0)} at index $idx")
            val yChild = pointChild.getChildColumnView(1)
            assert(yChild.getInt(idx) == innerRow.getInt(1),
              s"Expected y=${innerRow.getInt(1)} at index $idx")
          }
        }
      }
    }
  }

  test("array of strings") {   
    val arrayType = ArrayType(StringType, containsNull = false)
    
    val testData = Seq(
      (Seq("hello", "world"), false),
      (Seq(), false),
      (Seq("single"), false),
      (null.asInstanceOf[Seq[String]], true),
      (Seq("a", "b", "c", "d"), false)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        val arrayData = new org.apache.spark.sql.catalyst.util.GenericArrayData(
          value.map(UTF8String.fromString).toArray
        )
        InternalRow(arrayData)
      }
    }
    
    val expr = BoundReference(0, arrayType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(arrayType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            val actualList = result.getList(idx)
            assert(actualList.size() == expectedValue.length,
              s"Expected array size ${expectedValue.length}, got ${actualList.size()}")
            expectedValue.zipWithIndex.foreach { case (expectedElem, elemIdx) =>
              assert(actualList.get(elemIdx).toString == expectedElem,
                s"Expected '$expectedElem' at array index $elemIdx, row $idx")
            }
          }
        }
      }
    }
  }

  test("struct with nullable fields") {
    val structType = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true), // nullable field
      StructField("age", IntegerType, nullable = true)  // nullable field
    ))
    
    val testData = Seq(
      (InternalRow(1, UTF8String.fromString("Alice"), 30), false),
      (InternalRow(2, null, 25), false), // null name
      (InternalRow(3, UTF8String.fromString("Charlie"), null), false), // null age
      (InternalRow(4, null, null), false), // both nullable fields are null
      (null.asInstanceOf[InternalRow], true) // entire struct is null
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, structType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(structType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
        assert(result.getRowCount == rows.length)
        
        testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
          if (expectedIsNull) {
            assert(result.isNull(idx), s"Expected null at index $idx")
          } else {
            assert(!result.isNull(idx), s"Expected non-null at index $idx")
            assert(result.getNumChildren == 3)
            
            // Verify id field (non-nullable) - don't close child views
            val idChild = result.getChildColumnView(0)
            assert(idChild.getInt(idx) == expectedValue.getInt(0))
            
            // Verify name field (nullable)
            val nameChild = result.getChildColumnView(1)
            if (expectedValue.isNullAt(1)) {
              assert(nameChild.isNull(idx), s"Expected null name at index $idx")
            } else {
              assert(!nameChild.isNull(idx), s"Expected non-null name at index $idx")
              assert(nameChild.getJavaString(idx) == expectedValue.getUTF8String(1).toString)
            }
            
            // Verify age field (nullable)
            val ageChild = result.getChildColumnView(2)
            if (expectedValue.isNullAt(2)) {
              assert(ageChild.isNull(idx), s"Expected null age at index $idx")
            } else {
              assert(!ageChild.isNull(idx), s"Expected non-null age at index $idx")
              assert(ageChild.getInt(idx) == expectedValue.getInt(2))
            }
          }
        }
      }
    }
  }

  test("decimal 32-bit (precision <= 9)") {
    // Decimal with precision <= 9 uses 32-bit backing
    val decimalType = DecimalType(9, 2)
    
    val testData = Seq(
      (Decimal("1234567.89"), false),
      (Decimal("0.01"), false),
      (Decimal("-9999999.99"), false),
      (null.asInstanceOf[Decimal], true)
    )
    
    testProjection[Decimal](
      decimalType,
      testData,
      BoundReference(0, decimalType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        // Verify backing type is 32-bit
        assert(result.getType.getTypeId == DType.DTypeEnum.DECIMAL32)
        
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          val actual = result.getBigDecimal(idx)
          assert(actual.compareTo(expectedValue.toBigDecimal.bigDecimal) == 0,
            s"Expected $expectedValue, got $actual at index $idx")
        }
      }
    )
  }

  test("decimal 64-bit (precision <= 18)") {
    // Decimal with precision <= 18 uses 64-bit backing
    val decimalType = DecimalType(18, 4)
    
    val testData = Seq(
      (Decimal("12345678901234.5678"), false),
      (Decimal("0.0001"), false),
      (Decimal("-99999999999999.9999"), false),
      (null.asInstanceOf[Decimal], true)
    )
    
    testProjection[Decimal](
      decimalType,
      testData,
      BoundReference(0, decimalType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        // Verify backing type is 64-bit
        assert(result.getType.getTypeId == DType.DTypeEnum.DECIMAL64)
        
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          val actual = result.getBigDecimal(idx)
          assert(actual.compareTo(expectedValue.toBigDecimal.bigDecimal) == 0,
            s"Expected $expectedValue, got $actual at index $idx")
        }
      }
    )
  }

  test("decimal 128-bit (precision > 18)") {
    // Decimal with precision > 18 uses 128-bit backing
    val decimalType = DecimalType(38, 10)
    
    val testData = Seq(
      (Decimal("1234567890123456789012345678.0123456789"), false),
      (Decimal("0.0000000001"), false),
      (Decimal("-9999999999999999999999999999.9999999999"), false),
      (null.asInstanceOf[Decimal], true)
    )
    
    testProjection[Decimal](
      decimalType,
      testData,
      BoundReference(0, decimalType, true),
      (result, idx, expectedValue, expectedIsNull) => {
        // Verify backing type is 128-bit
        assert(result.getType.getTypeId == DType.DTypeEnum.DECIMAL128)
        
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          val actual = result.getBigDecimal(idx)
          assert(actual.compareTo(expectedValue.toBigDecimal.bigDecimal) == 0,
            s"Expected $expectedValue, got $actual at index $idx")
        }
      }
    )
  }

  test("array of structs") {
    val structType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val arrayType = ArrayType(structType, containsNull = true)
    
    val testData = Seq(
      (
        new GenericArrayData(Array(
          InternalRow(1, UTF8String.fromString("Alice")),
          InternalRow(2, UTF8String.fromString("Bob"))
        )),
        false
      ),
      (
        new GenericArrayData(Array(
          InternalRow(3, UTF8String.fromString("Charlie"))
        )),
        false
      ),
      (new GenericArrayData(Array.empty[InternalRow]), false),
      (null, true)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, arrayType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(arrayType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
      assert(result.getType.isNestedType)
      
      testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          val expectedArray = expectedValue.asInstanceOf[GenericArrayData]
          
          // Get the struct child which contains the struct fields
          val structChild = result.getChildColumnView(0)
          assert(structChild.getNumChildren == 2,
            s"Expected struct with 2 fields, got ${structChild.getNumChildren}")
          
          val idColumn = structChild.getChildColumnView(0)
          val nameColumn = structChild.getChildColumnView(1)
          
          // Get array size for this row
          val arrayList = result.getList(idx)
          val arraySize = arrayList.size()
          assert(arraySize == expectedArray.numElements(),
            s"Expected array size ${expectedArray.numElements()}, got $arraySize at index $idx")
          
          // Calculate offset for this row's elements
          val listOffset = {
            var offset = 0
            for (r <- 0 until idx) {
              if (!result.isNull(r)) {
                offset += result.getList(r).size()
              }
            }
            offset
          }
          
          // Verify each struct in the array
          (0 until arraySize).foreach { i =>
            val expectedStruct = expectedArray.getStruct(i, 2)
            val actualId = idColumn.getInt(listOffset + i)
            val actualName = nameColumn.getJavaString(listOffset + i)
            
            assert(actualId == expectedStruct.getInt(0),
              s"Expected id ${expectedStruct.getInt(0)} at array[$i], got $actualId")
            assert(actualName == expectedStruct.getUTF8String(1).toString,
              s"Expected name ${expectedStruct.getUTF8String(1)} at array[$i], got $actualName")
          }
        }
      }
    }
    }
  }

  test("struct containing array") {
    val structType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("tags", ArrayType(StringType, containsNull = false))
    ))
    
    val testData = Seq(
      (
        InternalRow(
          1,
          new GenericArrayData(Array(
            UTF8String.fromString("tag1"),
            UTF8String.fromString("tag2"),
            UTF8String.fromString("tag3")
          ))
        ),
        false
      ),
      (
        InternalRow(
          2,
          new GenericArrayData(Array(UTF8String.fromString("single")))
        ),
        false
      ),
      (InternalRow(3, new GenericArrayData(Array.empty[UTF8String])), false),
      (null, true)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, structType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(structType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
      assert(result.getNumChildren == 2, s"Expected 2 children, got ${result.getNumChildren}")
      
      testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          
          // Verify id field
          val idChild = result.getChildColumnView(0)
          assert(idChild.getInt(idx) == expectedValue.getInt(0),
            s"Expected id ${expectedValue.getInt(0)} at index $idx")
          
          // Verify tags array field
          val tagsChild = result.getChildColumnView(1)
          val tagsStringChild = tagsChild.getChildColumnView(0)
          
          val expectedTags = expectedValue.getArray(1)
          val actualTagsList = tagsChild.getList(idx)
          val actualSize = actualTagsList.size()
          
          assert(actualSize == expectedTags.numElements(),
            s"Expected ${expectedTags.numElements()} tags, got $actualSize at index $idx")
          
          // Calculate offset for this row's array elements
          val listOffset = {
            var offset = 0
            for (r <- 0 until idx) {
              if (!tagsChild.isNull(r)) {
                offset += tagsChild.getList(r).size()
              }
            }
            offset
          }
          
          // Verify each tag
          (0 until actualSize).foreach { i =>
            val expectedTag = expectedTags.getUTF8String(i).toString
            val actualTag = tagsStringChild.getJavaString(listOffset + i)
            assert(actualTag == expectedTag,
              s"Expected tag '$expectedTag' at position $i, got '$actualTag'")
          }
        }
      }
    }
    }
  }

  test("struct containing map") {
    val structType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("attributes", MapType(StringType, IntegerType, valueContainsNull = false))
    ))
    
    val testData = Seq(
      (
        InternalRow(
          1,
          ArrayBasedMapData(
            Array(UTF8String.fromString("x"), UTF8String.fromString("y")),
            Array(10, 20)
          )
        ),
        false
      ),
      (
        InternalRow(
          2,
          ArrayBasedMapData(
            Array(UTF8String.fromString("z")),
            Array(30)
          )
        ),
        false
      ),
      (InternalRow(3, ArrayBasedMapData(Array.empty[UTF8String], Array.empty[Int])), false),
      (null, true)
    )
    
    val rows = testData.map { case (value, isNull) =>
      if (isNull) {
        InternalRow(null)
      } else {
        InternalRow(value)
      }
    }
    
    val expr = BoundReference(0, structType, true)
    val projection = BridgeHostColumnProjection.create(Seq(expr))
    
    val resultType = GpuColumnVector.convertFrom(structType, true)
    withResource(new RapidsHostColumnBuilder(resultType, rows.length)) { builder =>
      projection.apply(rows.iterator, Array(builder))
      
      withResource(builder.build()) { result =>
      assert(result.getNumChildren == 2, s"Expected 2 children, got ${result.getNumChildren}")
      
      testData.zipWithIndex.foreach { case ((expectedValue, expectedIsNull), idx) =>
        if (expectedIsNull) {
          assert(result.isNull(idx), s"Expected null at index $idx")
        } else {
          assert(!result.isNull(idx), s"Expected non-null at index $idx")
          
          // Verify id field
          val idChild = result.getChildColumnView(0)
          assert(idChild.getInt(idx) == expectedValue.getInt(0),
            s"Expected id ${expectedValue.getInt(0)} at index $idx")
          
          // Verify map field (List<Struct<key, value>> in cuDF)
          val mapChild = result.getChildColumnView(1)
          val mapStructChild = mapChild.getChildColumnView(0)
          val keyColumn = mapStructChild.getChildColumnView(0)
          val valueColumn = mapStructChild.getChildColumnView(1)
          
          val expectedMap = expectedValue.getMap(1)
          val actualMapList = mapChild.getList(idx)
          val actualSize = actualMapList.size()
          
          assert(actualSize == expectedMap.numElements(),
            s"Expected ${expectedMap.numElements()} entries, got $actualSize at index $idx")
          
          // Calculate offset for this row's map elements
          val listOffset = {
            var offset = 0
            for (r <- 0 until idx) {
              if (!mapChild.isNull(r)) {
                offset += mapChild.getList(r).size()
              }
            }
            offset
          }
          
          // Build actual map from columns
          val actualMap = (0 until actualSize).map { i =>
            val key = keyColumn.getJavaString(listOffset + i)
            val value = valueColumn.getInt(listOffset + i)
            key -> value
          }.toMap
          
          // Convert expected map to comparable format
          val expectedMapScala = expectedMap.keyArray().array.zip(expectedMap.valueArray().array)
            .map { case (k, v) =>
              k.asInstanceOf[UTF8String].toString -> v.asInstanceOf[Int]
            }.toMap
          
          // Compare maps (order-agnostic)
          assert(actualMap.size == expectedMapScala.size,
            s"Map size mismatch at index $idx")
          expectedMapScala.foreach { case (expectedKey, expectedVal) =>
            assert(actualMap.contains(expectedKey),
              s"Missing key $expectedKey at index $idx")
            assert(actualMap(expectedKey) == expectedVal,
              s"Value mismatch for key $expectedKey at index $idx")
          }
        }
      }
    }
    }
  }
}
