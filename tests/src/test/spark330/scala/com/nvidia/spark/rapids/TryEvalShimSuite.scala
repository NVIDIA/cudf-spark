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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BitwiseAnd, Cast,
  Divide, Expression, Literal, Multiply, Subtract, TryEval}
import org.apache.spark.sql.rapids.{GpuAdd, GpuDivide, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, IntegerType, LongType,
  ShortType, StringType}

class TryEvalShimSuite extends AnyFunSuite {

  private val jitConf = new RapidsConf(Map(
    RapidsConf.ENABLE_PROJECT_AST_ANSI_ARITHMETIC.key -> "true"))
  private val noJitConf = new RapidsConf(Map.empty[String, String])

  private def wrap(expression: Expression, conf: RapidsConf): BaseExprMeta[_] = {
    val meta = GpuOverrides.wrapExpr(expression, conf, None)
    meta.tagForGpu()
    meta
  }

  private def binaryAttributes(dataType: DataType): (Expression, Expression) = (
    AttributeReference("lhs", dataType, nullable = false)(),
    AttributeReference("rhs", dataType, nullable = false)())

  test("TryEval lowers integral arithmetic to TRY row IR") {
    val operations = Seq[(String, (Expression, Expression) => Expression)](
      "add" -> ((lhs, rhs) => Add(lhs, rhs, failOnError = true)),
      "subtract" -> ((lhs, rhs) => Subtract(lhs, rhs, failOnError = true)),
      "multiply" -> ((lhs, rhs) => Multiply(lhs, rhs, failOnError = true)))

    for {
      dataType <- Seq(ByteType, ShortType, IntegerType, LongType)
      (name, operation) <- operations
    } {
      val (lhs, rhs) = binaryAttributes(dataType)
      val meta = wrap(TryEval(operation(lhs, rhs)), jitConf)

      assert(meta.canThisBeAst, s"$name $dataType should use project AST")
      assert(!meta.canThisBeLegacyAst, s"$name $dataType should remain row-IR-only")
      val gpuExpression = meta.convertToGpu().asInstanceOf[GpuExpression]
      gpuExpression match {
        case add: GpuAdd =>
          assert(add.tryMode && !add.failOnError)
        case subtract: GpuSubtract =>
          assert(subtract.tryMode && !subtract.failOnError)
        case multiply: GpuMultiply =>
          assert(multiply.tryMode && !multiply.failOnError)
        case other => fail(s"Unexpected $name conversion: $other")
      }
      assert(gpuExpression.nullable)
      assert(gpuExpression.selfUsesRowIrJitAst)
    }
  }

  test("TryEval integral arithmetic requires row IR JIT support") {
    val (lhs, rhs) = binaryAttributes(IntegerType)
    val meta = wrap(TryEval(Add(lhs, rhs, failOnError = true)), noJitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval lowers DOUBLE division to row IR") {
    val (lhs, rhs) = binaryAttributes(DoubleType)
    val meta = wrap(TryEval(Divide(lhs, rhs, failOnError = true)), jitConf)

    assert(meta.canThisBeAst, meta.explainAst(all = true))
    assert(!meta.canThisBeLegacyAst)
    val divide = meta.convertToGpu().asInstanceOf[GpuDivide]
    assert(!divide.failOnError)
    assert(divide.nullable)
    assert(divide.selfUsesRowIrJitAst)
  }

  test("TryEval DOUBLE division requires row IR JIT support") {
    val (lhs, rhs) = binaryAttributes(DoubleType)
    val meta = wrap(TryEval(Divide(lhs, rhs, failOnError = true)), noJitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval does not lower unsupported operations") {
    val (lhs, rhs) = binaryAttributes(IntegerType)
    val meta = wrap(TryEval(BitwiseAnd(lhs, rhs)), jitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval requires checked inner arithmetic") {
    val (lhs, rhs) = binaryAttributes(IntegerType)
    val meta = wrap(TryEval(Add(lhs, rhs, failOnError = false)), jitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval only lowers DOUBLE division") {
    val (doubleLhs, doubleRhs) = binaryAttributes(DoubleType)
    val doubleAdd = wrap(
      TryEval(Add(doubleLhs, doubleRhs, failOnError = true)), jitConf)
    assert(!doubleAdd.canThisBeReplaced)

    val (intLhs, intRhs) = binaryAttributes(IntegerType)
    val intDivide = wrap(
      TryEval(Divide(intLhs, intRhs, failOnError = true)), jitConf)
    assert(!intDivide.canThisBeReplaced)
  }

  test("TryEval requires checked inner division") {
    val (lhs, rhs) = binaryAttributes(DoubleType)
    val meta = wrap(TryEval(Divide(lhs, rhs, failOnError = false)), jitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval does not widen its error-catching scope") {
    val input = AttributeReference("input", StringType)()
    val fallible = Cast(input, IntegerType, None, ansiEnabled = true)
    val meta = wrap(TryEval(Add(fallible, Literal(1), failOnError = true)), jitConf)

    assert(!meta.canThisBeReplaced)
  }

  test("TryEval division does not widen its error-catching scope") {
    val input = AttributeReference("input", StringType)()
    val fallible = Cast(input, DoubleType, None, ansiEnabled = true)
    val safe = Literal(1.0d)

    Seq(
      Divide(fallible, safe, failOnError = true),
      Divide(safe, fallible, failOnError = true)).foreach { divide =>
      val meta = wrap(TryEval(divide), jitConf)
      assert(!meta.canThisBeReplaced)
    }
  }
}
