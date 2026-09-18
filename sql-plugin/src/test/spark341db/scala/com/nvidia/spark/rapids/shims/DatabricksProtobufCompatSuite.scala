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
{"spark":"341db"}
{"spark":"350db143"}
{"spark":"400db173"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.lang.reflect.InvocationTargetException

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rapids.protobuf.ProtobufDescriptorSource
import org.apache.spark.sql.types.{BinaryType, DataType}

class DatabricksProtobufCompatSuite extends AnyFunSuite {
  private abstract class FakeExpr extends LeafExpression with CodegenFallback {
    override def nullable: Boolean = true
    override def dataType: DataType = BinaryType
    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = null
    def messageName: String = "test.Message"
    def options: Map[String, String] = Map.empty
  }

  private case class DirectBytes(binaryDescriptorSet: Array[Byte]) extends FakeExpr
  private case class OptionalBytes(binaryDescriptorSet: Option[Array[Byte]]) extends FakeExpr

  test("alternate binaryDescriptorSet accessors accept direct and optional bytes") {
    val bytes = Array[Byte](1, 2, 3)
    Seq[Expression](DirectBytes(bytes), OptionalBytes(Some(bytes))).foreach { expr =>
      val info = SparkProtobufCompat.extractExprInfo(expr).fold(fail(_), identity)
      assert(info.descriptorSource == ProtobufDescriptorSource.DescriptorBytes(bytes))
    }
    assert(SparkProtobufCompat.extractExprInfo(OptionalBytes(None)).isLeft)
  }

  private class RecordingBuilder(pathError: Option[Throwable], bytesError: Option[Throwable]) {
    var calls = Vector.empty[Any]

    def buildDescriptor(name: String, source: Option[Any]): String = {
      val value = source.get
      calls :+= value
      value match {
        case _: String => pathError.foreach(throw _)
        case _: Array[Byte] => bytesError.foreach(throw _)
      }
      name
    }
  }

  private def invoke(builder: RecordingBuilder, read: String => Array[Byte]): AnyRef = {
    val method = builder.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[Option[_]])
    SparkProtobufCompat.invokeBuildDescriptor(method, builder, "test.Message",
      ProtobufDescriptorSource.DescriptorPath("test.desc"), read)
  }

  test("path-compatible builders do not read the descriptor file") {
    val builder = new RecordingBuilder(None, None)
    assert(invoke(builder, _ => fail("unexpected file read")) == "test.Message")
    assert(builder.calls == Vector("test.desc"))
  }

  test("path type mismatches retry exactly once with file bytes") {
    Seq(new ClassCastException("expected bytes"), new MatchError("test.desc")).foreach { cause =>
      val builder = new RecordingBuilder(Some(cause), None)
      var reads = 0
      assert(invoke(builder, path => {
        assert(path == "test.desc")
        reads += 1
        Array[Byte](1, 2, 3)
      }) == "test.Message")
      assert(reads == 1)
      assert(builder.calls.size == 2)
      assert(builder.calls.head == "test.desc")
      assert(builder.calls(1).asInstanceOf[Array[Byte]].sameElements(Array[Byte](1, 2, 3)))
    }
  }

  test("unrelated path failures are not retried") {
    val cause = new IllegalArgumentException("unknown message")
    val builder = new RecordingBuilder(Some(cause), None)
    val error = intercept[InvocationTargetException] {
      invoke(builder, _ => fail("unexpected file read"))
    }
    assert(error.getCause eq cause)
    assert(builder.calls == Vector("test.desc"))
  }

  test("retry failures retain the original error and the retry cause") {
    Seq(false, true).foreach { failRead =>
      val original = new ClassCastException("expected bytes")
      val retry = new IllegalArgumentException("invalid descriptor")
      val builder = new RecordingBuilder(Some(original), Some(retry))
      var reads = 0
      val error = intercept[RuntimeException] {
        invoke(builder, _ => {
          reads += 1
          if (failRead) throw retry
          Array[Byte](1, 2, 3)
        })
      }
      assert(reads == 1)
      assert(builder.calls.size == (if (failRead) 1 else 2))
      assert(error.getCause eq retry)
      assert(error.getSuppressed.length == 1)
      assert(error.getSuppressed.head.getCause eq original)
      assert(error.getMessage.contains("expected bytes"))
      assert(error.getMessage.contains("invalid descriptor"))
    }
  }
}
