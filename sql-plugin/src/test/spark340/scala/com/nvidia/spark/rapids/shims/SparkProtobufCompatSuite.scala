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
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rapids.protobuf._
import org.apache.spark.sql.types._

class SparkProtobufCompatSuite extends AnyFunSuite {
  private val compat = new BinarySparkProtobufCompat {}
  private val outputSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true)))

  private case class FakeExprChild() extends Expression {
    override def children: Seq[Expression] = Nil
    override def nullable: Boolean = true
    override def dataType: DataType = BinaryType
    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = null
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = {
      assert(newChildren.isEmpty)
      this
    }
  }

  private abstract class FakeBaseProtobufExpr(childExpr: Expression) extends UnaryExpression {
    override def child: Expression = childExpr
    override def nullable: Boolean = true
    override def dataType: DataType = outputSchema
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildInternal(newChild: Expression): Expression = this
  }

  private case class FakeBytesProtobufExpr(
      override val child: Expression,
      binaryFileDescriptorSet: Option[Array[Byte]] = Some(Array[Byte](1, 2, 3)))
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def options: scala.collection.Map[String, String] =
      Map("mode" -> "PERMISSIVE", "enums.as.ints" -> "true")
  }

  private case class FakeMissingOptionsExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
  }

  private case class FakePathProtobufExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def options: Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private object RecordingBuilder {
    var payload: Option[Any] = None
    def buildDescriptor(messageName: String, descriptor: Option[Any]): String = {
      payload = descriptor
      messageName
    }
  }

  private final class DescriptorWithExtensions(val descriptor: AnyRef)

  test("selected shim uses the expected Spark descriptor contract") {
    val legacy = org.apache.spark.SPARK_VERSION.startsWith("3.4.")
    val expr = if (legacy) FakePathProtobufExpr(FakeExprChild())
      else FakeBytesProtobufExpr(FakeExprChild())
    val info = SparkProtobufCompat.extractExprInfo(expr).fold(fail(_), identity)
    assert(info.messageName == "test.Message")
    val source = if (legacy) ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc")
      else ProtobufDescriptorSource.DescriptorBytes(Array[Byte](1, 2, 3))
    assert(info.descriptorSource == source)
    val method = RecordingBuilder.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])
    assert(SparkProtobufCompat.invokeBuildDescriptor(
      method, RecordingBuilder, info.messageName, source,
      _ => fail("expression descriptor source should not need conversion")) == "test.Message")
    if (legacy) {
      assert(RecordingBuilder.payload.contains("/tmp/test.desc"))
    } else {
      assert(RecordingBuilder.payload.get.asInstanceOf[Array[Byte]]
        .sameElements(Array[Byte](1, 2, 3)))
    }
    val descriptor = new FakeModernDescriptor("proto2")
    val version = org.apache.spark.SPARK_VERSION
    val raw = if (version.startsWith("4.2.") || version.startsWith("5.0.")) {
      new DescriptorWithExtensions(descriptor)
    } else {
      descriptor
    }
    assert(SparkProtobufCompat.unwrapMessageDescriptor(raw) eq descriptor)
  }

  private object FakeSpark35ProtobufUtils {
    var calls = 0
    def buildDescriptor(
        messageName: String,
        binaryFileDescriptorSet: Option[Array[Byte]]): String = {
      calls += 1
      s"$messageName:${binaryFileDescriptorSet.map(_.mkString(",")).getOrElse("none")}"
    }
  }

  private object FakeFailingProtobufUtils {
    var calls = 0
    def buildDescriptor(
        messageName: String,
        binaryFileDescriptorSet: Option[Array[Byte]]): String = {
      calls += 1
      val bytes = binaryFileDescriptorSet.getOrElse(Array.emptyByteArray)
      if (bytes.sameElements(Array[Byte](1, 2, 3))) {
        throw new IllegalArgumentException(s"Unknown message $messageName")
      }
      s"$messageName:${bytes.mkString(",")}"
    }
  }

  private final class FakeFileDescriptorProto(syntax: String) {
    def getSyntax: String = syntax
  }

  private final class FakeModernFileDescriptor(syntax: String) {
    def toProto: FakeFileDescriptorProto = new FakeFileDescriptorProto(syntax)
  }

  private final class FakeModernDescriptor(syntax: String) {
    def getFile: FakeModernFileDescriptor = new FakeModernFileDescriptor(syntax)
  }

  private final class FakeLegacyFileDescriptor(syntax: String) {
    def getSyntax: String = syntax
  }

  private final class FakeLegacyDescriptor(syntax: String) {
    def getFile: FakeLegacyFileDescriptor = new FakeLegacyFileDescriptor(syntax)
  }

  private final class FakeBrokenFileDescriptor

  private final class FakeBrokenDescriptor {
    def getFile: FakeBrokenFileDescriptor = new FakeBrokenFileDescriptor
  }

  private final class FakeDescriptorWithoutFile

  test("compat extracts a binary descriptor source") {
    val exprInfo = compat.extractExprInfo(FakeBytesProtobufExpr(FakeExprChild()))
    assert(exprInfo.isRight)
    val info = exprInfo.toOption.get
    assert(info.messageName == "test.Message")
    assert(info.options == Map("mode" -> "PERMISSIVE", "enums.as.ints" -> "true"))
    info.descriptorSource match {
      case ProtobufDescriptorSource.DescriptorBytes(bytes) =>
        assert(bytes.sameElements(Array[Byte](1, 2, 3)))
      case other =>
        fail(s"Unexpected descriptor source: $other")
    }
  }

  test("binary shim reads descriptor path as bytes before invoking the builder") {
    FakeSpark35ProtobufUtils.calls = 0
    val buildMethod = FakeSpark35ProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])
    var readCalls = 0

    val result = compat.invokeBuildDescriptor(
      buildMethod,
      FakeSpark35ProtobufUtils,
      "test.Message",
      ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc"),
      path => {
        assert(path == "/tmp/test.desc")
        readCalls += 1
        Array[Byte](1, 2, 3)
      })

    assert(readCalls == 1)
    assert(FakeSpark35ProtobufUtils.calls == 1)
    assert(result == "test.Message:1,2,3")
  }

  test("compat passes bytes directly to Spark 3.5 descriptor builder") {
    val buildMethod = FakeSpark35ProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])

    val result = compat.invokeBuildDescriptor(
      buildMethod,
      FakeSpark35ProtobufUtils,
      "test.Message",
      ProtobufDescriptorSource.DescriptorBytes(Array[Byte](4, 5, 6)),
      _ => fail("binary descriptor source should not read a file"))

    assert(result == "test.Message:4,5,6")
  }

  test("binary shim preserves the builder failure without retrying") {
    FakeFailingProtobufUtils.calls = 0
    val buildMethod = FakeFailingProtobufUtils.getClass.getMethod(
      "buildDescriptor", classOf[String], classOf[scala.Option[_]])

    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      compat.invokeBuildDescriptor(
        buildMethod,
        FakeFailingProtobufUtils,
        "test.Message",
        ProtobufDescriptorSource.DescriptorPath("/tmp/test.desc"),
        _ => Array[Byte](1, 2, 3))
    }

    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getCause.getMessage == "Unknown message test.Message")
    assert(ex.getSuppressed.isEmpty)
    assert(FakeFailingProtobufUtils.calls == 1)
  }

  test("binary shim rejects missing descriptor sets") {
    val result = compat.extractExprInfo(FakeBytesProtobufExpr(FakeExprChild(), None))
    assert(result == Left("from_protobuf requires a binary descriptor set"))
    assert(compat.extractExprInfo(FakePathProtobufExpr(FakeExprChild())).left.toOption
      .exists(_.contains("Cannot read binaryFileDescriptorSet")))
  }

  test("compat reports missing options accessor as cpu fallback reason") {
    val exprInfo = compat.extractExprInfo(FakeMissingOptionsExpr(FakeExprChild()))
    assert(exprInfo.left.toOption.exists(
      _.contains("Cannot read from_protobuf options via reflection")))
  }

  test("compat reads syntax through protobuf 4 FileDescriptor API") {
    assert(compat.readDescriptorSyntax(
      new FakeModernDescriptor("proto2")) == "PROTO2")
    assert(compat.readDescriptorSyntax(
      new FakeModernDescriptor("proto3")) == "PROTO3")
    assert(compat.readDescriptorSyntax(
      new FakeModernDescriptor("")) == "PROTO2")
    assert(compat.readDescriptorSyntax(
      new FakeLegacyDescriptor("PROTO2")) == "PROTO2")
    assert(compat.readDescriptorSyntax(
      new FakeBrokenDescriptor) == "")
    assert(compat.readDescriptorSyntax(
      new FakeDescriptorWithoutFile) == "")
  }

  private def checkMutationStability[T](
      create: Array[Byte] => T,
      read: T => Array[Byte],
      extract: T => Array[Byte]): Unit = {
    val input = Array[Byte](1, 2, 3)
    val value = create(input)
    val equalValue = create(input.clone())
    val hash = value.hashCode()
    val set = scala.collection.mutable.HashSet(value)
    input(0) = 9
    read(value)(1) = 9
    extract(value)(2) = 9
    assert(read(value).sameElements(Array[Byte](1, 2, 3)))
    assert(value == equalValue)
    assert(value.hashCode() == hash)
    assert(value.hashCode() == equalValue.hashCode())
    assert(set.contains(value) && set.contains(equalValue))
  }

  test("descriptor bytes remain stable after input and accessor mutation") {
    import ProtobufDescriptorSource.DescriptorBytes
    checkMutationStability[DescriptorBytes](DescriptorBytes.apply, _.bytes,
      value => DescriptorBytes.unapply(value).get)
  }

  test("binary defaults remain stable after input and accessor mutation") {
    import ProtobufDefaultValue.BinaryValue
    checkMutationStability[BinaryValue](BinaryValue.apply, _.value,
      value => BinaryValue.unapply(value).get)
  }

  private final class FakeBytesDefault {
    def toByteArray: Array[Byte] = Array[Byte](4, 5)
  }

  test("compat converts reflected defaults to neutral values") {
    val enumMetadata = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "READY")))

    assert(compat.toDefaultValue(Boolean.box(true), "BOOL", None) ==
      Right(ProtobufDefaultValue.BoolValue(true)))
    assert(compat.toDefaultValue(Int.box(7), "INT32", None) ==
      Right(ProtobufDefaultValue.IntValue(7L)))
    assert(compat.toDefaultValue(Float.box(1.5f), "FLOAT", None) ==
      Right(ProtobufDefaultValue.FloatValue(1.5f)))
    assert(compat.toDefaultValue(Double.box(2.5), "DOUBLE", None) ==
      Right(ProtobufDefaultValue.DoubleValue(2.5)))
    assert(compat.toDefaultValue("value", "STRING", None) ==
      Right(ProtobufDefaultValue.StringValue("value")))
    assert(compat.toDefaultValue(Array[Byte](4, 5), "BYTES", None) ==
      Right(ProtobufDefaultValue.BinaryValue(Array[Byte](4, 5))))
    assert(compat.toDefaultValue(new FakeBytesDefault, "BYTES", None) ==
      Right(ProtobufDefaultValue.BinaryValue(Array[Byte](4, 5))))
    assert(compat.toDefaultValue(Int.box(1), "ENUM", Some(enumMetadata)) ==
      Right(ProtobufDefaultValue.EnumValue(1, "READY")))
  }

  private final class FakeEnumValueDescriptor(val getNumber: Int, val getName: String)

  test("enum metadata snapshots mutable entries before caching numeric lookup") {
    val original = Vector(ProtobufEnumValue(0, "FIRST"), ProtobufEnumValue(0, "ALIAS"))
    val entries = scala.collection.mutable.ArrayBuffer(original: _*)
    val metadata = ProtobufEnumMetadata(entries)
    val equalValue = ProtobufEnumMetadata(original)
    val hash = metadata.hashCode()
    val set = scala.collection.mutable.HashSet(metadata)
    assert(metadata.defaultFromNumber(0) == ProtobufDefaultValue.EnumValue(0, "FIRST"))
    entries(0) = ProtobufEnumValue(0, "CHANGED")
    entries.clear()
    assert(metadata.values == original)
    assert(metadata.defaultFromNumber(0) == ProtobufDefaultValue.EnumValue(0, "FIRST"))
    assert(metadata == equalValue && metadata.hashCode() == hash)
    assert(set.contains(metadata) && set.contains(equalValue))
  }

  test("enum defaults preserve explicit aliases and use the first name for numeric values") {
    val metadata = Some(ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "FIRST"), ProtobufEnumValue(0, "ALIAS"))))
    Seq("FIRST", "ALIAS").foreach { name =>
      assert(compat.toDefaultValue(new FakeEnumValueDescriptor(0, name), "ENUM", metadata) ==
        Right(ProtobufDefaultValue.EnumValue(0, name)))
    }
    assert(compat.toDefaultValue(Int.box(0), "ENUM", metadata) ==
      Right(ProtobufDefaultValue.EnumValue(0, "FIRST")))
  }

  test("compat returns Left for unsupported default value types") {
    val result = compat.toDefaultValue(
      "opaque-default", "MESSAGE", None)

    assert(result.left.toOption.exists(_.contains("Unsupported protobuf default value type")))
  }
}
