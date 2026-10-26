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
{"spark": "410db183"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{GpuLiteral, VersionUtils}
import com.nvidia.spark.rapids.jni.Protobuf
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GetArrayStructFields,
  GetStructField,
  UnaryExpression
}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rapids.{
  GpuFromProtobuf,
  GpuGetArrayStructFieldsMeta,
  GpuGetStructFieldMeta
}
import org.apache.spark.sql.rapids.protobuf._
import org.apache.spark.sql.types._

class ProtobufExprShimsSuite extends AnyFunSuite {
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

  private case class FakePathProtobufExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] =
      Some("/tmp/test.desc".getBytes("UTF-8"))
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeBytesProtobufExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/bytes.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] = Some(Array[Byte](1, 2, 3))
    def options: scala.collection.Map[String, String] =
      Map("mode" -> "PERMISSIVE", "enums.as.ints" -> "true")
  }

  private case class FakeMissingOptionsExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] =
      Some("/tmp/test.desc".getBytes("UTF-8"))
  }

  private case class FakeDifferentMessageExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.OtherMessage"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] =
      Some("/tmp/test.desc".getBytes("UTF-8"))
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeDifferentDescriptorExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/other.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] =
      Some("/tmp/other.desc".getBytes("UTF-8"))
    def options: scala.collection.Map[String, String] = Map("mode" -> "FAILFAST")
  }

  private case class FakeDifferentOptionsExpr(override val child: Expression)
      extends FakeBaseProtobufExpr(child) {
    def messageName: String = "test.Message"
    def descFilePath: Option[String] = Some("/tmp/test.desc")
    def binaryFileDescriptorSet: Option[Array[Byte]] =
      Some("/tmp/test.desc".getBytes("UTF-8"))
    def options: scala.collection.Map[String, String] = Map("mode" -> "PERMISSIVE")
  }

  private case class FakeTypedUnaryExpr(
      dt: DataType,
      override val child: Expression = FakeExprChild()) extends UnaryExpression {
    override def nullable: Boolean = true
    override def dataType: DataType = dt
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
      throw new UnsupportedOperationException("not needed")
    override protected def withNewChildInternal(newChild: Expression): Expression = copy(child =
      newChild)
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

  private case class FakeMessageDescriptor(
      syntax: String,
      fields: Map[String, ProtobufFieldDescriptor],
      override val javaStringCheckUtf8: Boolean = false) extends ProtobufMessageDescriptor {
    override def findField(name: String): Option[ProtobufFieldDescriptor] = fields.get(name)
    override def fieldNames: Seq[String] = fields.keys.toSeq
  }

  private case class FakeFieldDescriptor(
      name: String,
      fieldNumber: Int,
      protoTypeName: String,
      isRepeated: Boolean = false,
      isRequired: Boolean = false,
      isInOneof: Boolean = false,
      defaultValue: Option[ProtobufDefaultValue] = None,
      defaultValueError: Option[String] = None,
      enumMetadata: Option[ProtobufEnumMetadata] = None,
      messageDescriptor: Option[ProtobufMessageDescriptor] = None,
      referencedTypeSyntax: Option[String] = None) extends ProtobufFieldDescriptor {
    override lazy val explicitDefaultValue: Either[String, Option[ProtobufDefaultValue]] =
      defaultValueError match {
        case Some(reason) => Left(reason)
        case None => Right(defaultValue)
      }
  }

  private val isSpark350OrLater = VersionUtils.cmpSparkVersion(3, 5, 0) >= 0

  private val permissiveOnGpu =
    Right(ProtobufPlannerOptions(enumsAsInts = false, failOnErrors = false))

  // Shaped like ProtobufDataToCatalyst before SPARK-46275.
  private class LegacyPermissiveExpr(val nullResultRow: Any)

  test("compat detects Spark versions that return a struct of null fields") {
    val legacy = SparkProtobufCompat.permissiveReturnsNullFieldsRow(classOf[LegacyPermissiveExpr])
    val current = SparkProtobufCompat.permissiveReturnsNullFieldsRow(classOf[FakeExprChild])
    assert(legacy)
    assert(!current)
  }

  test("compat parses permissive planner options") {
    assert(SparkProtobufCompat.parsePlannerOptions(Map("mode" -> "PERMISSIVE"),
      permissiveReturnsNullRow = false) == permissiveOnGpu)
    assert(SparkProtobufCompat.parsePlannerOptions(Map("mode" -> "PERMISSIVE"),
      permissiveReturnsNullRow = true) == Right(ProtobufPlannerOptions(
        enumsAsInts = false, failOnErrors = false, malformedRowsAsNullFields = true)))
    // FAILFAST never produces the struct of null fields.
    assert(SparkProtobufCompat.parsePlannerOptions(Map.empty[String, String],
      permissiveReturnsNullRow = true) ==
      Right(ProtobufPlannerOptions(enumsAsInts = false, failOnErrors = true)))
  }

  test("compat follows Spark support for enums.as.ints") {
    // Spark 3.4 ignores enums.as.ints and decodes enums as strings.
    assert(SparkProtobufCompat.parsePlannerOptions(Map("enums.as.ints" -> "true")) ==
      Right(ProtobufPlannerOptions(enumsAsInts = isSpark350OrLater, failOnErrors = true)))
  }

  test("compat rejects DROPMALFORMED mode") {
    val result = SparkProtobufCompat.parsePlannerOptions(Map("mode" -> "DROPMALFORMED"))

    assert(result.left.toOption.contains(
      "from_protobuf DROPMALFORMED mode is not supported on GPU"))
  }

  test("compat treats unknown parse modes as permissive") {
    assert(SparkProtobufCompat.parsePlannerOptions(Map("mode" -> "unknown"),
      permissiveReturnsNullRow = false) == permissiveOnGpu)
  }

  test("compat reads option keys case-insensitively like Spark") {
    assert(SparkProtobufCompat.parsePlannerOptions(Map("MODE" -> "PERMISSIVE"),
      permissiveReturnsNullRow = false) == permissiveOnGpu)
    assert(SparkProtobufCompat.parsePlannerOptions(Map("Enums.As.Ints" -> "true")) ==
      Right(ProtobufPlannerOptions(enumsAsInts = isSpark350OrLater, failOnErrors = true)))
    assert(SparkProtobufCompat.unsupportedOptions(
      Map("MODE" -> "FAILFAST", "Enums.As.Ints" -> "true")).isEmpty)
  }

  test("compat distinguishes decode semantics across message descriptor and options") {
    val child = FakeExprChild()

    assert(SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakePathProtobufExpr(child)))
    assert(SparkProtobufCompat.sameDecodeSemantics(
      FakeBytesProtobufExpr(child), FakeBytesProtobufExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentMessageExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentDescriptorExpr(child)))
    assert(!SparkProtobufCompat.sameDecodeSemantics(
      FakePathProtobufExpr(child), FakeDifferentOptionsExpr(child)))
  }

  test("compat reports missing options accessor as cpu fallback reason") {
    val exprInfo = SparkProtobufCompat.extractExprInfo(FakeMissingOptionsExpr(FakeExprChild()))
    assert(exprInfo.left.toOption.exists(
      _.contains("Cannot read from_protobuf options via reflection")))
  }

  test("compat detects unsupported options and proto3 syntax") {
    assert(SparkProtobufCompat.unsupportedOptions(Map("mode" -> "FAILFAST", "foo" -> "bar")) ==
      Seq("foo"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax("PROTO3"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax("EDITIONS"))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax(""))
    assert(!SparkProtobufCompat.isGpuSupportedProtoSyntax("null"))
    assert(SparkProtobufCompat.isGpuSupportedProtoSyntax("PROTO2"))
  }

  test("compat reads syntax through protobuf 4 FileDescriptor API") {
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeModernDescriptor("proto2")) == "PROTO2")
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeModernDescriptor("proto3")) == "PROTO3")
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeModernDescriptor("")) == "PROTO2")
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeLegacyDescriptor("PROTO2")) == "PROTO2")
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeBrokenDescriptor) == "")
    assert(SparkProtobufCompat.readDescriptorSyntax(
      new FakeDescriptorWithoutFile) == "")
  }

  test("compat rejects imported proto3 message and enum types") {
    val childSchema = StructType(Seq(StructField("value", IntegerType, nullable = true)))
    val proto3Child = FakeMessageDescriptor(
      syntax = "PROTO3",
      fields = Map("value" -> FakeFieldDescriptor("value", 1, "INT32")))
    val messageRoot = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map("child" -> FakeFieldDescriptor(
        name = "child",
        fieldNumber = 1,
        protoTypeName = "MESSAGE",
        messageDescriptor = Some(proto3Child),
        referencedTypeSyntax = Some("PROTO3"))))
    val messageSchema = StructType(Seq(
      StructField("child", childSchema, nullable = true)))

    val messageResult = SparkProtobufCompat.validateDescriptorGraph(
      messageSchema, messageRoot)
    assert(messageResult.left.toOption.exists(_.contains("field 'child'")))

    val enumRoot = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map("status" -> FakeFieldDescriptor(
        name = "status",
        fieldNumber = 1,
        protoTypeName = "ENUM",
        referencedTypeSyntax = Some("PROTO3"))))
    val enumSchema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val enumResult = SparkProtobufCompat.validateDescriptorGraph(enumSchema, enumRoot)
    assert(enumResult.left.toOption.exists(_.contains("field 'status'")))
  }

  test("compat rejects strict proto2 UTF-8 validation") {
    val schema = StructType(Seq(StructField("value", StringType, nullable = true)))
    val root = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map("value" -> FakeFieldDescriptor("value", 1, "STRING")),
      javaStringCheckUtf8 = true)

    val result = SparkProtobufCompat.validateDescriptorGraph(schema, root)
    assert(result.left.toOption.exists(_.contains("strict protobuf UTF-8 validation")))
  }

  test("compat rejects descriptor fields missing from the Spark schema") {
    // Spark drops fields of empty message types, but still validates them when parsing.
    val empty = FakeMessageDescriptor(syntax = "PROTO2", fields = Map.empty)
    val emptyField = FakeFieldDescriptor(
      name = "empty",
      fieldNumber = 2,
      protoTypeName = "MESSAGE",
      isRequired = true,
      messageDescriptor = Some(empty),
      referencedTypeSyntax = Some("PROTO2"))
    val child = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "value" -> FakeFieldDescriptor("value", 1, "INT32"),
        "empty" -> emptyField))
    val childSchema = StructType(Seq(StructField("value", IntegerType, nullable = true)))

    val rootResult = SparkProtobufCompat.validateDescriptorGraph(childSchema, child)
    assert(rootResult.left.toOption.exists(
      _.contains("root message has fields missing from the Spark schema: empty")))

    val root = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map("child" -> FakeFieldDescriptor(
        name = "child",
        fieldNumber = 1,
        protoTypeName = "MESSAGE",
        messageDescriptor = Some(child),
        referencedTypeSyntax = Some("PROTO2"))))
    val schema = StructType(Seq(StructField("child", childSchema, nullable = true)))

    val nestedResult = SparkProtobufCompat.validateDescriptorGraph(schema, root)
    assert(nestedResult.left.toOption.exists(
      _.contains("protobuf field 'child' has fields missing from the Spark schema: empty")))
  }

  test("compat returns Left for unsupported default value types") {
    val result = SparkProtobufCompat.toDefaultValue("opaque-default", "MESSAGE", None)

    assert(result.left.toOption.exists(_.contains("Unsupported protobuf default value type")))
  }

  test("extractor preserves typed enum defaults") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "EN"),
      ProtobufEnumValue(2, "ZH")))
    val msgDesc = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "language" -> FakeFieldDescriptor(
          name = "language",
          fieldNumber = 1,
          protoTypeName = "ENUM",
          defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
          enumMetadata = Some(enumMeta))))
    val schema = StructType(Seq(StructField("language", StringType, nullable = true)))

    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, msgDesc, enumsAsInts = false, "test.Message")

    assert(infos.isRight)
    assert(infos.toOption.get("language").defaultValue.contains(
      ProtobufDefaultValue.EnumValue(1, "EN")))
  }

  test("extractor does not synthesize implicit enum defaults") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(10, "TEN"),
      ProtobufEnumValue(1, "ONE"),
      ProtobufEnumValue(5, "FIVE")))
    val msgDesc = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "status" -> FakeFieldDescriptor(
          name = "status",
          fieldNumber = 1,
          protoTypeName = "ENUM",
          defaultValue = None,
          enumMetadata = Some(enumMeta))))
    val schema = StructType(Seq(StructField("status", StringType, nullable = true)))

    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, msgDesc, enumsAsInts = false, "test.Message")

    assert(infos.isRight)
    assert(infos.toOption.get("status").defaultValue.isEmpty)
  }

  test("enum metadata sorts values and names by number") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(10, "TEN"),
      ProtobufEnumValue(1, "ONE"),
      ProtobufEnumValue(5, "FIVE")))

    assert(enumMeta.validValues.sameElements(Array(1, 5, 10)))
    assert(enumMeta.orderedNames.map(new String(_, "UTF-8"))
      .sameElements(Array("ONE", "FIVE", "TEN")))
    assert(!enumMeta.hasAliases)
  }

  test("extractor records reflection failures as unsupported field info") {
    val msgDesc = FakeMessageDescriptor(
      syntax = "PROTO2",
      fields = Map(
        "ok" -> FakeFieldDescriptor(
          name = "ok",
          fieldNumber = 1,
          protoTypeName = "INT32"),
        "id" -> FakeFieldDescriptor(
          name = "id",
          fieldNumber = 2,
          protoTypeName = "INT32",
          defaultValueError =
            Some("Failed to read protobuf default value for field 'id': unsupported type"))))
    val schema = StructType(Seq(
      StructField("ok", IntegerType, nullable = true),
      StructField("id", IntegerType, nullable = true)))

    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, msgDesc, enumsAsInts = true, "test.Message")

    assert(infos.isRight)
    assert(infos.toOption.get("ok").isSupported)
    assert(!infos.toOption.get("id").isSupported)
    assert(infos.toOption.get("id").unsupportedReason.exists(
      _.contains("Failed to read protobuf default value for field 'id'")))
  }

  test("extractor preserves type mismatch reason over default reflection failure") {
    val fieldInfo = ProtobufSchemaExtractor.extractFieldInfo(
      StructField("id", StringType, nullable = true),
      FakeFieldDescriptor(
        name = "id",
        fieldNumber = 1,
        protoTypeName = "INT32",
        defaultValueError =
          Some("Failed to read protobuf default value for field 'id': unsupported type")),
      enumsAsInts = true)

    assert(fieldInfo.isRight)
    assert(!fieldInfo.toOption.get.isSupported)
    assert(fieldInfo.toOption.get.unsupportedReason.contains(
      "type mismatch: Spark StringType vs Protobuf INT32"))
  }

  test("extractor rejects oneof fields") {
    val fieldInfo = ProtobufSchemaExtractor.extractFieldInfo(
      StructField("value", IntegerType, nullable = true),
      FakeFieldDescriptor(
        name = "value",
        fieldNumber = 1,
        protoTypeName = "INT32",
        isInOneof = true),
      enumsAsInts = true)

    assert(fieldInfo.isRight)
    assert(!fieldInfo.toOption.get.isSupported)
    assert(fieldInfo.toOption.get.unsupportedReason.contains(
      "protobuf oneof fields are not supported on GPU"))
  }

  test("extractor gives explicit reason for unsupported FLOAT/DOUBLE widening mismatches") {
    val doubleFromFloat = ProtobufSchemaExtractor.extractFieldInfo(
      StructField("score", DoubleType, nullable = true),
      FakeFieldDescriptor(
        name = "score",
        fieldNumber = 1,
        protoTypeName = "FLOAT"),
      enumsAsInts = true)
    val floatFromDouble = ProtobufSchemaExtractor.extractFieldInfo(
      StructField("score", FloatType, nullable = true),
      FakeFieldDescriptor(
        name = "score",
        fieldNumber = 1,
        protoTypeName = "DOUBLE"),
      enumsAsInts = true)

    assert(doubleFromFloat.isRight)
    assert(!doubleFromFloat.toOption.get.isSupported)
    assert(doubleFromFloat.toOption.get.unsupportedReason.contains(
      "Spark DoubleType mapped to Protobuf FLOAT is not yet supported on GPU; " +
        "use FloatType or fall back to CPU"))
    assert(floatFromDouble.isRight)
    assert(!floatFromDouble.toOption.get.isSupported)
    assert(floatFromDouble.toOption.get.unsupportedReason.contains(
      "Spark FloatType mapped to Protobuf DOUBLE is not yet supported on GPU; " +
        "use DoubleType or fall back to CPU"))
  }

  test("validator preserves integer default semantics when widening uint32") {
    for {
      protoType <- Seq("UINT32", "INT32", "SINT32")
      (rawDefault, unsignedDefault) <- Seq(
        (0, 0L), (Int.MaxValue, Int.MaxValue.toLong),
        (Int.MinValue, 1L << 31), (-1, (1L << 32) - 1))
      sparkType <- Seq(IntegerType, LongType)
    } {
      withClue(s"$protoType raw default=$rawDefault output=$sparkType: ") {
        val defaultValue = SparkProtobufCompat.toDefaultValue(
          java.lang.Integer.valueOf(rawDefault), protoType, None)
          .fold(fail(_), identity)
        val field = StructField("value", sparkType)
        val info = ProtobufSchemaExtractor.extractFieldInfo(field,
          FakeFieldDescriptor("value", 1, protoType, defaultValue = Some(defaultValue)),
          enumsAsInts = false).fold(fail(_), identity)
        assert(info.isSupported)
        val outputType = if (sparkType == LongType) DType.INT64 else DType.INT32
        val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
          "value", field, info, -1, 0, outputType.getTypeId.getNativeId, isOutput = true)
          .fold(fail(_), identity)
        val expected = if (protoType == "UINT32" && sparkType == LongType) {
          unsignedDefault
        } else {
          rawDefault.toLong
        }
        assert(flat.hasDefaultValue)
        assert(flat.defaultInt == expected)
      }
    }
  }

  test("validator encodes enum-string defaults into both numeric and string payloads") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "EN")))
    val info = ProtobufFieldInfo(
      fieldNumber = 2,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = Protobuf.ENC_ENUM_STRING,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
      enumMetadata = Some(enumMeta),
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.language",
      field = StructField("language", StringType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6,
      isOutput = true)

    assert(flat.isRight)
    assert(flat.toOption.get.defaultInt == 1L)
    assert(new String(flat.toOption.get.defaultString, "UTF-8") == "EN")
    assert(flat.toOption.get.enumValidValues.sameElements(Array(0, 1)))
    assert(flat.toOption.get.enumNames
      .map(new String(_, "UTF-8"))
      .sameElements(Array("UNKNOWN", "EN")))
  }

  test("validator rejects enum-string field without enum metadata") {
    val info = ProtobufFieldInfo(
      fieldNumber = 2,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = Protobuf.ENC_ENUM_STRING,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
      enumMetadata = None,
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.language",
      field = StructField("language", StringType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6,
      isOutput = true)

    assert(flat.left.toOption.exists(_.contains("missing enum metadata")))
  }

  test("validator rejects enum aliases before JNI construction") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(0, "UNKNOWN"),
      ProtobufEnumValue(1, "FIRST"),
      ProtobufEnumValue(1, "ALIAS")))
    val info = ProtobufFieldInfo(
      fieldNumber = 1,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = Protobuf.ENC_ENUM_STRING,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = None,
      enumMetadata = Some(enumMeta))

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "status",
      field = StructField("status", StringType, nullable = true),
      fieldInfo = info,
      parentIdx = -1,
      depth = 0,
      outputTypeId = DType.STRING.getTypeId.getNativeId,
      isOutput = true)

    assert(flat.left.toOption.exists(_.contains("Enum aliases are not supported")))
  }

  test("validator rejects bytes defaults before JNI construction") {
    val info = ProtobufFieldInfo(
      fieldNumber = 1,
      protoTypeName = "BYTES",
      sparkType = BinaryType,
      encoding = Protobuf.ENC_DEFAULT,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.BinaryValue(Array[Byte](1, 2))),
      enumMetadata = None)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "payload",
      field = StructField("payload", BinaryType, nullable = true),
      fieldInfo = info,
      parentIdx = -1,
      depth = 0,
      outputTypeId = DType.LIST.getTypeId.getNativeId,
      isOutput = true)

    assert(flat.left.toOption.exists(_.contains("bytes defaults are not supported")))
  }

  test("validator returns Left for incompatible default type instead of throwing") {
    val info = ProtobufFieldInfo(
      fieldNumber = 3,
      protoTypeName = "FLOAT",
      sparkType = DoubleType,
      encoding = Protobuf.ENC_DEFAULT,
      isSupported = true,
      unsupportedReason = None,
      isRequired = false,
      defaultValue = Some(ProtobufDefaultValue.FloatValue(1.5f)),
      enumMetadata = None,
      isRepeated = false)

    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
      path = "common.score",
      field = StructField("score", DoubleType, nullable = true),
      fieldInfo = info,
      parentIdx = 0,
      depth = 1,
      outputTypeId = 6,
      isOutput = true)

    assert(flat.left.toOption.exists(
      _.contains("Incompatible default value for protobuf field 'common.score'")))
  }

  test("struct field meta resolves ordinal from converted child schema") {
    val originalStruct = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("c", IntegerType, nullable = true)))
    val prunedStruct = StructType(Seq(StructField("c", IntegerType, nullable = true)))
    val originalChild = FakeTypedUnaryExpr(originalStruct)
    val sparkExpr = GetStructField(originalChild, 2, Some("c"))
    val prunedChild = FakeTypedUnaryExpr(prunedStruct)

    assert(GpuGetStructFieldMeta.resolveField(sparkExpr, prunedChild.dataType) ==
      Right((0, prunedStruct.fields.head)))
  }

  test("array struct field meta resolves ordinal and numFields from converted child schema") {
    val originalStruct = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("c", IntegerType, nullable = true)))
    val prunedStruct = StructType(Seq(StructField("b", IntegerType, nullable = true)))
    val originalChild = FakeTypedUnaryExpr(ArrayType(originalStruct, containsNull = true))
    val sparkExpr = GetArrayStructFields(
      child = originalChild,
      field = originalStruct.fields(1),
      ordinal = 1,
      numFields = originalStruct.fields.length,
      containsNull = true)

    val prunedChild = FakeTypedUnaryExpr(ArrayType(prunedStruct, containsNull = true))

    assert(GpuGetArrayStructFieldsMeta.resolveField(sparkExpr, prunedChild.dataType) ==
      Right((0, prunedStruct.fields.head, 1)))
  }

  private def twoFieldSchemaArrays(): FlattenedSchemaArrays = FlattenedSchemaArrays(
    fieldNumbers = Array(1, 2),
    parentIndices = Array(-1, -1),
    depthLevels = Array(0, 0),
    wireTypes = Array(0, 2),
    outputTypeIds = Array(3, 6),
    encodings = Array(0, 0),
    isRepeated = Array(false, false),
    isRequired = Array(false, false),
    hasDefaultValue = Array(false, false),
    isOutput = Array(true, true),
    defaultInts = Array(0L, 0L),
    defaultFloats = Array(0.0, 0.0),
    defaultBools = Array(false, false),
    defaultStrings = Array(Array[Byte](1, 2), Array[Byte](3)),
    enumValidValues = Array(Array(0, 1), Array.emptyIntArray),
    enumNames = Array(Array("UNKNOWN".getBytes("UTF-8"), "EN".getBytes("UTF-8")),
      Array.empty[Array[Byte]]))

  test("GpuFromProtobuf semantic equality is content-based for schema arrays") {
    val expr1 = GpuFromProtobuf(outputSchema, twoFieldSchemaArrays(), failOnErrors = true,
      malformedRowsAsNullFields = false, FakeExprChild())
    val expr2 = GpuFromProtobuf(outputSchema, twoFieldSchemaArrays(), failOnErrors = true,
      malformedRowsAsNullFields = false, FakeExprChild())

    assert(expr1 == expr2)
    assert(expr1.hashCode() == expr2.hashCode())
    assert(scala.collection.immutable.HashSet(expr1).contains(expr2))
    assert(expr1.semanticEquals(expr2))
    assert(expr1.semanticHash() == expr2.semanticHash())
  }

  test("GpuFromProtobuf has side effects only when malformed input throws") {
    def expr(failOnErrors: Boolean) = GpuFromProtobuf(
      outputSchema, twoFieldSchemaArrays(), failOnErrors, malformedRowsAsNullFields = false,
      GpuLiteral(null, BinaryType))

    assert(expr(failOnErrors = true).hasSideEffects)
    assert(!expr(failOnErrors = false).hasSideEffects)
  }

  test("protobuf binary defaults use content-based equality") {
    val left = ProtobufDefaultValue.BinaryValue(Array[Byte](1, 2, 3))
    val right = ProtobufDefaultValue.BinaryValue(Array[Byte](1, 2, 3))

    assert(left == right)
    assert(left.hashCode() == right.hashCode())
  }

  test("flattened field descriptor uses content-based equality for array fields") {
    val left = FlattenedFieldDescriptor(
      fieldNumber = 1,
      parentIdx = -1,
      depth = 0,
      wireType = 2,
      outputTypeId = 6,
      encoding = 0,
      isRepeated = false,
      isRequired = false,
      hasDefaultValue = true,
      isOutput = true,
      defaultInt = 0L,
      defaultFloat = 0.0,
      defaultBool = false,
      defaultString = Array[Byte](1, 2),
      enumValidValues = Array(0, 1),
      enumNames = Array("A".getBytes("UTF-8"), "B".getBytes("UTF-8")))
    val right = FlattenedFieldDescriptor(
      fieldNumber = 1,
      parentIdx = -1,
      depth = 0,
      wireType = 2,
      outputTypeId = 6,
      encoding = 0,
      isRepeated = false,
      isRequired = false,
      hasDefaultValue = true,
      isOutput = true,
      defaultInt = 0L,
      defaultFloat = 0.0,
      defaultBool = false,
      defaultString = Array[Byte](1, 2),
      enumValidValues = Array(0, 1),
      enumNames = Array("A".getBytes("UTF-8"), "B".getBytes("UTF-8")))

    assert(left == right)
    assert(left.hashCode() == right.hashCode())
  }
}
