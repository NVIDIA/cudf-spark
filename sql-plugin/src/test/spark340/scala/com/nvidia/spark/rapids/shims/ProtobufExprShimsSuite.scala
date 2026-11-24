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
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400","420"],"carryForward":"all"}
{"spark":["350db143","400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{GpuLiteral, VersionUtils}
import com.nvidia.spark.rapids.jni.Protobuf.ENC_DEFAULT
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.rapids.protobuf.{FlattenedSchemaArrays, ProtobufDefaultValue,
  ProtobufEnumMetadata, ProtobufFieldDescriptor, ProtobufFieldInfo, ProtobufMessageDescriptor,
  ProtobufPlannerOptions, ProtobufSchemaValidator}
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

  test("compat detects unsupported options") {
    assert(SparkProtobufCompat.unsupportedOptions(Map("mode" -> "FAILFAST", "foo" -> "bar")) ==
      Seq("foo"))
  }

  private def fakeMessage(fields: Seq[ProtobufFieldDescriptor]): ProtobufMessageDescriptor =
    new ProtobufMessageDescriptor {
      def syntax: String = "PROTO2"
      def findField(name: String): Option[ProtobufFieldDescriptor] = fields.find(_.name == name)
      def fieldNames: Seq[String] = fields.map(_.name)
    }

  private def fakeField(
      fieldName: String,
      number: Int,
      typeName: String,
      nested: Option[ProtobufMessageDescriptor] = None): ProtobufFieldDescriptor =
    new ProtobufFieldDescriptor {
      def name: String = fieldName
      def fieldNumber: Int = number
      def protoTypeName: String = typeName
      def isRepeated: Boolean = false
      def isRequired: Boolean = false
      def isInOneof: Boolean = false
      def explicitDefaultValue: Either[String, Option[ProtobufDefaultValue]] = Right(None)
      def enumMetadata: Option[ProtobufEnumMetadata] = None
      def messageDescriptor: Option[ProtobufMessageDescriptor] = nested
      def referencedTypeSyntax: Option[String] = nested.map(_.syntax)
    }

  private val emptyMessageField = fakeField("empty", 2, "MESSAGE", Some(fakeMessage(Nil)))

  test("compat keeps empty-message fields missing from the Spark schema") {
    // Spark drops fields of empty message types, but still validates them when parsing.
    val descriptor = fakeMessage(Seq(fakeField("id", 1, "INT32"), emptyMessageField))
    val schema = StructType(Seq(StructField("id", IntegerType)))

    val hidden = SparkProtobufCompat.hiddenFieldsMissingFromSchema(
      schema, descriptor, "root message")

    assert(hidden.map(_.map(_.name)) == Right(Seq("empty")))
    assert(SparkProtobufCompat.hiddenFieldsMissingFromSchema(
      schema.add("empty", StructType(Seq(StructField("x", IntegerType)))), descriptor,
      "root message") == Right(Seq.empty))
  }

  test("compat rejects other descriptor fields missing from the Spark schema") {
    val descriptor = fakeMessage(Seq(
      fakeField("id", 1, "INT32"),
      fakeField("name", 2, "STRING"),
      fakeField("child", 3, "MESSAGE", Some(fakeMessage(Seq(fakeField("x", 1, "INT32")))))))
    val schema = StructType(Seq(StructField("id", IntegerType)))

    assert(SparkProtobufCompat.hiddenFieldsMissingFromSchema(
      schema, descriptor, "protobuf field 'child'") ==
      Left("protobuf field 'child' has fields missing from the Spark schema: name, child"))
  }

  test("the GPU decoder accepts a schema with a hidden empty-message field") {
    val idInfo = ProtobufFieldInfo(fieldNumber = 1, protoTypeName = "INT32",
      sparkType = IntegerType, encoding = ENC_DEFAULT, isSupported = true,
      unsupportedReason = None, isRequired = false, defaultValue = None, enumMetadata = None)
    val id = ProtobufSchemaValidator.toFlattenedFieldDescriptor("id",
      StructField("id", IntegerType), idInfo, parentIdx = -1, depth = 0,
      outputTypeId = DType.INT32.getTypeId.getNativeId, isOutput = true)
    val hidden = ProtobufSchemaValidator.toHiddenMessageFieldDescriptor(emptyMessageField,
      parentIdx = -1, depth = 0, outputTypeId = DType.STRUCT.getTypeId.getNativeId)

    val flatFields = (id, hidden) match {
      case (Right(idField), Right(hiddenField)) => Array(idField, hiddenField)
      case other => fail(s"unexpected planning failure: $other")
    }
    val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(flatFields)

    assert(arrays.isOutput.toSeq == Seq(true, false))
    // The JNI constructor rejects schemas the decoder cannot run.
    assert(arrays.toJniDescriptor.numFields() == 2)
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
}
