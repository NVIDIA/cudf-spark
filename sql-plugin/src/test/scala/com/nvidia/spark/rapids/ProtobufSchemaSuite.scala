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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.jni.Protobuf._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.protobuf._
import org.apache.spark.sql.types._

class ProtobufSchemaSuite extends AnyFunSuite {
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

  test("prepared enum schemas do not share mutable metadata arrays") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(1, "EN"),
      ProtobufEnumValue(0, "UNKNOWN")))
    val field = StructField("language", StringType)
    val info = ProtobufSchemaExtractor.extractFieldInfo(field,
      FakeFieldDescriptor("language", 1, "ENUM", enumMetadata = Some(enumMeta)),
      enumsAsInts = false).fold(fail(_), identity)

    def prepareSchema(): FlattenedSchemaArrays = {
      val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor(
        "language", field, info, -1, 0, DType.STRING.getTypeId.getNativeId, isOutput = true)
        .fold(fail(_), identity)
      ProtobufSchemaValidator.toFlattenedSchemaArrays(Array(flat))
    }

    val first = prepareSchema()
    val second = prepareSchema()
    first.enumValidValues(0)(0) = 99
    first.enumNames(0)(0)(0) = 'X'.toByte
    first.enumNames(0)(1) = "REPLACED".getBytes("UTF-8")

    val third = prepareSchema()
    for (schema <- Seq(second, third)) {
      assert(schema.enumValidValues(0).sameElements(Array(0, 1)))
      assert(schema.enumNames(0).map(new String(_, "UTF-8"))
        .sameElements(Array("UNKNOWN", "EN")))
      assert(schema.toJniDescriptor.numFields() == 1)
    }
  }

  test("flattened fields and schemas support hash lookup by array contents") {
    for (defaultFloat <- Seq(Double.NaN, 0.0, -0.0)) {
      val field = FlattenedFieldDescriptor(1, -1, 0, WT_VARINT,
        DType.STRING.getTypeId.getNativeId, ENC_ENUM_STRING,
        false, false, true, true, 1L, defaultFloat, false,
        "EN".getBytes("UTF-8"), Array(0, 1),
        Array("UNKNOWN".getBytes("UTF-8"), "EN".getBytes("UTF-8")))
      val equalField = field.copy(defaultString = field.defaultString.clone(),
        enumValidValues = field.enumValidValues.clone(),
        enumNames = field.enumNames.map(_.clone()))
      assert(field == equalField)
      assert(field.hashCode() == equalField.hashCode())
      assert(scala.collection.immutable.HashSet(field).contains(equalField))
      val schema = ProtobufSchemaValidator.toFlattenedSchemaArrays(Array(field))
      val equalSchema = ProtobufSchemaValidator.toFlattenedSchemaArrays(Array(equalField))
      assert(schema == equalSchema)
      assert(schema.hashCode() == equalSchema.hashCode())
      assert(scala.collection.immutable.HashSet(schema).contains(equalSchema))
      val withoutPayload = field.copy(defaultString = null, enumValidValues = null,
        enumNames = null)
      assert(scala.collection.immutable.HashSet(withoutPayload).contains(withoutPayload.copy()))
      val schemaWithoutPayload = ProtobufSchemaValidator.toFlattenedSchemaArrays(
        Array(withoutPayload))
      val equalSchemaWithoutPayload = ProtobufSchemaValidator.toFlattenedSchemaArrays(
        Array(withoutPayload.copy()))
      assert(schemaWithoutPayload == equalSchemaWithoutPayload)
      assert(schemaWithoutPayload.hashCode() == equalSchemaWithoutPayload.hashCode())
      assert(scala.collection.immutable.HashSet(schemaWithoutPayload)
        .contains(equalSchemaWithoutPayload))
    }
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

  test("validator preserves integer default semantics when widening uint32") {
    for {
      protoType <- Seq("UINT32", "INT32", "SINT32")
      (rawDefault, unsignedDefault) <- Seq(
        (0, 0L), (Int.MaxValue, Int.MaxValue.toLong),
        (Int.MinValue, 1L << 31), (-1, (1L << 32) - 1))
      sparkType <- Seq(IntegerType, LongType)
    } {
      withClue(s"$protoType raw default=$rawDefault output=$sparkType: ") {
        val field = StructField("value", sparkType)
        val info = ProtobufSchemaExtractor.extractFieldInfo(field,
          FakeFieldDescriptor("value", 1, protoType,
            defaultValue = Some(ProtobufDefaultValue.IntValue(rawDefault.toLong))),
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
      encoding = ENC_ENUM_STRING,
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
      parentIdx = -1,
      depth = 0,
      outputTypeId = DType.STRING.getTypeId.getNativeId,
      isOutput = true)

    assert(flat.isRight)
    assert(flat.toOption.get.defaultInt == 1L)
    assert(new String(flat.toOption.get.defaultString, "UTF-8") == "EN")
    assert(flat.toOption.get.enumValidValues.sameElements(Array(0, 1)))
    assert(flat.toOption.get.enumNames
      .map(new String(_, "UTF-8"))
      .sameElements(Array("UNKNOWN", "EN")))
    assert(ProtobufSchemaValidator.toFlattenedSchemaArrays(
      Array(flat.toOption.get)).toJniDescriptor.numFields() == 1)
  }

  test("validator rejects enum-string field without enum metadata") {
    val info = ProtobufFieldInfo(
      fieldNumber = 2,
      protoTypeName = "ENUM",
      sparkType = StringType,
      encoding = ENC_ENUM_STRING,
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
      parentIdx = -1,
      depth = 0,
      outputTypeId = DType.STRING.getTypeId.getNativeId,
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
      encoding = ENC_ENUM_STRING,
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
      encoding = ENC_DEFAULT,
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
      protoTypeName = "DOUBLE",
      sparkType = DoubleType,
      encoding = ENC_DEFAULT,
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
      parentIdx = -1,
      depth = 0,
      outputTypeId = DType.FLOAT64.getTypeId.getNativeId,
      isOutput = true)

    assert(flat.left.toOption.exists(
      _.contains("Incompatible default value for protobuf field 'common.score'")))
  }

  test("extractor maps scalar and repeated wire encodings") {
    for ((sparkType, protoType, encoding, wireType) <- Seq(
      (BooleanType, "BOOL", ENC_DEFAULT, WT_VARINT),
      (IntegerType, "SINT32", ENC_ZIGZAG, WT_VARINT),
      (IntegerType, "FIXED32", ENC_FIXED, WT_32BIT),
      (IntegerType, "SFIXED32", ENC_FIXED, WT_32BIT),
      (LongType, "FIXED64", ENC_FIXED, WT_64BIT),
      (FloatType, "FLOAT", ENC_DEFAULT, WT_32BIT),
      (DoubleType, "DOUBLE", ENC_DEFAULT, WT_64BIT),
      (StringType, "STRING", ENC_DEFAULT, WT_LEN),
      (BinaryType, "BYTES", ENC_DEFAULT, WT_LEN))) {
      for (repeated <- Seq(false, true)) {
        val dataType = if (repeated) ArrayType(sparkType) else sparkType
        assert(ProtobufSchemaExtractor.checkFieldSupport(
          dataType, protoType, repeated, enumsAsInts = false) == ((true, None, encoding)))
        assert(ProtobufSchemaExtractor.getWireType(protoType, encoding) == Right(wireType))
      }
    }
  }

  test("extractor accepts messages but rejects groups, maps, proto3, and strict UTF-8 schemas") {
    val nested = StructType(Seq(StructField("value", IntegerType)))
    for (dataType <- Seq(nested, ArrayType(nested))) {
      val repeated = dataType.isInstanceOf[ArrayType]
      assert(ProtobufSchemaExtractor.checkFieldSupport(
        dataType, "MESSAGE", repeated, enumsAsInts = false) == ((true, None, ENC_DEFAULT)))
      assert(!ProtobufSchemaExtractor.checkFieldSupport(
        dataType, "GROUP", repeated, enumsAsInts = false)._1)
    }
    // Spark decodes protobuf map fields, which are repeated messages, as MapType.
    assert(!ProtobufSchemaExtractor.checkFieldSupport(
      MapType(StringType, IntegerType), "MESSAGE", isRepeated = true, enumsAsInts = false)._1)
    val schema = StructType(Seq(StructField("value", StringType)))
    for (descriptor <- Seq(
      FakeMessageDescriptor("PROTO3", Map.empty),
      FakeMessageDescriptor("PROTO2", Map("value" -> FakeFieldDescriptor(
        "value", 1, "STRING")), javaStringCheckUtf8 = true))) {
      assert(ProtobufSchemaExtractor.analyzeAllFields(
        schema, descriptor, enumsAsInts = false, "test.Message").isLeft)
    }
    val field = FakeFieldDescriptor("status", 1, "ENUM", referencedTypeSyntax = Some("PROTO3"))
    assert(!ProtobufSchemaExtractor.extractFieldInfo(
      StructField("status", StringType), field, enumsAsInts = false).toOption.get.isSupported)
  }

  test("nested extracted schemas satisfy the JNI descriptor contract") {
    val childSchema = StructType(Seq(StructField("id", IntegerType)))
    val childDescriptor = FakeMessageDescriptor("PROTO2",
      Map("id" -> FakeFieldDescriptor("id", 1, "INT32")))
    val schema = StructType(Seq(StructField("single", childSchema),
      StructField("items", ArrayType(childSchema))))
    val descriptor = FakeMessageDescriptor("PROTO2", Map(
      "single" -> FakeFieldDescriptor("single", 1, "MESSAGE",
        messageDescriptor = Some(childDescriptor), referencedTypeSyntax = Some("PROTO2")),
      "items" -> FakeFieldDescriptor("items", 2, "MESSAGE", isRepeated = true,
        messageDescriptor = Some(childDescriptor), referencedTypeSyntax = Some("PROTO2"))))
    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, descriptor, enumsAsInts = false, "test.Message").fold(fail(_), identity)
    val childField = childSchema.fields.head
    val childInfo = ProtobufSchemaExtractor.extractFieldInfo(childField,
      childDescriptor.fields("id"), enumsAsInts = false).fold(fail(_), identity)
    val fields = schema.fields.zipWithIndex.flatMap { case (field, index) =>
      val parent = ProtobufSchemaValidator.toFlattenedFieldDescriptor(field.name,
        field, infos(field.name), -1, 0, DType.STRUCT.getTypeId.getNativeId, isOutput = true)
        .fold(fail(_), identity)
      val child = ProtobufSchemaValidator.toFlattenedFieldDescriptor(s"${field.name}.id",
        childField, childInfo, index * 2, 1, DType.INT32.getTypeId.getNativeId, isOutput = true)
        .fold(fail(_), identity)
      Array(parent, child)
    }
    val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(fields)
    assert(arrays.parentIndices.toSeq == Seq(-1, 0, -1, 2))
    assert(arrays.depthLevels.toSeq == Seq(0, 1, 0, 1))
    assert(arrays.isRepeated.toSeq == Seq(false, false, true, false))
    assert(arrays.toJniDescriptor.numFields() == 4)
  }

  test("repeated fields per message are not capped by the occurrence-scan batch size") {
    val info = ProtobufFieldInfo(1, "INT32", ArrayType(IntegerType), ENC_DEFAULT,
      isSupported = true, None, isRequired = false, None, None, isRepeated = true)
    val flat = ProtobufSchemaValidator.toFlattenedFieldDescriptor("items",
      StructField("items", ArrayType(IntegerType)), info, -1, 0,
      DType.INT32.getTypeId.getNativeId, isOutput = true).toOption.get
    val fields = (1 to 33).map(number => flat.copy(fieldNumber = number))
    val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(fields.toArray)
    assert(arrays.fieldNumbers.toSeq == (1 to 33))
    assert(arrays.isRepeated.forall(identity))
    assert(arrays.parentIndices.forall(_ == -1))
    assert(arrays.toJniDescriptor.numFields() == 33)
  }

  test("extractor rejects fixed32 widening to LongType") {
    for (protoType <- Seq("FIXED32", "SFIXED32")) {
      for (dataType <- Seq(LongType, ArrayType(LongType))) {
        val result = ProtobufSchemaExtractor.checkFieldSupport(
          dataType, protoType, dataType.isInstanceOf[ArrayType], enumsAsInts = false)
        assert(!result._1)
        assert(result._2.exists(_.contains("type mismatch")))
      }
    }
  }

  test("extractor preserves metadata failures without retrying failed enum reads") {
    val reason = "Failed to read protobuf default: enum read failed"
    for (defaultResult <- Seq(Left(reason), Right(None))) {
      var enumReads = 0
      val field = new ProtobufFieldDescriptor {
        def name: String = "status"
        def fieldNumber: Int = 1
        def protoTypeName: String = "ENUM"
        def isRepeated: Boolean = false
        def isRequired: Boolean = false
        def isInOneof: Boolean = false
        def explicitDefaultValue: Either[String, Option[ProtobufDefaultValue]] = defaultResult
        def enumMetadata: Option[ProtobufEnumMetadata] = {
          enumReads += 1
          throw new UnsupportedOperationException("enum read failed")
        }
        def messageDescriptor: Option[ProtobufMessageDescriptor] = None
        def referencedTypeSyntax: Option[String] = Some("PROTO2")
      }
      val schema = StructType(Seq(StructField("status", StringType)))
      val result = ProtobufSchemaExtractor.analyzeAllFields(schema,
        FakeMessageDescriptor("PROTO2", Map("status" -> field)), false, "test.Message")
      val info = result.toOption.get("status")
      assert(!info.isSupported && info.enumMetadata.isEmpty)
      assert(info.unsupportedReason.exists(_.contains("enum read failed")))
      assert(enumReads == (if (defaultResult.isLeft) 0 else 1))
      assert(ProtobufSchemaExtractor.extractFieldInfo(
        schema.fields.head, field, enumsAsInts = false).isLeft)
    }
  }

  test("extractor reports message descriptor lookup failures as Left") {
    val descriptor = new ProtobufMessageDescriptor {
      def syntax: String = "PROTO2"
      def findField(name: String): Option[ProtobufFieldDescriptor] =
        throw new UnsupportedOperationException("field lookup failed")
      def fieldNames: Seq[String] = Seq("id")
    }
    val result = ProtobufSchemaExtractor.analyzeAllFields(
      StructType(Seq(StructField("id", IntegerType))), descriptor, false, "test.Message")
    assert(result.left.toOption.exists(message =>
      message.contains("test.Message") && message.contains("field lookup failed")))
  }

  test("top-level extracted schemas satisfy the JNI descriptor contract") {
    val enumMeta = ProtobufEnumMetadata(Seq(
      ProtobufEnumValue(1, "EN"), ProtobufEnumValue(0, "UNKNOWN")))
    val schema = StructType(Seq(
      StructField("id", IntegerType), StructField("name", StringType),
      StructField("language", StringType), StructField("items", ArrayType(IntegerType))))
    val descriptor = FakeMessageDescriptor("PROTO2", Map(
      "id" -> FakeFieldDescriptor("id", 1, "INT32",
        defaultValue = Some(ProtobufDefaultValue.IntValue(7))),
      "name" -> FakeFieldDescriptor("name", 2, "STRING", isRequired = true),
      "language" -> FakeFieldDescriptor("language", 3, "ENUM",
        defaultValue = Some(ProtobufDefaultValue.EnumValue(1, "EN")),
        enumMetadata = Some(enumMeta)),
      "items" -> FakeFieldDescriptor("items", 4, "SINT32", isRepeated = true)))
    val infos = ProtobufSchemaExtractor.analyzeAllFields(
      schema, descriptor, enumsAsInts = false, "test.Message").toOption.get
    val types = Seq(DType.INT32, DType.STRING, DType.STRING, DType.INT32)
    val fields = schema.fields.zip(types).map { case (field, dtype) =>
      ProtobufSchemaValidator.toFlattenedFieldDescriptor(field.name, field, infos(field.name),
        -1, 0, dtype.getTypeId.getNativeId, isOutput = true).toOption.get
    }
    val arrays = ProtobufSchemaValidator.toFlattenedSchemaArrays(fields)
    assert(arrays.fieldNumbers.toSeq == Seq(1, 2, 3, 4))
    assert(arrays.defaultInts.toSeq == Seq(7L, 0L, 1L, 0L))
    assert(arrays.isRequired.toSeq == Seq(false, true, false, false))
    assert(arrays.hasDefaultValue.toSeq == Seq(true, false, true, false))
    assert(arrays.isRepeated.toSeq == Seq(false, false, false, true))
    assert(arrays.encodings.toSeq == Seq(ENC_DEFAULT, ENC_DEFAULT, ENC_ENUM_STRING, ENC_ZIGZAG))
    assert(new String(arrays.defaultStrings(2), "UTF-8") == "EN")
    assert(arrays.toJniDescriptor.numFields() == 4)
  }
}
