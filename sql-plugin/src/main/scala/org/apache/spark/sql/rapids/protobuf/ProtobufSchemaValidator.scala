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

package org.apache.spark.sql.rapids.protobuf

import com.nvidia.spark.rapids.jni.Protobuf._

import org.apache.spark.sql.types._

object ProtobufSchemaValidator {
  // Keep planner rejection aligned with JNI so unsupported schemas fall back before execution.
  val MAX_REPEATED_FIELDS_PER_MESSAGE: Int = 32

  private final case class JniDefaultValues(
      defaultInt: Long,
      defaultFloat: Double,
      defaultBool: Boolean,
      defaultString: Array[Byte])

  def toFlattenedFieldDescriptor(
      path: String,
      field: StructField,
      fieldInfo: ProtobufFieldInfo,
      parentIdx: Int,
      depth: Int,
      outputTypeId: Int,
      isOutput: Boolean): Either[String, FlattenedFieldDescriptor] = {
    validateFieldInfo(path, field, fieldInfo).flatMap { _ =>
      ProtobufSchemaExtractor
        .getWireType(fieldInfo.protoTypeName, fieldInfo.encoding)
        .flatMap { wireType =>
          encodeDefaultValue(path, field.dataType, fieldInfo).map { defaults =>
            val enumValidValues = fieldInfo.enumMetadata.map(_.validValues).orNull
            val enumNames =
              if (fieldInfo.encoding == ENC_ENUM_STRING) {
                fieldInfo.enumMetadata.map(_.orderedNames).orNull
              } else {
                null
              }

            FlattenedFieldDescriptor(
              fieldNumber = fieldInfo.fieldNumber,
              parentIdx = parentIdx,
              depth = depth,
              wireType = wireType,
              outputTypeId = outputTypeId,
              encoding = fieldInfo.encoding,
              isRepeated = fieldInfo.isRepeated,
              isRequired = fieldInfo.isRequired,
              hasDefaultValue = fieldInfo.hasDefaultValue,
              isOutput = isOutput,
              defaultInt = defaults.defaultInt,
              defaultFloat = defaults.defaultFloat,
              defaultBool = defaults.defaultBool,
              defaultString = defaults.defaultString,
              enumValidValues = enumValidValues,
              enumNames = enumNames
            )
          }
        }
    }
  }

  def validateFlattenedSchema(flatFields: Seq[FlattenedFieldDescriptor]): Either[String, Unit] = {
    var repeatedCount = 0
    flatFields.zipWithIndex.foreach { case (field, idx) =>
      if (field.parentIdx != -1 || field.depth != 0) {
        return Left(s"Only top-level protobuf fields are supported at position $idx")
      }
      if (field.isRepeated) {
        repeatedCount += 1
        if (repeatedCount > MAX_REPEATED_FIELDS_PER_MESSAGE) {
          return Left(
            s"Protobuf schema exceeds maximum supported repeated fields per message " +
              s"($MAX_REPEATED_FIELDS_PER_MESSAGE) at parent index ${field.parentIdx}")
        }
      }
      if (field.encoding == ENC_ENUM_STRING) {
        if (field.enumValidValues == null || field.enumNames == null) {
          return Left(s"Enum-string field at position $idx is missing enum metadata")
        }
        if (field.enumValidValues.length != field.enumNames.length) {
          return Left(s"Enum-string field at position $idx has mismatched enum metadata")
        }
      }
    }
    Right(())
  }

  def toFlattenedSchemaArrays(
      flatFields: Array[FlattenedFieldDescriptor]): FlattenedSchemaArrays = {
    FlattenedSchemaArrays(
      fieldNumbers = flatFields.map(_.fieldNumber),
      parentIndices = flatFields.map(_.parentIdx),
      depthLevels = flatFields.map(_.depth),
      wireTypes = flatFields.map(_.wireType),
      outputTypeIds = flatFields.map(_.outputTypeId),
      encodings = flatFields.map(_.encoding),
      isRepeated = flatFields.map(_.isRepeated),
      isRequired = flatFields.map(_.isRequired),
      hasDefaultValue = flatFields.map(_.hasDefaultValue),
      isOutput = flatFields.map(_.isOutput),
      defaultInts = flatFields.map(_.defaultInt),
      defaultFloats = flatFields.map(_.defaultFloat),
      defaultBools = flatFields.map(_.defaultBool),
      defaultStrings = flatFields.map(_.defaultString),
      enumValidValues = flatFields.map(_.enumValidValues),
      enumNames = flatFields.map(_.enumNames)
    )
  }

  private def validateFieldInfo(
      path: String,
      field: StructField,
      fieldInfo: ProtobufFieldInfo): Either[String, Unit] = {
    if (!fieldInfo.isSupported) {
      return Left(fieldInfo.unsupportedReason.getOrElse(s"Unsupported protobuf field '$path'"))
    }
    if (fieldInfo.isRepeated && fieldInfo.hasDefaultValue) {
      return Left(s"Repeated protobuf field '$path' cannot carry a default value")
    }

    fieldInfo.enumMetadata match {
      case Some(enumMeta) if enumMeta.values.isEmpty =>
        return Left(s"Enum field '$path' is missing enum values")
      case Some(enumMeta) if enumMeta.hasAliases =>
        return Left(s"Enum aliases are not supported on GPU for protobuf field '$path'")
      case Some(_) if fieldInfo.protoTypeName != "ENUM" =>
        return Left(s"Non-enum field '$path' should not carry enum metadata")
      case None if fieldInfo.protoTypeName == "ENUM" =>
        return Left(s"Enum field '$path' is missing enum metadata")
      case _ =>
    }

    if (fieldInfo.encoding == ENC_ENUM_STRING &&
        fieldInfo.enumMetadata.isEmpty) {
      return Left(s"Enum-string field '$path' is missing enum metadata")
    }

    if (field.dataType == BinaryType && fieldInfo.hasDefaultValue) {
      return Left(s"Protobuf bytes defaults are not supported on GPU for field '$path'")
    }

    Right(())
  }

  private def encodeDefaultValue(
      path: String,
      dataType: DataType,
      fieldInfo: ProtobufFieldInfo): Either[String, JniDefaultValues] = {
    val empty = JniDefaultValues(0L, 0.0, defaultBool = false, null)
    fieldInfo.defaultValue match {
      case None => Right(empty)
      case Some(defaultValue) =>
        val targetType = dataType match {
          case ArrayType(elementType, _) => elementType
          case other => other
        }
        (targetType, defaultValue) match {
          case (BooleanType, ProtobufDefaultValue.BoolValue(value)) =>
            Right(empty.copy(defaultBool = value))
          case (LongType, ProtobufDefaultValue.IntValue(value))
              if fieldInfo.protoTypeName == "UINT32" =>
            Right(empty.copy(defaultInt = Integer.toUnsignedLong(value.toInt)))
          case (IntegerType | LongType, ProtobufDefaultValue.IntValue(value)) =>
            Right(empty.copy(defaultInt = value))
          case (IntegerType | LongType, ProtobufDefaultValue.EnumValue(number, _)) =>
            Right(empty.copy(defaultInt = number.toLong))
          case (FloatType, ProtobufDefaultValue.FloatValue(value)) =>
            Right(empty.copy(defaultFloat = value.toDouble))
          case (DoubleType, ProtobufDefaultValue.DoubleValue(value)) =>
            Right(empty.copy(defaultFloat = value))
          case (StringType, ProtobufDefaultValue.StringValue(value)) =>
            Right(empty.copy(defaultString = value.getBytes("UTF-8")))
          case (StringType, ProtobufDefaultValue.EnumValue(number, name)) =>
            Right(empty.copy(
              defaultInt = number.toLong,
              defaultString = name.getBytes("UTF-8")))
          case _ =>
            Left(s"Incompatible default value for protobuf field '$path': $defaultValue")
        }
    }
  }
}
