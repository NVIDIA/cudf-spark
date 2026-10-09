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

import scala.collection.mutable

import com.nvidia.spark.rapids.jni.Protobuf.{WT_32BIT, WT_64BIT, WT_LEN, WT_VARINT}

import org.apache.spark.sql.rapids.GpuFromProtobuf
import org.apache.spark.sql.types._

object ProtobufSchemaExtractor {
  def analyzeAllFields(
      schema: StructType,
      msgDesc: ProtobufMessageDescriptor,
      enumsAsInts: Boolean,
      messageName: String): Either[String, Map[String, ProtobufFieldInfo]] = {
    val result = mutable.Map[String, ProtobufFieldInfo]()

    schema.fields.foreach { sf =>
      val fieldInfo = msgDesc.findField(sf.name) match {
        case None =>
          unsupportedFieldInfo(
            sf,
            None,
            s"Protobuf field '${sf.name}' not found in message '$messageName'")
        case Some(fd) =>
          extractFieldInfo(sf, fd, enumsAsInts) match {
            case Right(info) =>
              info
            case Left(reason) =>
              unsupportedFieldInfo(sf, Some(fd), reason)
          }
      }
      result(sf.name) = fieldInfo
    }

    Right(result.toMap)
  }

  def extractFieldInfo(
      sparkField: StructField,
      fieldDescriptor: ProtobufFieldDescriptor,
      enumsAsInts: Boolean): Either[String, ProtobufFieldInfo] = {
    val (isSupported, unsupportedReason, encoding) = if (fieldDescriptor.isInOneof) {
      (false,
        Some("protobuf oneof fields are not supported on GPU"),
        GpuFromProtobuf.ENC_DEFAULT)
    } else {
      checkFieldSupport(
        sparkField.dataType,
        fieldDescriptor.protoTypeName,
        fieldDescriptor.isRepeated,
        enumsAsInts)
    }

    val defaultValue = fieldDescriptor.defaultValueResult match {
      case Right(value) =>
        // Spark emits null for an absent enum unless its descriptor declares an explicit default.
        value
      case Left(_) if !isSupported =>
        // Preserve the primary unsupported reason from checkFieldSupport for fields that are
        // already known to be unsupported. Reflection/default extraction errors on those fields
        // should not mask the more actionable type-support message.
        None
      case Left(reason) =>
        return Left(reason)
    }

    Right(
      ProtobufFieldInfo(
        fieldNumber = fieldDescriptor.fieldNumber,
        protoTypeName = fieldDescriptor.protoTypeName,
        sparkType = sparkField.dataType,
        encoding = encoding,
        isSupported = isSupported,
        unsupportedReason = unsupportedReason,
        isRequired = fieldDescriptor.isRequired,
        defaultValue = defaultValue,
        enumMetadata = fieldDescriptor.enumMetadata,
        isRepeated = fieldDescriptor.isRepeated
      ))
  }

  private def unsupportedFieldInfo(
      sparkField: StructField,
      fieldDescriptor: Option[ProtobufFieldDescriptor],
      reason: String): ProtobufFieldInfo = {
    ProtobufFieldInfo(
      fieldNumber = fieldDescriptor.map(_.fieldNumber).getOrElse(-1),
      protoTypeName = fieldDescriptor.map(_.protoTypeName).getOrElse("UNKNOWN"),
      sparkType = sparkField.dataType,
      encoding = GpuFromProtobuf.ENC_DEFAULT,
      isSupported = false,
      unsupportedReason = Some(reason),
      isRequired = fieldDescriptor.exists(_.isRequired),
      defaultValue = None,
      enumMetadata = fieldDescriptor.flatMap(_.enumMetadata),
      isRepeated = fieldDescriptor.exists(_.isRepeated)
    )
  }

  def checkFieldSupport(
      sparkType: DataType,
      protoTypeName: String,
      isRepeated: Boolean,
      enumsAsInts: Boolean): (Boolean, Option[String], Int) = {

    if (isRepeated) {
      sparkType match {
        case ArrayType(elementType, _) =>
          elementType match {
            case BooleanType | IntegerType | LongType | FloatType | DoubleType |
                 StringType | BinaryType =>
              return checkScalarEncoding(elementType, protoTypeName, enumsAsInts)
            case _: StructType =>
              return (true, None, GpuFromProtobuf.ENC_DEFAULT)
            case _ =>
              return (
                false,
                Some(s"unsupported repeated element type: $elementType"),
                GpuFromProtobuf.ENC_DEFAULT)
          }
        case _ =>
          return (
            false,
            Some(s"repeated field should map to ArrayType, got: $sparkType"),
            GpuFromProtobuf.ENC_DEFAULT)
      }
    }

    if (protoTypeName == "MESSAGE") {
      sparkType match {
        case _: StructType =>
          return (true, None, GpuFromProtobuf.ENC_DEFAULT)
        case _ =>
          return (
            false,
            Some(s"nested message should map to StructType, got: $sparkType"),
            GpuFromProtobuf.ENC_DEFAULT)
      }
    }

    sparkType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType |
           StringType | BinaryType =>
      case other =>
        return (
          false,
          Some(s"unsupported Spark type: $other"),
          GpuFromProtobuf.ENC_DEFAULT)
    }

    checkScalarEncoding(sparkType, protoTypeName, enumsAsInts)
  }

  def checkScalarEncoding(
      sparkType: DataType,
      protoTypeName: String,
      enumsAsInts: Boolean): (Boolean, Option[String], Int) = {
    val encoding = (sparkType, protoTypeName) match {
      case (BooleanType, "BOOL") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (IntegerType, "INT32" | "UINT32") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (IntegerType, "SINT32") => Some(GpuFromProtobuf.ENC_ZIGZAG)
      case (IntegerType, "FIXED32" | "SFIXED32") => Some(GpuFromProtobuf.ENC_FIXED)
      case (LongType, "INT64" | "UINT64") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (LongType, "SINT64") => Some(GpuFromProtobuf.ENC_ZIGZAG)
      case (LongType, "FIXED64" | "SFIXED64") => Some(GpuFromProtobuf.ENC_FIXED)
      case (LongType, "INT32" | "UINT32" | "SINT32" | "FIXED32" | "SFIXED32") =>
        val enc = protoTypeName match {
          case "SINT32" => GpuFromProtobuf.ENC_ZIGZAG
          case "FIXED32" | "SFIXED32" => GpuFromProtobuf.ENC_FIXED
          case _ => GpuFromProtobuf.ENC_DEFAULT
        }
        Some(enc)
      case (FloatType, "FLOAT") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (DoubleType, "DOUBLE") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (StringType, "STRING") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (BinaryType, "BYTES") => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (IntegerType, "ENUM") if enumsAsInts => Some(GpuFromProtobuf.ENC_DEFAULT)
      case (StringType, "ENUM") if !enumsAsInts => Some(GpuFromProtobuf.ENC_ENUM_STRING)
      case _ => None
    }

    encoding match {
      case Some(enc) => (true, None, enc)
      case None =>
        val reason = (sparkType, protoTypeName) match {
          case (DoubleType, "FLOAT") =>
            "Spark DoubleType mapped to Protobuf FLOAT is not yet supported on GPU; " +
              "use FloatType or fall back to CPU"
          case (FloatType, "DOUBLE") =>
            "Spark FloatType mapped to Protobuf DOUBLE is not yet supported on GPU; " +
              "use DoubleType or fall back to CPU"
          case _ =>
            s"type mismatch: Spark $sparkType vs Protobuf $protoTypeName"
        }
        (false,
          Some(reason),
          GpuFromProtobuf.ENC_DEFAULT)
    }
  }

  def getWireType(protoTypeName: String, encoding: Int): Either[String, Int] = {
    val wireType = protoTypeName match {
      case "BOOL" | "INT32" | "UINT32" | "SINT32" | "INT64" | "UINT64" | "SINT64" | "ENUM" =>
        if (encoding == GpuFromProtobuf.ENC_FIXED) {
          if (protoTypeName.contains("64")) {
            WT_64BIT
          } else {
            WT_32BIT
          }
        } else {
          WT_VARINT
        }
      case "FIXED32" | "SFIXED32" | "FLOAT" =>
        WT_32BIT
      case "FIXED64" | "SFIXED64" | "DOUBLE" =>
        WT_64BIT
      case "STRING" | "BYTES" | "MESSAGE" =>
        WT_LEN
      case other =>
        return Left(
          s"Unknown protobuf type name '$other' - cannot determine wire type; falling back to CPU")
    }
    Right(wireType)
  }
}
