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
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.jni.Protobuf._

import org.apache.spark.sql.types._

object ProtobufSchemaExtractor {
  private val PROTO2_MAGIC_STRING = "PROTO2"

  /**
   * Collects top-level field metadata keyed by Spark field name. Right may contain unsupported
   * fields; Left describes a message-level rejection or metadata read failure.
   */
  def analyzeAllFields(
      schema: StructType,
      msgDesc: ProtobufMessageDescriptor,
      enumsAsInts: Boolean,
      messageName: String): Either[String, Map[String, ProtobufFieldInfo]] = try {
    if (msgDesc.syntax != PROTO2_MAGIC_STRING) {
      return Left("Only proto2 protobuf messages are supported on GPU")
    }
    if (msgDesc.javaStringCheckUtf8 && schema.fields.exists { sf =>
      msgDesc.findField(sf.name).exists(_.protoTypeName == "STRING")
    }) {
      return Left("Strict protobuf UTF-8 validation is not supported on GPU")
    }
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
  } catch {
    case NonFatal(t) =>
      Left(s"Failed to read protobuf metadata for message '$messageName': ${t.getMessage}")
  }

  /**
   * Returns field facts, including unsupported reasons, or Left when descriptor/default reads fail.
   */
  def extractFieldInfo(
      sparkField: StructField,
      fieldDescriptor: ProtobufFieldDescriptor,
      enumsAsInts: Boolean): Either[String, ProtobufFieldInfo] = try {
    extractFieldInfoInternal(sparkField, fieldDescriptor, enumsAsInts)
  } catch {
    case NonFatal(t) =>
      Left(s"Failed to read protobuf metadata for field '${sparkField.name}': ${t.getMessage}")
  }

  private def extractFieldInfoInternal(
      sparkField: StructField,
      fieldDescriptor: ProtobufFieldDescriptor,
      enumsAsInts: Boolean): Either[String, ProtobufFieldInfo] = {
    val (isSupported, unsupportedReason, encoding) = if (fieldDescriptor.isInOneof) {
      (false,
        Some("protobuf oneof fields are not supported on GPU"),
        ENC_DEFAULT)
    } else if (fieldDescriptor.referencedTypeSyntax.exists(_ != PROTO2_MAGIC_STRING)) {
      (false, Some("Only proto2 referenced types are supported on GPU"), ENC_DEFAULT)
    } else {
      checkFieldSupport(
        sparkField.dataType,
        fieldDescriptor.protoTypeName,
        fieldDescriptor.isRepeated,
        enumsAsInts)
    }

    val defaultValue = fieldDescriptor.explicitDefaultValue match {
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
        enumMetadata = if (isSupported) fieldDescriptor.enumMetadata else None,
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
      encoding = ENC_DEFAULT,
      isSupported = false,
      unsupportedReason = Some(reason),
      isRequired = fieldDescriptor.exists(_.isRequired),
      defaultValue = None,
      // The failed default conversion may have already attempted to read enum metadata.
      enumMetadata = None,
      isRepeated = fieldDescriptor.exists(_.isRepeated)
    )
  }

  /**
   * Checks the Spark/protobuf type pairing, unwrapping repeated fields.
   * Returns (supported, optional rejection reason, JNI encoding). Syntax, oneofs, defaults and
   * enum declarations are checked separately.
   */
  def checkFieldSupport(
      sparkType: DataType,
      protoTypeName: String,
      isRepeated: Boolean,
      enumsAsInts: Boolean): (Boolean, Option[String], Int) = {

    // Groups use their own wire types, which the GPU decoder does not parse.
    if (protoTypeName == "GROUP") {
      return (false, Some("protobuf group fields are not supported on GPU"), ENC_DEFAULT)
    }

    if (protoTypeName == "MESSAGE") {
      return (sparkType, isRepeated) match {
        case (_: StructType, false) | (ArrayType(_: StructType, _), true) =>
          (true, None, ENC_DEFAULT)
        case _ =>
          (false, Some(s"unsupported Spark type for a protobuf message field: $sparkType"),
            ENC_DEFAULT)
      }
    }

    if (isRepeated) {
      sparkType match {
        case ArrayType(elementType, _) =>
          elementType match {
            case BooleanType | IntegerType | LongType | FloatType | DoubleType |
                 StringType | BinaryType =>
              return checkScalarEncoding(elementType, protoTypeName, enumsAsInts)
            case _ =>
              return (
                false,
                Some(s"unsupported repeated element type: $elementType"),
                ENC_DEFAULT)
          }
        case _ =>
          return (
            false,
            Some(s"repeated field should map to ArrayType, got: $sparkType"),
            ENC_DEFAULT)
      }
    }

    sparkType match {
      case BooleanType | IntegerType | LongType | FloatType | DoubleType |
           StringType | BinaryType =>
      case other =>
        return (
          false,
          Some(s"unsupported Spark type: $other"),
          ENC_DEFAULT)
    }

    checkScalarEncoding(sparkType, protoTypeName, enumsAsInts)
  }

  /** Returns (supported, optional rejection reason, JNI encoding) for a scalar type pairing. */
  def checkScalarEncoding(
      sparkType: DataType,
      protoTypeName: String,
      enumsAsInts: Boolean): (Boolean, Option[String], Int) = {
    val encoding = (sparkType, protoTypeName) match {
      case (BooleanType, "BOOL") => Some(ENC_DEFAULT)
      case (IntegerType, "INT32" | "UINT32") => Some(ENC_DEFAULT)
      case (IntegerType, "SINT32") => Some(ENC_ZIGZAG)
      case (IntegerType, "FIXED32" | "SFIXED32") => Some(ENC_FIXED)
      case (LongType, "INT64" | "UINT64") => Some(ENC_DEFAULT)
      case (LongType, "SINT64") => Some(ENC_ZIGZAG)
      case (LongType, "FIXED64" | "SFIXED64") => Some(ENC_FIXED)
      case (LongType, "INT32" | "UINT32" | "SINT32") =>
        val enc = protoTypeName match {
          case "SINT32" => ENC_ZIGZAG
          case _ => ENC_DEFAULT
        }
        Some(enc)
      case (FloatType, "FLOAT") => Some(ENC_DEFAULT)
      case (DoubleType, "DOUBLE") => Some(ENC_DEFAULT)
      case (StringType, "STRING") => Some(ENC_DEFAULT)
      case (BinaryType, "BYTES") => Some(ENC_DEFAULT)
      case (IntegerType, "ENUM") if enumsAsInts => Some(ENC_DEFAULT)
      case (StringType, "ENUM") if !enumsAsInts => Some(ENC_ENUM_STRING)
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
          ENC_DEFAULT)
    }
  }

  /** Maps a protobuf type and JNI encoding to its wire type, or Left for an unknown type. */
  def getWireType(protoTypeName: String, encoding: Int): Either[String, Int] = {
    val wireType = protoTypeName match {
      case "BOOL" | "INT32" | "UINT32" | "SINT32" | "INT64" | "UINT64" | "SINT64" | "ENUM" =>
        if (encoding == ENC_FIXED) {
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
