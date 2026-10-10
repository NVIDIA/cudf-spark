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

import java.util.Arrays

/** A serialized FileDescriptorSet (schema), supplied as a file path or bytes. */
sealed trait ProtobufDescriptorSource

object ProtobufDescriptorSource {
  /** Descriptor file read by the Spark driver. */
  final case class DescriptorPath(path: String) extends ProtobufDescriptorSource
  /** Owns a snapshot of the serialized schema; accessors return copies. */
  final class DescriptorBytes private (private val snapshot: Array[Byte])
      extends ProtobufDescriptorSource {
    /** A copy that callers may modify without changing this descriptor source. */
    def bytes: Array[Byte] = snapshot.clone()

    override def equals(other: Any): Boolean = other match {
      case that: DescriptorBytes => Arrays.equals(snapshot, that.snapshot)
      case _ => false
    }

    override def hashCode(): Int = Arrays.hashCode(snapshot)
  }

  object DescriptorBytes {
    def apply(bytes: Array[Byte]): DescriptorBytes = new DescriptorBytes(bytes.clone())
    def unapply(value: DescriptorBytes): Option[Array[Byte]] = Option(value).map(_.bytes)
  }
}

/** Arguments extracted from Spark's from_protobuf expression, before descriptor resolution. */
final case class ProtobufExprInfo(
    messageName: String,
    descriptorSource: ProtobufDescriptorSource,
    options: Map[String, String])

/** An explicitly declared field default, represented without protobuf runtime classes. */
sealed trait ProtobufDefaultValue

object ProtobufDefaultValue {
  final case class BoolValue(value: Boolean) extends ProtobufDefaultValue
  final case class IntValue(value: Long) extends ProtobufDefaultValue
  final case class FloatValue(value: Float) extends ProtobufDefaultValue
  final case class DoubleValue(value: Double) extends ProtobufDefaultValue
  final case class StringValue(value: String) extends ProtobufDefaultValue
  /** Owns a snapshot of a bytes default; accessors return copies. */
  final class BinaryValue private (private val snapshot: Array[Byte]) extends ProtobufDefaultValue {
    /** A copy that callers may modify without changing this default. */
    def value: Array[Byte] = snapshot.clone()

    override def equals(other: Any): Boolean = other match {
      case that: BinaryValue => Arrays.equals(snapshot, that.snapshot)
      case _ => false
    }

    override def hashCode(): Int = Arrays.hashCode(snapshot)
  }

  object BinaryValue {
    def apply(value: Array[Byte]): BinaryValue = new BinaryValue(value.clone())
    def unapply(value: BinaryValue): Option[Array[Byte]] = Option(value).map(_.value)
  }
  /** Keeps the declared name because multiple aliases can share the same number. */
  final case class EnumValue(number: Int, name: String) extends ProtobufDefaultValue
}

/** One enum declaration; aliases have distinct names but share a number. */
final case class ProtobufEnumValue(number: Int, name: String)

/**
 * Enum entries in declaration order, including aliases. Numeric lookup uses the first name.
 */
final case class ProtobufEnumMetadata(values: Vector[ProtobufEnumValue]) {
  private lazy val namesByNumber: Map[Int, String] =
    values.reverseIterator.map(v => v.number -> v.name).toMap

  /** Uses the first declared alias, or the decimal number when no declaration matches. */
  def defaultFromNumber(number: Int): ProtobufDefaultValue.EnumValue = {
    val name = namesByNumber.getOrElse(number, number.toString)
    ProtobufDefaultValue.EnumValue(number, name)
  }
}

object ProtobufEnumMetadata {
  /** Snapshots mutable sequences as well as immutable ones. */
  def apply(values: scala.collection.Seq[ProtobufEnumValue]): ProtobufEnumMetadata =
    new ProtobufEnumMetadata(values.toVector)
}

/** Message metadata shared by all shims; reflective access may fail when read. */
trait ProtobufMessageDescriptor {
  /** Uppercase file syntax, or an empty string when it cannot be read. */
  def syntax: String
  /** File-level UTF-8 validation option; the reflective adapter uses true if reading fails. */
  def javaStringCheckUtf8: Boolean = false
  /** Looks up the protobuf field name, returning None when absent. */
  def findField(name: String): Option[ProtobufFieldDescriptor]
}

/** Field metadata; optional nested metadata is absent for unrelated field types. */
trait ProtobufFieldDescriptor {
  /** Protobuf declaration name, not its JSON name. */
  def name: String
  /** Protobuf wire field number. */
  def fieldNumber: Int
  /** Uppercase protobuf type, including the distinct MESSAGE and GROUP types. */
  def protoTypeName: String
  /** Whether the field has the repeated label. */
  def isRepeated: Boolean
  /** Whether the field has the proto2 required label. */
  def isRequired: Boolean
  /** Whether the field belongs to a real oneof, excluding synthetic ones for proto3 optional. */
  def isInOneof: Boolean
  /**
   * Right(None) means no explicit default; Left means the default could not be read or converted.
   */
  def explicitDefaultValue: Either[String, Option[ProtobufDefaultValue]]
  /** Declarations for ENUM fields, including aliases; None for other types. */
  def enumMetadata: Option[ProtobufEnumMetadata]
  /** Nested schema for MESSAGE and GROUP fields; None for other types. */
  def messageDescriptor: Option[ProtobufMessageDescriptor]
  /** Referenced type's file syntax for MESSAGE/GROUP/ENUM; Some("") means unreadable syntax. */
  def referencedTypeSyntax: Option[String]
}
