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

import java.util.{Arrays, Objects}

import com.nvidia.spark.rapids.jni.ProtobufSchemaDescriptor

import org.apache.spark.sql.types.DataType

/** Planning flags for enum representation and malformed-message handling. */
final case class ProtobufPlannerOptions(
    enumsAsInts: Boolean,
    failOnErrors: Boolean,
    malformedRowsAsNullFields: Boolean = false)

/** Extracted facts for one Spark field; unsupported fields retain a fallback reason. */
final case class ProtobufFieldInfo(
    fieldNumber: Int,
    protoTypeName: String,
    sparkType: DataType,
    encoding: Int,
    isSupported: Boolean,
    unsupportedReason: Option[String],
    isRequired: Boolean,
    defaultValue: Option[ProtobufDefaultValue],
    enumMetadata: Option[ProtobufEnumMetadata],
    isRepeated: Boolean = false) {
  /** Whether an explicit default value is available for JNI schema preparation. */
  def hasDefaultValue: Boolean = defaultValue.isDefined
}

/**
 * One JNI schema entry; parentIdx indexes the flattened sequence (-1 for a top-level field).
 * Array payloads are mutable and are compared by content.
 */
final case class FlattenedFieldDescriptor(
    fieldNumber: Int,
    parentIdx: Int,
    depth: Int,
    wireType: Int,
    outputTypeId: Int,
    encoding: Int,
    isRepeated: Boolean,
    isRequired: Boolean,
    hasDefaultValue: Boolean,
    isOutput: Boolean,
    defaultInt: Long,
    defaultFloat: Double,
    defaultBool: Boolean,
    defaultString: Array[Byte],
    enumValidValues: Array[Int],
    enumNames: Array[Array[Byte]]) {
  override def equals(other: Any): Boolean = other match {
    case that: FlattenedFieldDescriptor =>
      fieldNumber == that.fieldNumber &&
        parentIdx == that.parentIdx &&
        depth == that.depth &&
        wireType == that.wireType &&
        outputTypeId == that.outputTypeId &&
        encoding == that.encoding &&
        isRepeated == that.isRepeated &&
        isRequired == that.isRequired &&
        hasDefaultValue == that.hasDefaultValue &&
        isOutput == that.isOutput &&
        defaultInt == that.defaultInt &&
        java.lang.Double.compare(defaultFloat, that.defaultFloat) == 0 &&
        defaultBool == that.defaultBool &&
        Arrays.equals(defaultString, that.defaultString) &&
        Arrays.equals(enumValidValues, that.enumValidValues) &&
        Arrays.deepEquals(
          enumNames.asInstanceOf[Array[Object]],
          that.enumNames.asInstanceOf[Array[Object]])
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(
    Int.box(fieldNumber), Int.box(parentIdx), Int.box(depth),
    Int.box(wireType), Int.box(outputTypeId), Int.box(encoding),
    Boolean.box(isRepeated), Boolean.box(isRequired),
    Boolean.box(hasDefaultValue), Boolean.box(isOutput),
    Long.box(defaultInt), Double.box(defaultFloat), Boolean.box(defaultBool),
    Int.box(Arrays.hashCode(defaultString)),
    Int.box(Arrays.hashCode(enumValidValues)),
    Int.box(Arrays.deepHashCode(enumNames.asInstanceOf[Array[Object]])))
}

/** Mutable parallel JNI arguments indexed in flattened-field order, with content-based equality. */
final case class FlattenedSchemaArrays(
    fieldNumbers: Array[Int],
    parentIndices: Array[Int],
    depthLevels: Array[Int],
    wireTypes: Array[Int],
    outputTypeIds: Array[Int],
    encodings: Array[Int],
    isRepeated: Array[Boolean],
    isRequired: Array[Boolean],
    hasDefaultValue: Array[Boolean],
    isOutput: Array[Boolean],
    defaultInts: Array[Long],
    defaultFloats: Array[Double],
    defaultBools: Array[Boolean],
    defaultStrings: Array[Array[Byte]],
    enumValidValues: Array[Array[Int]],
    enumNames: Array[Array[Array[Byte]]]) {
  /** Builds the JNI schema; its constructor rejects schemas the decoder cannot run. */
  def toJniDescriptor: ProtobufSchemaDescriptor =
    new ProtobufSchemaDescriptor(fieldNumbers, parentIndices, depthLevels, wireTypes,
      outputTypeIds, encodings, isRepeated, isRequired, hasDefaultValue, isOutput, defaultInts,
      defaultFloats, defaultBools, defaultStrings, enumValidValues, enumNames)

  override def equals(other: Any): Boolean = other match {
    case that: FlattenedSchemaArrays =>
      Arrays.equals(fieldNumbers, that.fieldNumbers) &&
        Arrays.equals(parentIndices, that.parentIndices) &&
        Arrays.equals(depthLevels, that.depthLevels) &&
        Arrays.equals(wireTypes, that.wireTypes) &&
        Arrays.equals(outputTypeIds, that.outputTypeIds) &&
        Arrays.equals(encodings, that.encodings) &&
        Arrays.equals(isRepeated, that.isRepeated) &&
        Arrays.equals(isRequired, that.isRequired) &&
        Arrays.equals(hasDefaultValue, that.hasDefaultValue) &&
        Arrays.equals(isOutput, that.isOutput) &&
        Arrays.equals(defaultInts, that.defaultInts) &&
        Arrays.equals(defaultFloats, that.defaultFloats) &&
        Arrays.equals(defaultBools, that.defaultBools) &&
        Arrays.deepEquals(
          defaultStrings.asInstanceOf[Array[Object]],
          that.defaultStrings.asInstanceOf[Array[Object]]) &&
        Arrays.deepEquals(
          enumValidValues.asInstanceOf[Array[Object]],
          that.enumValidValues.asInstanceOf[Array[Object]]) &&
        Arrays.deepEquals(
          enumNames.asInstanceOf[Array[Object]],
          that.enumNames.asInstanceOf[Array[Object]])
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(
    Int.box(Arrays.hashCode(fieldNumbers)),
    Int.box(Arrays.hashCode(parentIndices)),
    Int.box(Arrays.hashCode(depthLevels)),
    Int.box(Arrays.hashCode(wireTypes)),
    Int.box(Arrays.hashCode(outputTypeIds)),
    Int.box(Arrays.hashCode(encodings)),
    Int.box(Arrays.hashCode(isRepeated)),
    Int.box(Arrays.hashCode(isRequired)),
    Int.box(Arrays.hashCode(hasDefaultValue)),
    Int.box(Arrays.hashCode(isOutput)),
    Int.box(Arrays.hashCode(defaultInts)),
    Int.box(Arrays.hashCode(defaultFloats)),
    Int.box(Arrays.hashCode(defaultBools)),
    Int.box(Arrays.deepHashCode(defaultStrings.asInstanceOf[Array[Object]])),
    Int.box(Arrays.deepHashCode(enumValidValues.asInstanceOf[Array[Object]])),
    Int.box(Arrays.deepHashCode(enumNames.asInstanceOf[Array[Object]])))
}
