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

import org.apache.spark.sql.types.DataType

final case class ProtobufPlannerOptions(
    enumsAsInts: Boolean,
    failOnErrors: Boolean)

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
  def hasDefaultValue: Boolean = defaultValue.isDefined
}

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

  override def hashCode(): Int = {
    var result = fieldNumber
    result = 31 * result + parentIdx
    result = 31 * result + depth
    result = 31 * result + wireType
    result = 31 * result + outputTypeId
    result = 31 * result + encoding
    result = 31 * result + isRepeated.hashCode()
    result = 31 * result + isRequired.hashCode()
    result = 31 * result + hasDefaultValue.hashCode()
    result = 31 * result + isOutput.hashCode()
    result = 31 * result + defaultInt.hashCode()
    result = 31 * result + defaultFloat.hashCode()
    result = 31 * result + defaultBool.hashCode()
    result = 31 * result + Arrays.hashCode(defaultString)
    result = 31 * result + Arrays.hashCode(enumValidValues)
    result = 31 * result + Arrays.deepHashCode(enumNames.asInstanceOf[Array[Object]])
    result
  }
}

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

  override def hashCode(): Int = {
    var result = Arrays.hashCode(fieldNumbers)
    result = 31 * result + Arrays.hashCode(parentIndices)
    result = 31 * result + Arrays.hashCode(depthLevels)
    result = 31 * result + Arrays.hashCode(wireTypes)
    result = 31 * result + Arrays.hashCode(outputTypeIds)
    result = 31 * result + Arrays.hashCode(encodings)
    result = 31 * result + Arrays.hashCode(isRepeated)
    result = 31 * result + Arrays.hashCode(isRequired)
    result = 31 * result + Arrays.hashCode(hasDefaultValue)
    result = 31 * result + Arrays.hashCode(isOutput)
    result = 31 * result + Arrays.hashCode(defaultInts)
    result = 31 * result + Arrays.hashCode(defaultFloats)
    result = 31 * result + Arrays.hashCode(defaultBools)
    result = 31 * result + Arrays.deepHashCode(defaultStrings.asInstanceOf[Array[Object]])
    result = 31 * result + Arrays.deepHashCode(enumValidValues.asInstanceOf[Array[Object]])
    result = 31 * result + Arrays.deepHashCode(enumNames.asInstanceOf[Array[Object]])
    result
  }
}
