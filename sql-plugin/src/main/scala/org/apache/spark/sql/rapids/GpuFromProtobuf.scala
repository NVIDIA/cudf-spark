/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.Arrays

import ai.rapids.cudf
import ai.rapids.cudf.{CudfException, DType}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.jni.{Protobuf, ProtobufSchemaDescriptor}
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types._

/**
 * GPU implementation for Spark's `from_protobuf` decode path.
 *
 * This is designed to replace `org.apache.spark.sql.protobuf.ProtobufDataToCatalyst` when
 * supported.
 *
 * The implementation uses a flattened schema representation where nested fields have parent
 * indices pointing to their containing message field. For pure scalar schemas, all fields
 * are top-level (parentIndices == -1, depthLevels == 0, isRepeated == false).
 *
 * Schema projection is supported: `decodedSchema` contains only the top-level fields and
 * nested children that are actually referenced by downstream operators. Downstream
 * `GetStructField` and `GetArrayStructFields` nodes derive their GPU ordinals from the
 * converted child schema, so they index into the pruned schema directly. Unreferenced fields
 * are never accessed, so no null-column filling is needed.
 *
 * @param decodedSchema The pruned schema containing only the fields decoded by the GPU.
 *                      Only fields referenced by downstream operators are included;
 *                      ordinal remapping ensures correct field access into the pruned output.
 * @param fieldNumbers Protobuf field numbers for all fields in flattened schema
 * @param parentIndices Parent indices for all fields (-1 for top-level)
 * @param depthLevels Nesting depth for all fields (0 for top-level)
 * @param wireTypes Wire types for all fields
 * @param outputTypeIds cuDF type IDs for all fields
 * @param encodings Encodings for all fields
 * @param isRepeated Whether each field is repeated
 * @param isRequired Whether each field is required
 * @param hasDefaultValue Whether each field has a default value
 * @param isOutput Whether each field should be included in the returned cudf STRUCT. Fields
 *                 marked false are decoded only to validate projected-out protobuf fields.
 * @param defaultInts Default int/long values
 * @param defaultFloats Default float/double values
 * @param defaultBools Default bool values
 * @param defaultStrings Default string/bytes values
 * @param enumValidValues Valid enum values for each field
 * @param enumNames Enum value names for enum-as-string fields. Parallel to enumValidValues.
 * @param failOnErrors If true, throw exception on malformed data
 */
case class GpuFromProtobuf(
    decodedSchema: StructType,
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
    enumNames: Array[Array[Array[Byte]]],
    failOnErrors: Boolean,
    child: Expression)
  extends GpuUnaryExpression with ExpectsInputTypes with NullIntolerantShim with Logging {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = decodedSchema

  override def nullable: Boolean = true

  override def equals(other: Any): Boolean = other match {
    case that: GpuFromProtobuf =>
      decodedSchema == that.decodedSchema &&
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
        GpuFromProtobuf.deepEquals(defaultStrings, that.defaultStrings) &&
        GpuFromProtobuf.deepEquals(enumValidValues, that.enumValidValues) &&
        GpuFromProtobuf.deepEquals(enumNames, that.enumNames) &&
        failOnErrors == that.failOnErrors &&
        child == that.child
    case _ => false
  }

  override def hashCode(): Int = {
    var result = decodedSchema.hashCode()
    result = 31 * result + Arrays.hashCode(fieldNumbers)
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
    result = 31 * result + GpuFromProtobuf.deepHashCode(defaultStrings)
    result = 31 * result + GpuFromProtobuf.deepHashCode(enumValidValues)
    result = 31 * result + GpuFromProtobuf.deepHashCode(enumNames)
    result = 31 * result + failOnErrors.hashCode()
    result = 31 * result + child.hashCode()
    result
  }

  // ProtobufSchemaDescriptor is a pure-Java immutable holder for validated schema arrays.
  // It does not own native resources, so task-scoped close hooks are not required here.
  @transient private lazy val protobufSchema = new ProtobufSchemaDescriptor(
    fieldNumbers, parentIndices, depthLevels, wireTypes, outputTypeIds, encodings,
    isRepeated, isRequired, hasDefaultValue, isOutput, defaultInts, defaultFloats, defaultBools,
    defaultStrings, enumValidValues, enumNames)

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Input null mask is propagated to the output struct by the C++ decoder,
    // so no mergeAndSetValidity call is needed here.
    try {
      Protobuf.decodeToStruct(input.getBase, protobufSchema, failOnErrors)
    } catch {
      case e: CudfException if failOnErrors =>
        throw new org.apache.spark.SparkException("Malformed protobuf message", e)
      case e: CudfException =>
        logWarning(s"Unexpected CudfException in PERMISSIVE mode: ${e.getMessage}", e)
        throw e
    }
  }
}

object GpuFromProtobuf {
  val ENC_DEFAULT = 0
  val ENC_FIXED   = 1
  val ENC_ZIGZAG  = 2
  val ENC_ENUM_STRING = 3

  /**
   * Maps a Spark DataType to the corresponding cuDF native type ID.
   * Note: The encoding (varint/zigzag/fixed) is determined by the protobuf field type,
   * not the Spark data type, so it must be set separately based on the protobuf schema.
   *
   * @return Some(typeId) for supported types, None for unsupported types
   */
  def sparkTypeToCudfIdOpt(dt: DataType): Option[Int] = dt match {
    case BooleanType => Some(DType.BOOL8.getTypeId.getNativeId)
    case IntegerType => Some(DType.INT32.getTypeId.getNativeId)
    case LongType => Some(DType.INT64.getTypeId.getNativeId)
    case FloatType => Some(DType.FLOAT32.getTypeId.getNativeId)
    case DoubleType => Some(DType.FLOAT64.getTypeId.getNativeId)
    case StringType => Some(DType.STRING.getTypeId.getNativeId)
    case BinaryType => Some(DType.LIST.getTypeId.getNativeId)
    case _ => None
  }

  /**
   * Check if a Spark DataType is supported by the GPU protobuf decoder.
   */
  def isTypeSupported(dt: DataType): Boolean = sparkTypeToCudfIdOpt(dt).isDefined

  private def deepEquals[T](left: Array[T], right: Array[T]): Boolean =
    Arrays.deepEquals(left.asInstanceOf[Array[Object]], right.asInstanceOf[Array[Object]])

  private def deepHashCode[T](arr: Array[T]): Int =
    Arrays.deepHashCode(arr.asInstanceOf[Array[Object]])
}
