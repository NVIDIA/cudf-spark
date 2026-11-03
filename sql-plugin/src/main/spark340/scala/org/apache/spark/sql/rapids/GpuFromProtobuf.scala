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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnView, CudfColumnSizeOverflowException, CudfException, DType}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.Protobuf
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.rapids.protobuf.FlattenedSchemaArrays
import org.apache.spark.sql.types._

/**
 * GPU implementation for Spark's `from_protobuf` decode path.
 *
 * This is designed to replace `org.apache.spark.sql.protobuf.ProtobufDataToCatalyst` when
 * supported.
 *
 * The schema uses the JNI flattened representation: nested message fields follow their parent
 * entry and point to it through parentIndices. The planner decodes and returns every field, so
 * every entry has isOutput == true.
 *
 * @param decodedSchema Spark output schema
 * @param schema Flattened JNI schema; callers must not mutate its arrays after construction.
 * @param failOnErrors If true, throw on malformed data; otherwise return null for malformed rows
 * @param malformedRowsAsNullFields Return a struct of null fields instead of null for malformed
 *                                  rows, like Spark without SPARK-46275 in PERMISSIVE mode
 * @param child BinaryType expression containing serialized protobuf messages
 */
case class GpuFromProtobuf(
    decodedSchema: StructType,
    schema: FlattenedSchemaArrays,
    failOnErrors: Boolean,
    malformedRowsAsNullFields: Boolean,
    child: Expression)
  extends GpuUnaryExpression with ExpectsInputTypes with NullIntolerantShim {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = decodedSchema

  override def nullable: Boolean = true

  // FAILFAST throws on malformed rows, so conditionals must not decode rows they do not select.
  override def hasSideEffects: Boolean = super.hasSideEffects || failOnErrors

  @transient private lazy val protobufSchema = schema.toJniDescriptor

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Input null mask is propagated to the output struct by the C++ decoder,
    // so no mergeAndSetValidity call is needed here.
    val decoded = try {
      Protobuf.decodeToStruct(input.getBase, protobufSchema, failOnErrors)
    } catch {
      case e: CudfColumnSizeOverflowException => throw e
      // The decoder reports malformed input and its own limits through the same exception type.
      case e: CudfException if failOnErrors =>
        throw QueryExecutionErrors.malformedProtobufMessageDetectedInMessageParsingError(e)
    }
    if (malformedRowsAsNullFields) restoreInputValidity(decoded, input.getBase) else decoded
  }

  // Building a struct null mask nulls its fields too, so malformed rows already have only null
  // fields and just need to be valid again. Equal null counts mean no row was malformed.
  private def restoreInputValidity(
      decoded: cudf.ColumnVector,
      input: cudf.ColumnView): cudf.ColumnVector = withResource(decoded) { _ =>
    if (decoded.getNullCount == input.getNullCount) {
      decoded.incRefCount()
    } else {
      withResource((0 until decoded.getNumChildren).safeMap(decoded.getChildColumnView)) {
        children =>
          withResource(ColumnView.makeStructView(decoded.getRowCount, children: _*)) { struct =>
            struct.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
          }
      }
    }
  }
}

object GpuFromProtobuf {
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
}
