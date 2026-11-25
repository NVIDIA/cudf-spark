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
{"spark":["340"],"carryForward":"sameMinor"}
{"spark":["350","400","420"],"carryForward":"all"}
{"spark":["350db143","400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

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
 * entry and point to it through parentIndices. The planner returns every field of the Spark
 * schema, and message fields Spark cannot represent carry isOutput == false so that they are
 * parsed and validated; the decoder returns those as all-null columns, which this expression
 * drops to match the Spark schema.
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
    val visible = dropHiddenColumns(decoded)
    if (malformedRowsAsNullFields) restoreInputValidity(visible, input.getBase) else visible
  }

  @transient private lazy val hasHiddenFields = schema.isOutput.contains(false)

  /**
   * The decoder returns an all-null column for every hidden field, and the planner appends those
   * fields after the Spark fields at each level, so the trailing columns have to be dropped here.
   * Takes ownership of the decoded column.
   */
  private def dropHiddenColumns(decoded: cudf.ColumnVector): cudf.ColumnVector = {
    if (!hasHiddenFields) {
      decoded
    } else {
      withResource(decoded) { _ =>
        withResource(new ArrayBuffer[ColumnView]) { toClose =>
          val trimmed = trimHiddenColumns(decoded, decodedSchema, toClose)
          if (trimmed == decoded) {
            decoded.incRefCount()
          } else {
            toClose += trimmed
            trimmed.copyToColumnVector()
          }
        }
      }
    }
  }

  /**
   * Returns a view of the column without its trailing hidden children, or the column itself when
   * the type has none. Views are zero-copy, so the caller owns the ones added to toClose.
   */
  private def trimHiddenColumns(
      col: ColumnView,
      dataType: DataType,
      toClose: ArrayBuffer[ColumnView]): ColumnView = {
    def addToClose(view: ColumnView): ColumnView = {
      toClose += view
      view
    }
    dataType match {
      case st: StructType =>
        val children = (0 until st.length).map(i => addToClose(col.getChildColumnView(i)))
        val trimmed = children.zipWithIndex.map { case (child, i) =>
          val newChild = trimHiddenColumns(child, st(i).dataType, toClose)
          if (newChild eq child) newChild else addToClose(newChild)
        }
        val changed = col.getNumChildren != st.length ||
          trimmed.zip(children).exists { case (newChild, child) => newChild ne child }
        if (changed) {
          new ColumnView(col.getType, col.getRowCount,
            Optional.of(col.getNullCount.asInstanceOf[java.lang.Long]), col.getValid,
            col.getOffsets, trimmed.toArray)
        } else {
          col
        }
      case ArrayType(elementType, _) =>
        val child = addToClose(col.getChildColumnView(0))
        val newChild = trimHiddenColumns(child, elementType, toClose)
        if (newChild eq child) col else col.replaceListChild(addToClose(newChild))
      case _ => col
    }
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
