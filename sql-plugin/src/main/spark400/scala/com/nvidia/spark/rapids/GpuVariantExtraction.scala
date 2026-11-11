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

/*** spark-rapids-shim-json-lines
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, VariantOperationResult, VariantUtils}
import com.nvidia.spark.rapids.Arm.withResource

/**
 * Adapts Spark's physical Variant layout to cuDF and scopes ownership of raw extraction results.
 * This is not a validator for the complete Variant encoding or an implementation of Spark casts.
 */
private[rapids] object GpuVariantExtraction {
  def hasRequiredApis(loadClass: String => Class[_]): Boolean = {
    try {
      val utils = loadClass("ai.rapids.cudf.VariantUtils")
      val result = loadClass("ai.rapids.cudf.VariantOperationResult")
      val status = loadClass("ai.rapids.cudf.VariantOperationStatus")
      utils.getMethod("getVariantFieldValueWithStatus", classOf[ColumnView], classOf[String])
          .getReturnType == result &&
        utils.getMethod("castVariantValueWithStatus",
          classOf[ColumnView], classOf[DType], classOf[ColumnView]).getReturnType == result &&
        classOf[AutoCloseable].isAssignableFrom(result) &&
        result.getMethod("getValue").getReturnType == classOf[ColumnVector] &&
        result.getMethod("getStatus").getReturnType == classOf[ColumnVector] &&
        result.getMethod("close").getReturnType == java.lang.Void.TYPE &&
        status.getMethod("getNativeId").getReturnType == java.lang.Integer.TYPE &&
        status.getField("SUCCESS").getType == status &&
        status.getField("ROW_NULL").getType == status
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException | _: NoSuchFieldException |
          _: LinkageError => false
    }
  }

  /**
   * The callback borrows both result columns. Retain any returned column before this scope closes.
   */
  def withRawExtraction[T](
      variantStruct: ColumnView,
      path: String)(f: VariantOperationResult => T): T = {
    withCudfVariantView(variantStruct) { cudfVariant =>
      withResource(VariantUtils.getVariantFieldValueWithStatus(cudfVariant, path))(f)
    }
  }

  // Keep physical access separate from extraction policy: is_variant_null, for example, must not
  // validate metadata or traverse a path.
  private def withCudfVariantView[T](variantStruct: ColumnView)(f: ColumnView => T): T = {
    require(variantStruct.getType == DType.STRUCT,
      s"expected Variant struct input, got ${variantStruct.getType}")
    require(variantStruct.getNumChildren == 2,
      s"expected Variant struct with value and metadata children, got " +
        s"${variantStruct.getNumChildren} children")

    variantStruct match {
      case owned: ColumnVector => withNormalizedVariantView(owned)(f)
      case _ =>
        // A borrowed view can carry a nonzero parent offset. getChildColumnView and getValid
        // expose unsliced children and the original mask, so rebuilding it directly loses that
        // offset. Materialize views first; the normal owning-column execution path needs no copy.
        withResource(variantStruct.copyToColumnVector()) { normalized =>
          withNormalizedVariantView(normalized)(f)
        }
    }
  }

  private def withNormalizedVariantView[T](variantStruct: ColumnVector)(f: ColumnView => T): T = {
    withResource(variantStruct.getChildColumnView(0)) { value =>
      withResource(variantStruct.getChildColumnView(1)) { metadata =>
        withByteList(metadata) { metadataBytes =>
          withByteList(value) { valueBytes =>
            withResource(new ColumnView(DType.STRUCT, variantStruct.getRowCount,
              Optional.of[java.lang.Long](variantStruct.getNullCount), variantStruct.getValid,
              null.asInstanceOf[ai.rapids.cudf.BaseDeviceMemoryBuffer],
              Array[ColumnView](metadataBytes, valueBytes)))(f)
          }
        }
      }
    }
  }

  private def withByteList[T](cv: ColumnView)(f: ColumnView => T): T = {
    cv.getType match {
      case DType.LIST => f(cv)
      case DType.STRING => withResource(toByteList(cv))(f)
      case other =>
        throw new IllegalArgumentException(
          s"expected Variant physical binary child to be STRING or LIST, got $other")
    }
  }

  private def toByteList(cv: ColumnView): ColumnVector = {
    val dataBuf = Option(cv.getData)
    withResource(new ColumnView(DType.UINT8, dataBuf.map(_.getLength).getOrElse(0L),
      Optional.of(0L), dataBuf.orNull, null)) { data =>
      withResource(new ColumnView(DType.LIST, cv.getRowCount,
        Optional.of[java.lang.Long](cv.getNullCount),
        cv.getValid, cv.getOffsets, Array(data))) { byteList =>
        byteList.copyToColumnVector()
      }
    }
  }
}
