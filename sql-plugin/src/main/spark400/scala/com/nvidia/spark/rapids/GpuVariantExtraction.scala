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

import java.lang.reflect.Modifier

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, VariantOperationResult,
  VariantOperationStatus, VariantUtils}
import com.nvidia.spark.rapids.Arm.withResource

/** Status-aware extraction for decimal targets; existing scalar extraction is unchanged. */
private[rapids] object GpuVariantExtraction {
  lazy val isAvailable: Boolean = hasRequiredApis(
    name => Class.forName(name, false, classOf[VariantUtils].getClassLoader))

  def hasRequiredApis(loadClass: String => Class[_]): Boolean = {
    try {
      val utils = loadClass("ai.rapids.cudf.VariantUtils")
      val result = loadClass("ai.rapids.cudf.VariantOperationResult")
      val status = loadClass("ai.rapids.cudf.VariantOperationStatus")
      val extraction = utils.getMethod("getVariantFieldValueWithStatus",
        classOf[ColumnView], classOf[String])
      val cast = utils.getMethod("castVariantValueWithStatus",
        classOf[ColumnView], classOf[DType], classOf[ColumnView])
      Modifier.isStatic(extraction.getModifiers) && extraction.getReturnType == result &&
        Modifier.isStatic(cast.getModifiers) && cast.getReturnType == result &&
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

  /** The callback borrows both result columns. Retain returned columns before this scope closes. */
  def withRawExtraction[T](
      variantStruct: ColumnView,
      path: String)(f: VariantOperationResult => T): T = {
    require(variantStruct.getType == DType.STRUCT && variantStruct.getNumChildren == 2,
      "expected Variant struct with value and metadata children")
    variantStruct match {
      case owned: ColumnVector => withNormalizedExtraction(owned, path)(f)
      case _ =>
        // Rebuilding a borrowed parent with unsliced children loses its offset and validity.
        withResource(variantStruct.copyToColumnVector()) { normalized =>
          withNormalizedExtraction(normalized, path)(f)
        }
    }
  }

  private def withNormalizedExtraction[T](
      input: ColumnVector,
      path: String)(f: VariantOperationResult => T): T = {
    withResource(input.getChildColumnView(0)) { value =>
      withResource(input.getChildColumnView(1)) { metadata =>
        GpuVariantGet.withCudfVariantView(input, metadata, value) { cudfVariant =>
          withResource(VariantUtils.getVariantFieldValueWithStatus(cudfVariant, path))(f)
        }
      }
    }
  }

  def allRowsCovered(original: ColumnView, decoded: ColumnView): Boolean = {
    withResource(Scalar.fromUnsignedByte(VariantOperationStatus.ROW_NULL.getNativeId.toByte)) {
      rowNull =>
        withResource(original.equalTo(rowNull)) { parentNull =>
          withResource(decodedSuccessfully(decoded)) { successful =>
            withResource(parentNull.or(successful)) { covered =>
              BoolUtils.isAllValidTrue(covered)
            }
          }
        }
    }
  }

  private def decodedSuccessfully(decoded: ColumnView): ColumnVector = {
    withResource(Scalar.fromUnsignedByte(VariantOperationStatus.SUCCESS.getNativeId.toByte)) {
      success => decoded.equalTo(success)
    }
  }
}
