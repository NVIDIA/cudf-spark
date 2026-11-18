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

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar, VariantOperationResult,
  VariantUtils}
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
        utils.getMethod("castVariantValue", classOf[ColumnView], classOf[DType])
          .getReturnType == classOf[ColumnVector] &&
        classOf[AutoCloseable].isAssignableFrom(result) &&
        result.getMethod("getValue").getReturnType == classOf[ColumnVector] &&
        result.getMethod("getStatus").getReturnType == classOf[ColumnVector] &&
        result.getMethod("close").getReturnType == java.lang.Void.TYPE &&
        status.getMethod("getNativeId").getReturnType == java.lang.Integer.TYPE &&
        status.getField("VARIANT_NULL").getType == status &&
        status.getField("MALFORMED_VARIANT").getType == status
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

  /**
   * Returns true when a row could fail Spark's Variant constructor, which runs before Spark reads
   * the path. The constructor rejects metadata that is empty or has a version other than 1, and a
   * value or metadata longer than `sizeLimit` bytes. cuDF checks the metadata header only when it
   * resolves an object key and never checks the sizes. Rows under a SQL-null parent are ignored
   * by the header check; the size check may count them, which only costs a fallback.
   *
   * @param checkMetadataHeader false when cuDF already validated the metadata header
   */
  def failsVariantConstructor(
      variantStruct: ColumnView,
      checkMetadataHeader: Boolean,
      sizeLimit: Long = SparkVariantSizeLimit): Boolean = {
    withCudfVariantView(variantStruct) { cudfVariant =>
      exceedsSizeLimit(cudfVariant, sizeLimit) ||
        (checkMetadataHeader && hasInvalidMetadataHeader(cudfVariant))
    }
  }

  // VariantUtil.SIZE_LIMIT outside Spark's own tests (4.1 lowers it to 16 MiB under
  // spark.testing). Databricks does not expose VariantUtil.
  private val SparkVariantSizeLimit = 128L * 1024 * 1024
  private val VersionMask: Byte = 0x0F
  private val MetadataVersion: Byte = 1

  private def exceedsSizeLimit(cudfVariant: ColumnView, sizeLimit: Long): Boolean = {
    (0 until cudfVariant.getNumChildren).exists { i =>
      withResource(cudfVariant.getChildColumnView(i)) { bytes =>
        // No row can be longer than all of the child's bytes, so most batches need no kernel.
        val totalBytes = withResource(bytes.getChildColumnView(0))(_.getRowCount)
        totalBytes > sizeLimit && withResource(bytes.countElements()) { lengths =>
          withResource(lengths.max()) { longest => longest.isValid && longest.getInt > sizeLimit }
        }
      }
    }
  }

  private def hasInvalidMetadataHeader(cudfVariant: ColumnView): Boolean = {
    val badHeader = withResource(cudfVariant.getChildColumnView(0)) { metadata =>
      withResource(metadata.extractListElement(0)) { header =>
        withResource(Scalar.fromUnsignedByte(VersionMask)) { mask =>
          withResource(header.bitAnd(mask)) { version =>
            withResource(Scalar.fromUnsignedByte(MetadataVersion)) { expected =>
              withResource(version.notEqualTo(expected)) { mismatch =>
                // Empty or null metadata yields a null header, which is also invalid.
                withResource(Scalar.fromBool(true)) { invalid =>
                  mismatch.replaceNulls(invalid)
                }
              }
            }
          }
        }
      }
    }
    // Only rows with a non-null parent reach Spark's constructor.
    val parentBadHeader = withResource(badHeader) {
      _.mergeAndSetValidity(BinaryOp.BITWISE_AND, cudfVariant)
    }
    withResource(parentBadHeader) { bad =>
      withResource(bad.any()) { any => any.isValid && any.getBoolean }
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
      case DType.STRING => withByteListView(cv)(f)
      case other =>
        throw new IllegalArgumentException(
          s"expected Variant physical binary child to be STRING or LIST, got $other")
    }
  }

  // Reinterprets STRING as LIST<UINT8> without a copy. The native LIST view copies the child view
  // by value and borrows the string buffers, which the owning input keeps alive during f.
  private def withByteListView[T](cv: ColumnView)(f: ColumnView => T): T = {
    val dataBuf = Option(cv.getData)
    val dataLength = dataBuf.map(_.getLength).getOrElse(0L)
    // A LIST view reads 32-bit offsets. cuDF switches to 64-bit offsets from Int.MaxValue bytes
    // by default; LIBCUDF_LARGE_STRINGS_THRESHOLD can lower that, which cudf-spark does not do.
    require(dataLength < Int.MaxValue,
      s"Variant binary child with $dataLength bytes needs 64-bit offsets, which is not supported")
    withResource(new ColumnView(DType.UINT8, dataLength, Optional.of(0L), dataBuf.orNull,
        null)) { data =>
      withResource(new ColumnView(DType.LIST, cv.getRowCount,
          Optional.of[java.lang.Long](cv.getNullCount), cv.getValid, cv.getOffsets,
          Array(data)))(f)
    }
  }
}
