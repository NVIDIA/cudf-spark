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

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, Table, VariantOperationResult,
  VariantUtils}
import ai.rapids.cudf.ast.{AstExpression, BinaryOperation, BinaryOperator, ColumnReference,
  Literal}
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
   * The extraction result for one batch, together with the normalized cuDF Variant view it was
   * computed from, so that the checks below reuse that view instead of rebuilding it.
   */
  final class Extraction private[GpuVariantExtraction] (
      result: VariantOperationResult,
      cudfVariant: ColumnView,
      val path: String) {
    def getValue: ColumnVector = result.getValue
    def getStatus: ColumnVector = result.getStatus
    def rowCount: Long = cudfVariant.getRowCount
    def parentNullCount: Long = cudfVariant.getNullCount

    /**
     * Returns true when a row could fail Spark's Variant constructor, which runs before Spark
     * reads the path. The constructor rejects metadata that is empty or has a version other than
     * 1, and a value or metadata longer than `sizeLimit` bytes. cuDF never checks the sizes, and
     * checks the metadata header only when it resolves an object key, which for a path that
     * starts with an object key happens for every row with a non-null parent. Rows under a
     * SQL-null parent are ignored by the header check; the size check may count them, which only
     * costs a fallback.
     */
    def failsVariantConstructor(sizeLimit: Long = SparkVariantSizeLimit): Boolean = {
      exceedsSizeLimit(cudfVariant, sizeLimit) ||
        (!path.startsWith("$.") && hasInvalidMetadataHeader(cudfVariant))
    }

    /**
     * Returns true when a row may have reported MISSING_PATH because a path step met a primitive
     * with a type ID that Spark does not recognize. Spark raises an error for such a row, while
     * cuDF reports the step as missing. Checks the root value of every row with a non-null parent,
     * and the value at every proper prefix of the path: a prefix that resolves to such a primitive
     * leaves the next step missing. A leaf with such a type ID is rejected by every decoder, so
     * it is not checked here.
     */
    def hasUnrecognizedPrimitiveStep: Boolean = {
      hasUnrecognizedPrimitiveStep(GpuVariantGet.UnrecognizedPrimitiveTypeIds)
    }

    private[rapids] def hasUnrecognizedPrimitiveStep(unrecognizedIds: Seq[Int]): Boolean = {
      // Rows under a SQL-null parent can carry any bytes, so the root is read through a view
      // with the parent's validity. Every other row reaches the root: a decoded value, a Variant
      // null or a missing step under an object or array root cannot have such a root, and the
      // caller has already ruled out malformed rows.
      def root = withResource(cudfVariant.getChildColumnView(1)) { value =>
        withResource(value.getChildColumnView(0)) { bytes =>
          withResource(new ColumnView(DType.LIST, cudfVariant.getRowCount,
              Optional.of[java.lang.Long](cudfVariant.getNullCount), cudfVariant.getValid,
              value.getOffsets, Array(bytes))) { parentValue =>
            withResource(parentValue.extractListElement(0)) { header =>
              anyUnrecognizedPrimitive(header, unrecognizedIds)
            }
          }
        }
      }
      def atPrefix(prefix: String) =
        withResource(VariantUtils.getVariantFieldValueWithStatus(cudfVariant, prefix)) { step =>
          withResource(step.getValue.extractListElement(0)) { header =>
            anyUnrecognizedPrimitive(header, unrecognizedIds)
          }
        }
      unrecognizedIds.nonEmpty && (pathPrefixes(path) match {
        // A path the shared parser cannot split falls back rather than skipping the prefixes.
        case None => true
        case Some(prefixes) => root || prefixes.exists(atPrefix)
      })
    }
  }

  /**
   * Returns false when a binary child cannot be viewed as LIST<UINT8>: a STRING child that holds
   * Int.MaxValue bytes or more uses 64-bit offsets, which a LIST view cannot read. Such a batch
   * has to be evaluated on the CPU.
   */
  def supportsLayout(variantStruct: ColumnView): Boolean = {
    (0 until variantStruct.getNumChildren).forall { i =>
      withResource(variantStruct.getChildColumnView(i)) { child =>
        child.getType != DType.STRING || dataLength(child) < Int.MaxValue
      }
    }
  }

  /**
   * The callback borrows both result columns. Retain any returned column before this scope closes.
   * Callers check `supportsLayout` first.
   */
  def withRawExtraction[T](
      variantStruct: ColumnView,
      path: String)(f: Extraction => T): T = {
    withCudfVariantView(variantStruct) { cudfVariant =>
      withResource(VariantUtils.getVariantFieldValueWithStatus(cudfVariant, path)) { result =>
        f(new Extraction(result, cudfVariant, path))
      }
    }
  }

  /**
   * Spark's VariantUtil.SIZE_LIMIT as this JVM sees it: 128 MiB in Spark 4.0, and in 4.1 unless
   * spark.testing lowers it to 16 MiB. Where VariantUtil is not visible, as on Databricks, the
   * smallest known limit keeps the check conservative; a lower limit only costs a fallback.
   */
  private[rapids] lazy val SparkVariantSizeLimit: Long = {
    try {
      Class.forName("org.apache.spark.types.variant.VariantUtil", true, getClass.getClassLoader)
        .getField("SIZE_LIMIT").getInt(null).toLong
    } catch {
      case _: ReflectiveOperationException | _: LinkageError => SmallestSparkVariantSizeLimit
    }
  }
  private val SmallestSparkVariantSizeLimit = 16L * 1024 * 1024
  private val VersionMask: Byte = 0x0F
  private val MetadataVersion: Byte = 1

  /**
   * The proper prefixes of a path, from the same parser that accepted it: `$.a.b[0]` gives `$.a`
   * and `$.a.b`. None for a path that parser does not accept.
   */
  private[rapids] def pathPrefixes(path: String): Option[Seq[String]] = {
    GpuVariantGet.supportedPathSteps(path).map { steps =>
      (1 until steps.length).map(n => "$" + steps.take(n).mkString)
    }
  }

  /** Groups sorted IDs into inclusive runs: 17, 18, 19, 21, 22 gives (17, 19) and (21, 22). */
  private[rapids] def idRanges(ids: Seq[Int]): Seq[(Int, Int)] = {
    ids.sorted.foldLeft(List.empty[(Int, Int)]) {
      case ((lo, hi) :: rest, id) if id == hi + 1 => (lo, id) :: rest
      case (acc, id) => (id, id) :: acc
    }.reverse
  }

  // A primitive (basic type 0) has its type ID in the upper six bits of the header byte, so a run
  // of IDs is a range of header values with the low two bits clear.
  // The AST is compiled per call: a CompiledExpression kept for the JVM's lifetime is reported
  // as leaked, and compiling costs about 0.1 ms.
  private def anyUnrecognizedPrimitive(header: ColumnView, unrecognizedIds: Seq[Int]): Boolean = {
    val h = new ColumnReference(0)
    val inRange = idRanges(unrecognizedIds).map { case (lo, hi) =>
      new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND,
        new BinaryOperation(BinaryOperator.GREATER_EQUAL, h, Literal.ofInt(lo << 2)),
        new BinaryOperation(BinaryOperator.LESS_EQUAL, h, Literal.ofInt(hi << 2)))
    }.reduce[AstExpression](new BinaryOperation(BinaryOperator.NULL_LOGICAL_OR, _, _))
    val unrecognized = new BinaryOperation(BinaryOperator.NULL_LOGICAL_AND,
      new BinaryOperation(BinaryOperator.EQUAL,
        new BinaryOperation(BinaryOperator.BITWISE_AND, h, Literal.ofInt(3)), Literal.ofInt(0)),
      inRange)
    // AST literals are signed, so the UINT8 header is widened first.
    val flags = withResource(header.castTo(DType.INT32)) { widened =>
      withResource(new Table(widened)) { table =>
        withResource(unrecognized.compile())(_.computeColumn(table))
      }
    }
    withResource(flags) { f =>
      withResource(f.any()) { any => any.isValid && any.getBoolean }
    }
  }

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

  // Counts the rows with a valid header under a non-null parent. The metadata list is viewed with
  // the parent's validity, so rows under a SQL-null parent give a null header, as does empty
  // metadata. The metadata's own null mask is not consulted: Spark never writes null metadata
  // under a non-null Variant, and such a row is read through its offsets.
  private def hasInvalidMetadataHeader(cudfVariant: ColumnView): Boolean = {
    val expected = cudfVariant.getRowCount - cudfVariant.getNullCount
    expected > 0 && expected != withResource(cudfVariant.getChildColumnView(0)) { metadata =>
      withResource(metadata.getChildColumnView(0)) { bytes =>
        withResource(new ColumnView(DType.LIST, cudfVariant.getRowCount,
            Optional.of[java.lang.Long](cudfVariant.getNullCount), cudfVariant.getValid,
            metadata.getOffsets, Array(bytes))) { parentMetadata =>
          withResource(parentMetadata.extractListElement(0)) { header =>
            withResource(Scalar.fromUnsignedByte(VersionMask)) { mask =>
              withResource(header.bitAnd(mask)) { version =>
                withResource(Scalar.fromUnsignedByte(MetadataVersion)) { one =>
                  withResource(version.equalTo(one)) { matches =>
                    withResource(matches.sum(DType.INT64)) { count =>
                      if (count.isValid) count.getLong else 0L
                    }
                  }
                }
              }
            }
          }
        }
      }
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
    val dataBuf = cv.getData
    val length = dataLength(cv)
    // A LIST view reads 32-bit offsets. cuDF switches to 64-bit offsets from Int.MaxValue bytes
    // by default; LIBCUDF_LARGE_STRINGS_THRESHOLD can lower that, which cudf-spark does not do.
    // supportsLayout sends larger children to the CPU before extraction.
    require(length < Int.MaxValue,
      s"Variant binary child with $length bytes needs 64-bit offsets, which is not supported")
    withResource(new ColumnView(DType.UINT8, length, Optional.of(0L), dataBuf,
        null)) { data =>
      withResource(new ColumnView(DType.LIST, cv.getRowCount,
          Optional.of[java.lang.Long](cv.getNullCount), cv.getValid, cv.getOffsets,
          Array(data)))(f)
    }
  }

  private def dataLength(cv: ColumnView): Long = Option(cv.getData).map(_.getLength).getOrElse(0L)
}
