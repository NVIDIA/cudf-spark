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

import java.util.{Arrays, Optional}

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, HostColumnVector,
  VariantOperationStatus, VariantUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{BooleanType, ByteType, Decimal, DecimalType, DoubleType,
  FloatType, IntegerType, LongType, ShortType, StringType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

class GpuVariantDecimalSuite extends AnyFunSuite {
  private val byteListType = new HostColumnVector.ListType(
    true, new HostColumnVector.BasicType(false, DType.UINT8))
  private val metadata = Array[Byte](1, 1, 0, 2, 'i'.toByte, 'd'.toByte)

  // Use known encodings rather than the OSS VariantBuilder, which DBR does not expose.
  private def decimal(coefficient: BigInt, scale: Int, width: Int): Array[Byte] = {
    val header = Map(4 -> 32, 8 -> 36, 16 -> 40)(width).toByte
    Array(header, scale.toByte) ++ Array.tabulate(width) { i =>
      ((coefficient >> (8 * i)) & 255).toByte
    }
  }

  private def obj(payload: Array[Byte]): Array[Byte] =
    Array[Byte](2, 1, 0, 0, payload.length.toByte) ++ payload

  private def binaryRows(rows: Seq[Array[Byte]], strings: Boolean): ColumnVector = {
    if (strings) {
      ColumnVector.fromUTF8Strings(rows: _*)
    } else {
      ColumnVector.fromLists(byteListType,
        rows.map(bytes => Arrays.asList(bytes.map(Byte.box): _*)): _*)
    }
  }

  private def withVariant[T](rows: Seq[Array[Byte]], strings: Boolean = false)
      (f: ColumnVector => T): T = {
    withResource(binaryRows(rows, strings)) { values =>
      withResource(binaryRows(rows.map(_ => metadata), !strings)) { metas =>
        withResource(ColumnVector.makeStruct(rows.length, values, metas))(f)
      }
    }
  }

  private def cpu(target: DecimalType): VariantGet =
    VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$.id"),
      target, failOnError = false, timeZoneId = Some("UTC"))

  private def cpuValues(rows: Seq[Array[Byte]], target: DecimalType): Seq[String] = {
    val expression = cpu(target)
    rows.map { bytes =>
      Option(expression.eval(InternalRow(new VariantVal(bytes, metadata))))
        .map(_.asInstanceOf[Decimal].toJavaBigDecimal.toPlainString).orNull
    }
  }

  private def assertDecoderFallback(rows: Seq[Array[Byte]], target: DecimalType): Unit = {
    withVariant(rows) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        val decoded = GpuVariantDecimal.decode(raw, target)
        withResource(decoded)(_ => assert(decoded.isEmpty))
      }
    }
  }

  private def assertValues(result: ColumnVector, target: DecimalType,
      expected: Seq[String]): Unit = {
    assert(result.getType == DecimalUtil.createCudfDecimal(target))
    withResource(result.copyToHost()) { host =>
      assert(host.getRowCount == expected.length)
      expected.indices.foreach { i =>
        if (expected(i) == null) {
          assert(host.isNull(i), s"row $i should be null")
        } else {
          assert(!host.isNull(i), s"row $i should be ${expected(i)}")
          assert(host.getBigDecimal(i).compareTo(new java.math.BigDecimal(expected(i))) == 0,
            s"row $i: ${host.getBigDecimal(i)} != ${expected(i)}")
        }
      }
    }
  }

  private def noFallback: ColumnVector = fail("unexpected decimal CPU fallback")

  private def assertGpu(payloads: Seq[Array[Byte]], target: DecimalType,
      expected: Seq[String]): Unit = {
    val rows = payloads.map(obj)
    assert(cpuValues(rows, target) == expected)
    withVariant(rows) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { result =>
          assertValues(result, target, expected)
        }
      }
    }
  }

  test("decimal widths use signed HALF_UP rather than truncation or HALF_EVEN") {
    Seq(4, 8, 16).foreach { width =>
      val coefficients = Seq(12499, 12500, 12501, -12499, -12500, -12501, 0)
      assertGpu(coefficients.map(c => decimal(BigInt(c), 4, width)), DecimalType(3, 1),
        Seq("1.2", "1.3", "1.3", "-1.2", "-1.3", "-1.3", "0.0"))
    }
  }

  test("rounding carry precedes declared precision overflow and physical narrowing") {
    assertGpu(Seq(994, 995, -994, -995).map(c => decimal(BigInt(c), 2, 4)),
      DecimalType(2, 1), Seq("9.9", null, "-9.9", null))
    assertGpu(Seq(9994, 9995, -9995).map(c => decimal(BigInt(c), 3, 4)),
      DecimalType(4, 2), Seq("9.99", "10.00", "-10.00"))
    Seq(1, 9, 10, 18, 19, 38).foreach { precision =>
      // For precision 38, a 38-digit temporary guard would require fallback. The final
      // precision boundary is covered below with a CPU-oracle fallback assertion.
      if (precision < 38) {
        val bound = BigInt(10).pow(precision)
        assertGpu(Seq(bound - 1, bound, -bound + 1, -bound)
          .map(c => decimal(c, 0, 16)), DecimalType(precision, 0),
          Seq((bound - 1).toString, null, (-bound + 1).toString, null))
      }
    }
  }

  test("the decoder supports every precision and legal nonnegative scale on GPU") {
    for (precision <- 1 to 38; scale <- 0 to precision) {
      val target = DecimalType(precision, scale)
      val rows = Seq(obj(decimal(BigInt(1), 2, 4)))
      withVariant(rows) { variant =>
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { result =>
            assertValues(result, target, cpuValues(rows, target))
          }
        }
      }
    }
  }

  test("distinct requested scales preserve the forwarded scale and final rounding") {
    Seq(0 -> "1", 1 -> "1.2", 2 -> "1.23", 3 -> "1.235").foreach {
      case (scale, expected) =>
        assertGpu(Seq(decimal(BigInt(12345), 4, 4)), DecimalType(8, scale), Seq(expected))
    }
  }

  test("scale 38 is exact at one quantum and the maximum valid coefficient") {
    val max = BigInt(10).pow(38) - 1
    assertGpu(Seq(BigInt(1), -BigInt(1), max, -max, BigInt(0))
      .map(c => decimal(c, 38, 16)), DecimalType(38, 38),
      Seq("0." + "0" * 37 + "1", "-0." + "0" * 37 + "1",
        "0." + "9" * 38, "-0." + "9" * 38, "0." + "0" * 38))
  }

  test("temporary overflow and 39-digit guards request CPU conversion") {
    val max = BigInt(10).pow(38) - 1
    Seq((decimal(max, 0, 16), DecimalType(38, 0), max.toString),
      (decimal(-max, 0, 16), DecimalType(38, 0), (-max).toString),
      (decimal(BigInt(1), 0, 4), DecimalType(38, 37), "1." + "0" * 37),
      (decimal(BigInt(1), 0, 4), DecimalType(38, 38), null)).foreach {
      case (payload, target, expected) =>
        val rows = Seq(obj(decimal(BigInt(0), 0, 4)), obj(payload))
        val values = cpuValues(rows, target)
        assert(values.last == expected)
        assertDecoderFallback(rows, target)
    }
  }

  test("int128 extrema at scale38 preserve malformed-source errors without abs overflow") {
    Seq(-(BigInt(1) << 127), (BigInt(1) << 127) - 1).foreach { coefficient =>
      val rows = Seq(obj(decimal(coefficient, 38, 16)))
      val target = DecimalType(38, 38)
      intercept[Exception](cpuValues(rows, target))
      assertDecoderFallback(rows, target)
    }
  }

  test("width-specific source validation rejects malformed coefficients and scales") {
    Seq((4, 9), (8, 18), (16, 38)).foreach { case (width, precision) =>
      val bound = BigInt(10).pow(precision)
      val malformed = Seq(decimal(bound, precision, width),
        decimal(-bound, precision, width), decimal(BigInt(1), precision + 1, width),
        decimal(BigInt(1), 255, width),
        decimal(bound, 0, width), decimal(BigInt(1), 0, width).dropRight(1),
        decimal(BigInt(1), 0, width).take(1))
      malformed.foreach { payload =>
        val rows = Seq(obj(decimal(BigInt(125), 2, 4)), obj(payload))
        val target = DecimalType(5, 1)
        intercept[Exception](cpuValues(rows, target))
        assertDecoderFallback(rows, target)
      }
      // The neighboring legal coefficient must not be rejected by an inclusive bound.
      assertGpu(Seq(decimal(bound - 1, precision, width),
        decimal(-bound + 1, precision, width)), DecimalType(5, 1), Seq("1.0", "-1.0"))
    }
  }

  test("nondecimal coercions request CPU conversion") {
    val string = Array[Byte](17, '1'.toByte, '.'.toByte, '2'.toByte, '5'.toByte)
    val integer = Array[Byte](12, 7)
    val bool = Array[Byte](4)
    val float = Array[Byte](56, 0, 0, -96, 63)
    val double = Array[Byte](28, 0, 0, 0, 0, 0, 0, -12, 63)
    Seq(string, integer, bool, float, double, Array[Byte](0)).foreach { payload =>
      val rows = Seq(obj(decimal(BigInt(125), 2, 4)), obj(payload))
      val target = DecimalType(4, 1)
      assertDecoderFallback(rows, target)
    }
  }

  test("eager source validation rejects an invalid scale with a negative bound exponent") {
    val target = DecimalType(5, 0)
    // cuDF accepts scale38 for this physical encoding and truncates its coefficient to zero.
    // Spark rejects scale38 for decimal4. Eager validation computes exponent9+1-38=-28.
    withVariant(Seq(obj(decimal(BigInt(1), 38, 4)))) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        val guardType = DType.create(DType.DTypeEnum.DECIMAL128, -1)
        withResource(VariantUtils.castVariantValueWithStatus(
            raw.getValue, guardType, raw.getStatus)) { decoded =>
          withResource(decoded.getStatus.copyToHost()) { host =>
            assert(host.getByte(0) == VariantOperationStatus.SUCCESS.getNativeId)
          }
        }
        val decoded = GpuVariantDecimal.decode(raw, target)
        withResource(decoded)(_ => assert(decoded.isEmpty))
      }
    }
  }

  test("owning extraction inputs do not gain an observable retain close cycle") {
    val target = DecimalType(4, 1)
    withVariant(Seq(obj(decimal(BigInt(125), 2, 4)))) { variant =>
      var closes = 0
      val references = variant.getRefCount
      val previous = variant.setEventHandler(new ColumnVector.EventHandler {
        override def onClosed(column: ColumnVector, refCount: Int): Unit = {
          assert(column eq variant)
          assert(refCount > 0)
          closes += 1
        }
      })
      try {
        withResource(GpuVariantExtraction.extractOrElse(variant, "$.id") { raw =>
          GpuVariantDecimal.decode(raw, target)
        } { noFallback })(out => assertValues(out, target, Seq("1.3")))
        assert(variant.getRefCount == references)
        assert(closes == 0)
      } finally {
        variant.setEventHandler(previous)
      }
    }
  }

  test("doColumnar retains decimal output without constructing a CPU bridge") {
    Seq(false, true).foreach { strings =>
      val rows = Seq(obj(decimal(BigInt(125), 2, 4)))
      withVariant(rows, strings) { variant =>
        withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
          val target = DecimalType(4, 1)
          val expression = GpuVariantGet(Literal(0), "$.id", target, null)
          withResource(expression.doColumnar(input)) { result =>
            assertValues(result, target, Seq("1.3"))
          }
        }
      }
    }
  }

  test("decimal results outlive extraction and original status remains unchanged on reuse") {
    val target = DecimalType(4, 1)
    withVariant(Seq(obj(decimal(BigInt(125), 2, 4)))) { variant =>
      withResource(GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        withResource(raw.getStatus.copyToHost()) { before =>
          assert(before.getByte(0) == VariantOperationStatus.SUCCESS.getNativeId)
          withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { first =>
            assertValues(first, target, Seq("1.3"))
          }
          withResource(raw.getStatus.copyToHost()) { after =>
            assert(after.getByte(0) == before.getByte(0))
          }
          GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)
        }
      }) { result =>
        assertValues(result, target, Seq("1.3"))
      }
    }
    // This second row must change from extraction SUCCESS to decoder TYPE_MISMATCH.
    // Unlike success-only rows, it distinguishes copying the prior status from mutating it.
    val mixed = Seq(obj(decimal(BigInt(125), 2, 4)), obj(Array[Byte](12, 7)))
    withVariant(mixed) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        (0 until 2).foreach { _ =>
          val decoded = GpuVariantDecimal.decode(raw, target)
          withResource(decoded)(_ => assert(decoded.isEmpty))
          withResource(raw.getStatus.copyToHost()) { host =>
            assert(host.getByte(0) == VariantOperationStatus.SUCCESS.getNativeId)
            assert(host.getByte(1) == VariantOperationStatus.SUCCESS.getNativeId)
          }
        }
      }
    }
  }

  test("empty batches and null-parent real slices avoid decimal CPU fallback") {
    val target = DecimalType(4, 1)
    withVariant(Seq.empty) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { result =>
          assert(result.getRowCount == 0)
        }
      }
    }
    val rows = (0 until 100).map { i =>
      if (i % 7 == 0) Array[Byte](0xfc.toByte) else obj(decimal(BigInt(i * 10 + 5), 2, 4))
    }
    withVariant(rows) { variant =>
      val validity = (0 until 100).map { i =>
        if (i % 7 == 0) null else java.lang.Integer.valueOf(i)
      }
      withResource(ColumnVector.fromBoxedInts(validity: _*)) { mask =>
        withResource(variant.getChildColumnView(0)) { values =>
          withResource(variant.getChildColumnView(1)) { metas =>
            withResource(new ColumnView(DType.STRUCT, 100,
                Optional.of[java.lang.Long](mask.getNullCount), mask.getValid,
                null, Array[ColumnView](values, metas))) { nullable =>
              withResource(nullable.splitAsViews(31, 96)) { views =>
                GpuVariantExtraction.withRawExtraction(views(1), "$.id") { raw =>
                  withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { out =>
                    val expected = (31 until 96).map { i =>
                      if (i % 7 == 0) null else new java.math.BigDecimal(i * 10 + 5)
                        .movePointLeft(2).setScale(1, java.math.RoundingMode.HALF_UP).toPlainString
                    }
                    assertValues(out, target, expected)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("all SQL-null parents hide malformed decimal children") {
    val target = DecimalType(38, 38)
    val rows = Seq.fill(3)(obj(decimal(BigInt(1), 255, 4)))
    withVariant(rows) { variant =>
      withResource(ColumnVector.fromBoxedInts(null, null, null)) { mask =>
        withResource(variant.getChildColumnView(0)) { values =>
          withResource(variant.getChildColumnView(1)) { metas =>
            withResource(new ColumnView(DType.STRUCT, 3, Optional.of(3L), mask.getValid,
                null, Array[ColumnView](values, metas))) { nullable =>
              GpuVariantExtraction.withRawExtraction(nullable, "$.id") { raw =>
                withResource(GpuVariantDecimal.decode(raw, target).getOrElse(noFallback)) { out =>
                  assertValues(out, target, Seq(null, null, null))
                }
              }
            }
          }
        }
      }
    }
  }

  test("fallback starts after extraction closes and preserves failure identity and input reuse") {
    val target = DecimalType(4, 1)
    // A string requests fallback, while the same input remains available after failure.
    val rows = Seq(obj(Array[Byte](17, '1'.toByte, '.'.toByte, '2'.toByte, '5'.toByte)))
    withVariant(rows) { variant =>
      Seq[Throwable](new IllegalStateException("decimal fallback failure"),
        new OutOfMemoryError("injected decimal fallback OOM"),
        new NoClassDefFoundError("injected decimal fallback linkage error")).foreach { failure =>
        var value: ColumnVector = null
        var status: ColumnVector = null
        var calls = 0
        val actual = intercept[Throwable] {
          withResource(GpuVariantExtraction.extractOrElse(variant, "$.id") { raw =>
            value = raw.getValue
            status = raw.getStatus
            GpuVariantDecimal.decode(raw, target)
          } {
            // This assertion distinguishes releasing after a failing callback from releasing
            // before CPU conversion starts, using the same helper as production doColumnar.
            assert(value.getRefCount == 0)
            assert(status.getRefCount == 0)
            calls += 1
            throw failure
          })(_ => fail("fallback must throw"))
        }
        assert(actual eq failure)
        assert(calls == 1)
        assert(value.getRefCount == 0)
        assert(status.getRefCount == 0)
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          val decoded = GpuVariantDecimal.decode(raw, target)
          withResource(decoded)(_ => assert(decoded.isEmpty))
        }
      }
    }
  }

  test("legacy scalar missing paths retain their existing no-bridge execution") {
    withVariant(Seq(obj(decimal(BigInt(125), 2, 4)))) { variant =>
      withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
        Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
          StringType).foreach { target =>
          // The unchanged scalar path accepts a raw null. A generic status-path rewrite
          // would instead try to construct this deliberately absent CPU fallback.
          val expression = GpuVariantGet(Literal(0), "$.missing", target, null)
          withResource(expression.doColumnar(input)) { result =>
            assert(result.getRowCount == 1)
            assert(result.getNullCount == 1)
          }
        }
      }
    }
  }

  test("decimal missing and encoded-null rows request CPU conversion of the complete input") {
    val target = DecimalType(4, 1)
    val rows = Seq(obj(decimal(BigInt(125), 2, 4)), Array[Byte](2, 0, 0), obj(Array[Byte](0)))
    assert(cpuValues(rows, target) == Seq("1.3", null, null))
    assertDecoderFallback(rows, target)
  }

  test("an empty encoded value is not an empty batch or SQL-null input") {
    val target = DecimalType(4, 1)
    val rows = Seq(Array.emptyByteArray)
    intercept[Exception](cpuValues(rows, target))
    assertDecoderFallback(rows, target)
  }

  test("nested object and array decimals decode on GPU without constructing a CPU bridge") {
    val payload = obj(decimal(BigInt(125), 2, 4))
    val array = Array[Byte](3, 1, 0, payload.length.toByte) ++ payload
    Seq((obj(payload), "$.id.id"), (obj(array), "$.id[0].id")).foreach {
      case (row, path) =>
        withVariant(Seq(row)) { variant =>
          withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
            val target = DecimalType(9, 6)
            val expression = GpuVariantGet(Literal(0), path, target, null)
            withResource(expression.doColumnar(input)) { result =>
              assertValues(result, target, Seq("1.250000"))
            }
          }
        }
    }
  }

  test("one expression alternates GPU and CPU batches without sticky fallback or stale results") {
    // The real CPU bridge uses spillable columnar-to-row conversion, even for a one-row batch.
    SpillFramework.initialize(new RapidsConf(Map(
      RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key -> "1MB",
      RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_COUNT.key -> "1",
      RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key -> "1MB",
      RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_COUNT.key -> "1")))
    try {
      checkExpressionReuse()
    } finally {
      SpillFramework.shutdown()
    }
  }

  private def checkExpressionReuse(): Unit = {
    val target = DecimalType(4, 1)
    val child = GpuBoundReference(0, VariantType, nullable = true)(NamedExpression.newExprId, "v")
    val expression = GpuVariantGet(child, "$.id", target, cpu(target))
    val processing = new LocalGpuMetric
    val waiting = new LocalGpuMetric
    expression.injectMetrics(Map(GpuMetric.CPU_BRIDGE_PROCESSING_TIME -> processing,
      GpuMetric.CPU_BRIDGE_WAIT_TIME -> waiting))
    val batches = Seq(
      (Seq(obj(decimal(BigInt(125), 2, 4))), "1.3", false),
      (Seq(obj(Array[Byte](17, '3'.toByte, '.'.toByte, '4'.toByte, '5'.toByte))), "3.5", true),
      (Seq(obj(decimal(BigInt(275), 2, 4))), "2.8", false),
      (Seq(obj(Array[Byte](17, '6'.toByte, '.'.toByte, '2'.toByte, '5'.toByte))), "6.3", true),
      (Seq(obj(decimal(BigInt(-375), 2, 4))), "-3.8", false))
    batches.foreach { case (rows, expected, fallback) =>
      val previousProcessing = processing.value
      val previousWaiting = waiting.value
      withVariant(rows) { variant =>
        withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
          withResource(expression.doColumnar(input)) { result =>
            assertValues(result, target, Seq(expected))
          }
        }
      }
      if (fallback) {
        assert(processing.value > previousProcessing)
        assert(waiting.value > previousWaiting)
      } else {
        assert(processing.value == previousProcessing)
        assert(waiting.value == previousWaiting)
      }
    }
  }

  test("missing and encoded-null statuses do not hide malformed object field IDs") {
    // Native binary lookup can visit valid id0 first, while Spark's small-object linear
    // traversal first sees invalid id1. The metadata dictionary contains only id0 = "id".
    val row = Array[Byte](2, 2, 1, 0, 0, 0, 1, 0)
    val target = DecimalType(4, 1)
    Seq(("$.missing", VariantOperationStatus.MISSING_PATH),
      ("$.id", VariantOperationStatus.VARIANT_NULL)).foreach { case (path, expectedStatus) =>
      val expression = cpu(target).copy(path = Literal(path))
      def cpuValue: String = {
        Option(expression.eval(InternalRow(new VariantVal(row, metadata))))
          .map(_.asInstanceOf[Decimal].toJavaBigDecimal.toPlainString).orNull
      }
      if (!ShimLoader.getShimVersion.isInstanceOf[DatabricksShimVersion]) {
        // The concrete counterexample relies on OSS's small-object linear traversal.
        // A vendor runtime may use a different lookup order.
        val error = intercept[Exception](cpuValue)
        assert(error.getMessage.contains("MALFORMED_VARIANT"))
      }
      withVariant(Seq(row)) { variant =>
        val decoded = GpuVariantExtraction.withRawExtraction(variant, path) { raw =>
          withResource(raw.getStatus.copyToHost()) { host =>
            assert(host.getByte(0) == expectedStatus.getNativeId)
          }
          GpuVariantDecimal.decode(raw, target)
        }
        withResource(decoded)(_ => assert(decoded.isEmpty))
      }
    }
  }
}
