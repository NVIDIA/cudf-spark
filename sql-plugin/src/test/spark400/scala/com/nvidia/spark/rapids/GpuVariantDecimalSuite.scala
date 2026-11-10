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
  VariantOperationStatus}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
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

  private def resultColumn(values: Seq[String], target: DecimalType): ColumnVector =
    withResource(ColumnVector.fromStrings(values: _*)) { strings =>
      strings.castTo(DecimalUtil.createCudfDecimal(target))
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
        withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { result =>
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

  test("every precision and legal nonnegative scale reaches decimal dispatch on GPU") {
    for (precision <- 1 to 38; scale <- 0 to precision) {
      val target = DecimalType(precision, scale)
      val rows = Seq(obj(decimal(BigInt(1), 2, 4)))
      withVariant(rows) { variant =>
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { result =>
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

  test("temporary overflow and 39-digit guards fallback once using original Spark values") {
    val max = BigInt(10).pow(38) - 1
    Seq((decimal(max, 0, 16), DecimalType(38, 0), max.toString),
      (decimal(-max, 0, 16), DecimalType(38, 0), (-max).toString),
      (decimal(BigInt(1), 0, 4), DecimalType(38, 37), "1." + "0" * 37),
      (decimal(BigInt(1), 0, 4), DecimalType(38, 38), null)).foreach {
      case (payload, target, expected) =>
        val rows = Seq(obj(decimal(BigInt(0), 0, 4)), obj(payload))
        val values = cpuValues(rows, target)
        assert(values.last == expected)
        withVariant(rows) { variant =>
          var calls = 0
          GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
            withResource(GpuVariantDecimal.decode(raw, target) {
              calls += 1
              resultColumn(cpuValues(rows, target), target)
            }) { result =>
              assert(calls == 1)
              assertValues(result, target, values)
            }
          }
        }
    }
  }

  test("int128 extrema at scale38 preserve malformed-source errors without abs overflow") {
    Seq(-(BigInt(1) << 127), (BigInt(1) << 127) - 1).foreach { coefficient =>
      val rows = Seq(obj(decimal(coefficient, 38, 16)))
      val target = DecimalType(38, 38)
      val expected = intercept[Exception](cpuValues(rows, target))
      withVariant(rows) { variant =>
        var calls = 0
        val actual = intercept[Exception] {
          GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
            withResource(GpuVariantDecimal.decode(raw, target) {
              calls += 1
              resultColumn(cpuValues(rows, target), target)
            })(_ => fail("int128 extrema are malformed Spark decimals"))
          }
        }
        assert(calls == 1)
        assert(actual.getClass == expected.getClass)
        assert(actual.getMessage == expected.getMessage)
      }
    }
  }

  test("width-specific source validation rejects malformed coefficients and scales") {
    Seq((4, 9), (8, 18), (16, 38)).foreach { case (width, precision) =>
      val bound = BigInt(10).pow(precision)
      val malformed = Seq(decimal(bound, precision, width),
        decimal(-bound, precision, width), decimal(BigInt(1), precision + 1, width),
        decimal(bound, 0, width), decimal(BigInt(1), 0, width).dropRight(1),
        decimal(BigInt(1), 0, width).take(1))
      malformed.foreach { payload =>
        val rows = Seq(obj(decimal(BigInt(125), 2, 4)), obj(payload))
        val target = DecimalType(5, 1)
        val expected = intercept[Exception](cpuValues(rows, target))
        withVariant(rows) { variant =>
          var calls = 0
          val actual = intercept[Exception] {
            GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
              withResource(GpuVariantDecimal.decode(raw, target) {
                calls += 1
                resultColumn(cpuValues(rows, target), target)
              })(_ => fail("malformed decimal must reach Spark's error"))
            }
          }
          assert(calls == 1)
          assert(actual.getClass == expected.getClass)
          assert(actual.getMessage == expected.getMessage)
        }
      }
      // The neighboring legal coefficient must not be rejected by an inclusive bound.
      assertGpu(Seq(decimal(bound - 1, precision, width),
        decimal(-bound + 1, precision, width)), DecimalType(5, 1), Seq("1.0", "-1.0"))
    }
  }

  test("nondecimal coercions fallback once and use Spark's source-specific casts") {
    val string = Array[Byte](17, '1'.toByte, '.'.toByte, '2'.toByte, '5'.toByte)
    val integer = Array[Byte](12, 7)
    val bool = Array[Byte](4)
    val float = Array[Byte](56, 0, 0, -96, 63)
    val double = Array[Byte](28, 0, 0, 0, 0, 0, 0, -12, 63)
    Seq(string, integer, bool, float, double, Array[Byte](0)).foreach { payload =>
      val rows = Seq(obj(decimal(BigInt(125), 2, 4)), obj(payload))
      val target = DecimalType(4, 1)
      val expected = cpuValues(rows, target)
      withVariant(rows) { variant =>
        var calls = 0
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(GpuVariantDecimal.decode(raw, target) {
            calls += 1
            resultColumn(cpuValues(rows, target), target)
          }) { result =>
            assert(calls == 1)
            assertValues(result, target, expected)
          }
        }
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
          withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { first =>
            assertValues(first, target, Seq("1.3"))
          }
          withResource(raw.getStatus.copyToHost()) { after =>
            assert(after.getByte(0) == before.getByte(0))
          }
          GpuVariantDecimal.decode(raw, target)(noFallback)
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
          var calls = 0
          withResource(GpuVariantDecimal.decode(raw, target) {
            calls += 1
            resultColumn(cpuValues(mixed, target), target)
          }) { out =>
            assert(calls == 1)
            assertValues(out, target, Seq("1.3", "7.0"))
          }
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
        withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { result =>
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
                  withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { out =>
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
                withResource(GpuVariantDecimal.decode(raw, target)(noFallback)) { out =>
                  assertValues(out, target, Seq(null, null, null))
                }
              }
            }
          }
        }
      }
    }
  }

  test("fallback failures close extraction columns and allow sequential decimal reuse") {
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
          GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
            value = raw.getValue
            status = raw.getStatus
            withResource(GpuVariantDecimal.decode(raw, target) {
              calls += 1
              throw failure
            })(_ => fail("fallback must throw"))
          }
        }
        assert(actual eq failure)
        assert(calls == 1)
        assert(value.getRefCount == 0)
        assert(status.getRefCount == 0)
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(GpuVariantDecimal.decode(raw, target) {
            resultColumn(cpuValues(rows, target), target)
          })(out => assertValues(out, target, Seq("1.3")))
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

  test("decimal missing and encoded-null rows fallback once on the complete input") {
    val target = DecimalType(4, 1)
    val rows = Seq(obj(decimal(BigInt(125), 2, 4)), Array[Byte](2, 0, 0), obj(Array[Byte](0)))
    assert(cpuValues(rows, target) == Seq("1.3", null, null))
    withVariant(rows) { variant =>
      var calls = 0
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        withResource(GpuVariantDecimal.decode(raw, target) {
          calls += 1
          resultColumn(cpuValues(rows, target), target)
        }) { result =>
          assert(calls == 1)
          assertValues(result, target, Seq("1.3", null, null))
        }
      }
    }
  }

  test("an empty encoded value is not an empty batch or SQL-null input") {
    val target = DecimalType(4, 1)
    val rows = Seq(Array.emptyByteArray)
    val expected = intercept[Exception](cpuValues(rows, target))
    withVariant(rows) { variant =>
      var calls = 0
      val actual = intercept[Exception] {
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(GpuVariantDecimal.decode(raw, target) {
            calls += 1
            resultColumn(cpuValues(rows, target), target)
          })(_ => fail("Spark must reject an empty encoded Variant"))
        }
      }
      assert(calls == 1)
      assert(actual.getClass == expected.getClass)
      assert(actual.getMessage == expected.getMessage)
    }
  }
}
