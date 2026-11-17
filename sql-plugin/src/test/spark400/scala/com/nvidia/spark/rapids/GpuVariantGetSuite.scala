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
  VariantOperationResult, VariantOperationStatus, VariantUtils}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType,
  DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType,
  TimestampNTZType, TimestampType, VariantType}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class GpuVariantGetSuite extends AnyFunSuite {
  private val byteListType = new HostColumnVector.ListType(
    true, new HostColumnVector.BasicType(false, DType.UINT8))
  private val encodedMetadata = Array[Byte](1, 1, 0, 2, 'i'.toByte, 'd'.toByte)
  private val encodedValue = Array[Byte](2, 1, 0, 0, 2, 12, 1)

  private def makeBinaryColumn(bytes: Array[Byte], asString: Boolean): ColumnVector = {
    if (asString) {
      ColumnVector.fromUTF8Strings(bytes)
    } else {
      ColumnVector.fromLists(byteListType, Arrays.asList(bytes.map(Byte.box): _*))
    }
  }

  private def assertMixedVariantExtraction(
      valueAsString: Boolean,
      metadataAsString: Boolean): Unit = {
    withResource(makeBinaryColumn(encodedValue, valueAsString)) { value =>
      withResource(makeBinaryColumn(encodedMetadata, metadataAsString)) { metadata =>
        withResource(ColumnVector.makeStruct(1, value, metadata)) { variant =>
          withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
            val expression = GpuVariantGet(Literal(0), "$.id", IntegerType, null)
            withResource(expression.doColumnar(input)) { result =>
              withResource(result.copyToHost()) { host =>
                assert(!host.isNull(0))
                assert(host.getInt(0) == 1)
              }
            }
          }
        }
      }
    }
  }

  // Spark Variant binary encoding for [true, 1.25f, -2.5d]. Keep this encoded because
  // Databricks Spark 4.0 does not expose the OSS org.apache.spark.types.variant API.
  private val exactScalarArrayValue = Array[Byte](
    3, 3, 0, 1, 6, 15, 4, 56, 0, 0, -96, 63, 28, 0, 0, 0, 0, 0, 0, 4, -64)
  private val emptyMetadata = Array[Byte](1, 0, 0)

  private def binaryRows(rows: Seq[Array[Byte]], asString: Boolean): ColumnVector = {
    if (asString) {
      ColumnVector.fromUTF8Strings(rows: _*)
    } else {
      val lists = rows.map(bytes => Arrays.asList(bytes.map(Byte.box): _*))
      ColumnVector.fromLists(byteListType, lists: _*)
    }
  }

  private def withVariant[T](
      rows: Seq[Array[Byte]],
      metadata: Array[Byte] = encodedMetadata,
      valueAsString: Boolean = false,
      metadataAsString: Boolean = false)(f: ColumnView => T): T = {
    withResource(binaryRows(rows, valueAsString)) { values =>
      withResource(binaryRows(rows.map(_ => metadata), metadataAsString)) { metas =>
        withResource(ColumnVector.makeStruct(rows.length, values, metas))(f)
      }
    }
  }

  private def integerObject(value: Long, width: Int): Array[Byte] = {
    val tag = Map(1 -> 12, 2 -> 16, 4 -> 20, 8 -> 24)(width).toByte
    val payload = Array(tag) ++ Array.tabulate(width)(i => (value >> (8 * i)).toByte)
    Array[Byte](2, 1, 0, 0, payload.length.toByte) ++ payload
  }

  private def noFallback: ColumnVector = fail("unexpected CPU fallback")

  private def decode(raw: VariantOperationResult, input: ColumnView, dt: DataType)(
      fallback: => ColumnVector): ColumnVector =
    GpuVariantGet.decodeVariantField(raw, dt, input.getNullCount)(fallback)

  private def withParentNulls[T](variant: ColumnView, nullRows: Set[Int])(
      f: ColumnView => T): T = {
    val rows = variant.getRowCount.toInt
    val validity = (0 until rows).map { i =>
      if (nullRows(i)) null else java.lang.Integer.valueOf(i)
    }
    withResource(ColumnVector.fromBoxedInts(validity: _*)) { mask =>
      withResource(variant.getChildColumnView(0)) { value =>
        withResource(variant.getChildColumnView(1)) { metadata =>
          withResource(new ColumnView(DType.STRUCT, rows,
              Optional.of[java.lang.Long](mask.getNullCount), mask.getValid, null,
              Array[ColumnView](value, metadata)))(f)
        }
      }
    }
  }

  /** Checks the GPU result and returns Some, or returns None when the batch fell back. */
  private def decodeOrFallback(variant: ColumnView, dt: DataType, path: String = "$.id")(
      check: HostColumnVector => Unit): Option[Unit] = {
    GpuVariantExtraction.withRawExtraction(variant, path) { raw =>
      var calls = 0
      withResource(decode(raw, variant, dt) {
        calls += 1
        ColumnVector.fromBoxedInts(Seq.fill(variant.getRowCount.toInt)(
          null.asInstanceOf[java.lang.Integer]): _*)
      }) { result =>
        if (calls == 0) Some(withResource(result.copyToHost())(check)) else None
      }
    }
  }

  private val emptyObject = Array[Byte](2, 0, 0)
  private val stringId = Array[Byte](2, 1, 0, 0, 2, 5, 'x'.toByte)

  test("SQL-null parents, missing paths and decoded values stay on the GPU") {
    withVariant(Seq(encodedValue, emptyObject, encodedValue, emptyObject)) { variant =>
      withParentNulls(variant, Set(2)) { nullable =>
        Seq(IntegerType, LongType, ByteType).foreach { dt =>
          withClue(dt) {
            assert(decodeOrFallback(nullable, dt) { host =>
              assert(!host.isNull(0))
              Seq(1, 2, 3).foreach(i => assert(host.isNull(i)))
            }.isDefined)
          }
        }
      }
    }
    // A batch where every row is missing still needs no CPU work.
    withVariant(Seq(emptyObject, emptyObject)) { variant =>
      Seq(IntegerType, StringType, BooleanType, DoubleType).foreach { dt =>
        withClue(dt) {
          assert(decodeOrFallback(variant, dt)(host => assert(host.getRowCount == 2)).isDefined)
        }
      }
    }
  }

  test("malformed extraction, Variant null and type mismatches invoke the CPU fallback") {
    val variantNull = Array[Byte](2, 1, 0, 0, 1, 0)
    Seq(
      "malformed" -> Seq(encodedValue, Array.emptyByteArray),
      "malformed with missing" -> Seq(encodedValue, emptyObject, Array.emptyByteArray),
      "Variant null" -> Seq(encodedValue, variantNull),
      "type mismatch" -> Seq(encodedValue, stringId)).foreach { case (name, rows) =>
      withClue(name) {
        withVariant(rows) { variant =>
          assert(decodeOrFallback(variant, IntegerType)(_ => ()).isEmpty)
        }
      }
    }
    // A malformed row under a SQL-null parent is hidden, so the batch stays on the GPU.
    withVariant(Seq(encodedValue, Array.emptyByteArray, emptyObject)) { variant =>
      withParentNulls(variant, Set(1)) { nullable =>
        assert(decodeOrFallback(nullable, IntegerType) { host =>
          assert(host.getInt(0) == 1)
          assert(host.isNull(1) && host.isNull(2))
        }.isDefined)
      }
    }
  }

  test("all integer widths decode independently and retain the original extraction status") {
    val numbers = Seq(7L, 300L, 40000L, 3000000000L, -9L)
    val rows = numbers.zip(Seq(1, 2, 4, 8, 1)).map { case (n, width) =>
      integerObject(n, width)
    }
    withVariant(rows) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        assert(raw.getStatus.getValid == null)
        withResource(raw.getStatus.copyToHost()) { before =>
          withResource(VariantUtils.castVariantValueWithStatus(
              raw.getValue, DType.INT8, raw.getStatus)) { first =>
            withResource(first.getStatus.copyToHost()) { host =>
              assert(host.getByte(1) == VariantOperationStatus.TYPE_MISMATCH.getNativeId)
              assert(before.getByte(1) == VariantOperationStatus.SUCCESS.getNativeId)
            }
          }
          withResource(decode(raw, variant, LongType)(noFallback)) { result =>
            withResource(result.copyToHost()) { host =>
              numbers.indices.foreach(i => assert(host.getLong(i) == numbers(i)))
            }
          }
          withResource(raw.getStatus.copyToHost()) { after =>
            numbers.indices.foreach(i => assert(after.getByte(i) == before.getByte(i)))
          }
          withResource(decode(raw, variant, ByteType)(noFallback)) { result =>
            withResource(result.copyToHost()) { host =>
              assert(host.getByte(0) == 7)
              assert(host.getByte(4) == -9)
              Seq(1, 2, 3).foreach(i => assert(host.isNull(i)))
            }
          }
        }
      }
    }
  }

  test("a SQL-null parent hides poisoned children without CPU fallback") {
    withVariant(Seq(Array[Byte](0xfc.toByte)), metadata = Array[Byte](0)) { variant =>
      withResource(ColumnVector.fromBoxedInts(null.asInstanceOf[java.lang.Integer])) { mask =>
        withResource(variant.getChildColumnView(0)) { value =>
          withResource(variant.getChildColumnView(1)) { metadata =>
            withResource(new ColumnView(DType.STRUCT, 1, Optional.of(1L), mask.getValid,
                null, Array[ColumnView](value, metadata))) { nullable =>
              GpuVariantExtraction.withRawExtraction(nullable, "$[0]") { raw =>
                withResource(raw.getStatus.copyToHost()) { host =>
                  assert(host.getByte(0) == VariantOperationStatus.ROW_NULL.getNativeId)
                }
                withResource(decode(raw, nullable, IntegerType)(noFallback)) {
                  result => assert(result.getNullCount == 1)
                }
                withResource(decode(raw, nullable, StringType)(noFallback)) {
                  result =>
                    assert(result.getType == DType.STRING)
                    assert(result.getNullCount == 1)
                }
              }
            }
          }
        }
      }
    }
  }

  test("missing paths stay on the GPU and Variant null invokes the whole-batch fallback once") {
    withVariant(Seq(encodedValue, encodedValue)) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.missing") { raw =>
        withResource(raw.getStatus.copyToHost()) { host =>
          assert(host.getByte(0) == VariantOperationStatus.MISSING_PATH.getNativeId)
          assert(host.getByte(1) == VariantOperationStatus.MISSING_PATH.getNativeId)
        }
        withResource(decode(raw, variant, IntegerType)(noFallback)) {
          result => assert(result.getNullCount == 2)
        }
      }
    }
    Seq((Array[Byte](2, 1, 0, 0, 1, 0), "$.id", VariantOperationStatus.VARIANT_NULL))
      .foreach { case (bytes, path, status) =>
      withVariant(Seq(bytes, bytes)) { variant =>
        var calls = 0
        GpuVariantExtraction.withRawExtraction(variant, path) { raw =>
          withResource(raw.getStatus.copyToHost()) { host =>
            assert(host.getByte(0) == status.getNativeId)
            assert(host.getByte(1) == status.getNativeId)
          }
          withResource(decode(raw, variant, IntegerType) {
            calls += 1
            ColumnVector.fromInts(71, 83)
          }) { result =>
            assert(calls == 1)
            withResource(result.copyToHost()) { host =>
              assert(host.getInt(0) == 71)
              assert(host.getInt(1) == 83)
            }
          }
        }
      }
    }
  }

  test("native Variant-null status must not hide Spark's metadata constructor error") {
    // [null]: array tag, one element, offsets 0/1, then the encoded null payload.
    // A root null followed by [0] would instead report MISSING_PATH.
    val bytes = Array[Byte](3, 1, 0, 1, 0)
    val invalidMetadata = Array[Byte](0)
    val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$[0]"),
      IntegerType, failOnError = false, timeZoneId = Some("UTC"))
    val input = InternalRow(new VariantVal(bytes, invalidMetadata))
    val expected = intercept[Exception](cpu.eval(input))
    withVariant(Seq(bytes), metadata = invalidMetadata) { variant =>
      var calls = 0
      val actual = intercept[Exception] {
        GpuVariantExtraction.withRawExtraction(variant, "$[0]") { raw =>
          withResource(raw.getStatus.copyToHost()) { host =>
            assert(host.getByte(0) == VariantOperationStatus.VARIANT_NULL.getNativeId)
          }
          decode(raw, variant, IntegerType) {
            calls += 1
            cpu.eval(input)
            fail("Spark must reject invalid metadata even for Variant null")
          }
        }
      }
      assert(calls == 1)
      assert(actual.getClass == expected.getClass)
      assert(actual.getMessage == expected.getMessage)
    }
  }

  test("empty and truncated encoded bodies match Spark") {
    // Spark rejects an empty body but returns NULL when an array path meets a primitive root,
    // without reading its payload. Only the empty body needs the CPU fallback.
    Seq(Array.emptyByteArray, Array[Byte](12), Array[Byte](24, 1)).foreach { bytes =>
      val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$[0]"),
        IntegerType, failOnError = false, timeZoneId = Some("UTC"))
      val sparkThrows = scala.util.Try(
        cpu.eval(InternalRow(new VariantVal(bytes, emptyMetadata)))) match {
        case scala.util.Success(value) => assert(value == null); false
        case scala.util.Failure(_) => true
      }
      withVariant(Seq(bytes), metadata = emptyMetadata) { variant =>
        withClue(bytes.mkString("[", ",", "]")) {
          val gpu = decodeOrFallback(variant, IntegerType, "$[0]")(h => assert(h.isNull(0)))
          assert(sparkThrows == bytes.isEmpty)
          assert(gpu.isEmpty == sparkThrows)
        }
      }
    }
  }

  test("known gap: an unrecognized root reported as a missing path returns NULL as on main") {
    // cuDF reports MISSING_PATH when an array path meets an unrecognized root type ID, while
    // Spark rejects the value. Missing paths stay on the GPU, so this returns NULL until cuDF
    // reports MALFORMED_VARIANT for it. Spark, Delta and parse_json never write these bytes.
    val bytes = Array[Byte](0xfc.toByte)
    val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$[0]"),
      IntegerType, failOnError = false, timeZoneId = Some("UTC"))
    intercept[Exception](cpu.eval(InternalRow(new VariantVal(bytes, emptyMetadata))))
    withVariant(Seq(bytes), metadata = emptyMetadata) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$[0]") { raw =>
        withResource(raw.getStatus.copyToHost()) { host =>
          assert(host.getByte(0) == VariantOperationStatus.MISSING_PATH.getNativeId)
        }
        withResource(decode(raw, variant, IntegerType)(noFallback)) {
          result => assert(result.getNullCount == 1)
        }
      }
    }
  }

  test("decoded output outlives extraction and input remains reusable after callback failures") {
    withVariant(Seq(encodedValue)) { variant =>
      withResource(GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        decode(raw, variant, IntegerType)(noFallback)
      }) { result =>
        withResource(result.copyToHost())(host => assert(host.getInt(0) == 1))
      }
      Seq[Throwable](new IllegalStateException("callback failure"),
        new OutOfMemoryError("injected callback OOM"),
        new NoClassDefFoundError("injected callback linkage error")).foreach { failure =>
        var rawValue: ColumnVector = null
        var rawStatus: ColumnVector = null
        val actual = intercept[Throwable] {
          GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
            rawValue = raw.getValue
            rawStatus = raw.getStatus
            throw failure
          }
        }
        assert(actual eq failure)
        assert(rawValue.getRefCount == 0)
        assert(rawStatus.getRefCount == 0)
        // This checks sequential reuse, not allocation OOM or the task retry machinery.
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(decode(raw, variant, IntegerType)(noFallback)) { result =>
            withResource(result.copyToHost())(host => assert(host.getInt(0) == 1))
          }
        }
      }
    }
  }

  test("empty batches and nonzero-offset real views preserve mixed binary rows") {
    Seq((false, true), (true, false)).foreach { case (valueString, metadataString) =>
      withVariant(Seq.empty, valueAsString = valueString,
          metadataAsString = metadataString) { variant =>
        GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
          withResource(decode(raw, variant, IntegerType)(noFallback)) { result =>
            assert(result.getRowCount == 0)
          }
        }
      }
      val rows = (0 until 100).map(i => integerObject(i, 1))
      withVariant(rows, valueAsString = valueString,
          metadataAsString = metadataString) { variant =>
        val validity = (0 until 100).map { i =>
          if (i % 7 == 0) null else java.lang.Integer.valueOf(i)
        }
        withResource(ColumnVector.fromBoxedInts(validity: _*)) { mask =>
          withResource(variant.getChildColumnView(0)) { value =>
            withResource(variant.getChildColumnView(1)) { metadata =>
              withResource(new ColumnView(DType.STRUCT, 100,
                  Optional.of[java.lang.Long](mask.getNullCount), mask.getValid,
                  null, Array[ColumnView](value, metadata))) { nullable =>
                // Real offsets cross packed validity words and end with a partial word.
                withResource(nullable.splitAsViews(31, 96)) { views =>
                  GpuVariantExtraction.withRawExtraction(views(1), "$.id") { raw =>
                    assert(raw.getStatus.getValid == null)
                    withResource(decode(raw, views(1), IntegerType)(noFallback)) {
                      result =>
                        withResource(result.copyToHost()) { host =>
                          assert(host.getRowCount == 65)
                          (0 until 65).foreach { i =>
                            if ((i + 31) % 7 == 0) assert(host.isNull(i))
                            else assert(host.getInt(i) == i + 31)
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
    }
  }

  test("extracts Variant with mixed binary child representations") {
    assertMixedVariantExtraction(valueAsString = true, metadataAsString = false)
    assertMixedVariantExtraction(valueAsString = false, metadataAsString = true)
  }

  test("directly decodes exact Boolean, Float, and Double Variant values") {
    withResource(makeBinaryColumn(exactScalarArrayValue, asString = false)) { value =>
      withResource(makeBinaryColumn(emptyMetadata, asString = false)) { metadata =>
        withResource(ColumnVector.makeStruct(1, value, metadata)) { variant =>
          withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
            def assertDecoded(path: String, dataType: DataType)(check: HostColumnVector => Unit)
                : Unit = {
              // A null fallback makes any unexpected CPU bridge use fail this test.
              val expression = GpuVariantGet(Literal(0), path, dataType, null)
              withResource(expression.doColumnar(input)) { result =>
                withResource(result.copyToHost()) { host =>
                  assert(host.getRowCount == 1)
                  assert(!host.isNull(0))
                  check(host)
                }
              }
            }

            assertDecoded("$[0]", BooleanType)(host => assert(host.getBoolean(0)))
            assertDecoded("$[1]", FloatType)(host => assert(host.getFloat(0) == 1.25f))
            assertDecoded("$[2]", DoubleType)(host => assert(host.getDouble(0) == -2.5))
          }
        }
      }
    }
  }

  test("doColumnar directly decodes exact String Variant values without CPU fallback") {
    val expected = "hi\u0000\u00e9"
    val utf8 = expected.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    // Object field id contains a short string: low bits 01, upper bits UTF-8 byte length.
    val payload = Array(((utf8.length << 2) | 1).toByte) ++ utf8
    val bytes = Array[Byte](2, 1, 0, 0, payload.length.toByte) ++ payload
    Seq((false, true), (true, false)).foreach { case (valueString, metadataString) =>
      withResource(makeBinaryColumn(bytes, valueString)) { value =>
        withResource(makeBinaryColumn(encodedMetadata, metadataString)) { metadata =>
          withResource(ColumnVector.makeStruct(1, value, metadata)) { variant =>
            withResource(new GpuColumnVector(VariantType, variant.incRefCount())) { input =>
              // A null CPU fallback fails if doColumnar accidentally enters the bridge.
              val expression = GpuVariantGet(Literal(0), "$.id", StringType, null)
              withResource(expression.doColumnar(input)) { result =>
                assert(result.getType == DType.STRING)
                withResource(result.copyToHost()) { host =>
                  assert(host.getRowCount == 1)
                  assert(!host.isNull(0))
                  assert(host.getJavaString(0) == expected)
                }
              }
            }
          }
        }
      }
    }
  }

  test("supported Variant target types") {
    Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      StringType).foreach { dataType =>
      assert(GpuVariantGet.isSupportedTargetType(dataType))
    }
    Seq(BinaryType, DateType, DecimalType(10, 2), TimestampType, TimestampNTZType).foreach {
      dataType => assert(!GpuVariantGet.isSupportedTargetType(dataType))
    }
  }

  test("supported Variant paths") {
    Seq("$.field", "$._field", "$.a1.b_2", "$.items[0].name", "$[0]", "$[0][01]",
      "$[2147483647]").foreach { path =>
      assert(GpuVariantGet.parseSupportedPath(path).contains(path))
    }
  }

  test("unsupported Variant paths") {
    Seq("$", "$['field']", "$[\"field\"]", "$.bad-field", "field", "$.a.", "$.1a",
      "$[-1]", "$[+1]", "$[*]", "$[]", "$[2147483648]")
        .foreach { path =>
          assert(GpuVariantGet.parseSupportedPath(path).isEmpty)
        }
  }

  test("Variant path expression must be a string literal") {
    val utf8Path = UTF8String.fromString("$.field")
    assert(GpuVariantGet.parseSupportedPath(Literal(utf8Path, StringType)).contains("$.field"))
    assert(GpuVariantGet.parseSupportedPath(Literal("$.field")).contains("$.field"))
    val nonLiteralPath = BoundReference(0, StringType, nullable = false)
    assert(GpuVariantGet.parseSupportedPath(nonLiteralPath).isEmpty)
  }
}
