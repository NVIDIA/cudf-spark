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
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.internal.SQLConf
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

  private def decode(raw: GpuVariantExtraction.Extraction, dt: DataType)(
      fallback: => ColumnVector): ColumnVector =
    GpuVariantGet.decodeVariantField(raw, dt).getOrElse(fallback)

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
      withResource(decode(raw, dt) {
        calls += 1
        ColumnVector.fromBoxedInts(Seq.fill(variant.getRowCount.toInt)(
          null.asInstanceOf[java.lang.Integer]): _*)
      }) { result =>
        if (calls == 0) Some(withResource(result.copyToHost())(check)) else None
      }
    }
  }

  private val emptyObject = Array[Byte](2, 0, 0)
  private val variantNull = Array[Byte](2, 1, 0, 0, 1, 0)
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

  test("malformed extraction and type mismatches invoke the CPU fallback") {
    Seq(
      "malformed" -> Seq(encodedValue, Array.emptyByteArray),
      "malformed with missing" -> Seq(encodedValue, emptyObject, Array.emptyByteArray),
      "type mismatch" -> Seq(encodedValue, stringId),
      "type mismatch with Variant null" -> Seq(variantNull, stringId)).foreach {
      case (name, rows) =>
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
          withResource(decode(raw, LongType)(noFallback)) { result =>
            withResource(result.copyToHost()) { host =>
              numbers.indices.foreach(i => assert(host.getLong(i) == numbers(i)))
            }
          }
          withResource(raw.getStatus.copyToHost()) { after =>
            numbers.indices.foreach(i => assert(after.getByte(i) == before.getByte(i)))
          }
          withResource(decode(raw, ByteType)(noFallback)) { result =>
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
                withResource(decode(raw, IntegerType)(noFallback)) {
                  result => assert(result.getNullCount == 1)
                }
                withResource(decode(raw, StringType)(noFallback)) {
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

  test("missing paths stay on the GPU and a type mismatch invokes the whole-batch fallback once") {
    withVariant(Seq(encodedValue, encodedValue)) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.missing") { raw =>
        withResource(raw.getStatus.copyToHost()) { host =>
          assert(host.getByte(0) == VariantOperationStatus.MISSING_PATH.getNativeId)
          assert(host.getByte(1) == VariantOperationStatus.MISSING_PATH.getNativeId)
        }
        withResource(decode(raw, IntegerType)(noFallback)) {
          result => assert(result.getNullCount == 2)
        }
      }
    }
    withVariant(Seq(variantNull, stringId)) { variant =>
      var calls = 0
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        withResource(decode(raw, IntegerType) {
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

  test("Variant nulls stay on the GPU as NULL for every target type") {
    Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      StringType).foreach { dt =>
      withClue(dt) {
        val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$.id"),
          dt, failOnError = false, timeZoneId = Some("UTC"))
        assert(cpu.eval(InternalRow(new VariantVal(variantNull, encodedMetadata))) == null)
        withVariant(Seq(variantNull, emptyObject, variantNull)) { variant =>
          GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
            withResource(raw.getStatus.copyToHost()) { host =>
              assert(host.getByte(0) == VariantOperationStatus.VARIANT_NULL.getNativeId)
            }
          }
          assert(decodeOrFallback(variant, dt) { host =>
            (0 until 3).foreach(i => assert(host.isNull(i)))
          }.isDefined)
        }
      }
    }
    withVariant(Seq(encodedValue, variantNull)) { variant =>
      assert(decodeOrFallback(variant, IntegerType) { host =>
        assert(host.getInt(0) == 1)
        assert(host.isNull(1))
      }.isDefined)
    }
  }

  test("metadata that Spark rejects falls back when the path starts with an array index") {
    // cuDF validates the metadata only when it resolves an object key, while Spark's Variant
    // constructor rejects bad metadata before it reads the path.
    val values = Seq(
      "missing" -> (Array[Byte](12, 1), None),
      "decoded" -> (Array[Byte](3, 1, 0, 2, 12, 1), Some(1)),
      "Variant null" -> (Array[Byte](3, 1, 0, 1, 0), None))
    val badMetadata = Seq(Array[Byte](2, 0, 0), Array[Byte](0), Array.emptyByteArray)
    for ((name, (bytes, expected)) <- values; asString <- Seq(false, true)) {
      withClue(s"$name string=$asString") {
        for (metadata <- badMetadata; path <- Seq("$[0]", "$[0].id")) {
          withClue(s"${metadata.mkString("[", ",", "]")} $path") {
            val cpu = VariantGet(BoundReference(0, VariantType, nullable = true),
              Literal(path), IntegerType, failOnError = false, timeZoneId = Some("UTC"))
            val error = intercept[Exception] {
              cpu.eval(InternalRow(new VariantVal(bytes, metadata)))
            }
            assert(error.getMessage.contains("[MALFORMED_VARIANT]"))
            withVariant(Seq(bytes), metadata, metadataAsString = asString) { variant =>
              assert(decodeOrFallback(variant, IntegerType, path)(_ => ()).isEmpty)
            }
          }
        }
        withVariant(Seq(bytes), emptyMetadata, metadataAsString = asString) { variant =>
          assert(decodeOrFallback(variant, IntegerType, "$[0]") { host =>
            expected match {
              case Some(value) => assert(host.getInt(0) == value)
              case None => assert(host.isNull(0))
            }
          }.isDefined)
        }
      }
    }
    // A SQL-null parent hides its bad metadata.
    val array = Array[Byte](3, 1, 0, 2, 12, 1)
    withResource(binaryRows(Seq(array, array), asString = false)) { values =>
      withResource(binaryRows(Seq(emptyMetadata, Array[Byte](2, 0, 0)), asString = false)) {
        metadata =>
          withResource(ColumnVector.makeStruct(2, values, metadata)) { variant =>
            assert(decodeOrFallback(variant, IntegerType, "$[0]")(_ => ()).isEmpty)
            withParentNulls(variant, Set(1)) { nullable =>
              assert(decodeOrFallback(nullable, IntegerType, "$[0]") { host =>
                assert(host.getInt(0) == 1)
                assert(host.isNull(1))
              }.isDefined)
            }
          }
      }
    }
  }

  test("cuDF rejects bad metadata when the path starts with an object key") {
    // decodeVariantField skips the metadata check for these paths, whatever the root holds.
    val roots = Seq(encodedValue, Array[Byte](3, 1, 0, 2, 12, 1), Array[Byte](12, 1),
      Array[Byte](0))
    val badMetadata = Seq(Array[Byte](2, 0, 0), Array[Byte](0), Array.emptyByteArray)
    for (bytes <- roots; metadata <- badMetadata; path <- Seq("$.id", "$.id[0]")) {
      withClue(s"${bytes.mkString("[", ",", "]")} ${metadata.mkString("[", ",", "]")} $path") {
        withVariant(Seq(bytes), metadata) { variant =>
          GpuVariantExtraction.withRawExtraction(variant, path) { raw =>
            withResource(raw.getStatus.copyToHost()) { host =>
              assert(host.getByte(0) == VariantOperationStatus.MALFORMED_VARIANT.getNativeId)
            }
          }
          assert(decodeOrFallback(variant, IntegerType, path)(_ => ()).isEmpty)
        }
      }
    }
  }

  test("rows longer than the Variant size limit fail the constructor check") {
    def fails(variant: ColumnView, limit: Long = 4): Boolean =
      GpuVariantExtraction.withRawExtraction(variant, "$.id")(_.failsVariantConstructor(limit))
    val small = Array[Byte](12, 1)
    val large = Array[Byte](3, 1, 0, 2, 12, 1)
    Seq(false, true).foreach { asString =>
      withClue(s"string=$asString") {
        def variant[T](rows: Seq[Array[Byte]], metadata: Array[Byte])(f: ColumnView => T): T =
          withVariant(rows, metadata, valueAsString = asString, metadataAsString = asString)(f)
        // Each child holds more bytes than the limit in total, but no single row does.
        variant(Seq(small, small, small), emptyMetadata)(v => assert(!fails(v)))
        variant(Seq(small, large), emptyMetadata)(v => assert(fails(v)))
        variant(Seq(small), encodedMetadata)(v => assert(fails(v)))
        variant(Seq(large), encodedMetadata)(v => assert(!fails(v, limit = 6)))
      }
    }
    withVariant(Seq(large)) { variant =>
      assert(!GpuVariantExtraction.withRawExtraction(variant, "$[0]")(_.failsVariantConstructor()))
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
          decode(raw, IntegerType) {
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

  test("a path step that meets an unrecognized primitive type ID falls back as Spark rejects it") {
    // cuDF reports MISSING_PATH when an object or array step meets a primitive with a type ID
    // that Spark does not recognize, while Spark rejects the value. The GPU checks the root and
    // the value at every proper prefix of the path whenever a row is missing.
    def primitive(id: Int, payload: Int): Array[Byte] =
      Array((id << 2).toByte) ++ new Array[Byte](payload)
    def field(bytes: Array[Byte]): Array[Byte] =
      Array[Byte](2, 1, 0, 0, bytes.length.toByte) ++ bytes
    def element(bytes: Array[Byte]): Array[Byte] =
      Array[Byte](3, 1, 0, bytes.length.toByte) ++ bytes
    def placements(p: Array[Byte]): Seq[(Array[Byte], String)] = Seq(
      p -> "$[0]", p -> "$.id", field(p) -> "$.id.id", field(p) -> "$.id[0]",
      element(p) -> "$[0].id", element(p) -> "$[0][0]", field(element(p)) -> "$.id[0].id")
    def spark(bytes: Array[Byte], path: String) = scala.util.Try {
      VariantGet(BoundReference(0, VariantType, nullable = true), Literal(path), IntegerType,
        failOnError = false, timeZoneId = Some("UTC"))
        .eval(InternalRow(new VariantVal(bytes, encodedMetadata)))
    }
    // Type IDs 17 to 19 and 21 to 63 are unrecognized; 20 (UUID) and short strings are not.
    // cuDF already reports MALFORMED_VARIANT for an object field whose value has ID 21 or more,
    // so IDs 17 to 19 cover the prefix check for object fields; root and array placements cover
    // it for every ID.
    for ((id, payload) <- Seq(17 -> 8, 18 -> 8, 19 -> 8, 21 -> 0, 40 -> 4, 63 -> 0);
        (bytes, path) <- placements(primitive(id, payload))) {
      withClue(s"id=$id $path") {
        assert(spark(bytes, path).isFailure)
        withVariant(Seq(encodedValue, emptyObject, bytes)) { variant =>
          assert(decodeOrFallback(variant, IntegerType, path)(_ => ()).isEmpty)
          // The same bytes under a SQL-null parent stay hidden.
          withParentNulls(variant, Set(2)) { nullable =>
            assert(decodeOrFallback(nullable, IntegerType, path)(_ => ()).isDefined)
          }
        }
      }
    }
    for (p <- Seq(primitive(20, 16), Array[Byte](5, 'x'.toByte), primitive(16, 4));
        (bytes, path) <- placements(p)) {
      withClue(s"${p.head} $path") {
        assert(spark(bytes, path).get == null)
        withVariant(Seq(bytes)) { variant =>
          assert(decodeOrFallback(variant, IntegerType, path)(h => assert(h.isNull(0))).isDefined)
        }
      }
    }
  }

  test("the unrecognized type IDs come from Spark and drive the header check") {
    // Spark 4.0 to 4.2 recognize primitive type IDs 0 to 16 and 20. Check only what every build
    // must agree on, so a runtime that recognizes a different set of newer IDs still passes.
    val unrecognized = GpuVariantGet.UnrecognizedPrimitiveTypeIds
    assert((0 to 16).forall(id => !unrecognized.contains(id)))
    assert((21 to 63).forall(unrecognized.contains))
    assert(GpuVariantExtraction.idRanges(Seq(21, 17, 19, 18, 22, 63)) ==
      Seq((17, 19), (21, 22), (63, 63)))
    val uuid = Array[Byte](0x50) ++ new Array[Byte](16)
    withVariant(Seq(encodedValue, uuid)) { variant =>
      GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        // A build that does not recognize UUID (20) must fall back where this Spark does not.
        assert(!raw.hasUnrecognizedPrimitiveStep)
        assert(raw.hasUnrecognizedPrimitiveStep(Seq(20)))
        assert(!raw.hasUnrecognizedPrimitiveStep(Seq(16, 21)))
        assert(!raw.hasUnrecognizedPrimitiveStep(Seq.empty))
      }
    }
  }

  test("only Spark's unknown-primitive error marks a type ID as unrecognized") {
    // Spark's own exception class is private[spark], so stand in for it.
    class ConditionError(condition: String) extends RuntimeException(condition)
        with SparkThrowable {
      override def getCondition: String = condition
    }
    // Unrelated failures are logged and treated as unrecognized, the conservative choice.
    val ids = GpuVariantGet.probeUnrecognizedIds { id =>
      if (id == 30) {
        throw new IllegalStateException("unrelated")
      } else if (id == 31) {
        throw new NoClassDefFoundError("unrelated")
      } else if (id == 32) {
        throw new ConditionError("MALFORMED_VARIANT")
      } else if (id > 40) {
        throw new ConditionError("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT")
      }
    }
    assert(ids == Seq(30, 31, 32) ++ (41 to 63))
  }

  test("path prefixes come from the parser that accepts the path") {
    Seq(
      "$.a" -> Seq(),
      "$[0]" -> Seq(),
      "$.a.b" -> Seq("$.a"),
      "$.a[0].b_1" -> Seq("$.a", "$.a[0]"),
      "$[1][2].c[3]" -> Seq("$[1]", "$[1][2]", "$[1][2].c")).foreach { case (path, prefixes) =>
      withClue(path) {
        assert(GpuVariantExtraction.pathPrefixes(path).contains(prefixes))
        assert(GpuVariantGet.supportedPathSteps(path).map(_.mkString("$", "", "")).contains(path))
      }
    }
    Seq("$", "$.", "$['a']", "$.a.*", "$[99999999999]", "a").foreach { path =>
      withClue(path)(assert(GpuVariantExtraction.pathPrefixes(path).isEmpty))
    }
  }

  test("ASCII keys are found among non-ASCII keys sorted in Spark's UTF-16 order") {
    // Spark sorts object keys by UTF-16 code units, so it writes "id" < U+1F600 < U+FF21, while
    // UTF-8 byte order puts U+FF21 before U+1F600. ASCII keys sort first in both orders, so
    // cuDF's byte-order binary search still finds them.
    val keys = Seq("\uFF21", "\uD83D\uDE00", "id").map(_.getBytes("UTF-8"))
    val offsets = keys.scanLeft(0)(_ + _.length).map(_.toByte)
    val metadata = Array[Byte](1, 3) ++ offsets ++ keys.flatten
    val bytes = Array[Byte](2, 3, 2, 1, 0, 0, 2, 4, 6, 12, 3, 12, 1, 12, 2)
    val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$.id"),
      IntegerType, failOnError = false, timeZoneId = Some("UTC"))
    assert(cpu.eval(InternalRow(new VariantVal(bytes, metadata))) == 3)
    withVariant(Seq(bytes, emptyObject), metadata) { variant =>
      assert(decodeOrFallback(variant, IntegerType, "$.id") { host =>
        assert(host.getInt(0) == 3)
        assert(host.isNull(1))
      }.isDefined)
    }
  }

  test("known gap: unsorted or out-of-range object field ids read as missing paths") {
    // cuDF binary-searches the field ids, while Spark scans objects with fewer than 32 fields
    // linearly and probes larger ones in a different order. Spark returns
    // the value for unsorted ids and rejects an id outside the dictionary; cuDF reports
    // MISSING_PATH for both. Spark and parse_json never write such objects.
    val ab = Array[Byte](1, 2, 0, 1, 2, 'a'.toByte, 'b'.toByte)
    val b = Array[Byte](1, 1, 0, 1, 'b'.toByte)
    // 40 keys k00..k39, and an object that lists their ids in reverse. From 32 fields Spark also
    // binary-searches, but probes differently, so it still finds k20.
    val keys40 = (0 until 40).map(i => f"k$i%02d".getBytes("UTF-8"))
    val metadata40 = Array[Byte](1, 40) ++ (0 to 40).map(i => (3 * i).toByte) ++ keys40.flatten
    val reversed40 = Array[Byte](2, 40) ++ (39 to 0 by -1).map(_.toByte) ++
      (0 to 40).map(i => (2 * i).toByte) ++ Seq.fill(40)(Seq[Byte](12, 1)).flatten
    Seq(
      ("40 unsorted ids", metadata40, reversed40, "$.k20", true),
      ("unsorted ids", ab, Array[Byte](2, 2, 1, 0, 0, 2, 4, 12, 1, 12, 2), "$.b", true),
      ("id outside the dictionary", b, Array[Byte](2, 2, 5, 0, 0, 2, 4, 12, 1, 12, 2), "$.c",
        false)).foreach { case (name, metadata, bytes, path, sparkReturnsValue) =>
      withClue(name) {
        val cpu = VariantGet(BoundReference(0, VariantType, nullable = true), Literal(path),
          IntegerType, failOnError = false, timeZoneId = Some("UTC"))
        val spark = scala.util.Try(cpu.eval(InternalRow(new VariantVal(bytes, metadata))))
        assert(spark.isSuccess == sparkReturnsValue)
        if (sparkReturnsValue) assert(spark.get == 1)
        withVariant(Seq(bytes), metadata) { variant =>
          GpuVariantExtraction.withRawExtraction(variant, path) { raw =>
            withResource(raw.getStatus.copyToHost()) { host =>
              assert(host.getByte(0) == VariantOperationStatus.MISSING_PATH.getNativeId)
            }
            withResource(decode(raw, IntegerType)(noFallback)) {
              result => assert(result.getNullCount == 1)
            }
          }
        }
      }
    }
  }

  test("decoded output outlives extraction and input remains reusable after callback failures") {
    withVariant(Seq(encodedValue)) { variant =>
      withResource(GpuVariantExtraction.withRawExtraction(variant, "$.id") { raw =>
        decode(raw, IntegerType)(noFallback)
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
          withResource(decode(raw, IntegerType)(noFallback)) { result =>
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
          withResource(decode(raw, IntegerType)(noFallback)) { result =>
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
                    withResource(decode(raw, IntegerType)(noFallback)) {
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
      StringType, DecimalType(1, 0), DecimalType(9, 2), DecimalType(38, 38)).foreach { dataType =>
      assert(GpuVariantGet.isSupportedTargetType(dataType))
    }
    Seq(BinaryType, DateType, TimestampType, TimestampNTZType).foreach { dataType =>
      assert(!GpuVariantGet.isSupportedTargetType(dataType))
    }
    // OSS's low-level constructor permits precision zero; a vendor may reject it earlier.
    scala.util.Try(DecimalType(0, 0)).foreach { target =>
      assert(!GpuVariantGet.isSupportedTargetType(target))
    }
    val sqlConf = new SQLConf
    sqlConf.setConfString("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    SQLConf.withExistingConf(sqlConf) {
      assert(!GpuVariantGet.isSupportedTargetType(DecimalType(10, -2)))
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
