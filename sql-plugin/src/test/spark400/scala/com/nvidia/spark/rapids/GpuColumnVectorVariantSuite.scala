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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.parquet.{ParquetCachedBatchSerializer, ParquetSchemaUtils}
import org.apache.parquet.schema.{MessageTypeParser, Type}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecializedGetters,
  UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.types.VariantType
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class GpuColumnVectorVariantSuite extends AnyFunSuite {
  private val byteListType = new HostColumnVector.ListType(
    true, new HostColumnVector.BasicType(false, DType.UINT8))
  private val variantSchema = StructType(Seq(StructField("v", VariantType, nullable = true)))

  private def parquetType(schema: String): Type =
    MessageTypeParser.parseMessageType(schema).asGroupType().getType("v")

  private def boxedBytes(bytes: Array[Byte]): java.util.List[java.lang.Byte] =
    Arrays.asList(bytes.map(Byte.box): _*)

  private def assertVariantConversion(
      rows: Seq[SpecializedGetters],
      expected: Seq[Option[VariantVal]],
      nullable: Boolean): Unit = {
    val converter = GpuRowToColumnConverter.getConverterForType(VariantType, nullable)
    withResource(new RapidsHostColumnBuilder(
        GpuColumnVector.convertFrom(VariantType, nullable), rows.length)) { builder =>
      val actualSize = rows.map(converter.append(_, 0, builder)).sum
      val childNullSize = Integer.BYTES + 0.125
      val expectedSize = expected.map {
        case None => 2 * childNullSize + 0.125
        case Some(value) =>
          value.getValue.length + value.getMetadata.length + 2 * childNullSize +
            (if (nullable) 0.125 else 0.0)
      }.sum
      assert(actualSize == expectedSize)
      withResource(new RapidsHostColumnVector(VariantType, builder.build())) { actual =>
        assert(actual.getBase.getRowCount == expected.length)
        expected.zipWithIndex.foreach {
          case (None, index) => assert(actual.isNullAt(index))
          case (Some(value), index) =>
            assert(!actual.isNullAt(index))
            assert(actual.getVariant(index).getValue.sameElements(value.getValue))
            assert(actual.getVariant(index).getMetadata.sameElements(value.getMetadata))
        }
      }
    }
  }

  private def assertHostVariantChildren(
      value: ColumnVector,
      metadata: ColumnVector,
      valueBytes: Array[Byte],
      metadataBytes: Array[Byte]): Unit = {
    withResource(ColumnVector.makeStruct(1, value, metadata)) { variant =>
      withResource(new RapidsHostColumnVector(VariantType, variant.copyToHost())) { host =>
        assert(host.getChild(0).getBinary(0).sameElements(valueBytes))
        assert(host.getChild(1).getBinary(0).sameElements(metadataBytes))
        assert(host.getVariant(0).getValue.sameElements(valueBytes))
        assert(host.getVariant(0).getMetadata.sameElements(metadataBytes))
      }
    }
  }

  test("Variant detection uses the Spark type and is excluded from the GPU cache path") {
    assert(GpuColumnVector.isVariantType(VariantType))
    assert(!GpuColumnVector.isVariantType(StringType))
    val serializer = new ParquetCachedBatchSerializer()
    assert(!serializer.isSupportedByCudf(VariantType))
    assert(!serializer.isSupportedByCudf(ArrayType(VariantType, containsNull = true)))
    assert(!serializer.isSupportedByCudf(
      MapType(StringType, VariantType, valueContainsNull = true)))
    assert(!serializer.isSupportedByCudf(
      StructType(Seq(StructField("v", VariantType, nullable = true)))))
  }

  test("Variant row conversion preserves value and metadata bytes") {
    val values = Seq(
      Some(new VariantVal(Array.emptyByteArray, Array.emptyByteArray)),
      None,
      Some(new VariantVal(Array[Byte](1, 2, 3), Array[Byte](4))),
      Some(new VariantVal(Array.fill[Byte](4097)(5), Array[Byte](6, 7, 8, 9))))
    val genericRows = values.map(value => new GenericInternalRow(Array[Any](value.orNull)))

    assert(GpuRowToColumnConverter.supportsType(VariantType))
    assertVariantConversion(genericRows, values, nullable = true)

    val toUnsafe = UnsafeProjection.create(variantSchema)
    val unsafeRows = genericRows.map(row => toUnsafe(row).copy())
    assertVariantConversion(unsafeRows, values, nullable = true)

    val nonNullValues = values.flatten
    val nonNullRows = nonNullValues.map(value => new GenericInternalRow(Array[Any](value)))
    assertVariantConversion(nonNullRows, nonNullValues.map(Some(_)), nullable = false)
  }

  test("nested Variant row conversion preserves nulls and bytes") {
    val first = new VariantVal(Array[Byte](1, 2), Array[Byte](3))
    val second = new VariantVal(Array[Byte](4), Array[Byte](5, 6))

    val nestedStructType = StructType(Seq(
      StructField("v", VariantType, nullable = true)))
    val structRows = Seq(
      new GenericInternalRow(Array[Any](new GenericInternalRow(Array[Any](first)))),
      new GenericInternalRow(Array[Any](null)),
      new GenericInternalRow(Array[Any](new GenericInternalRow(Array[Any](null)))))
    val structConverter = GpuRowToColumnConverter.getConverterForType(
      nestedStructType, nullable = true)
    assert(GpuRowToColumnConverter.supportsType(nestedStructType))
    withResource(new RapidsHostColumnBuilder(
        GpuColumnVector.convertFrom(nestedStructType, true), structRows.length)) {
      builder =>
        structRows.foreach(structConverter.append(_, 0, builder))
        withResource(new RapidsHostColumnVector(nestedStructType, builder.build())) { actual =>
          val nested = actual.getStruct(0).getVariant(0)
          assert(nested.getValue.sameElements(first.getValue))
          assert(nested.getMetadata.sameElements(first.getMetadata))
          assert(actual.isNullAt(1))
          assert(actual.getStruct(2).isNullAt(0))
        }
    }

    val nestedArrayType = ArrayType(VariantType, containsNull = true)
    val arrayRows = Seq(
      new GenericInternalRow(Array[Any](new GenericArrayData(Array[Any](first, null, second)))),
      new GenericInternalRow(Array[Any](null)),
      new GenericInternalRow(Array[Any](new GenericArrayData(Array.empty[Any]))))
    val arrayConverter = GpuRowToColumnConverter.getConverterForType(
      nestedArrayType, nullable = true)
    assert(GpuRowToColumnConverter.supportsType(nestedArrayType))
    withResource(new RapidsHostColumnBuilder(
        GpuColumnVector.convertFrom(nestedArrayType, true), arrayRows.length)) { builder =>
      arrayRows.foreach(arrayConverter.append(_, 0, builder))
      withResource(new RapidsHostColumnVector(nestedArrayType, builder.build())) { actual =>
        val array = actual.getArray(0)
        assert(array.numElements() == 3)
        assert(array.getVariant(0).getValue.sameElements(first.getValue))
        assert(array.isNullAt(1))
        assert(array.getVariant(2).getMetadata.sameElements(second.getMetadata))
        assert(actual.isNullAt(1))
        assert(actual.getArray(2).numElements() == 0)
      }
    }
  }

  test("null parent struct cascades to a non-nullable Variant field") {
    val nestedType = StructType(Seq(StructField("v", VariantType, nullable = false)))
    val variant = new VariantVal(Array[Byte](1, 2), Array[Byte](3))
    val converter = GpuRowToColumnConverter.getConverterForType(nestedType, nullable = true)
    withResource(new RapidsHostColumnBuilder(GpuColumnVector.convertFrom(nestedType, true), 2)) {
      builder =>
        val nullBytes = converter.append(new GenericInternalRow(Array[Any](null)), 0, builder)
        assert(nullBytes == 8.5)
        converter.append(new GenericInternalRow(Array[Any](
          new GenericInternalRow(Array[Any](variant)))), 0, builder)
        withResource(new RapidsHostColumnVector(nestedType, builder.build())) { actual =>
          assert(actual.isNullAt(0))
          val nonNull = actual.getStruct(1).getVariant(0)
          assert(nonNull.getValue.sameElements(variant.getValue))
          assert(nonNull.getMetadata.sameElements(variant.getMetadata))
        }
    }
  }

  test("map Variant values preserve nulls and bytes") {
    val nestedType = MapType(StringType, VariantType, valueContainsNull = true)
    val variant = new VariantVal(Array[Byte](1, 2), Array[Byte](3))
    val keys = new GenericArrayData(Array[Any](UTF8String.fromString("value"),
      UTF8String.fromString("null")))
    val values = new GenericArrayData(Array[Any](variant, null))
    val map = new ArrayBasedMapData(keys, values)
    val rows = Seq(new GenericInternalRow(Array[Any](map)),
      new GenericInternalRow(Array[Any](null)))
    val converter = GpuRowToColumnConverter.getConverterForType(nestedType, nullable = true)
    withResource(new RapidsHostColumnBuilder(
        GpuColumnVector.convertFrom(nestedType, true), rows.length)) { builder =>
      rows.foreach(converter.append(_, 0, builder))
      withResource(new RapidsHostColumnVector(nestedType, builder.build())) { actual =>
        val result = actual.getMap(0)
        assert(result.numElements() == 2)
        assert(result.keyArray().getUTF8String(0) == UTF8String.fromString("value"))
        val value = result.valueArray().getVariant(0)
        assert(value.getValue.sameElements(variant.getValue))
        assert(value.getMetadata.sameElements(variant.getMetadata))
        assert(result.valueArray().isNullAt(1))
        assert(actual.isNullAt(1))
      }
    }
  }

  test("Variant conversion requires value and metadata byte children") {
    withResource(ColumnVector.fromLists(
        byteListType, Arrays.asList(Byte.box(1.toByte)))) { value =>
      withResource(ColumnVector.fromLists(
          byteListType, Arrays.asList(Byte.box(2.toByte)))) { metadata =>
        withResource(ColumnVector.makeStruct(1, value, metadata)) { valid =>
          assert(GpuColumnVector.typeConversionAllowed(valid, VariantType))
        }
        withResource(ColumnVector.makeStruct(1, value)) { missingMetadata =>
          assert(!GpuColumnVector.typeConversionAllowed(missingMetadata, VariantType))
        }
      }
    }
  }

  test("Variant conversion accepts string and mixed binary children") {
    withResource(ColumnVector.fromStrings("value")) { value =>
      withResource(ColumnVector.fromStrings("metadata")) { metadata =>
        withResource(ColumnVector.makeStruct(1, value, metadata)) { strings =>
          assert(GpuColumnVector.typeConversionAllowed(strings, VariantType))
        }
        withResource(ColumnVector.fromLists(
            byteListType, Arrays.asList(Byte.box(2.toByte)))) { metadataBytes =>
          withResource(ColumnVector.makeStruct(1, value, metadataBytes)) { mixed =>
            assert(GpuColumnVector.typeConversionAllowed(mixed, VariantType))
          }
        }
      }
    }
  }

  test("Variant conversion rejects non-byte children") {
    withResource(ColumnVector.fromInts(1)) { value =>
      withResource(ColumnVector.fromLists(
          byteListType, Arrays.asList(Byte.box(2.toByte)))) { metadata =>
        withResource(ColumnVector.makeStruct(1, value, metadata)) { invalid =>
          assert(!GpuColumnVector.typeConversionAllowed(invalid, VariantType))
        }
      }
    }
  }

  test("Variant conversion rejects extra children") {
    withResource(ColumnVector.fromLists(
        byteListType, Arrays.asList(Byte.box(1.toByte)))) { value =>
      withResource(ColumnVector.fromLists(
          byteListType, Arrays.asList(Byte.box(2.toByte)))) { metadata =>
        withResource(ColumnVector.fromLists(
            byteListType, Arrays.asList(Byte.box(3.toByte)))) { extra =>
          withResource(ColumnVector.makeStruct(1, value, metadata, extra)) { invalid =>
            assert(!GpuColumnVector.typeConversionAllowed(invalid, VariantType))
          }
        }
      }
    }
  }

  test("Variant host children materialize as Spark binary columns") {
    val valueBytes = "value".getBytes(UTF_8)
    val metadataBytes = "metadata".getBytes(UTF_8)

    withResource(ColumnVector.fromStrings("value")) { value =>
      withResource(ColumnVector.fromStrings("metadata")) { metadata =>
        assertHostVariantChildren(value, metadata, valueBytes, metadataBytes)
      }
    }

    withResource(ColumnVector.fromLists(byteListType, boxedBytes(valueBytes))) { value =>
      withResource(ColumnVector.fromLists(
          byteListType, boxedBytes(metadataBytes))) { metadata =>
        assertHostVariantChildren(value, metadata, valueBytes, metadataBytes)
      }
    }
  }

  test("Variant memory estimates include child offsets, payload, and validity") {
    val rowCount = 64L
    val offsets = GpuBatchUtils.calculateOffsetBufferSize(rowCount) * 2
    val payload = VariantType.defaultSize * rowCount
    val validity = GpuBatchUtils.calculateValidityBufferSize(rowCount)

    assert(GpuBatchUtils.minGpuMemory(
      VariantType, nullable = false, rowCount = rowCount) == offsets)
    assert(GpuBatchUtils.minGpuMemory(
      VariantType, nullable = true, rowCount = rowCount) == offsets + validity)
    assert(GpuBatchUtils.minGpuMemory(
      VariantType, nullable = false, rowCount = rowCount, includeOffset = false) == 0)
    assert(GpuBatchUtils.estimateGpuMemory(
      VariantType, nullable = false, rowCount = rowCount) == offsets + payload)
    assert(GpuBatchUtils.estimateGpuMemory(
      VariantType, nullable = true, rowCount = rowCount) == offsets + payload + validity)
    assert(!GpuBatchUtils.isFixedWidth(VariantType))
    assert(GpuBatchUtils.isVariableWidth(VariantType))
  }

  test("null Variant scalar is an invalid struct") {
    withResource(GpuScalar(null, VariantType)) { variant =>
      assert(!variant.isValid)
      assert(!variant.getBase.isValid)
      assert(variant.getBase.getType == DType.STRUCT)
    }
  }

  test("Variant Parquet physical schema validation") {
    val valid = parquetType("""
      message spark_schema {
        optional group v {
          required binary value;
          required binary metadata;
        }
      }
      """)
    val reversed = parquetType("""
      message spark_schema {
        optional group v {
          required binary metadata;
          required binary value;
        }
      }
      """)
    assert(ParquetSchemaUtils.isVariantPhysicalType(valid))
    assert(ParquetSchemaUtils.isVariantPhysicalType(reversed))

    val invalidSchemas = Seq(
      "message spark_schema { required binary v; }",
      """message spark_schema {
        optional group v { required binary value; }
      }""",
      """message spark_schema {
        optional group v {
          required binary value;
          required binary metadata;
          required binary extra;
        }
      }""",
      """message spark_schema {
        optional group v {
          required binary value;
          required binary meta;
        }
      }""",
      """message spark_schema {
        optional group v {
          optional binary value;
          required binary metadata;
        }
      }""",
      """message spark_schema {
        optional group v {
          required group value { required binary bytes; }
          required binary metadata;
        }
      }""",
      """message spark_schema {
        optional group v {
          required int32 value;
          required binary metadata;
        }
      }""")

    invalidSchemas.foreach { schema =>
      assert(!ParquetSchemaUtils.isVariantPhysicalType(parquetType(schema)), schema)
    }
  }

  test("Variant Parquet fields are clipped into Spark child order") {
    val reversed = MessageTypeParser.parseMessageType("""
      message spark_schema {
        optional group v {
          required binary metadata;
          required binary value;
        }
      }
      """)

    val clipped = ParquetSchemaUtils.clipParquetSchema(
      reversed, variantSchema, caseSensitive = true, useFieldId = false)
    val variant = clipped.asGroupType().getType("v").asGroupType()
    assert(variant.getType(0).getName == "value")
    assert(variant.getType(1).getName == "metadata")
  }

  test("nested Variant Parquet fields are clipped into Spark child order") {
    val reversed = MessageTypeParser.parseMessageType("""
      message spark_schema {
        optional group variants (LIST) {
          repeated group list {
            optional group element {
              required binary metadata;
              required binary value;
            }
          }
        }
      }
      """)
    val schema = StructType(Seq(StructField(
      "variants", ArrayType(VariantType, containsNull = true), nullable = true)))

    val clipped = ParquetSchemaUtils.clipParquetSchema(
      reversed, schema, caseSensitive = true, useFieldId = false)
    val variant = clipped.asGroupType().getType("variants").asGroupType()
      .getType(0).asGroupType()
      .getType(0).asGroupType()
    assert(variant.getType(0).getName == "value")
    assert(variant.getType(1).getName == "metadata")
  }

  test("map value Variant Parquet fields are clipped into Spark child order") {
    val reversed = MessageTypeParser.parseMessageType("""
      message spark_schema {
        optional group variants (MAP) {
          repeated group key_value {
            required binary key;
            optional group value {
              required binary metadata;
              required binary value;
            }
          }
        }
      }
      """)
    val schema = StructType(Seq(StructField(
      "variants", MapType(StringType, VariantType, valueContainsNull = true), nullable = true)))

    val clipped = ParquetSchemaUtils.clipParquetSchema(
      reversed, schema, caseSensitive = true, useFieldId = false)
    val variant = clipped.asGroupType().getType("variants").asGroupType()
      .getType(0).asGroupType()
      .getType(1).asGroupType()
    assert(variant.getType(0).getName == "value")
    assert(variant.getType(1).getName == "metadata")
  }
}
