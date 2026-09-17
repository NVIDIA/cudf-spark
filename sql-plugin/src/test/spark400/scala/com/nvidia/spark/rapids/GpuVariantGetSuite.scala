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

import java.util.Arrays

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, IntegerType, LongType,
  ShortType, StringType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

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

  test("extracts Variant with mixed binary child representations") {
    assertMixedVariantExtraction(valueAsString = true, metadataAsString = false)
    assertMixedVariantExtraction(valueAsString = false, metadataAsString = true)
  }

  test("supported Variant target types") {
    Seq(ByteType, ShortType, IntegerType, LongType, StringType).foreach { dataType =>
      assert(GpuVariantGet.isSupportedTargetType(dataType))
    }
    Seq(BooleanType, DoubleType).foreach { dataType =>
      assert(!GpuVariantGet.isSupportedTargetType(dataType))
    }
  }

  test("supported Variant paths") {
    Seq("$.field", "$._field", "$.a1.b_2").foreach { path =>
      assert(GpuVariantGet.parseSupportedPath(path).contains(path))
    }
  }

  test("unsupported Variant paths") {
    Seq("$", "$.items[0]", "$['field']", "$.bad-field", "field", "$.a.", "$.1a")
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
