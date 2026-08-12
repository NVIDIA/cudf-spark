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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Arrays

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.VariantType

class GpuColumnVectorVariantSuite extends AnyFunSuite {
  private val byteListType = new HostColumnVector.ListType(
    true, new HostColumnVector.BasicType(false, DType.UINT8))

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
}
