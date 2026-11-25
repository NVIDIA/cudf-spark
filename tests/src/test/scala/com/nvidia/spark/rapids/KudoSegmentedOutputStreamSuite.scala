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
package com.nvidia.spark.rapids

import java.io.ByteArrayOutputStream

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.kudo.{KudoSerializer, OpenByteArrayOutputStream}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

class KudoSegmentedOutputStreamSuite extends AnyFunSuite {
  test("Kudo host slices preserve wire bytes across default-size segments") {
    val segmentSize = 128 * 1024
    val large = "abc\u03bb" * 70000
    withResource(HostColumnVector.fromBoxedInts(10, 11, null, 13, 14, 15)) { ints =>
      withResource(HostColumnVector.fromStrings("excluded", large, null, "", large, large)) {
        strings =>
          val serializer = new KudoSerializer(
            GpuColumnVector.from(Array[DataType](IntegerType, StringType)))
          val columns = Array(ints, strings)
          val contiguous = new OpenByteArrayOutputStream()
          val segmented = new SegmentedByteArrayOutputStream(segmentSize)
          val expected = serializer.writeToStreamWithMetrics(columns, contiguous, 1, 5)
          val actual = serializer.writeToStreamWithMetrics(columns, segmented, 1, 5)
          segmented.close()
          val bytes = new ByteArrayOutputStream()
          withResource(segmented.takeBuffer()) { owned =>
            owned.writeTo(bytes)
          }
          assert(actual.getWrittenBytes == expected.getWrittenBytes)
          assert(segmented.getCount == contiguous.getCount)
          assert(bytes.toByteArray.sameElements(contiguous.toByteArray))
      }
    }
  }
}
