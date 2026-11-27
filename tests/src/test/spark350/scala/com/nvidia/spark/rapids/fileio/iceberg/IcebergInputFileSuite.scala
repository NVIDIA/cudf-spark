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
{"spark":["350"],"carryForward":"all"}
{"spark":["400","411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.fileio.iceberg

import java.nio.file.Files
import java.util.OptionalLong

import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import org.apache.hadoop.fs.Path
import org.apache.iceberg.io.{FileIO, InputFile}
import org.mockito.Mockito.{mock, verifyNoInteractions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IcebergInputFileSuite extends AnyFunSuite with Matchers {
  test("negative known lengths are rejected by the input file wrapper") {
    val delegate = mock(classOf[InputFile])
    for (length <- Seq(-1L, Long.MinValue)) {
      intercept[IllegalArgumentException] {
        new IcebergInputFile(delegate, OptionalLong.of(length))
      }.getMessage shouldBe "length must be non-negative"
    }
    verifyNoInteractions(delegate)
  }

  test("both known-length FileIO overloads reject negative lengths before delegate access") {
    val delegate = mock(classOf[FileIO])
    val io: RapidsFileIO = new IcebergFileIO(delegate)
    val path = new Path("s3://bucket/file.parquet")
    for (length <- Seq(-1L, Long.MinValue)) {
      intercept[IllegalArgumentException] {
        io.newInputFile(path.toString, length)
      }.getMessage shouldBe "length must be non-negative"
      intercept[IllegalArgumentException] {
        io.newInputFile(path, length)
      }.getMessage shouldBe "length must be non-negative"
    }
    verifyNoInteractions(delegate)
  }

  test("known lengths including zero avoid delegate length lookups") {
    val delegate = mock(classOf[InputFile])
    for (length <- Seq(0L, 17L, Long.MaxValue)) {
      new IcebergInputFile(delegate, OptionalLong.of(length)).getLength shouldBe length
    }
    verifyNoInteractions(delegate)
  }

  test("unknown lengths fall back to the actual file size") {
    val path = Files.createTempFile("iceberg-input-file", ".bin")
    try {
      Files.write(path, Array[Byte](1, 2, 3))
      val delegate = org.apache.iceberg.Files.localInput(path.toFile)
      new IcebergInputFile(delegate).getLength shouldBe 3L
      new IcebergInputFile(delegate, OptionalLong.empty()).getLength shouldBe 3L
    } finally {
      Files.deleteIfExists(path)
    }
  }
}
