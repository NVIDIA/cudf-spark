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

import org.apache.iceberg.io.{FileIO, InputFile}
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IcebergFileIOSuite extends AnyFunSuite with Matchers {
  test("known file sizes are forwarded to Iceberg FileIO") {
    val delegate = mock(classOf[FileIO])
    val input = mock(classOf[InputFile])
    when(delegate.newInputFile("s3://bucket/key", 123L)).thenReturn(input)
    val file = new IcebergFileIO(delegate).newInputFile("s3://bucket/key", 123L)
    file.getDelegate shouldBe input
    verify(delegate).newInputFile("s3://bucket/key", 123L)
    verify(delegate, never()).newInputFile("s3://bucket/key")
  }
}
