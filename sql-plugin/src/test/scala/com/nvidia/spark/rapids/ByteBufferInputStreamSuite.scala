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

import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite

class ByteBufferInputStreamSuite extends AnyFunSuite {

  test("zero-length reads return zero without consuming input") {
    val dest = new Array[Byte](1)
    val stream = new ByteBufferInputStream(ByteBuffer.wrap(Array[Byte](42)))

    assertResult(0)(stream.read(dest, 0, 0))
    assertResult(42)(stream.read())
    assertResult(0)(stream.read(dest, dest.length, 0))
    assertResult(-1)(stream.read(dest, 0, 1))
    assertResult(0)(stream.read(dest, dest.length, 0))
  }

  test("array reads validate arguments even at end of stream") {
    val stream = new ByteBufferInputStream(ByteBuffer.allocate(0))
    val dest = new Array[Byte](1)

    assertThrows[NullPointerException](stream.read(null, 0, 0))
    assertThrows[IndexOutOfBoundsException](stream.read(dest, -1, 0))
    assertThrows[IndexOutOfBoundsException](stream.read(dest, 0, -1))
    assertThrows[IndexOutOfBoundsException](stream.read(dest, dest.length + 1, 0))
    assertThrows[IndexOutOfBoundsException](stream.read(dest, dest.length, 1))
  }

  test("skip ignores non-positive values and caps positive values") {
    val stream = new ByteBufferInputStream(ByteBuffer.wrap(Array[Byte](1, 2)))

    assertResult(0L)(stream.skip(-1))
    assertResult(0L)(stream.skip(0))
    assertResult(1)(stream.read())
    assertResult(1L)(stream.skip(Long.MaxValue))
    assertResult(-1)(stream.read())
    assertResult(0L)(stream.skip(1))
  }
}
