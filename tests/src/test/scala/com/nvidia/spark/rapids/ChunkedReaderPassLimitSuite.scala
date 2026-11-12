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

import org.scalatest.funsuite.AnyFunSuite

class ChunkedReaderPassLimitSuite extends AnyFunSuite {
  private val MiB = 1024L * 1024
  private val GiB = 1024 * MiB
  private val MinLimit = MultiFileReaderUtils.MIN_CHUNKED_READER_PASS_LIMIT_BYTES

  private def passLimit(chunkSize: Long, pool: Long): Long =
    MultiFileReaderUtils.chunkedReaderPassLimit(chunkSize, pool)

  test("large pool uses the chunk size") {
    assert(passLimit(GiB, 30 * GiB) == GiB)
    assert(passLimit(2 * GiB, 90 * GiB) == 2 * GiB)
  }

  test("small pool caps the limit at 1/8 of the pool") {
    assert(passLimit(2 * GiB, 9 * GiB) == 9 * GiB / 8)
    assert(passLimit(GiB, 4 * GiB) == 512 * MiB)
  }

  test("limit never goes below the minimum") {
    assert(passLimit(GiB, GiB) == MinLimit)
    assert(passLimit(64 * MiB, 30 * GiB) == MinLimit)
  }

  test("unknown pool size uses the chunk size") {
    assert(passLimit(GiB, 0L) == GiB)
  }
}
