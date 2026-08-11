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

package com.nvidia.spark.rapids.parquet

import org.scalatest.funsuite.AnyFunSuite

class PartitionFileReadHistogramSuite extends AnyFunSuite {

  test("summarize uses nearest-rank percentiles") {
    val summary = PartitionFileReadHistogram.summarize(
      Array.tabulate(20)(index => (20 - index).toDouble))

    assert(summary.count == 20)
    assert(summary.min == 1.0)
    assert(summary.p50 == 10.0)
    assert(summary.p75 == 15.0)
    assert(summary.p95 == 19.0)
    assert(summary.max == 20.0)
  }

  test("summarize supports a single sample") {
    val summary = PartitionFileReadHistogram.summarize(Array(42.5))

    assert(summary.count == 1)
    assert(summary.min == 42.5)
    assert(summary.p50 == 42.5)
    assert(summary.p75 == 42.5)
    assert(summary.p95 == 42.5)
    assert(summary.max == 42.5)
  }
}
