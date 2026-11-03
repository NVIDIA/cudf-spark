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

package com.nvidia.spark.rapids.perf

import com.nvidia.spark.rapids.perf.ScanSplitSizer._
import org.scalatest.funsuite.AnyFunSuite

class ScanSplitSizerSuite extends AnyFunSuite {

  private val GiB = 1024L * 1024 * 1024
  private val MiB = 1024L * 1024

  test("raw split is the target batch divided by the ratio") {
    assert(rawSplit(4.0, GiB) == 256 * MiB)
    assert(rawSplit(0.5, GiB) == 2 * GiB)
    assert(rawSplit(3.0, 10L) == 3L) // truncates
  }

  test("raw split has no answer for unusable inputs") {
    Seq(0.0, -1.0, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { r =>
      assert(rawSplit(r, GiB) == 0L, s"ratio $r")
    }
    assert(rawSplit(2.0, 0L) == 0L)
    assert(rawSplit(2.0, -1L) == 0L)
  }

  test("bound returns the static split when there is no raw split") {
    assert(bound(0L, 100 * GiB, 96, 123L) == 123L)
    assert(bound(-5L, 100 * GiB, 96, -1L) == -1L)
  }

  test("bound clamps to [64 MiB, min(4 GiB, listed / minPartitionNum)]") {
    // inside the range
    assert(bound(256 * MiB, 1000 * GiB, 96, -1L) == 256 * MiB)
    // floor
    assert(bound(MiB, 1000 * GiB, 96, -1L) == MIN_SPLIT_BYTES)
    // absolute ceiling
    assert(bound(10 * GiB, 10000 * GiB, 96, -1L) == MAX_SPLIT_BYTES)
    // parallelism ceiling: 96 GiB over 96 slots
    assert(bound(3 * GiB, 96 * GiB, 96, -1L) == GiB)
    // a parallelism ceiling below the floor keeps the static split
    assert(bound(3 * GiB, 96 * MiB, 96, 123L) == 123L)
    // no parallelism known: only the absolute bounds apply
    assert(bound(3 * GiB, 96 * MiB, 0, -1L) == 3 * GiB)
    // unknown listed size leaves a ceiling of one byte, so the static split applies
    assert(bound(3 * GiB, 0L, 96, 123L) == 123L)
  }

  test("bound honours a configured floor and cap") {
    // cap below the default: a large raw split is held at the configured cap
    assert(bound(3 * GiB, 1000 * GiB, 96, -1L, 64 * MiB, 512 * MiB) == 512 * MiB)
    // floor above the default: a small raw split is raised to the configured floor
    assert(bound(MiB, 1000 * GiB, 96, -1L, 256 * MiB, 4 * GiB) == 256 * MiB)
    // the parallelism ceiling still applies below the configured cap
    assert(bound(3 * GiB, 96 * GiB, 96, -1L, 64 * MiB, 8 * GiB) == GiB)
    // a parallelism ceiling below the configured floor keeps the static split
    assert(bound(3 * GiB, 96 * GiB, 96, 123L, 2 * GiB, 4 * GiB) == 123L)
    // no parallelism known: the configured cap is the only ceiling
    assert(bound(3 * GiB, 96 * MiB, 0, -1L, 64 * MiB, GiB) == GiB)
  }

  private def mpn(
      files: Option[Int] = None,
      leaf: Option[Int] = None,
      default: Option[Int] = None,
      instances: Option[Int] = None,
      cores: Option[Int] = None,
      registered: Int = 7): Long =
    minPartitionNum(files, leaf, default, instances, cores, registered)

  test("split cap: spark.sql.files.minPartitionNum wins when set") {
    assert(mpn(files = Some(500), leaf = Some(3), default = Some(200),
      instances = Some(6), cores = Some(16)) == 500L)
  }

  test("split cap: leafNodeDefaultParallelism, then spark.default.parallelism") {
    assert(mpn(leaf = Some(40), default = Some(200), instances = Some(6), cores = Some(16)) == 40)
    assert(mpn(default = Some(200), instances = Some(6), cores = Some(16)) == 200L)
  }

  test("split cap: configured executor slots when no parallelism is set") {
    var registeredRead = false
    def registered: Int = { registeredRead = true; 32 }
    assert(minPartitionNum(None, None, None, Some(6), Some(16), registered) == 96L)
    assert(!registeredRead, "registered cores must not be consulted when slots are configured")
  }

  test("split cap: registered cores only when the slots are not configured") {
    assert(mpn(instances = Some(6)) == 7L)
    assert(mpn(cores = Some(16)) == 7L)
    assert(mpn() == 7L)
  }

  test("split cap: non-positive settings are ignored") {
    assert(mpn(files = Some(0), default = Some(-1), instances = Some(6), cores = Some(16)) == 96L)
    assert(mpn(instances = Some(0), cores = Some(16)) == 7L)
  }
}
