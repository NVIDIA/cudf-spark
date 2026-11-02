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
package org.apache.spark.sql.rapids.execution

import org.scalatest.funsuite.AnyFunSuite

class BroadcastBuildAdmissionSuite extends AnyFunSuite {
  test("accept empty input and a build exactly at the byte ceiling") {
    assert(BroadcastBuildAdmission.accepts(Array.empty[Long], 0, 10))
    assert(BroadcastBuildAdmission.accepts(Array(4L, 6L), 2, 10))
  }

  test("reject oversized builds without overflowing the size sum") {
    assert(!BroadcastBuildAdmission.accepts(Array(4L, 7L), 2, 10))
    assert(!BroadcastBuildAdmission.accepts(Array(Long.MaxValue, 1L), 2, Long.MaxValue))
  }

  test("reject unavailable or inconsistent row counts and cuDF row-count overflow") {
    assert(!BroadcastBuildAdmission.accepts(Array(10L), 0, 10))
    assert(!BroadcastBuildAdmission.accepts(Array.empty[Long], -1, 10))
    assert(!BroadcastBuildAdmission.accepts(Array(10L), Int.MaxValue.toLong + 1, 10))
    assert(!BroadcastBuildAdmission.accepts(Array(-1L), 2, 10))
    assert(!BroadcastBuildAdmission.accepts(Array.empty[Long], 0, 0))
  }
}
