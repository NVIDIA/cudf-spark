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

package com.nvidia.spark.rapids.shims

import java.io.IOException

import org.mockito.Mockito.{doAnswer, doThrow, verify}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.orc.Reader

class OrcShimsSuite extends AnyFunSuite with MockitoSugar {

  test("closeReader clears and restores interrupt status") {
    val reader = mock[Reader]
    var closedWithInterruptSet = true
    doAnswer { _ =>
      closedWithInterruptSet = Thread.currentThread().isInterrupted
      null
    }.when(reader).close()

    Thread.interrupted()
    Thread.currentThread().interrupt()
    try {
      OrcShims.closeReader(reader)

      assert(!closedWithInterruptSet)
      assert(Thread.currentThread().isInterrupted)
      verify(reader).close()
    } finally {
      Thread.interrupted()
    }
  }

  test("closeReader restores interrupt status after non-fatal close failure") {
    val reader = mock[Reader]
    doThrow(new IOException("close failed")).when(reader).close()

    Thread.interrupted()
    Thread.currentThread().interrupt()
    try {
      OrcShims.closeReader(reader)

      assert(Thread.currentThread().isInterrupted)
      verify(reader).close()
    } finally {
      Thread.interrupted()
    }
  }
}
