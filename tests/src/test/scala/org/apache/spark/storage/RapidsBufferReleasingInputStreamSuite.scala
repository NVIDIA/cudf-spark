/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package org.apache.spark.storage

import java.io.ByteArrayInputStream
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.network.buffer.ManagedBuffer

class RapidsBufferReleasingInputStreamSuite extends AnyFunSuite {
  test("concurrent worker and completion closes release the buffer only once") {
    val entered = new CountDownLatch(1)
    val proceed = new CountDownLatch(1)
    val closes = new AtomicInteger()
    val failure = new AtomicReference[Throwable]()
    val buffer = mock(classOf[ManagedBuffer])
    val delegate = new ByteArrayInputStream(Array.emptyByteArray) {
      override def close(): Unit = {
        if (closes.incrementAndGet() == 1) {
          entered.countDown()
          assert(proceed.await(5, TimeUnit.SECONDS))
        }
      }
    }
    val blockId = ShuffleBlockId(1, 1, 1)
    val result = RapidsShuffleBlockFetcherIterator.SuccessFetchResult(
      blockId, 0, null, 0, buffer, false)
    val stream = new RapidsBufferReleasingInputStream(
      delegate, null, result, blockId, 0, null, false, false, None)
    val worker = new Thread(() => {
      try stream.close() catch { case t: Throwable => failure.set(t) }
    })
    worker.start()
    try {
      assert(entered.await(5, TimeUnit.SECONDS))
      // The first close is still inside the delegate when completion closes it again.
      stream.close()
    } finally {
      proceed.countDown()
      worker.join(5000)
    }
    assert(!worker.isAlive)
    assert(failure.get() == null)
    stream.close()
    verify(buffer, times(1)).release()
  }
}
