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

package com.nvidia.spark.rapids.shuffle.ucx

import java.nio.ByteBuffer

import scala.collection.mutable

import com.nvidia.spark.rapids.{HashedPriorityQueue, LogCaptureUtils, MetaUtils}
import com.nvidia.spark.rapids.shuffle.{PendingTransferRequest, RapidsShuffleClient,
  RapidsShuffleFetchHandler}
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

/**
 * Queue admission, as `UCXShuffleTransport.queuePending` applies it under the queue's lock. A
 * test cannot build the transport itself: its thread factories need the executor's GPU, and its
 * constructor starts the throttle thread.
 */
class UCXShuffleTransportSuite extends AnyFunSuite with MockitoSugar {

  private def queue(): HashedPriorityQueue[PendingTransferRequest] =
    new HashedPriorityQueue[PendingTransferRequest](10,
      (a: PendingTransferRequest, b: PendingTransferRequest) =>
        java.lang.Long.compare(a.getLength, b.getLength))

  private def transfer(handler: RapidsShuffleFetchHandler): PendingTransferRequest =
    PendingTransferRequest(mock[RapidsShuffleClient],
      MetaUtils.buildTableMeta(1, 100, ByteBuffer.allocate(8), 10), handler)

  test("a reader whose task is done gets none of its transfers queued") {
    val (altList, validHandlers) = (queue(), mutable.HashSet[RapidsShuffleFetchHandler]())
    val done = mock[RapidsShuffleFetchHandler]
    when(done.isDone).thenReturn(true)

    val logs = LogCaptureUtils.captureLogsFrom(Seq(classOf[UCXShuffleTransport].getName)) {
      assert(!UCXShuffleTransport.admit(altList, validHandlers, Seq(transfer(done))))
    }

    assert(altList.isEmpty)
    assert(!validHandlers.contains(done))
    // a refusal shows at DEBUG, since the refused transfers are never sent
    assert(logs.exists(_.contains("whose task is done")))
  }

  test("a reader whose task is running gets its transfers queued") {
    val (altList, validHandlers) = (queue(), mutable.HashSet[RapidsShuffleFetchHandler]())
    val running = mock[RapidsShuffleFetchHandler]
    // a reply's tables come in one call, and each must be queued
    val reqs = Seq(transfer(running), transfer(running))

    assert(UCXShuffleTransport.admit(altList, validHandlers, reqs))

    assertResult(2)(altList.size)
    assert(validHandlers.contains(running))
  }
}
