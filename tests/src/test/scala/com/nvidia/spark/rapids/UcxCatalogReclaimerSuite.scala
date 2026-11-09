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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{FAILED, REMOVED}
import org.scalatest.funsuite.AnyFunSuite

class UcxCatalogReclaimerSuite extends AnyFunSuite {
  private val watermark = 100L

  private def requests(shuffleIds: Int*): Array[ShuffleReclaimRequest] =
    shuffleIds.map(ShuffleReclaimRequest(_, watermark)).toArray

  private def removed(shuffleId: Int, w: Long): ShuffleReclaimResult =
    ShuffleReclaimResult(shuffleId, w, REMOVED, 1, 10L, 1L, None)

  test("each reclaim's result is sent before the next reclaim starts") {
    val record = new ArrayBuffer[String]()
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer(
      (shuffleId, w, _) => {
        record += s"reclaim $shuffleId"
        removed(shuffleId, w)
      },
      result => {
        record += s"send ${result.shuffleId}"
        sent += result
      })
    reclaimer.reclaimRequests(requests(1, 2, 3), () => false)
    assertResult(Seq("reclaim 1", "send 1", "reclaim 2", "send 2", "reclaim 3", "send 3"))(
      record)
    assert(sent.forall(_.watermark == watermark), sent)
  }

  test("a reclaim that throws is sent as FAILED with the exception's class and message, and " +
      "the next reclaim still runs") {
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer(
      (shuffleId, w, _) =>
        if (shuffleId == 1) {
          throw new IllegalStateException("the catalog is gone")
        } else {
          removed(shuffleId, w)
        },
      result => sent += result)
    reclaimer.reclaimRequests(requests(1, 2), () => false)
    assertResult(Seq(1, 2))(sent.map(_.shuffleId))
    val failed = sent.head
    assertResult((FAILED, watermark, 0, 0L))(
      (failed.outcome, failed.watermark, failed.buffers, failed.bytes))
    assertResult(Some("java.lang.IllegalStateException: the catalog is gone"))(failed.error)
    assertResult(REMOVED)(sent(1).outcome)
  }
}
