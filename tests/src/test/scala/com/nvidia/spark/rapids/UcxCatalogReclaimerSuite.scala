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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{ABSENT, FAILED, REMOVED}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.api.plugin.PluginContext

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

  test("a reclaim that throws an interrupt or a fatal error propagates it and sends no result") {
    Seq[Throwable](new InterruptedException("injected"), new LinkageError("injected")).foreach {
      error =>
        val sent = new ArrayBuffer[ShuffleReclaimResult]()
        val reclaimer = new UcxCatalogReclaimer((_, _, _) => throw error, result => sent += result)
        val thrown = intercept[Throwable](reclaimer.reclaimRequests(requests(1), () => false))
        assert(thrown eq error, thrown)
        assert(sent.isEmpty, sent)
    }
  }

  test("sendTo sends each result in a message of its own, with the executor id it read once") {
    val ctx = mock[PluginContext]
    when(ctx.executorID()).thenReturn("7")
    val send = UcxCatalogReclaimer.sendTo(ctx)
    val results = Seq(removed(1, watermark), removed(2, watermark))
    results.foreach(send)
    verify(ctx, times(1)).executorID()
    val messages = ArgumentCaptor.forClass(classOf[AnyRef])
    verify(ctx, times(2)).send(messages.capture())
    assertResult(results.map(r => ("7", Seq(r))))(messages.getAllValues.asScala.map {
      case RapidsShuffleReclaimResultMsg(executorId, sentResults) =>
        (executorId, sentResults.toSeq)
      case other => fail(s"sendTo sent $other")
    })
  }

  test("a closed reclaimer's poll asks the driver nothing") {
    val asked = new ArrayBuffer[AnyRef]()
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer((id, w, _) => removed(id, w), result => sent += result)
    reclaimer.poll("1", message => {
      asked += message
      RapidsShuffleReclaimResponseMsg(requests(1))
    }, () => true)
    assert(asked.isEmpty, asked)
    assert(sent.isEmpty, sent)
  }

  test("a poll given an empty reply or an unexpected one reclaims nothing and sends nothing") {
    val reclaimed = new ArrayBuffer[Int]()
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer((id, w, _) => {
      reclaimed += id
      removed(id, w)
    }, result => sent += result)
    Seq[AnyRef](RapidsShuffleReclaimResponseMsg(Array.empty),
        RapidsShuffleCleanupResponseMsg(Array.empty), null).foreach { reply =>
      reclaimer.poll("1", _ => reply, () => false)
    }
    assert(reclaimed.isEmpty, reclaimed)
    assert(sent.isEmpty, sent)
  }

  test("the default reclaim answers ABSENT for a shuffle that this executor never held") {
    // another suite in this JVM may leave a SparkEnv, so the shuffle id is one no suite uses
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    new UcxCatalogReclaimer(send = result => sent += result)
      .reclaimRequests(requests(Int.MaxValue), () => false)
    assertResult(Seq((Int.MaxValue, ABSENT)))(sent.map(r => (r.shuffleId, r.outcome)))
  }
}
