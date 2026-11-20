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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{JustRowsColumnarBatch, LogCaptureUtils, RapidsConf, RapidsShuffleReclaimResponseMsg, ShuffleReclaimOutcome, ShuffleReclaimRequest, ShuffleReclaimResult, SpillableColumnarBatch, UcxCatalogReclaimer}
import com.nvidia.spark.rapids.ShuffleReclaimOutcome.{ABSENT, DEFERRED, FAILED, REMOVED}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.GpuShuffleEnv

/** The reclaim of a shuffle's cached broadcast builds around its catalog reclaim, with no GPU. */
class GpuShuffleEnvReclaimSuite extends AnyFunSuite {
  private val shuffleId = 21
  private val watermark = 100L
  private val buildBytes = 64L

  /**
   * A shuffle environment whose cache holds one build of the shuffle. `events` records each
   * disposal and each catalog reclaim in the order they ran.
   *
   * @param disposalFailure what the disposal throws, if anything
   */
  private class Fixture(disposalFailure: Option[Throwable] = None) {
    val events = new ArrayBuffer[String]()
    val cache = new ExecutorBroadcastCache[SpillableColumnarBatch](owned => {
      owned.close()
      events += "dispose"
      disposalFailure.foreach(failure => throw failure)
    })
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String])) {
      override protected def createBroadcastCache():
          ExecutorBroadcastCache[SpillableColumnarBatch] = cache
    }
    env.broadcastCache.getOrBuild[Unit](shuffleId, () => new JustRowsColumnarBatch(1) {
      override val sizeInBytes: Long = buildBytes
    }, () => (), () => ())(_ => (), _ => ())

    /** A catalog reclaim that records itself and answers with `outcome`. */
    def catalogReclaim(outcome: ShuffleReclaimOutcome): ShuffleReclaimResult = {
      events += "catalog reclaim"
      ShuffleReclaimResult(shuffleId, watermark, outcome, 1, 10L, 0L, None)
    }
  }

  test("a reclaim detaches the shuffle's build first, and keeps whichever outcome the catalog " +
      "answers") {
    Seq(REMOVED, ABSENT, DEFERRED).foreach { outcome =>
      val fixture = new Fixture
      try {
        withClue(s"$outcome: ") {
          val (result, builds, bytes) = fixture.env.reclaimBroadcastAndThen(shuffleId,
            () => false)(fixture.catalogReclaim(outcome))
          assertResult(outcome)(result.outcome)
          assertResult((1, buildBytes))((builds, bytes))
          assertResult(Seq("dispose", "catalog reclaim"))(fixture.events)
          assertResult(0)(fixture.cache.activeEntryCount)
        }
      } finally {
        fixture.cache.close()
      }
    }
  }

  test("a FAILED reclaim, whose catalog reclaim throws, detaches the shuffle's build first") {
    val fixture = new Fixture
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer((id, _, isStopped) =>
      fixture.env.reclaimBroadcastAndThen(id, isStopped) {
        fixture.events += "catalog reclaim"
        throw new IllegalStateException("catalog reclaim failed")
      }._1,
      result => sent += result)
    try {
      reclaimer.poll("1", _ => RapidsShuffleReclaimResponseMsg(
        Array(ShuffleReclaimRequest(shuffleId, watermark))), () => false)
      assertResult(Seq(FAILED))(sent.map(_.outcome))
      assertResult(Seq("dispose", "catalog reclaim"))(fixture.events)
      assertResult(0)(fixture.cache.activeEntryCount)
    } finally {
      fixture.cache.close()
    }
  }

  test("a fatal disposal failure still runs the catalog reclaim, and then reaches the caller") {
    val fixture = new Fixture(Some(new InterruptedException("disposal interrupted")))
    try {
      intercept[InterruptedException] {
        fixture.env.reclaimBroadcastAndThen(shuffleId, () => false)(
          fixture.catalogReclaim(REMOVED))
      }
      assertResult(Seq("dispose", "catalog reclaim"))(fixture.events)
    } finally {
      fixture.cache.close()
    }
  }

  test("a disposal failure logs a warning, and the reclaim keeps the catalog's outcome") {
    val fixture = new Fixture(Some(new IllegalStateException("disposal failed")))
    try {
      var outcome: Option[ShuffleReclaimOutcome] = None
      val warnings = LogCaptureUtils.captureLogsFrom(Seq(fixture.env.getClass.getName), "WARN") {
        outcome = Some(fixture.env.reclaimBroadcastAndThen(shuffleId, () => false)(
          fixture.catalogReclaim(REMOVED))._1.outcome)
      }
      assertResult(Some(REMOVED))(outcome)
      assertResult(Seq("dispose", "catalog reclaim"))(fixture.events)
      assert(warnings.exists(_.startsWith(
        s"Failed to dispose a cached broadcast build of shuffle $shuffleId")), warnings.toSeq)
    } finally {
      fixture.cache.close()
    }
  }
}
