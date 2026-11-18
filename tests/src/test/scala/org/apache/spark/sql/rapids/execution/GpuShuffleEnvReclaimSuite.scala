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
import org.apache.spark.storage.BlockManagerId

/** The reclaim of a shuffle's cached broadcast builds around its catalog reclaim, with no GPU. */
class GpuShuffleEnvReclaimSuite extends AnyFunSuite {
  private val shuffleId = 21
  private val watermark = 100L
  private val buildBytes = 64L

  /**
   * A shuffle environment whose cache holds two builds of the shuffle: a stale one, whose output
   * is at the watermark, and a rerun's, whose output is above it. `events` records each disposal
   * and each catalog reclaim in the order they ran.
   *
   * @param disposalFailure what each disposal throws, if anything
   */
  private class Fixture(disposalFailure: Option[Throwable] = None) {
    val events = new ArrayBuffer[String]()
    val cache = new ExecutorBroadcastCache[SpillableColumnarBatch](owned => {
      owned.close()
      events += s"dispose ${owned.numRows()} rows"
      disposalFailure.foreach(failure => throw failure)
    })
    val env = new GpuShuffleEnv(new RapidsConf(Map.empty[String, String])) {
      override protected def createBroadcastCache():
          ExecutorBroadcastCache[SpillableColumnarBatch] = cache
    }
    cacheBuild(mapId = watermark, rows = 1)
    cacheBuild(mapId = watermark + 1, rows = 2)

    private def cacheBuild(mapId: Long, rows: Int): Unit = {
      val identity = BroadcastBuildIdentity(Seq(
        BroadcastBuildIdentity.MapOutput(0, mapId, BlockManagerId("a", "host-a", 1000))))
      env.broadcastCache.getOrBuild[Unit](shuffleId, () => new JustRowsColumnarBatch(rows) {
        override val sizeInBytes: Long = buildBytes
      }, () => (), () => (), identity = identity)(_ => (), _ => ())
    }

    /** A catalog reclaim that records itself and answers with `outcome`. */
    def catalogReclaim(outcome: ShuffleReclaimOutcome): ShuffleReclaimResult = {
      events += "catalog reclaim"
      ShuffleReclaimResult(shuffleId, watermark, outcome, 1, 10L, 0L, None)
    }
  }

  Seq(REMOVED, ABSENT, DEFERRED).foreach { outcome =>
    test(s"a reclaim that the catalog answers $outcome detaches the stale build first, and " +
        "keeps the catalog's outcome") {
      val fixture = new Fixture
      try {
        val (result, builds, bytes) = fixture.env.reclaimBroadcastAndThen(shuffleId, watermark,
          () => false)(fixture.catalogReclaim(outcome))
        assertResult(outcome)(result.outcome)
        assertResult((1, buildBytes))((builds, bytes))
        assertResult(Seq("dispose 1 rows", "catalog reclaim"))(fixture.events)
        assertResult(1)(fixture.cache.activeEntryCount)
      } finally {
        fixture.cache.close()
      }
    }
  }

  test("a FAILED reclaim, whose catalog reclaim throws, detaches the stale build first") {
    val fixture = new Fixture
    val sent = new ArrayBuffer[ShuffleReclaimResult]()
    val reclaimer = new UcxCatalogReclaimer((id, w, isStopped) =>
      fixture.env.reclaimBroadcastAndThen(id, w, isStopped) {
        fixture.events += "catalog reclaim"
        throw new IllegalStateException("catalog reclaim failed")
      }._1,
      result => sent += result)
    try {
      reclaimer.poll("1", _ => RapidsShuffleReclaimResponseMsg(
        Array(ShuffleReclaimRequest(shuffleId, watermark))), () => false)
      assertResult(Seq(FAILED))(sent.map(_.outcome))
      assertResult(Seq("dispose 1 rows", "catalog reclaim"))(fixture.events)
      assertResult(1)(fixture.cache.activeEntryCount)
    } finally {
      fixture.cache.close()
    }
  }

  test("a catalog reclaim that throws runs after the detach, and its error reaches the caller") {
    val fixture = new Fixture
    val failure = new IllegalStateException("catalog reclaim failed")
    try {
      val error = intercept[IllegalStateException] {
        fixture.env.reclaimBroadcastAndThen(shuffleId, watermark, () => false) {
          fixture.events += "catalog reclaim"
          throw failure
        }
      }
      assert(error eq failure)
      assertResult(Seq("dispose 1 rows", "catalog reclaim"))(fixture.events)
    } finally {
      fixture.cache.close()
    }
  }

  // In the two tests below every disposal throws, so the cache stays open with the rerun's build.

  test("a fatal disposal failure still runs the catalog reclaim, and then reaches the caller") {
    val fixture = new Fixture(Some(new InterruptedException("disposal interrupted")))
    try {
      intercept[InterruptedException] {
        fixture.env.reclaimBroadcastAndThen(shuffleId, watermark, () => false)(
          fixture.catalogReclaim(REMOVED))
      }
      assertResult(Seq("dispose 1 rows", "catalog reclaim"))(fixture.events)
    } finally {
      // clears the interrupt that a suppressed second failure would restore
      Thread.interrupted()
    }
  }

  test("a disposal failure logs a warning, and the reclaim keeps the catalog's outcome") {
    val fixture = new Fixture(Some(new IllegalStateException("disposal failed")))
    var outcome: Option[ShuffleReclaimOutcome] = None
    val warnings = LogCaptureUtils.captureLogsFrom(Seq(fixture.env.getClass.getName), "WARN") {
      outcome = Some(fixture.env.reclaimBroadcastAndThen(shuffleId, watermark, () => false)(
        fixture.catalogReclaim(REMOVED))._1.outcome)
    }
    assertResult(Some(REMOVED))(outcome)
    assertResult(Seq("dispose 1 rows", "catalog reclaim"))(fixture.events)
    assert(warnings.exists(_.startsWith(
      s"Failed to dispose a cached broadcast build of shuffle $shuffleId")), warnings.toSeq)
  }
}
