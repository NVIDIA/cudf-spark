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

import java.nio.file.Files
import java.time.Duration
import java.util

import com.nvidia.spark.history.MetricStores
import com.nvidia.spark.rapids.HistoryMetricsManager
import org.scalatest.funsuite.AnyFunSuite

/**
 * The scan split lookup against the real local provider and a real SQLite database, wired as the
 * driver plugin wires them: the tiers are one request, the store returns every tier's readings,
 * and a scan finds the reading of an earlier scan.
 */
class ScanSplitLookupEndToEndSuite extends AnyFunSuite {

  private val MiB = 1024L * 1024
  private val Policy = HistoryPolicy(Duration.ofMillis(500), Duration.ofSeconds(5))

  /** The size bucket, `floor(log2(listed bytes))`, of the scans below: 2^30 listed bytes. */
  private val Size = 30L

  private val plan = 0x573590a3b4d841c2L

  private val day1 = PlanKey(plan, 19L)
  private val day2 = PlanKey(plan, 20L)

  /** Recording and lookup over the installed real store. */
  private final class Env(heuristic: ScanSplitHeuristic) {

    /** The lookup as `fileSourceSplit` makes it: the tier number from 1 and the ratio, if any. */
    def find(key: PlanKey, size: Long,
        aggregate: RatioAggregate = RatioAggregate.Latest): Option[(Int, Double)] = {
      readings(key, size).flatMap(aggregate.choose)
        .collect { case (ratio, tier) if MetricHistory.isUsable(ratio) => (tier + 1, ratio) }
    }

    /** The readings of every tier of the search list, as the store returns them. */
    def readings(key: PlanKey, size: Long): Option[IndexedSeq[IndexedSeq[Double]]] = {
      heuristic.historyOf.lookup(ScanHistoryKey(key, size).searchList,
        System.currentTimeMillis(), Policy, Some(Duration.ofDays(7)))
    }

    /** Records `ratio` (asynchronous in the store) and waits until its own key finds it. */
    def record(key: PlanKey, size: Long, ratio: Double): Unit = {
      heuristic.recordAll(ScanContext.forObservation(
        ScanHistoryKey(key, size), 1, () => ReadTotals((ratio * MiB).toLong, MiB, 1L)))
      val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
      while (find(key, size) != Some((1, ratio)) && System.nanoTime() < deadline) {
        Thread.sleep(20L)
      }
      assert(find(key, size) == Some((1, ratio)))
      Thread.sleep(5L) // the next reading is newer
    }
  }

  /** Runs `body` against a real store in a new database file of its own. */
  private def withHistory(body: Env => Unit): Unit = {
    val database = Files.createTempFile("scan-split-lookup", ".db")
    val manager = new HistoryMetricsManager
    try {
      val configuration = new util.HashMap[String, String]()
      configuration.put("spark.rapids.sql.history.metrics.local.path", database.toString)
      try {
        manager.initialize(configuration, "application-1", "attempt-1", "26.12.0-test", "local",
          HistoryHeuristics.catalog)
        val heuristic = new ScanSplitHeuristic(() => MetricStores.current())
        heuristic.enable(Policy)
        body(new Env(heuristic))
      } finally {
        manager.shutdown()
      }
    } finally {
      Seq("", "-wal", "-shm", "-journal").foreach { suffix =>
        Files.deleteIfExists(database.resolveSibling(database.getFileName.toString + suffix))
      }
    }
  }

  test("tier 1: the same plan, literals and size") {
    withHistory { env =>
      env.record(day1, Size, 4.0)
      assert(env.find(day1, Size) == Some((1, 4.0)))
    }
  }

  test("tier 2 hits a size bucket above or below and misses two away") {
    withHistory { env =>
      env.record(day1, Size, 4.0)
      assert(env.find(day1, Size + 1) == Some((2, 4.0)))
      assert(env.find(day1, Size - 1) == Some((2, 4.0)))
      assert(env.find(day1, Size + 2).isEmpty && env.find(day1, Size - 2).isEmpty)
    }
  }

  test("tier 3 hits with other literals within one bucket, and misses another plan") {
    withHistory { env =>
      env.record(day1, Size, 4.0)
      for (size <- Seq(Size - 1, Size, Size + 1)) {
        assert(env.find(day2, size) == Some((3, 4.0)), s"size=$size")
      }
      assert(env.find(day2, Size + 2).isEmpty && env.find(day2, Size - 2).isEmpty)
      // A different plan finds nothing, even by one bit, whatever its literals and size.
      val plans = Seq(0L, plan ^ 1L, plan + 1, ~plan, plan ^ Long.MinValue)
      for (p <- plans; l <- Seq(day1.literals, day2.literals, PlanKey.NONE);
           size <- Seq(Size - 1, Size, Size + 1)) {
        assert(env.find(PlanKey(p, l), size).isEmpty, s"plan=$p literals=$l size=$size")
      }
    }
  }

  test("tier order: tier 1 beats a newer reading of another day, then tier 2 beats tier 3") {
    withHistory { env =>
      env.record(day2, Size, 16.0)
      env.record(day1, Size, 4.0) // newer than day 2's
      assert(env.find(day1, Size) == Some((1, 4.0)))
      assert(env.find(day2, Size) == Some((1, 16.0)))
      assert(env.find(day2, Size + 1) == Some((2, 16.0)))
      // Another day has no reading of its own: tier 3 takes the newest of the others.
      assert(env.find(PlanKey(plan, 21L), Size) == Some((3, 4.0)))
    }
  }

  test("the store returns every tier's readings, each once, in its closest tier") {
    withHistory { env =>
      env.record(day1, Size, 4.0)
      env.record(day1, Size + 1, 5.0)
      env.record(day2, Size, 6.0)
      env.record(day1, Size, 3.0)
      // Tier 3 (any literals) also matches day 1's readings; each is returned in its closest tier.
      assert(env.readings(day1, Size).contains(Vector(Vector(3.0, 4.0), Vector(5.0), Vector(6.0))))
      assert(env.readings(PlanKey(plan, 21L), Size)
        .contains(Vector(Vector(), Vector(), Vector(3.0, 6.0, 5.0, 4.0))))
      assert(env.readings(PlanKey(plan ^ 1L, 21L), Size).contains(Vector(Vector(), Vector(),
        Vector())))
    }
  }

  test("extreme hashes work in every tier: negative, minimum, maximum, NONE") {
    val keys = Seq(PlanKey(Long.MinValue, 19L), PlanKey(Long.MaxValue, -1L),
      PlanKey(-12345L, Long.MinValue), PlanKey(1L, PlanKey.NONE))
    withHistory { env =>
      keys.zipWithIndex.foreach { case (key, i) => env.record(key, Size, 2.0 + i) }
      keys.zipWithIndex.foreach { case (key, i) =>
        val ratio = 2.0 + i
        assert(env.find(key, Size) == Some((1, ratio)))
        assert(env.find(key, Size + 1) == Some((2, ratio)))
        assert(env.find(PlanKey(key.plan, 777L), Size - 1) == Some((3, ratio)))
        assert(env.find(PlanKey(key.plan ^ 1L, key.literals), Size).isEmpty)
      }
    }
  }

  /** Records one reading for each day of `ratios`, oldest first, all of one plan and size. */
  private def days(env: Env, ratios: Double*): Unit =
    ratios.zipWithIndex.foreach { case (ratio, i) =>
      env.record(PlanKey(plan, 100L + i), Size, ratio)
    }

  private val newDay = PlanKey(plan, 999L)

  test("consistent: tier 3 readings more than 3x apart give no ratio, so Spark's split") {
    // Pruning that varies by day: most days decode little, one day decodes four times as much.
    withHistory { env =>
      days(env, 1.0, 4.0, 1.25)
      assert(env.find(newDay, Size) == Some((3, 1.25)))
      assert(env.find(newDay, Size, RatioAggregate.Consistent).isEmpty)
    }
  }

  test("consistent: tier 3 readings within 3x give their mean") {
    withHistory { env =>
      days(env, 3.0, 4.0, 3.5)
      assert(env.find(newDay, Size) == Some((3, 3.5)))
      assert(env.find(newDay, Size, RatioAggregate.Consistent) == Some((3, 3.5)))
      days(env, 6.0, 3.0) // 6 is twice the smallest, within 3x: still the mean
      val mean = env.find(newDay, Size, RatioAggregate.Consistent).map(_._2).get
      assert(math.abs(mean - 3.9) < 1e-9, mean) // (3 + 4 + 3.5 + 3 + 6) / 5
    }
  }

  test("consistent: a farther tier that agrees is used when a closer one disagrees") {
    withHistory { env =>
      env.record(newDay, Size, 1.0)
      env.record(newDay, Size, 4.0)
      days(env, 2.0, 2.5)
      assert(env.find(newDay, Size) == Some((1, 4.0)))
      assert(env.find(newDay, Size, RatioAggregate.Consistent) == Some((3, 2.25)))
    }
  }

  test("consistent: one reading is used as is, ahead of farther tiers") {
    withHistory { env =>
      days(env, 1.0, 8.0)
      env.record(newDay, Size, 2.0)
      // Tier 1 has one reading, which agrees with itself; tier 3 disagrees.
      assert(env.find(newDay, Size, RatioAggregate.Consistent) == Some((1, 2.0)))
      assert(env.find(PlanKey(plan, 1000L), Size, RatioAggregate.Consistent).isEmpty)
    }
  }
}
