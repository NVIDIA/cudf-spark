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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import org.mockito.ArgumentMatchers.{anyInt, anyLong}
import org.mockito.Mockito.{never, verify}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockResolver}
import org.apache.spark.sql.rapids.shims.GpuShuffleBlockResolver
import org.apache.spark.util.collection.OpenHashSet

/**
 * Regression tests for issue #14695: when the RAPIDS shuffle manager is using a
 * [[GpuShuffleBlockResolver]] (e.g. multi-threaded / skipMerge=false path),
 * `unregisterShuffle` must drive `removeDataByMap` on the wrapped
 * [[IndexShuffleBlockResolver]] for every tracked map id so that the on-disk
 * shuffle files are removed when Spark GCs the shuffle id upstream.
 */
class RapidsShuffleManagerUnregisterSuite extends AnyFunSuite with MockitoSugar {

  // Subclass that lets us swap in a controlled `shuffleBlockResolver` and
  // populate the `taskIdMapsForShuffle` book-keeping that `getWriter` would
  // normally fill in during a real shuffle.
  private class TestableRapidsShuffleManager(
      conf: SparkConf,
      injectedResolver: ShuffleBlockResolver)
    extends RapidsShuffleInternalManagerBase(conf, isDriver = true) {

    override def shuffleBlockResolver: ShuffleBlockResolver = injectedResolver

    def trackMapForShuffle(shuffleId: Int, mapId: Long): Unit = {
      val ids = taskIdMapsForShuffle.computeIfAbsent(
        shuffleId, _ => new OpenHashSet[Long](16))
      ids.synchronized {
        ids.add(mapId)
      }
    }
  }

  private def newConf(): SparkConf = new SparkConf(loadDefaults = false)
    .set("spark.app.id", "sampleApp")
    // Default rapids.shuffle.mode is MULTITHREADED, but be explicit so the
    // test does not rely on default values.
    .set("spark.rapids.shuffle.mode", "MULTITHREADED")
    .set("spark.rapids.shuffle.multiThreaded.writer.threads", "0")
    .set("spark.rapids.shuffle.multiThreaded.reader.threads", "0")

  test("unregisterShuffle removes data via wrapped IndexShuffleBlockResolver " +
       "when using GpuShuffleBlockResolver (issue #14695)") {
    val mockIsbr = mock[IndexShuffleBlockResolver]
    val gpuResolver = new GpuShuffleBlockResolver(mockIsbr, null)

    val manager = new TestableRapidsShuffleManager(newConf(), gpuResolver)

    val shuffleId = 7
    val mapIds = Seq(11L, 22L, 33L)
    mapIds.foreach(manager.trackMapForShuffle(shuffleId, _))

    val result = manager.unregisterShuffle(shuffleId)

    mapIds.foreach { mid =>
      verify(mockIsbr).removeDataByMap(shuffleId, mid)
    }
    assert(result, "expected SortShuffleManager.unregisterShuffle to return true")
  }

  test("unregisterShuffle on shuffle id with no tracked maps does not call " +
       "removeDataByMap (issue #14695)") {
    val mockIsbr = mock[IndexShuffleBlockResolver]
    val gpuResolver = new GpuShuffleBlockResolver(mockIsbr, null)

    val manager = new TestableRapidsShuffleManager(newConf(), gpuResolver)

    val unknownShuffleId = 99
    val result = manager.unregisterShuffle(unknownShuffleId)

    verify(mockIsbr, never()).removeDataByMap(anyInt(), anyLong())
    assert(result)
  }

  test("unregisterShuffle still cleans up when the resolver is a plain " +
       "IndexShuffleBlockResolver (compat path, issue #14695)") {
    val mockIsbr = mock[IndexShuffleBlockResolver]

    val manager = new TestableRapidsShuffleManager(newConf(), mockIsbr)

    val shuffleId = 5
    val mapIds = Seq(101L, 202L)
    mapIds.foreach(manager.trackMapForShuffle(shuffleId, _))

    val result = manager.unregisterShuffle(shuffleId)

    mapIds.foreach { mid =>
      verify(mockIsbr).removeDataByMap(shuffleId, mid)
    }
    assert(result)
  }
}
