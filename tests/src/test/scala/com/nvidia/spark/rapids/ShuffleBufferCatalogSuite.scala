/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.shuffle.RapidsShuffleTestHelper
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.storage.ShuffleBlockId

class ShuffleBufferCatalogSuite
  extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
  }

  test("registered shuffles should be active") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.registerShuffle(123)
    assertResult(true)(shuffleCatalog.hasActiveShuffle(123))
    shuffleCatalog.unregisterShuffle(123)
    assertResult(false)(shuffleCatalog.hasActiveShuffle(123))
  }

  test("adding a degenerate batch") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val tableMeta = mock[TableMeta]
    // need to register the shuffle id first
    assertThrows[IllegalStateException] {
      shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    }
    shuffleCatalog.registerShuffle(1)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1,1L,1), tableMeta)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    assertResult(tableMeta)(storedMetas.head)
  }

  test("adding a contiguous batch adds it to the spill store") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.registerShuffle(1)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
    assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    val storedMetas = shuffleCatalog.blockIdToMetas(ShuffleBlockId(1, 1L, 1))
    assertResult(1)(storedMetas.size)
    shuffleCatalog.unregisterShuffle(1)
  }

  test("get a columnar batch iterator from catalog") {
    val shuffleCatalog = new ShuffleBufferCatalog()
    shuffleCatalog.registerShuffle(1)
    // add metadata only table
    val tableMeta = RapidsShuffleTestHelper.mockTableMeta(0)
    shuffleCatalog.addDegenerateRapidsBuffer(ShuffleBlockId(1, 1L, 1), tableMeta)
    val ct = RapidsShuffleTestHelper.buildContiguousTable(1000)
    shuffleCatalog.addContiguousTable(ShuffleBlockId(1, 1L, 1), ct, -1)
    val iter =
      shuffleCatalog.getColumnarBatchIterator(
        ShuffleBlockId(1, 1L, 1), Array[DataType](IntegerType))
    withResource(iter.toArray) { cbs =>
      assertResult(2)(cbs.length)
      assertResult(0)(cbs.head.numRows())
      assertResult(1)(cbs.head.numCols())
      assertResult(1000)(cbs.last.numRows())
      assertResult(1)(cbs.last.numCols())
      shuffleCatalog.unregisterShuffle(1)
    }
  }
}
