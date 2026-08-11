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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids.{RapidsConf, SpillableBuffer}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class SpillableColumnarBatchSuite extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SpillFramework.initialize(new RapidsConf(new SparkConf()))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SpillFramework.shutdown()
  }

  test("close updates catalog") {
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    val deviceHandle = SpillableBuffer(DeviceMemoryBuffer.allocate(1234), -1)
    assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    deviceHandle.close()
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }
}
