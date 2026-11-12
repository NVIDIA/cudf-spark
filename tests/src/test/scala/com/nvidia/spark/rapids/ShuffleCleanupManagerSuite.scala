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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkContext

class ShuffleCleanupManagerSuite extends AnyFunSuite {

  test("init replaces a manager that another SparkContext built, as when that context's plugin " +
      "init failed before Spark could shut the plugin down") {
    val first = mock[SparkContext]
    val second = mock[SparkContext]
    try {
      ShuffleCleanupManager.init(first)
      val firstManager = ShuffleCleanupManager.get
      ShuffleCleanupManager.init(first)
      assert(ShuffleCleanupManager.get eq firstManager, "init replaced its own context's manager")
      ShuffleCleanupManager.init(second)
      assert(ShuffleCleanupManager.get ne firstManager,
        "the next SparkContext got the manager of a context that never shut it down")
    } finally {
      ShuffleCleanupManager.shutdown()
    }
  }
}
