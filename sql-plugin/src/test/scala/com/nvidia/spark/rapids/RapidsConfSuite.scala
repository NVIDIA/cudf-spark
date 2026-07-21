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

class RapidsConfSuite extends AnyFunSuite {

  private val numPreferredLocations =
    RapidsConf.ICEBERG_FILE_CACHE_LOCALITY_NUM_PREFERRED_LOCATIONS

  test("Iceberg file-cache locality preferred locations default and custom values") {
    val defaultConf = new RapidsConf(Map.empty[String, String])
    assert(defaultConf.icebergFileCacheLocalityNumPreferredLocations == 5)
    Seq(1, 7).foreach { value =>
      val conf = new RapidsConf(Map(numPreferredLocations.key -> value.toString))
      assert(conf.icebergFileCacheLocalityNumPreferredLocations == value)
    }
    assert(!numPreferredLocations.isStartUpOnly)
  }

  test("Iceberg file-cache locality preferred locations must be a positive integer") {
    Seq("0", "-1", "not-an-integer").foreach { value =>
      val conf = new RapidsConf(Map(numPreferredLocations.key -> value))
      assertThrows[IllegalArgumentException] {
        conf.icebergFileCacheLocalityNumPreferredLocations
      }
    }
  }
}
