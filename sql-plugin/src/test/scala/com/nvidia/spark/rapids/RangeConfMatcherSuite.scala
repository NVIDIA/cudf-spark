/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

class RangeConfMatcherSuite extends AnyFunSuite {

  test("empty") {
    val conf = new RapidsConf(Map(RapidsConf.PROFILE_EXECUTORS.key -> ""))
    val matcher = new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
    assert(!matcher.contains("x"))
    assert(!matcher.contains(0))
  }

  test("bad ranges") {
    Seq("-", "-4", "4-", "4-3", "d-4", "4-d", "23a-24b", "3-5,8,x-y").foreach { v =>
      val conf = new RapidsConf(Map(RapidsConf.PROFILE_EXECUTORS.key -> v))
      assertThrows[IllegalArgumentException] {
        new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
      }
    }
  }

  test("singles") {
    Seq("driver", "0,driver", "0, driver", "driver, 0", "1, driver, x").foreach { v =>
      val conf = new RapidsConf(Map(RapidsConf.PROFILE_EXECUTORS.key -> v))
      val matcher = new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
      assert(matcher.contains("driver"))
      assert(!matcher.contains("driverx"))
      assert(!matcher.contains("xdriver"))
      assert(!matcher.contains("drive"))
      assert(!matcher.contains("drive"))
    }
  }

  test("range only") {
    Seq("7-7", "3-7", "2-30", "2-3,5-7,8-10", "2-3, 5-7, 8-10",
        " 2 - 3, 5 - 7, 8 - 10").foreach { v =>
      val conf = new RapidsConf(Map(RapidsConf.PROFILE_EXECUTORS.key -> v))
      val matcher = new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
      assert(matcher.contains("7"))
      assert(matcher.contains(7))
      assert(!matcher.contains("0"))
      assert(!matcher.contains(0))
      assert(!matcher.contains("70"))
      assert(!matcher.contains(70))
    }
  }

  test("singles range mix") {
    Seq("driver,7-10", "driver, 7 - 10", "driver, 7-10", "3-5,7,1-3,driver").foreach { v =>
      val conf = new RapidsConf(Map(RapidsConf.PROFILE_EXECUTORS.key -> v))
      val matcher = new RangeConfMatcher(conf, RapidsConf.PROFILE_EXECUTORS)
      assert(matcher.contains("driver"))
      assert(!matcher.contains("driverx"))
      assert(!matcher.contains("xdriver"))
      assert(!matcher.contains("drive"))
      assert(!matcher.contains("drive"))
      assert(matcher.contains("7"))
      assert(matcher.contains(7))
      assert(!matcher.contains("0"))
      assert(!matcher.contains(0))
      assert(!matcher.contains("70"))
      assert(!matcher.contains(70))
    }
  }
}
