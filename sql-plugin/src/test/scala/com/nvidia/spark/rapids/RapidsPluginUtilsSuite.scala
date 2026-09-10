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

import com.nvidia.spark.rapids.shims.ShuffleManagerShimUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class RapidsPluginUtilsSuite extends AnyFunSuite {
  test("shuffle manager auto-configuration follows Spark initialization support") {
    val conf = new SparkConf(false)

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    if (ShuffleManagerShimUtils.supportsAutoConfiguration) {
      assert(conf.get("spark.shuffle.manager") === ShimLoader.getRapidsShuffleManagerClass)
    } else {
      assert(!conf.contains("spark.shuffle.manager"))
    }
  }

  test("shuffle manager auto-configuration preserves an explicit setting") {
    val conf = new SparkConf(false)
      .set("spark.shuffle.manager", "custom.ShuffleManager")

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    assert(conf.get("spark.shuffle.manager") === "custom.ShuffleManager")
  }

  test("shuffle manager auto-configuration allows the RAPIDS shuffle data IO plugin") {
    val conf = new SparkConf(false)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleDataIO")

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    if (ShuffleManagerShimUtils.supportsAutoConfiguration) {
      assert(conf.get("spark.shuffle.manager") === ShimLoader.getRapidsShuffleManagerClass)
    } else {
      assert(!conf.contains("spark.shuffle.manager"))
    }
  }

  test("shuffle manager auto-configuration preserves an incompatible shuffle data IO plugin") {
    val conf = new SparkConf(false)
      .set("spark.shuffle.sort.io.plugin.class", "custom.ShuffleDataIO")

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    assert(!conf.contains("spark.shuffle.manager"))
  }

  test("shuffle manager is not auto-configured on Dataproc") {
    val conf = new SparkConf(false)
      .set("spark.dataproc.engine", "lightningEngine")

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    assert(!conf.contains("spark.shuffle.manager"))
  }

  test("shuffle manager runtime setting does not control auto-configuration") {
    val conf = new SparkConf(false)
      .set(RapidsConf.SHUFFLE_MANAGER_ENABLED.key, "false")

    RapidsShuffleManagerAutoConfigurator.configure(conf)

    if (ShuffleManagerShimUtils.supportsAutoConfiguration) {
      assert(conf.get("spark.shuffle.manager") === ShimLoader.getRapidsShuffleManagerClass)
    } else {
      assert(!conf.contains("spark.shuffle.manager"))
    }
  }
}
