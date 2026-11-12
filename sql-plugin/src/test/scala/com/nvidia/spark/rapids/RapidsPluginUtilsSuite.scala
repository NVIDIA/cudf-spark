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

import java.net.URL

import com.nvidia.spark.rapids.shims.ShuffleManagerShimUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._

import org.apache.spark.SparkConf

class RapidsPluginUtilsSuite extends AnyFunSuite {
  test("main JAR resource selection accepts both artifact names and excludes submodules") {
    val plugin = RapidsPluginUtils.PLUGIN_PROPS_FILENAME
    val jni = RapidsPluginUtils.JNI_PROPS_FILENAME
    val cudf = RapidsPluginUtils.CUDF_PROPS_FILENAME
    for {
      artifact <- Seq("cudf-spark", "rapids-4-spark")
      scalaVersion <- Seq("2.12", "2.13")
    } {
      val jarName = s"${artifact}_$scalaVersion-1.0-cuda12.jar"
      val mainJar = s"jar:file:/tmp/$jarName"
      val submodule = s"$artifact-aggregator_$scalaVersion-1.0-spark330.jar"
      val resources = Table(
        ("url", "propName", "expected"),
        (s"$mainJar!/$plugin", plugin, true),
        (s"$mainJar!/spark330/$plugin", plugin, false),
        (s"jar:file:/tmp/$submodule!/$plugin", plugin, false),
        (s"jar:file:/tmp/$artifact-sql-plugin_$scalaVersion-1.0.jar!/$plugin", plugin, false),
        (s"jar:file:/tmp/$artifact-integration-tests_$scalaVersion-1.0.jar!/$plugin",
          plugin, false),
        (s"jar:file:/tmp/$artifact-jni-1.0-cuda12.jar!/$jni", jni, true),
        (s"$mainJar!/$jni", jni, true),
        (s"$mainJar!/spark330/$jni", jni, false),
        (s"jar:file:/tmp/cudf-1.0-cuda12.jar!/$cudf", cudf, true),
        (s"$mainJar!/$plugin", jni, false),
        (s"file:/tmp/classes/$plugin", plugin, false),
        (s"file:/tmp/$jarName/$plugin", plugin, false),
        (s"jar:file:/tmp/$submodule/$jarName!/$plugin", plugin, true))

      forAll(resources) { (url, propName, expected) =>
        assert(RapidsPluginUtils.isMainJarResource(new URL(url), propName) === expected)
      }
    }
  }

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
