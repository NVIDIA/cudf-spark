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

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuShuffleEnv

/**
 * The check that both plugins run before they start the UCX shuffle reclaim. Each test changes
 * one input of a setup the check admits. The check is a pure function of its inputs, so the
 * suite needs no SparkContext, shuffle manager or GPU.
 */
class UcxReclaimAdmissionSuite extends AnyFunSuite {
  private val gateKey = "spark.rapids.shuffle.ucx.cleanupOnQueryEnd.enabled"
  private val modeKey = "spark.rapids.shuffle.mode"
  private val managerKey = "spark.shuffle.manager"

  private val base = new SparkConf(false)
    .set(managerKey, GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
    .set(modeKey, "UCX")
    .set(gateKey, "true")

  private def refusal(conf: SparkConf, isDataBricks: Boolean): Option[String] =
    UcxShuffleReclaim.refusal(new RapidsConf(conf), conf, isDataBricks)

  private def assertRefusalNames(refused: Option[String], names: String*): Unit = {
    assert(refused.isDefined, "the setup was admitted")
    names.foreach { name =>
      assert(refused.exists(_.contains(name)), s"the reason does not name $name: $refused")
    }
  }

  test("a UCX setup with the gate on and the RAPIDS shuffle manager is admitted") {
    assertResult(None)(refusal(base.clone, isDataBricks = false))
  }

  test("an unset gate key refuses, with a reason that names the key") {
    assertRefusalNames(refusal(base.clone.remove(gateKey), isDataBricks = false), gateKey)
  }

  test("MULTITHREADED mode refuses, with a reason that names MULTITHREADED") {
    assertRefusalNames(refusal(base.clone.set(modeKey, "MULTITHREADED"), isDataBricks = false),
      "MULTITHREADED")
  }

  test("an unset or non-RAPIDS shuffle manager refuses, with a reason that names " +
      "spark.shuffle.manager") {
    assertRefusalNames(refusal(base.clone.remove(managerKey), isDataBricks = false),
      managerKey)
    assertRefusalNames(refusal(base.clone.set(managerKey, "sort"), isDataBricks = false),
      managerKey)
  }

  test("a Databricks shim refuses, with a reason that names Databricks") {
    assertRefusalNames(refusal(base.clone, isDataBricks = true), "Databricks")
  }

  test("the old fetch protocol does not refuse") {
    val oldProtocol = base.clone.set("spark.shuffle.useOldFetchProtocol", "true")
    assertResult(None)(refusal(oldProtocol, isDataBricks = false))
  }

  test("spark.rapids.shuffle.enabled set to false does not refuse, since the check reads no " +
      "runtime key") {
    val shuffleOff = base.clone.set("spark.rapids.shuffle.enabled", "false")
    assertResult(None)(refusal(shuffleOff, isDataBricks = false))
  }

  test("explainOnly mode refuses, with a reason that names spark.rapids.sql.mode") {
    // no GPU is set up in this mode, so the endpoint's poll thread could not start
    val explainOnly = base.clone.set("spark.rapids.sql.mode", "explainOnly")
    assertRefusalNames(refusal(explainOnly, isDataBricks = false), "spark.rapids.sql.mode")
  }
}
