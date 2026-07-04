/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

class RapidsExecutorPluginSuite extends AnyFunSuite {
  private val jitFeatureConf = Map(
    RapidsConf.ENABLE_PROJECT_AST.key -> "true",
    RapidsConf.ENABLE_PROJECT_AST_ANSI_ARITHMETIC.key -> "true")

  test("project AST JIT requires executor deployment configuration") {
    val configured = new RapidsConf(jitFeatureConf +
      (GpuProjectAstExec.LIBCUDF_JIT_EXECUTOR_ENV_KEY -> "1"))
    GpuProjectAstExec.requireJitRuntimeConfigured(configured)

    val error = intercept[IllegalArgumentException] {
      GpuProjectAstExec.requireJitRuntimeConfigured(new RapidsConf(jitFeatureConf))
    }
    assert(error.getMessage.contains(
      "spark.executorEnv.LIBCUDF_JIT_ENABLED=1"))
  }

  test("project AST JIT requires executor process environment") {
    val conf = new RapidsConf(jitFeatureConf)
    GpuProjectAstExec.requireJitExecutorEnvironment(
      conf, Map(GpuProjectAstExec.LIBCUDF_JIT_ENV_KEY -> "1"))

    val error = intercept[IllegalStateException] {
      GpuProjectAstExec.requireJitExecutorEnvironment(conf, Map.empty)
    }
    assert(error.getMessage.contains(
      "LIBCUDF_JIT_ENABLED=1 is not set in the executor process environment"))
  }

  test("project AST JIT runtime preflight preserves the cause") {
    val conf = new RapidsConf(jitFeatureConf)
    val cause = new RuntimeException("missing nvJitLinkCreate")
    val error = intercept[IllegalStateException] {
      GpuProjectAstExec.initializeJitRuntime(conf, () => throw cause)
    }
    assert(error.getMessage.contains("libnvrtc"))
    assert(error.getMessage.contains("libnvJitLink"))
    assert(error.getCause eq cause)
  }

  test("cudf version check") {
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7", "8"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7.2"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7", "8.7"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7", "7.2.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.0.1.3"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0", "7.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1.3"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.2"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.2.3"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.0"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.1"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.1.1"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1", "7.0.1-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.1"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.1-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2-special", "7.0.1-special"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-special", "7.0.2-special"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2.2.2", "7.0.2.2"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.2.2.2", "7.0.2.2.2"))
    assert(RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-20220101.001122-12", "7.0.1-SNAPSHOT"))
    assert(!RapidsExecutorPlugin.cudfVersionSatisfied("7.0.1-SNAPSHOT", "7.0.1-20220101.001122-12"))
  }
}
