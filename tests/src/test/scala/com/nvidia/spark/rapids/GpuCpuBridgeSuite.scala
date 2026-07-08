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

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, BoundReference, Literal,
  NamedExpression}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Unit tests for the CPU Bridge expression functionality.
 * 
 * Tests cover:
 * - Bridge expression structure and properties
 * - Configuration enabling/disabling
 * - Bridge compatibility checking
 */
class GpuCpuBridgeSuite extends SparkQueryCompareTestSuite {

  // ============================================================================
  // Bridge Expression Structure Tests
  // ============================================================================
  
  test("GpuCpuBridgeExpression properties") {
    val cpuExpression = Add(
      BoundReference(0, LongType, nullable = false),
      Literal(100L)
    )
    
    val gpuInput = GpuBoundReference(0, LongType, nullable = false)(
      NamedExpression.newExprId, "x")
    val bridgeExpr = GpuCpuBridgeExpression(
      gpuInputs = Seq(gpuInput),
      cpuExpression = cpuExpression,
      outputDataType = LongType,
      outputNullable = false
    )
    
    assert(bridgeExpr.dataType == LongType)
    assert(!bridgeExpr.nullable)
    assert(bridgeExpr.prettyName == "gpu_cpu_bridge")
    assert(bridgeExpr.hasSideEffects) // Bridge always reports hasSideEffects
    assert(bridgeExpr.children.size == 2) // gpuInput + cpuExpression
    val bridgeSql = bridgeExpr.sql
    assert(bridgeSql.startsWith("gpu_cpu_bridge("))
    assert(bridgeSql.contains("gpuInputs=["))
    assert(bridgeSql.contains("cpuExpression="))
  }
  
  test("GpuCpuBridgeExpression with nullable output") {
    val cpuExpression = Add(
      BoundReference(0, IntegerType, nullable = true),
      Literal(10)
    )
    
    val gpuInput = GpuBoundReference(0, IntegerType, nullable = true)(null, "a")
    val bridgeExpr = GpuCpuBridgeExpression(
      gpuInputs = Seq(gpuInput),
      cpuExpression = cpuExpression,
      outputDataType = IntegerType,
      outputNullable = true
    )
    
    assert(bridgeExpr.dataType == IntegerType)
    assert(bridgeExpr.nullable)
    assert(bridgeExpr.gpuInputs.size == 1)
    assert(bridgeExpr.cpuExpression == cpuExpression)
  }
  
  test("GpuCpuBridgeExpression with multiple GPU inputs") {
    // CPU expression: input0 + input1
    val cpuExpression = Add(
      BoundReference(0, IntegerType, nullable = true),
      BoundReference(1, IntegerType, nullable = true)
    )
    
    val gpuInput0 = GpuBoundReference(0, IntegerType, nullable = true)(null, "a")
    val gpuInput1 = GpuBoundReference(1, IntegerType, nullable = true)(null, "b")
    val bridgeExpr = GpuCpuBridgeExpression(
      gpuInputs = Seq(gpuInput0, gpuInput1),
      cpuExpression = cpuExpression,
      outputDataType = IntegerType,
      outputNullable = true
    )
    
    assert(bridgeExpr.gpuInputs.size == 2)
    assert(bridgeExpr.children.size == 3) // 2 gpuInputs + 1 cpuExpression
  }
  
  test("GpuCpuBridgeExpression prettyName method") {
    val cpuExpression = Add(
      BoundReference(0, IntegerType, nullable = true),
      Literal(5)
    )
    
    val gpuInput = GpuBoundReference(0, IntegerType, nullable = true)(null, "x")
    val bridgeExpr = GpuCpuBridgeExpression(
      gpuInputs = Seq(gpuInput),
      cpuExpression = cpuExpression,
      outputDataType = IntegerType,
      outputNullable = true
    )
    
    assert(bridgeExpr.prettyName == "gpu_cpu_bridge")
  }

  test("Bridge allow-list error reports allowed and disallowed CPU expressions") {
    val cpuExpression = Add(
      Abs(BoundReference(0, IntegerType, nullable = true)),
      Literal(1))
    val bridgeExpr = GpuCpuBridgeExpression(
      gpuInputs = Seq(GpuBoundReference(0, IntegerType, nullable = true)(
        NamedExpression.newExprId, "x")),
      cpuExpression = cpuExpression,
      outputDataType = IntegerType,
      outputNullable = true)
    val conf = new RapidsConf(Map(RapidsConf.TEST_ALLOWED_NONGPU.key -> "Add"))

    val error = intercept[IllegalArgumentException] {
      new GpuTransitionOverrides().assertBridgeExpressionsAllowed(bridgeExpr, conf)
    }

    assert(error.getMessage.contains("DISALLOWED EXPRESSIONS"))
    assert(error.getMessage.contains("(Abs) [NOT ALLOWED]"))
    assert(error.getMessage.contains("ALLOWED EXPRESSIONS (for context)"))
    assert(error.getMessage.contains("(Add) [ALLOWED]"))
  }

  test("GPU assertion rejects an unapproved CPU expression") {
    val error = intercept[IllegalArgumentException] {
      new GpuTransitionOverrides().assertIsOnTheGpu(
        Abs(Literal(1)), new RapidsConf(Map.empty[String, String]))
    }

    assert(error.getMessage.contains("Abs"))
    assert(error.getMessage.contains("is not columnar"))
  }
  
  // ============================================================================
  // Configuration Tests
  // ============================================================================
  
  test("Bridge disallow list config") {
    val conf = new RapidsConf(Map(
      RapidsConf.BRIDGE_DISALLOW_LIST.key -> "org.example.Expr1, org.example.Expr2"
    ))
    
    val disallowList = conf.bridgeDisallowList
    assert(disallowList.contains("org.example.Expr1"))
    assert(disallowList.contains("org.example.Expr2"))
    assert(disallowList.size == 2)
  }
  
  test("Empty bridge disallow list config") {
    val conf = new RapidsConf(Map(
      RapidsConf.BRIDGE_DISALLOW_LIST.key -> ""
    ))
    
    assert(conf.bridgeDisallowList.isEmpty)
  }
  
  test("Bridge disallow list with whitespace") {
    val conf = new RapidsConf(Map(
      RapidsConf.BRIDGE_DISALLOW_LIST.key -> "  org.example.Expr1 ,  org.example.Expr2  "
    ))
    
    val disallowList = conf.bridgeDisallowList
    assert(disallowList.contains("org.example.Expr1"))
    assert(disallowList.contains("org.example.Expr2"))
    assert(disallowList.size == 2)
  }
  
  test("Bridge default config is enabled") {
    // With no config set, bridge should be enabled by default (feature is complete)
    val conf = new RapidsConf(Map.empty[String, String])
    assert(conf.isCpuBridgeEnabled)
  }
}
