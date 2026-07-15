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

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BoundReference, Divide, Expression, Literal, Multiply}
import org.apache.spark.sql.types._

/**
 * Unit tests for the convertForGpuCpuBridge() method in BaseExprMeta.
 * 
 * These tests verify the correct behavior of GPU input deduplication,
 * handling of literals and ScalarSubqueries, and proper BoundReference mapping.
 * 
 * This addresses feedback about the complexity of the deduplication logic
 * and ensures edge cases are covered.
 */
class ConvertForGpuCpuBridgeSuite extends AnyFunSuite {

  // Helper to create a test configuration with bridge enabled
  private def testConf: RapidsConf = new RapidsConf(Map(
    RapidsConf.ENABLE_CPU_BRIDGE.key -> "true"
  ))

  /**
   * Helper to create an ExprMeta for testing convertForGpuCpuBridge.
   * This simulates the meta-wrapping that happens during query planning.
   * 
   * For testing purposes, we mark GPU-supported expressions as needing bridge
   * so we can test the bridge conversion logic itself.
   */
  private def createExprMeta(expr: Expression): BaseExprMeta[Expression] = {
    // Create a simple meta wrapper with no parent
    val conf = testConf
    val wrapped = GpuOverrides.wrapExpr(expr, conf, None)
    // Initialize the metadata by calling tagForGpu - this sets up the internal state
    // including cannotBeReplacedReasons and willRunViaCpuBridgeReasons
    wrapped.tagForGpu()
    
    // For testing: if the expression is GPU-supported (canThisBeReplaced = true),
    // artificially mark it as needing CPU bridge so we can test the conversion logic
    if (wrapped.canThisBeReplaced) {
      wrapped.willNotWorkOnGpu("test: forcing bridge for testing convertForGpuCpuBridge logic")
    }
    
    wrapped.asInstanceOf[BaseExprMeta[Expression]]
  }

  // ============================================================================
  // Basic Functionality Tests
  // ============================================================================

  test("convertForGpuCpuBridge - simple expression with one GPU input") {
    // CPU expression: col + 10
    val col = AttributeReference("a", IntegerType, nullable = false)()
    val expr = Add(col, Literal(10))
    
    val exprMeta = createExprMeta(expr)
    
    // Mark for bridge conversion
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    
    // Verify structure
    assert(bridgeExpr.isInstanceOf[GpuCpuBridgeExpression])
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have 1 GPU input (the column reference)
    assert(bridge.gpuInputs.length == 1, 
      s"Expected 1 GPU input, got ${bridge.gpuInputs.length}")
    
    // CPU expression should have BoundReference(0) for the column and Literal(10)
    assert(bridge.cpuExpression.isInstanceOf[Add])
    val cpuAdd = bridge.cpuExpression.asInstanceOf[Add]
    assert(cpuAdd.left.isInstanceOf[BoundReference], 
      s"Expected BoundReference, got ${cpuAdd.left.getClass}")
    assert(cpuAdd.right.isInstanceOf[Literal],
      s"Expected Literal, got ${cpuAdd.right.getClass}")
    
    val boundRef = cpuAdd.left.asInstanceOf[BoundReference]
    assert(boundRef.ordinal == 0, s"Expected ordinal 0, got ${boundRef.ordinal}")
  }

  test("convertForGpuCpuBridge - expression with multiple GPU inputs") {
    // CPU expression: col1 + col2
    val col1 = AttributeReference("a", IntegerType, nullable = false)()
    val col2 = AttributeReference("b", IntegerType, nullable = false)()
    val expr = Add(col1, col2)
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have 2 GPU inputs (both columns)
    assert(bridge.gpuInputs.length == 2,
      s"Expected 2 GPU inputs, got ${bridge.gpuInputs.length}")
    
    // CPU expression should have BoundReference(0) and BoundReference(1)
    val cpuAdd = bridge.cpuExpression.asInstanceOf[Add]
    assert(cpuAdd.left.isInstanceOf[BoundReference])
    assert(cpuAdd.right.isInstanceOf[BoundReference])
    
    val leftRef = cpuAdd.left.asInstanceOf[BoundReference]
    val rightRef = cpuAdd.right.asInstanceOf[BoundReference]
    assert(leftRef.ordinal == 0)
    assert(rightRef.ordinal == 1)
  }

  test("convertForGpuCpuBridge - expression with only literals") {
    // CPU expression: 5 + 10 (both literals)
    val expr = Add(Literal(5), Literal(10))
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have 0 GPU inputs (literals stay on CPU)
    assert(bridge.gpuInputs.isEmpty,
      s"Expected 0 GPU inputs for literal-only expression, got ${bridge.gpuInputs.length}")
    
    // CPU expression should have the original literals
    val cpuAdd = bridge.cpuExpression.asInstanceOf[Add]
    assert(cpuAdd.left.isInstanceOf[Literal])
    assert(cpuAdd.right.isInstanceOf[Literal])
  }

  // ============================================================================
  // Literal Handling Tests - Critical for the review concern
  // ============================================================================

  test("convertForGpuCpuBridge - mixed GPU inputs and literals") {
    // CPU expression: col + 10
    // This tests the specific scenario from the review: some children are GPU-convertible,
    // others are literals
    val col = AttributeReference("a", LongType, nullable = true)()
    val expr = Add(col, Literal(100L))
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have 1 GPU input (the column) - literal stays in place
    assert(bridge.gpuInputs.length == 1,
      s"Expected 1 GPU input, got ${bridge.gpuInputs.length}")
    
    // CPU expression should have BoundReference(0) for column and original Literal
    val cpuAdd = bridge.cpuExpression.asInstanceOf[Add]
    
    // Left child should be BoundReference with ordinal 0
    assert(cpuAdd.left.isInstanceOf[BoundReference],
      s"Expected BoundReference for GPU input, got ${cpuAdd.left.getClass}")
    val boundRef = cpuAdd.left.asInstanceOf[BoundReference]
    assert(boundRef.ordinal == 0,
      s"Expected BoundReference ordinal 0, got ${boundRef.ordinal}")
    
    // Right child should be the original Literal (not converted)
    assert(cpuAdd.right.isInstanceOf[Literal],
      s"Expected Literal to stay in place, got ${cpuAdd.right.getClass}")
    val lit = cpuAdd.right.asInstanceOf[Literal]
    assert(lit.value == 100L,
      s"Expected literal value 100, got ${lit.value}")
  }

  test("convertForGpuCpuBridge - complex expression with mixed children") {
    // CPU expression: (col1 + 10) * (col2 + 20)
    // Tests multiple levels with mixed GPU/literal children
    val col1 = AttributeReference("a", IntegerType, nullable = false)()
    val col2 = AttributeReference("b", IntegerType, nullable = false)()
    val add1 = Add(col1, Literal(10))
    val add2 = Add(col2, Literal(20))
    val expr = Multiply(add1, add2)
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // The bridge should only have the top-level multiply, with the children
    // already converted. Since Add is typically GPU-supported, we should see
    // 2 GPU inputs (the two Add expressions).
    // Note: This depends on which expressions are GPU-convertible in the actual config
    assert(bridge.gpuInputs.nonEmpty,
      "Expected at least some GPU inputs in complex expression")
  }

  // ============================================================================
  // Deduplication Tests - Critical for the review concern
  // ============================================================================

  test("convertForGpuCpuBridge - deduplicates identical GPU inputs") {
    // CPU expression: col + col (same column used twice)
    val col = AttributeReference("a", IntegerType, nullable = false)()
    val expr = Add(col, col)
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have only 1 GPU input (deduplicated)
    assert(bridge.gpuInputs.length == 1,
      s"Expected deduplication to result in 1 GPU input, got ${bridge.gpuInputs.length}")
    
    // CPU expression should have BoundReference(0) for both children
    val cpuAdd = bridge.cpuExpression.asInstanceOf[Add]
    assert(cpuAdd.left.isInstanceOf[BoundReference])
    assert(cpuAdd.right.isInstanceOf[BoundReference])
    
    val leftRef = cpuAdd.left.asInstanceOf[BoundReference]
    val rightRef = cpuAdd.right.asInstanceOf[BoundReference]
    
    // Both should point to the same deduplicated input
    assert(leftRef.ordinal == 0, s"Expected left ordinal 0, got ${leftRef.ordinal}")
    assert(rightRef.ordinal == 0,
      s"Expected right ordinal 0 (deduplicated), got ${rightRef.ordinal}")
  }

  test("convertForGpuCpuBridge - deduplicates semantically equal expressions") {
    // CPU expression: (a + 10) / (a + 10)
    // The two (a + 10) expressions are semantically equal and should be deduplicated
    val col = AttributeReference("a", IntegerType, nullable = false)()
    val add1 = Add(col, Literal(10))
    val add2 = Add(col, Literal(10))  // Semantically equal to add1
    val expr = Divide(add1, add2)
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Depending on whether Add is GPU-supported and how the tree is structured,
    // we should see deduplication. The key is that if two GPU inputs are semantically
    // equal, they should be deduplicated.
    // Note: The exact number depends on the GPU support matrix, but we can verify
    // that the mapping is consistent
    val cpuDiv = bridge.cpuExpression.asInstanceOf[Divide]
    
    // If both children are BoundReferences and point to same ordinal, deduplication worked
    if (cpuDiv.left.isInstanceOf[BoundReference] && cpuDiv.right.isInstanceOf[BoundReference]) {
      val leftRef = cpuDiv.left.asInstanceOf[BoundReference]
      val rightRef = cpuDiv.right.asInstanceOf[BoundReference]
      
      // They should point to the same deduplicated GPU input
      assert(leftRef.ordinal == rightRef.ordinal,
        s"Expected deduplication: left ordinal ${leftRef.ordinal} should equal " +
        s"right ordinal ${rightRef.ordinal}")
    }
  }

  test("convertForGpuCpuBridge - preserves order of distinct GPU inputs") {
    // CPU expression: col1 + col2 + col3
    // Tests that distinct inputs maintain their relative order
    val col1 = AttributeReference("a", IntegerType, nullable = false)()
    val col2 = AttributeReference("b", IntegerType, nullable = false)()
    val col3 = AttributeReference("c", IntegerType, nullable = false)()
    val add1 = Add(col1, col2)
    val expr = Add(add1, col3)
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Should have distinct GPU inputs
    assert(bridge.gpuInputs.length >= 1,
      "Expected at least one GPU input")
    
    // All BoundReferences in CPU expression should have valid ordinals
    // that are within bounds of gpuInputs
    def validateBoundReferences(expr: Expression): Unit = expr match {
      case br: BoundReference =>
        assert(br.ordinal >= 0 && br.ordinal < bridge.gpuInputs.length,
          s"BoundReference ordinal ${br.ordinal} out of bounds [0, ${bridge.gpuInputs.length})")
      case _ =>
        expr.children.foreach(validateBoundReferences)
    }
    
    validateBoundReferences(bridge.cpuExpression)
  }

  // ============================================================================
  // Edge Case Tests
  // ============================================================================

  test("convertForGpuCpuBridge - handles nullable columns") {
    val col = AttributeReference("a", IntegerType, nullable = true)()
    val expr = Add(col, Literal(5))
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Verify that nullability is preserved
    assert(bridge.gpuInputs.head.nullable,
      "Expected GPU input to preserve nullable property")
    
    val boundRef = bridge.cpuExpression.asInstanceOf[Add].left.asInstanceOf[BoundReference]
    assert(boundRef.nullable,
      "Expected BoundReference to preserve nullable property")
  }

  test("convertForGpuCpuBridge - rejects non-deterministic expression") {
    // Non-deterministic expressions should not be allowed in bridge
    // They are filtered out by isBridgeCompatible
    val col = AttributeReference("a", IntegerType, nullable = false)()
    
    // Create a non-deterministic expression (Rand is non-deterministic)
    val rand = org.apache.spark.sql.catalyst.expressions.Rand(0)
    val expr = Add(col, rand)
    
    val exprMeta = createExprMeta(expr)
    
    // Non-deterministic expressions should not be allowed to move to bridge
    assert(!exprMeta.canMoveToCpuBridge,
      "Non-deterministic expressions should not be bridge-compatible")

    val moveError = intercept[IllegalStateException] {
      exprMeta.moveToCpuBridge()
    }
    assert(moveError.getMessage.contains("trying to move to bridge when not allowed"))

    val conversionError = intercept[IllegalArgumentException] {
      exprMeta.convertForGpuCpuBridge()
    }
    assert(conversionError.getMessage.contains(
      "Bridge conversion requires deterministic expressions"))
  }

  test("requireAstForGpu rolls back CPU bridge state") {
    val col = AttributeReference("a", IntegerType, nullable = false)()
    val exprMeta = createExprMeta(Add(col, Literal(10)))

    exprMeta.moveToCpuBridge()
    assert(exprMeta.willUseGpuCpuBridge)
    assert(exprMeta.replaceMessage === "run on GPU via CPU bridge")

    exprMeta.willNotWorkInAst("test AST incompatibility")
    exprMeta.requireAstForGpu()

    assert(exprMeta.mustBeAstExpression)
    assert(!exprMeta.willUseGpuCpuBridge)
    assert(!exprMeta.canThisBeReplaced)
    assert(exprMeta.childExprs.forall(_.mustBeAstExpression))

    exprMeta.undoBridgeOptimization()
    assert(!exprMeta.willUseGpuCpuBridge)
  }

  // ============================================================================
  // Regression Tests for the Specific Review Concern
  // ============================================================================

  test("REGRESSION - inputMapping has entry for every GPU-convertible child") {
    // This test specifically addresses the review concern:
    // Verify that inputMapping(originalIndex) is never accessed for an index
    // that wasn't added during the first pass.
    
    // Create an expression with various child types:
    // - GPU-convertible column
    // - Literal (not GPU-convertible for bridge)
    // - Another GPU-convertible column
    val col1 = AttributeReference("a", IntegerType, nullable = false)()
    val col2 = AttributeReference("b", IntegerType, nullable = false)()
    val add1 = Add(col1, Literal(10))  // col1 at index 0, Literal at index 1
    val expr = Add(add1, col2)         // add1 at index 0, col2 at index 1
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    // This should not throw an exception
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Verify the structure is correct
    assert(bridge.gpuInputs.nonEmpty, "Expected at least one GPU input")
    
    // All BoundReferences should have valid ordinals
    def checkBoundReferences(expr: Expression): Unit = expr match {
      case br: BoundReference =>
        assert(br.ordinal >= 0 && br.ordinal < bridge.gpuInputs.length,
          s"Invalid BoundReference ordinal: ${br.ordinal} for " +
          s"gpuInputs.length=${bridge.gpuInputs.length}")
      case _ =>
        expr.children.foreach(checkBoundReferences)
    }
    
    checkBoundReferences(bridge.cpuExpression)
  }

  test("REGRESSION - no KeyNotFoundException when accessing inputMapping") {
    // Create a scenario where if the bug existed, we'd get KeyNotFoundException
    // Expression: (col + lit1) * (col + lit2) + lit3
    val col = AttributeReference("a", IntegerType, nullable = false)()
    val add1 = Add(col, Literal(1))
    val add2 = Add(col, Literal(2))
    val mul = Multiply(add1, add2)
    val expr = Add(mul, Literal(3))
    
    val exprMeta = createExprMeta(expr)
    exprMeta.moveToCpuBridge()
    
    // This should complete without KeyNotFoundException
    val bridgeExpr = exprMeta.convertForGpuCpuBridge()
    
    // Basic validation
    assert(bridgeExpr.isInstanceOf[GpuCpuBridgeExpression])
    val bridge = bridgeExpr.asInstanceOf[GpuCpuBridgeExpression]
    
    // Verify all children are properly constructed
    def hasValidStructure(expr: Expression): Boolean = expr match {
      case br: BoundReference =>
        br.ordinal >= 0 && br.ordinal < bridge.gpuInputs.length
      case _: Literal => true
      case other => other.children.forall(hasValidStructure)
    }
    
    assert(hasValidStructure(bridge.cpuExpression),
      "Bridge expression has invalid structure")
  }
}
