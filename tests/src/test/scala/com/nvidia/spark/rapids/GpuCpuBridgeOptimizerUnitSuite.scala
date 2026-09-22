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

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, And, AttributeReference,
  BoundReference, CaseWhen, GreaterThan, Literal, MonotonicallyIncreasingID, Multiply, ScalaUDF,
  XxHash64}
import org.apache.spark.sql.types.LongType

class GpuCpuBridgeOptimizerUnitSuite extends AnyFunSuite {

  private def conf(): RapidsConf = new RapidsConf(Map(
    RapidsConf.ENABLE_CPU_BRIDGE.key -> "true"
  ))

  private def wrap(
      e: org.apache.spark.sql.catalyst.expressions.Expression,
      rapidsConf: RapidsConf = conf()): BaseExprMeta[_] = {
    val meta = GpuOverrides.wrapExpr(e, rapidsConf, None)
    meta.tagForGpu()
    meta
  }

  test("(a*2) + (a*3) prefers CPU multiplies under CPU add") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val mul1 = Multiply(a, Literal(2L))
    val mul2 = Multiply(a, Literal(3L))
    val add = Add(mul1, mul2)

    val addMeta = wrap(add)
    // simulate add disabled on GPU
    addMeta.willNotWorkOnGpu("disabled for test")


    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)


    assert(addMeta.willUseGpuCpuBridge)
    val addChildren = addMeta.childExprs
    assert(addChildren.size == 2)
    val leftMult: BaseExprMeta[_] = addChildren.head
    val rightMult: BaseExprMeta[_] = addChildren(1)
    assert(leftMult.willUseGpuCpuBridge)
    assert(rightMult.willUseGpuCpuBridge)
    val leftMultChildren = leftMult.childExprs
    assert(leftMultChildren.size == 2)
    // attr a is not marked for CPU or GPU. It will be co-located with the parent expression.
    assert(leftMultChildren(1).willUseGpuCpuBridge) // lit 2
    val rightMultChildren = rightMult.childExprs
    assert(rightMultChildren.size == 2)
    // attr a is not marked for CPU or GPU. It will be co-located with the parent expression.
    assert(rightMultChildren(1).willUseGpuCpuBridge) // lit 3
  }

  test("Under CPU Add parent with shared leaf, xxhash64 (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val hash = XxHash64(Seq(a), 42L)
    val add = Add(a, hash)

    val addMeta = wrap(add)
    // force Add to CPU
    addMeta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)

    assert(addMeta.willUseGpuCpuBridge)

    // Find child meta for xxhash64
    val maybeHashMeta = addMeta.childExprs.find(m => m.wrapped.isInstanceOf[XxHash64])
    assert(maybeHashMeta.isDefined, "Expected xxhash64 under Add")
    val hashMeta = maybeHashMeta.get
    // Siblings share a leaf (a) so CPU is strictly cheaper than GPU.
    assert(hashMeta.willUseGpuCpuBridge,
      "xxhash64 should use CPU bridge under CPU Add when siblings share a leaf")
  }

  test("No CPU requirements stays on GPU (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val mul = Multiply(Literal(2L), a)
    val mulMeta = wrap(mul)
    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(mulMeta)
    assert(!mulMeta.willUseGpuCpuBridge)
  }

  test("MonotonicallyIncreasingID is not CPU bridge compatible (unit)") {
    val meta = wrap(MonotonicallyIncreasingID())
    meta.willNotWorkOnGpu("disabled for test")

    assert(!meta.canMoveToCpuBridge)
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(meta))
    assert(!meta.willUseGpuCpuBridge)
  }

  test("ScalaUDF can use CPU bridge (unit)") {
    val udf = ScalaUDF((value: Long) => value, LongType, Seq(Literal(1L)))
    val meta = wrap(udf)
    meta.willNotWorkOnGpu("disabled for test")

    assert(meta.canMoveToCpuBridge)
    GpuCpuBridgeOptimizer.checkAndOptimizeExpressionMetas(Seq(meta))
    assert(meta.willUseGpuCpuBridge)
  }

  test("CPU child tie prefers GPU (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val gt = GreaterThan(a, Literal(5L))
    val andExpr = And(gt, Literal(true))

    val andMeta = wrap(andExpr)
    val compareMeta = andMeta.childExprs.head
    compareMeta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(andMeta)

    // Root GPU cost: bridge the comparison child, import a once, export the child result.
    // Root CPU cost: run the comparison in the CPU region, import a once, export the AND result.
    // Both cost 2, so the GPU-biased tie-break should keep the AND on GPU.
    assert(!andMeta.willUseGpuCpuBridge, "AND should prefer GPU on a true tie")
    assert(compareMeta.willUseGpuCpuBridge, "Comparison should use CPU bridge under AND")
    assert(!andMeta.childExprs(1).willUseGpuCpuBridge,
      "Scalar literal child should stay on GPU side of the tie")
  }

  test("Non-AST subtree optimization keeps AST join skeleton on GPU (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val b = AttributeReference("b", LongType, nullable = false)()
    val condition = GreaterThan(Add(a, Literal(10L)), Add(b, Literal(5L)))

    val conditionMeta = wrap(condition)
    val addMetas = conditionMeta.childExprs.filter(_.wrapped.isInstanceOf[Add])
    assert(addMetas.size == 2)
    addMetas.foreach(_.willNotWorkOnGpu("disabled for test"))

    GpuCpuBridgeOptimizer.checkAndOptimizeNonAstSubtrees(conditionMeta)

    assert(!conditionMeta.willUseGpuCpuBridge, "GreaterThan should remain GPU/AST-capable")
    assert(addMetas.forall(_.willUseGpuCpuBridge),
      s"Expected Add children to use CPU bridge: $addMetas")
  }

  test("Second pass includes CPU imports under GPU parent (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val compare = GreaterThan(a, Literal(5L))
    val childAnd = And(compare, Literal(true))
    val rootAnd = And(childAnd, Literal(true))

    val rootMeta = wrap(rootAnd)
    rootMeta.childExprs.head.childExprs.head.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(rootMeta)

    assert(!rootMeta.willUseGpuCpuBridge, "Root AND should remain on GPU")
    val childAndMeta = rootMeta.childExprs.head
    assert(!childAndMeta.willUseGpuCpuBridge,
      "Child AND should stay on GPU when CPU import cost makes CPU a tie")
    assert(childAndMeta.childExprs.head.willUseGpuCpuBridge,
      "Forced comparison should still use CPU bridge")
  }

  test("Nested CaseWhen with CPU Add uses bridges only where needed (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val b = AttributeReference("b", LongType, nullable = false)()
    val c = AttributeReference("c", LongType, nullable = false)()

    val thenExpr = Add(Add(a, b), Literal(5L))
    val elseExpr = Add(Add(a, c), Literal(2L))
    val cw = CaseWhen(Seq((GreaterThan(a, Literal(0L)), thenExpr)), Some(elseExpr))

    val root = wrap(cw)
    // Tag and force all Adds to CPU bridge
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(root)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(root)

    // Verify that all inner Adds are on CPU bridge
    def collectAdds(meta: BaseExprMeta[_], 
      acc: scala.collection.mutable.ListBuffer[BaseExprMeta[_]]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) acc += meta
      meta.childExprs.foreach(collectAdds(_, acc))
    }
    val adds = scala.collection.mutable.ListBuffer[BaseExprMeta[_]]()
    collectAdds(root, adds)
    assert(adds.nonEmpty, "Expected Add expressions inside CaseWhen")
    assert(adds.forall(_.willUseGpuCpuBridge), s"Expected all Adds on CPU bridge: $adds")
  }

  test("Heuristic: CPU XxHash64 with 13 inputs favors GPU children (unit)") {
    val inputs = (1 to 13).map(i => AttributeReference(s"a$i", LongType, nullable = false)())
    val hash = XxHash64(inputs, 42L)

    val hashMeta = wrap(hash)
    // Force parent to CPU to trigger subset selection
    hashMeta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(hashMeta)

    assert(hashMeta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    // With independent per-child choice under CPU parent, ties favor GPU → expect 
    // no children on CPU
    val childOnCpu = hashMeta.childExprs.filter(_.willUseGpuCpuBridge)
    assert(childOnCpu.isEmpty, s"Expected all children on GPU under " +
      s"CPU XxHash64: ${hashMeta.childExprs}")
  }

  test("Heuristic boundary: 7 vs 8 XxHash64 inputs yield consistent placement (unit)") {
    def run(n: Int): (Boolean, Seq[Boolean]) = {
      val inputs = (1 to n).map(i => AttributeReference(s"a$i", LongType, nullable = false)())
      val hash = XxHash64(inputs, 42L)
      val meta = wrap(hash)
      meta.willNotWorkOnGpu("disabled for test")
      GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)
      (meta.willUseGpuCpuBridge, meta.childExprs.map(_.willUseGpuCpuBridge))
    }

    val (m7, kids7) = run(7)   // exact enumeration path
    val (m8, kids8) = run(8)   // heuristic path

    assert(m7 && m8, "Parents should be on CPU bridge")
    // Expect all children on GPU in both cases under tie-breaking that prefers GPU
    assert(kids7.forall(_ == false), s"Expected all 7 children on GPU: $kids7")
    assert(kids8.forall(_ == false), s"Expected all 8 children on GPU: $kids8")
  }

  test("Heuristic handles heterogeneous CPU and GPU child capabilities (unit)") {
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.ENABLE_CPU_BRIDGE.key -> "true",
      RapidsConf.BRIDGE_DISALLOW_LIST.key -> classOf[Abs].getName))
    val children = Seq(
      Add(BoundReference(0, LongType, nullable = false), Literal(1L)),
      Abs(BoundReference(1, LongType, nullable = false))) ++
      (1L to 6L).map(Literal(_))
    val rootMeta = wrap(XxHash64(children, 42L), rapidsConf)
    rootMeta.willNotWorkOnGpu("disabled for test")

    val addMeta = rootMeta.childExprs.head
    addMeta.willNotWorkOnGpu("disabled for test")
    val absMeta = rootMeta.childExprs(1)

    assert(!addMeta.canThisBeReplaced && addMeta.canMoveToCpuBridge)
    assert(absMeta.canThisBeReplaced && !absMeta.canMoveToCpuBridge)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(rootMeta)

    assert(rootMeta.willUseGpuCpuBridge)
    assert(addMeta.willUseGpuCpuBridge)
    assert(!absMeta.willUseGpuCpuBridge)
    assert(rootMeta.childExprs.drop(2).forall(_.willUseGpuCpuBridge),
      "Scalar literals should co-locate with the CPU parent")
  }

  test("Heuristic: 13 mixed Add/Multiply children into XxHash64 under CPU (unit)") {
    // Use distinct leaves per child to avoid shared-leaf advantage for CPU
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        val ai = AttributeReference(s"a$i", LongType, nullable = false)()
        if ((i & 1) == 0) Add(ai, Literal(i.toLong)) else Multiply(ai, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)
    meta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(meta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    // Under the current heuristic, each child compares 
    // cpuCost (0) vs gpuAlt (gpuCost + moveOut = 1),
    // so it strictly prefers CPU per child. The parent-level import charge is added only once
    // after selection, so the heuristic still yields all CPU children here.
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == true), s"Expected all children on CPU: $onCpu")
  }

  test("Heuristic: shared leaf under CPU parent → CPU strictly cheaper (unit)") {
    // All children share the same leaf, so importing once for the CPU parent is cheaper
    val a = AttributeReference("a", LongType, nullable = false)()
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        if ((i & 1) == 0) Add(a, Literal(i.toLong)) else Multiply(a, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)
    // Force parent to CPU
    meta.willNotWorkOnGpu("disabled for test")

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(meta.willUseGpuCpuBridge, "Parent XxHash64 should be on CPU bridge")
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == true), s"Expected all children on CPU: $onCpu")
  }

  test("Heuristic: distinct leaves under GPU parent → GPU strictly cheaper (unit)") {
    // Parent stays on GPU; moving CPU child outputs would cost 1 per child → choose GPU
    val children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      (1 to 13).map { i =>
        val ai = AttributeReference(s"a$i", LongType, nullable = false)()
        if ((i & 1) == 0) Add(ai, Literal(i.toLong)) else Multiply(ai, Literal(i.toLong))
      }
    val hash = XxHash64(children, 42L)
    val meta = wrap(hash)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(meta)

    assert(!meta.willUseGpuCpuBridge, "Parent should remain on GPU")
    val onCpu = meta.childExprs.map(_.willUseGpuCpuBridge)
    assert(onCpu.forall(_ == false), s"Expected all children on GPU: $onCpu")
  }

  test("Deeply nested bridges (4+ levels) merge correctly (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val b = AttributeReference("b", LongType, nullable = false)()
    val c = AttributeReference("c", LongType, nullable = false)()
    val d = AttributeReference("d", LongType, nullable = false)()

    // Create deeply nested structure: (((a + b) + c) + d) + 100
    // Force all Adds to CPU to create nested bridge structure
    val innerAdd1 = Add(a, b)                    // Level 1: a + b  
    val innerAdd2 = Add(innerAdd1, c)            // Level 2: (a + b) + c
    val innerAdd3 = Add(innerAdd2, d)            // Level 3: ((a + b) + c) + d
    val outerAdd = Add(innerAdd3, Literal(100L)) // Level 4: (((a + b) + c) + d) + 100

    val rootMeta = wrap(outerAdd)
    
    // Force all Add expressions to CPU bridge to create nested bridges
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(rootMeta)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(rootMeta)

    // Verify that optimization completed without errors and creates bridges
    assert(rootMeta.willUseGpuCpuBridge, "Root Add should be on CPU bridge")
    
    // Count depth of bridge nesting in the final converted expression
    val converted = rootMeta.convertToGpu()
    def countBridgeDepth(expr: Any): Int = expr match {
      case bridge: GpuCpuBridgeExpression =>
        val depths = bridge.gpuInputs.map(countBridgeDepth)
        1 + (if (depths.nonEmpty) depths.max else 0)
      case _ => 0
    }
    
    val bridgeDepth = countBridgeDepth(converted)
    // After merging, we should have fewer nested bridges than the original structure
    assert(bridgeDepth >= 1, s"Expected at least 1 bridge level, got $bridgeDepth")
    assert(bridgeDepth < 4, s"Bridge merging should reduce nesting, got $bridgeDepth levels")
  }

  test("All scalar children under CPU parent (unit)") {
    // Test edge case where all children are scalar-like (Literals)
    val lit1 = Literal(10L)
    val lit2 = Literal(20L) 
    val lit3 = Literal(30L)
    val add = Add(Add(lit1, lit2), lit3)

    val addMeta = wrap(add)
    // Force Add to CPU
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(addMeta)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)

    // All expressions should be on CPU bridge since parent is forced to CPU
    // and scalars should co-locate with their consumer
    assert(addMeta.willUseGpuCpuBridge, "Root Add should be on CPU bridge")
    
    // Find all nested Add expressions
    def collectAdds(meta: BaseExprMeta[_], 
      acc: scala.collection.mutable.ListBuffer[BaseExprMeta[_]]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) acc += meta
      meta.childExprs.foreach(collectAdds(_, acc))
    }
    val adds = scala.collection.mutable.ListBuffer[BaseExprMeta[_]]()
    collectAdds(addMeta, adds)
    
    assert(adds.nonEmpty, "Expected nested Add expressions")
    assert(adds.forall(_.willUseGpuCpuBridge), "All Adds should be on CPU bridge")
    
    // Literals should be co-located (on bridge) with their consuming expressions
    def collectLiterals(meta: BaseExprMeta[_], 
      acc: scala.collection.mutable.ListBuffer[BaseExprMeta[_]]): Unit = {
      if (meta.wrapped.isInstanceOf[Literal]) acc += meta
      meta.childExprs.foreach(collectLiterals(_, acc))
    }
    val literals = scala.collection.mutable.ListBuffer[BaseExprMeta[_]]()
    collectLiterals(addMeta, literals)
    
    assert(literals.nonEmpty, "Expected Literal expressions")
    assert(literals.forall(_.willUseGpuCpuBridge), 
      "All Literals should co-locate with CPU parent on bridge")
  }

  test("Performance boundary: heuristic vs exact enumeration (unit)") {
    // Test that results are consistent between exact (n=6) and heuristic (n=8) paths
    def createTestExpr(n: Int): BaseExprMeta[_] = {
      val inputs = (1 to n).map(i => AttributeReference(s"col$i", LongType, nullable = false)())
      val hash = XxHash64(inputs, 42L)
      val meta = wrap(hash)
      meta.willNotWorkOnGpu("disabled for test") // Force to CPU for subset enumeration
      meta
    }

    val exact6 = createTestExpr(6)
    val exact7 = createTestExpr(7)  // Still exact
    val heuristic8 = createTestExpr(8)  // Heuristic

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(exact6)
    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(exact7)
    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(heuristic8)

    // All should be on CPU bridge
    assert(exact6.willUseGpuCpuBridge, "6-input hash should be on CPU bridge")
    assert(exact7.willUseGpuCpuBridge, "7-input hash should be on CPU bridge")
    assert(heuristic8.willUseGpuCpuBridge, "8-input hash should be on CPU bridge")

    // Children placement should be consistent (all GPU due to tie-breaking)
    val children6 = exact6.childExprs.map(_.willUseGpuCpuBridge)
    val children7 = exact7.childExprs.map(_.willUseGpuCpuBridge)
    val children8 = heuristic8.childExprs.map(_.willUseGpuCpuBridge)

    assert(children6.forall(_ == false), s"6-input children should be GPU: $children6")
    assert(children7.forall(_ == false), s"7-input children should be GPU: $children7")
    assert(children8.forall(_ == false), s"8-input children should be GPU: $children8")
  }

  test("Literal co-location with consumer (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val lit = Literal(42L)
    
    // Mix Literal with AttributeReference in CPU expression
    val add = Add(a, lit)
    
    val addMeta = wrap(add)
    addMeta.willNotWorkOnGpu("disabled for test") // Force to CPU

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(addMeta)

    assert(addMeta.willUseGpuCpuBridge, "Add should be on CPU bridge")
    
    // Literal should co-locate with its consumer (zero move cost)
    val literalMeta = addMeta.childExprs.find(_.wrapped.isInstanceOf[Literal])
    assert(literalMeta.isDefined, "Expected Literal child")
    assert(literalMeta.get.willUseGpuCpuBridge, 
      "Literal should co-locate with CPU consumer")
  }

  test("Multiple Literals under same parent (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val lit1 = Literal(10L)
    val lit2 = Literal(20L)
    
    // Expression with multiple Literals: a + lit1 + lit2
    val add1 = Add(a, lit1)
    val add2 = Add(add1, lit2)
    
    val rootMeta = wrap(add2)
    // Force all Adds to CPU
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(rootMeta)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(rootMeta)

    // All Literals should co-locate with their CPU consumers
    def findLiterals(meta: BaseExprMeta[_], 
      acc: scala.collection.mutable.ListBuffer[BaseExprMeta[_]]): Unit = {
      if (meta.wrapped.isInstanceOf[Literal]) acc += meta
      meta.childExprs.foreach(findLiterals(_, acc))
    }
    val literals = scala.collection.mutable.ListBuffer[BaseExprMeta[_]]()
    findLiterals(rootMeta, literals)
    
    assert(literals.length >= 2, s"Expected at least 2 Literals, found ${literals.length}")
    assert(literals.forall(_.willUseGpuCpuBridge), 
      "All Literals should co-locate with CPU consumers")
  }

  test("Literal vs AttributeReference cost comparison (unit)") {
    val a = AttributeReference("a", LongType, nullable = false)()
    val b = AttributeReference("b", LongType, nullable = false)()
    val lit = Literal(100L)
    
    // Compare: (a + b) vs (a + literal) under CPU parent
    // The Literal version should be preferred due to zero move cost
    val addAttr = Add(a, b)      // Requires moving both a and b
    val addLit = Add(a, lit)     // Literal has zero move cost
    
    val hash = XxHash64(Seq(addAttr, addLit), 42L)
    val hashMeta = wrap(hash)
    hashMeta.willNotWorkOnGpu("disabled for test") // Force parent to CPU
    
    // Force both Adds to be comparable
    def forceAdds(meta: BaseExprMeta[_]): Unit = {
      if (meta.wrapped.isInstanceOf[Add]) meta.willNotWorkOnGpu("disabled for test")
      meta.childExprs.foreach(forceAdds)
    }
    forceAdds(hashMeta)

    GpuCpuBridgeOptimizer.optimizeByMinimizingMovement(hashMeta)

    // Both Add expressions should be on CPU bridge, but the optimizer should
    // account for the Literal's zero move cost in overall calculations
    val addMetas = hashMeta.childExprs.filter(_.wrapped.isInstanceOf[Add])
    assert(addMetas.length == 2, "Expected 2 Add expressions")
    assert(addMetas.forall(_.willUseGpuCpuBridge), "Both Adds should be on CPU bridge")
  }
}

