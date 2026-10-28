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

import com.nvidia.spark.rapids.ProjectAstTestUtils.collectExpressions
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, And, AttributeReference,
  BoundReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, Greatest, LessThan,
  Literal, Multiply, Or, Subtract}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuAnd, GpuGreatest, GpuMultiply, GpuOr, GpuSubtract}
import org.apache.spark.sql.types.{FloatType, LongType}

class GpuProjectAstPlannerSuite extends AnyFunSuite {
  private val a = AttributeReference("a", LongType)()
  private val b = AttributeReference("b", LongType)()
  private val c = AttributeReference("c", LongType)()
  private val d = AttributeReference("d", LongType)()
  private val inputs = Seq(a, b, c, d)

  private def conf(
      legacy: Boolean,
      jit: Boolean,
      tiered: Boolean = true,
      ansi: Boolean = false): SQLConf = {
    val sqlConf = new SQLConf
    sqlConf.setConfString("spark.sql.ansi.enabled", ansi.toString)
    sqlConf.setConfString(RapidsConf.ENABLE_PROJECT_AST.key, legacy.toString)
    sqlConf.setConfString(RapidsConf.ENABLE_PROJECT_AST_JIT.key, jit.toString)
    sqlConf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, tiered.toString)
    sqlConf
  }

  private def convert(expression: Expression, sqlConf: SQLConf): GpuExpression = {
    val meta = GpuOverrides.wrapExpr(expression, new RapidsConf(sqlConf), None)
    meta.tagForGpu()
    meta.convertToGpu().asInstanceOf[GpuExpression]
  }

  private def bind(expressions: Seq[Expression], sqlConf: SQLConf): GpuTieredProject =
    GpuBindReferences.bindGpuReferencesTieredNoMetrics(
      expressions, inputs, sqlConf,
      enableAstJit = RapidsConf.ENABLE_PROJECT_AST_JIT.get(sqlConf),
      enableAst = RapidsConf.ENABLE_PROJECT_AST.get(sqlConf))

  for (legacy <- Seq(false, true); jit <- Seq(false, true)) {
    test(s"independent backend switches: legacy=$legacy, jit=$jit") {
      val sqlConf = conf(legacy, jit)
      SQLConf.withExistingConf(sqlConf) {
        val expression = convert(Alias(Multiply(Add(a, b), c), "result")(), sqlConf)
        val tiers = bind(Seq(expression), sqlConf).exprTiers
        assertResult(if (jit) 1 else 0)(collectExpressions[GpuAstJitExpression](tiers.flatten).size)
        assertResult(if (legacy && !jit) 1 else 0)(
          collectExpressions[GpuProjectAstExpression](tiers.flatten).size)
      }
    }
  }

  for (tiered <- Seq(false, true); jit <- Seq(false, true)) {
    test(s"standalone arithmetic stays regular unless JIT accepts it: tiered=$tiered, jit=$jit") {
      val sqlConf = conf(legacy = true, jit = jit, tiered = tiered)
      SQLConf.withExistingConf(sqlConf) {
        val expressions = Seq(Add(a, b), Subtract(a, Literal(1L)), Multiply(c, d))
            .zipWithIndex.map { case (expression, index) =>
              convert(Alias(expression, s"result_$index")(), sqlConf)
            }
        val tiers = bind(expressions, sqlConf).exprTiers
        assertResult(1)(tiers.size)
        assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
        assertResult(if (jit) 2 else 0)(
          collectExpressions[GpuAstJitExpression](tiers.flatten).size)
        assertResult(1)(collectExpressions[GpuSubtract](tiers.flatten).size)
      }
    }
  }

  test("standalone comparison retains legacy AST eligibility") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val expression = convert(Alias(EqualTo(a, b), "result")(), sqlConf)
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(1)(collectExpressions[GpuProjectAstExpression](tiers.flatten).size)
    }
  }

  test("simple arithmetic below a regular root does not create an AST tier") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val expression = GpuAlias(GpuGreatest(Seq(
        convert(Add(a, b), sqlConf), convert(Subtract(c, d), sqlConf))), "result")()
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(1)(tiers.size)
      assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
      assertResult(1)(collectExpressions[GpuAdd](tiers.flatten).size)
      assertResult(1)(collectExpressions[GpuSubtract](tiers.flatten).size)
      assertResult(1)(collectExpressions[GpuGreatest](tiers.flatten).size)
    }
  }

  test("compound shared AST remains fused before simple regular consumers") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val shared = Subtract(Add(a, b), c)
      val expressions = Seq(
        convert(Alias(Multiply(shared, Literal(2L)), "first")(), sqlConf),
        convert(Alias(Multiply(shared, d), "second")(), sqlConf))
      val tiers = bind(expressions, sqlConf).exprTiers
      assertResult(Seq(1, 0))(tiers.map(collectExpressions[GpuProjectAstExpression](_).size))
      assertResult(1)(collectExpressions[GpuAdd](tiers.head).size)
      assertResult(1)(collectExpressions[GpuSubtract](tiers.head).size)
      assertResult(2)(collectExpressions[GpuMultiply](tiers.last).size)
      assert(collectExpressions[GpuAdd](tiers.last).isEmpty)
      assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
    }
  }

  test("both backends preserve a complete interpreted AST instead of splitting for JIT") {
    val sqlConf = conf(legacy = true, jit = true)
    SQLConf.withExistingConf(sqlConf) {
      val expression = convert(Alias(Subtract(Add(a, b), c), "result")(), sqlConf)
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(1)(tiers.size)
      assertResult(1)(collectExpressions[GpuProjectAstExpression](tiers.flatten).size)
      assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
      assert(collectExpressions[GpuAdd](tiers.flatten).nonEmpty)
    }
  }

  for (jit <- Seq(false, true)) {
    test(s"shared outputs use backend-specific CSE: jit=$jit") {
      val sqlConf = conf(legacy = true, jit = jit)
      SQLConf.withExistingConf(sqlConf) {
        val expressions = Seq(
          convert(Alias(Multiply(Add(a, b), c), "first")(), sqlConf),
          convert(Alias(Multiply(Add(a, b), d), "second")(), sqlConf))
        val tiers = bind(expressions, sqlConf).exprTiers
        if (jit) {
          val roots = collectExpressions[GpuAstJitExpression](tiers.flatten)
          assertResult(1)(tiers.size)
          assertResult(2)(roots.size)
          assertResult(1)(roots.map(_.groupId).distinct.size)
          assert(roots.forall(_.child.find(_.isInstanceOf[GpuAdd]).nonEmpty))
        } else {
          assertResult(2)(tiers.size)
          assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
          assertResult(1)(collectExpressions[GpuAdd](tiers.head).size)
          assert(collectExpressions[GpuAdd](tiers.last).isEmpty)
          assertResult(2)(collectExpressions[GpuMultiply](tiers.last).size)
          assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
        }
      }
    }
  }

  test("legacy AST extracts a compound child below an unsupported root") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val product = convert(Multiply(Add(a, b), d), sqlConf)
      val expression = GpuAlias(GpuGreatest(Seq(product, c)), "result")()
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(Seq(1, 0))(tiers.map(collectExpressions[GpuProjectAstExpression](_).size))
      assert(collectExpressions[GpuGreatest](tiers.last).nonEmpty)
      assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
    }
  }

  for (reverse <- Seq(false, true); compound <- Seq(false, true)) {
    test(s"maximal cross-backend sharing: reverse=$reverse, compound remainder=$compound") {
      val sqlConf = conf(legacy = true, jit = true)
      SQLConf.withExistingConf(sqlConf) {
        val shared = Multiply(Add(a, b), c)
        val right = if (compound) Subtract(c, d) else d
        val expressions = Seq(
          convert(Alias(Subtract(shared, right), "legacy")(), sqlConf),
          convert(Alias(shared, "jit")(), sqlConf))
        val ordered = if (reverse) expressions.reverse else expressions
        val tiers = bind(ordered, sqlConf).exprTiers
        assertResult(2)(tiers.size)
        assertResult(Seq(1, 0))(tiers.map(collectExpressions[GpuAstJitExpression](_).size))
        assertResult(Seq(0, if (compound) 1 else 0))(
          tiers.map(collectExpressions[GpuProjectAstExpression](_).size))
        assertResult(1)(collectExpressions[GpuAdd](tiers.flatten).size)
        assert(collectExpressions[GpuAdd](tiers.last).isEmpty)
        assertResult(ordered.map(_.asInstanceOf[GpuAlias].exprId))(
          tiers.last.map(_.asInstanceOf[GpuAlias].exprId))
      }
    }
  }

  test("sharing does not reduce a legacy consumer to simple arithmetic") {
    val sqlConf = conf(legacy = true, jit = true)
    SQLConf.withExistingConf(sqlConf) {
      // Materializing a+b would leave t-c on the regular backend and split the JIT root into
      // another program, so both outputs keep it inline.
      val expressions = Seq(
        convert(Alias(Subtract(Add(a, b), c), "legacy")(), sqlConf),
        convert(Alias(Multiply(Add(a, b), d), "jit")(), sqlConf))
      val tiers = bind(expressions, sqlConf).exprTiers
      assertResult(1)(tiers.size)
      assertResult(1)(collectExpressions[GpuAstJitExpression](tiers.head).size)
      val legacy = collectExpressions[GpuProjectAstExpression](tiers.head)
      assertResult(1)(legacy.size)
      assert(legacy.head.child.find(_.isInstanceOf[GpuAdd]).isDefined)
      assertResult(2)(collectExpressions[GpuAdd](tiers.head).size)
    }
  }

  for (compound <- Seq(false, true)) {
    test(s"legacy region over regular children needs an inline operation: compound=$compound") {
      val sqlConf = conf(legacy = true, jit = false)
      SQLConf.withExistingConf(sqlConf) {
        val right = if (compound) Multiply(c, d) else c
        val expression = convert(Alias(Subtract(Greatest(Seq(a, b)), right), "result")(), sqlConf)
        val tiers = bind(Seq(expression), sqlConf).exprTiers
        // Without an inline operation, the region would be t-c after GREATEST is materialized.
        assertResult(if (compound) 2 else 1)(tiers.size)
        assertResult(if (compound) 1 else 0)(
          collectExpressions[GpuProjectAstExpression](tiers.last).size)
        assertResult(1)(collectExpressions[GpuGreatest](tiers.flatten).size)
      }
    }
  }

  for (jit <- Seq(false, true)) {
    test(s"AST region below a CPU bridge does not materialize the bridge: jit=$jit") {
      val sqlConf = conf(legacy = !jit, jit = jit)
      SQLConf.withExistingConf(sqlConf) {
        val sum = convert(Multiply(Add(a, b), c), sqlConf)
        val cpu = BoundReference(0, LongType, nullable = true)
        val bridge = GpuCpuBridgeExpression(Seq(sum), cpu, LongType, outputNullable = true)
        val afterBridge = convert(Add(a, c), sqlConf)
            .withNewChildren(Seq(bridge, c)).asInstanceOf[GpuExpression]
        val product = convert(Multiply(a, d), sqlConf)
            .withNewChildren(Seq(afterBridge, d)).asInstanceOf[GpuExpression]
        val tiers = bind(Seq(GpuAlias(product, "result")()), sqlConf).exprTiers
        val astCounts = if (jit) tiers.map(collectExpressions[GpuAstJitExpression](_).size)
          else tiers.map(collectExpressions[GpuProjectAstExpression](_).size)
        // The bridge may have side effects, so its consumer evaluates it instead of a tier output.
        assertResult(Seq(1, 0))(astCounts)
        val boundBridge = collectExpressions[GpuCpuBridgeExpression](tiers.last).head
        assertResult(cpu)(boundBridge.cpuExpression)
        assert(boundBridge.gpuInputs.head.isInstanceOf[GpuBoundReference])
        assertResult(1)(collectExpressions[GpuMultiply](tiers.last).size)
      }
    }
  }

  test("simple arithmetic around a CPU bridge stays regular") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val sum = convert(Add(a, b), sqlConf)
      val cpu = BoundReference(0, LongType, nullable = true)
      val bridge = GpuCpuBridgeExpression(Seq(sum), cpu, LongType, outputNullable = true)
      val product = convert(Multiply(a, c), sqlConf)
          .withNewChildren(Seq(bridge, c)).asInstanceOf[GpuExpression]
      val tiers = bind(Seq(GpuAlias(product, "result")()), sqlConf).exprTiers
      assert(collectExpressions[GpuProjectAstExpressionBase](tiers.flatten).isEmpty)
      assertResult(1)(collectExpressions[GpuAdd](tiers.flatten).size)
      assertResult(1)(collectExpressions[GpuMultiply](tiers.flatten).size)
      val boundBridge = collectExpressions[GpuCpuBridgeExpression](tiers.flatten).head
      assertResult(cpu)(boundBridge.cpuExpression)
    }
  }

  test("legacy eligibility survives CSE copies but preserves type rejection") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val left = AttributeReference("left", FloatType)()
      val right = AttributeReference("right", FloatType)()
      val comparison = convert(EqualTo(left, right), sqlConf)
      val copied = comparison.withNewChildren(Seq(right, left))
      val tiers = GpuProjectAstPlanner.buildExprTiers(
        Seq(GpuAlias(copied, "result")()), sqlConf, enableAst = true, enableAstJit = false)
      assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
    }
  }

  test("bound legacy selection survives driver-to-executor serialization") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val expression = convert(Alias(Subtract(Add(a, b), c), "result")(), sqlConf)
      val serializer = new JavaSerializer(new SparkConf(false)).newInstance()
      val project = bind(Seq(expression), sqlConf)
      val restored = serializer.deserialize[GpuTieredProject](serializer.serialize(project))
      val tiers = restored.exprTiers
      assertResult(1)(collectExpressions[GpuProjectAstExpression](tiers.flatten).size)
      assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
    }
  }

  test("operator replacement cannot inherit another operator's legacy eligibility") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      val supported = convert(Add(a, b), sqlConf)
      val replacement = GpuGreatest(Seq(a, b))
      replacement.copyTagsFrom(supported)
      val tiers = bind(Seq(GpuAlias(replacement, "result")()), sqlConf).exprTiers
      assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
    }
  }

  test("tiering disabled keeps unsupported roots intact") {
    val sqlConf = conf(legacy = true, jit = false, tiered = false)
    SQLConf.withExistingConf(sqlConf) {
      val expression = GpuAlias(GpuGreatest(Seq(convert(Add(a, b), sqlConf), c)), "result")()
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(1)(tiers.size)
      assert(collectExpressions[GpuProjectAstExpression](tiers.flatten).isEmpty)
    }
  }

  for (ansi <- Seq(false, true)) {
    test(s"conditional branches only materialize side-effect-free producers: ansi=$ansi") {
      val sqlConf = conf(legacy = true, jit = true, ansi = ansi)
      SQLConf.withExistingConf(sqlConf) {
        val branch = convert(Add(a, b), sqlConf)
        val expression = GpuAlias(GpuIf(GpuLiteral(false), branch, GpuLiteral(1L)), "result")()
        val tiers = bind(Seq(expression), sqlConf).exprTiers
        // ANSI addition can overflow for a row that IF does not select.
        assertResult(if (ansi) 1 else 2)(tiers.size)
        assertResult(if (ansi) 0 else 1)(
          collectExpressions[GpuAstJitExpression](tiers.flatten).size)
        assertResult(1)(collectExpressions[GpuAdd](tiers.flatten).size)
        assert(collectExpressions[GpuIf](tiers.last).nonEmpty)
      }
    }
  }

  for (operator <- Seq("AND", "OR"); jit <- Seq(false, true)) {
    test(s"$operator keeps an operand with side effects in its parent: jit=$jit") {
      val sqlConf = conf(legacy = true, jit = jit, ansi = true)
      SQLConf.withExistingConf(sqlConf) {
        val skippable = GreaterThan(Add(b, Literal(1L)), Literal(0L))
        val condition = if (operator == "AND") And(LessThan(a, Literal(0L)), skippable)
          else Or(GreaterThanOrEqual(a, Literal(0L)), skippable)
        val tiers = bind(Seq(convert(Alias(condition, "result")(), sqlConf)), sqlConf).exprTiers
        // A tier output would evaluate the ANSI addition for rows that short circuit.
        assert(collectExpressions[GpuAdd](tiers.init.flatten).isEmpty)
        val parent = if (operator == "AND") collectExpressions[GpuAnd](tiers.last)
          else collectExpressions[GpuOr](tiers.last)
        assertResult(1)(collectExpressions[GpuAdd](parent).size)
        assert(collectExpressions[GpuProjectAstExpressionBase](tiers.last).isEmpty)
      }
    }
  }

  for (jit <- Seq(false, true)) {
    test(s"AND keeps a CPU bridge operand in its parent: jit=$jit") {
      val sqlConf = conf(legacy = true, jit = jit)
      SQLConf.withExistingConf(sqlConf) {
        val cpu = Abs(BoundReference(0, LongType, nullable = true))
        val bridge = GpuCpuBridgeExpression(
          Seq(convert(Add(a, b), sqlConf)), cpu, LongType, outputNullable = true)
        val condition = convert(And(LessThan(a, Literal(0L)), GreaterThan(c, Literal(0L))), sqlConf)
        val comparison = condition.children(1)
        val bridgedComparison = comparison.withNewChildren(Seq(bridge, comparison.children(1)))
        val expression = GpuAlias(
          condition.withNewChildren(Seq(condition.children.head, bridgedComparison)), "result")()
        val tiers = bind(Seq(expression), sqlConf).exprTiers
        // The CPU expression may throw for rows where the left operand is false.
        assert(collectExpressions[GpuCpuBridgeExpression](tiers.init.flatten).isEmpty)
        val parent = collectExpressions[GpuAnd](tiers.last)
        assertResult(1)(collectExpressions[GpuCpuBridgeExpression](parent).size)
        assert(collectExpressions[GpuProjectAstExpressionBase](tiers.last).isEmpty)
      }
    }
  }

  test("a node that is not a region root can still join an enclosing legacy region") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      // greatest(a,b)+c alone would leave t+c, but it is inline in (t+c)*d.
      val expression = convert(
        Alias(Multiply(Add(Greatest(Seq(a, b)), c), d), "result")(), sqlConf)
      val tiers = bind(Seq(expression), sqlConf).exprTiers
      assertResult(2)(tiers.size)
      val legacy = collectExpressions[GpuProjectAstExpression](tiers.last)
      assertResult(1)(legacy.size)
      assert(legacy.head.child.isInstanceOf[GpuMultiply])
      assert(legacy.head.child.find(_.isInstanceOf[GpuAdd]).isDefined)
    }
  }

  test("planning-time wrappers only show a backend that runs the final root") {
    val sqlConf = conf(legacy = true, jit = false)
    SQLConf.withExistingConf(sqlConf) {
      def wrap(expressions: Seq[Expression], projectOutputs: Boolean): Seq[Expression] =
        if (projectOutputs) {
          GpuProjectAstPlanner.wrapProjectOutputs(
            expressions, sqlConf, enableAst = true, enableAstJit = false)
        } else {
          GpuProjectAstPlanner.wrapOutputs(
            expressions, sqlConf, enableAst = true, enableAstJit = false)
        }
      // CSE extracts a+b and leaves simple arithmetic, so neither output runs interpreted AST.
      val shared = Seq(
        convert(Alias(Multiply(Add(a, b), c), "first")(), sqlConf),
        convert(Alias(Multiply(Add(a, b), d), "second")(), sqlConf))
      assertResult(2)(
        collectExpressions[GpuProjectAstExpression](wrap(shared, projectOutputs = false)).size)
      assert(
        collectExpressions[GpuProjectAstExpression](wrap(shared, projectOutputs = true)).isEmpty)
      val single = Seq(convert(Alias(Subtract(Add(a, b), c), "result")(), sqlConf))
      assertResult(1)(
        collectExpressions[GpuProjectAstExpression](wrap(single, projectOutputs = true)).size)
    }
  }

  test("planning-time JIT wrapper follows an output forwarded from an earlier tier") {
    val sqlConf = conf(legacy = false, jit = true)
    SQLConf.withExistingConf(sqlConf) {
      val expressions = Seq(
        convert(Alias(Multiply(Add(a, b), c), "jit")(), sqlConf),
        GpuAlias(GpuGreatest(Seq(convert(Add(a, b), sqlConf), d)), "regular")())
      val wrapped = GpuProjectAstPlanner.wrapProjectOutputs(
        expressions, sqlConf, enableAst = false, enableAstJit = true)
      // The JIT output is computed with a+b in tier 0 and forwarded by the final tier.
      assertResult(2)(bind(expressions, sqlConf).exprTiers.size)
      assert(GpuAstJitExpression.extractTopLevel(wrapped.head).isDefined)
      assert(GpuProjectAstExpressionBase.extractTopLevel(wrapped(1)).isEmpty)
    }
  }

  test("legacy preserves non-null literal handling without compiling JIT") {
    val sqlConf = conf(legacy = true, jit = true)
    SQLConf.withExistingConf(sqlConf) {
      val expressions = Seq(convert(Alias(Literal(3L), "value")(), sqlConf),
        convert(Alias(Literal.create(null, LongType), "null_value")(), sqlConf))
      val tiers = bind(expressions, sqlConf).exprTiers
      assertResult(1)(collectExpressions[GpuProjectAstExpression](tiers.flatten).size)
      assert(collectExpressions[GpuAstJitExpression](tiers.flatten).isEmpty)
    }
  }
}
