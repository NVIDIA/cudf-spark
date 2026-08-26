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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, Expression, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{BinaryExecNode, InSubqueryExec => SparkInSubqueryExec,
  LeafExecNode, ReusedSubqueryExec, SparkPlan, SubqueryExec, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

private case class PlanValidationLeafExec(marker: Int = 0) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationLeafExec is not executable")
}

private case class PlanValidationUnaryExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationUnaryExec is not executable")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

private case class PlanValidationBinaryExec(left: SparkPlan, right: SparkPlan)
    extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationBinaryExec is not executable")
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)
}

private case class PlanValidationExpressionExec(
    testExpressions: Seq[Expression]) extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationExpressionExec is not executable")
}

private case class PlanValidationGpuExec(
    testGpuExpressions: Seq[Expression]) extends LeafExecNode with GpuExec {
  override def output: Seq[Attribute] = Seq.empty
  override def gpuExpressions: Seq[Expression] = testGpuExpressions
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationGpuExec is not executable")
  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException("PlanValidationGpuExec is not executable")
}

private case class PlanValidationRequiredLeafExec() extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationRequiredLeafExec is not executable")
}

private case class PlanValidationDisallowedExec() extends LeafExecNode {
  override def output: Seq[Attribute] = Seq.empty
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationDisallowedExec is not executable")
}

// Test double for the structural wrapper supplied by the optional Delta module.
private case class RapidsDeltaWriteExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("RapidsDeltaWriteExec is not executable")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

private case class PlanValidationExchangeExec(child: SparkPlan) extends Exchange {
  override def doCanonicalize(): SparkPlan = this
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("PlanValidationExchangeExec is not executable")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

class TestPlanValidatorSuite extends AnyFunSuite {
  private def context(
      marker: String,
      requiredExec: String = ""): TestPlanValidator.ValidationContext = {
    TestPlanValidator.ValidationContext(Map(
      RapidsConf.SQL_ENABLED.key -> "true",
      RapidsConf.TEST_CONF.key -> "true",
      RapidsConf.TEST_ALLOWED_NONGPU.key -> Seq(
        "PlanValidationLeafExec",
        "PlanValidationUnaryExec",
        "PlanValidationBinaryExec",
        "PlanValidationExpressionExec",
        "PlanValidationRequiredLeafExec",
        "PlanValidationExchangeExec").mkString(","),
      RapidsConf.TEST_VALIDATE_EXECS_ONGPU.key -> requiredExec,
      "spark.rapids.sql.test.validation.marker" -> marker),
      adaptiveEnabled = false,
      shouldValidate = true)
  }

  test("disabled context skips validation without weakening missing-context detection") {
    val gpuPlan = PlanValidationGpuExec(Seq.empty)
    val error = intercept[IllegalStateException] {
      TestPlanValidator.validatePlan(gpuPlan)
    }
    assert(error.getMessage.contains("missing its test validation configuration context"))

    TestPlanValidator.tagForValidation(
      gpuPlan, TestPlanValidator.validationDisabledContext)
    TestPlanValidator.validatePlan(gpuPlan)
  }

  test("nearest context wins when a later planning pass wraps an older pass") {
    val olderContext = context("older", "PlanValidationLeafExec")
    val newerContext = context("newer", "MissingFromFinalPlan")
    val olderPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), olderContext)
    val finalPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(olderPlan), newerContext)

    assert(TestPlanValidator.resolveValidationContext(finalPlan).contains(newerContext))
    val error = intercept[IllegalArgumentException] {
      TestPlanValidator.validatePlan(finalPlan)
    }
    assert(error.getMessage.contains("MissingFromFinalPlan"))
  }

  test("an untagged final wrapper resolves the nearest tagged descendant") {
    val expectedContext = context("wrapped")
    val taggedPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), expectedContext)
    val finalPlan = PlanValidationUnaryExec(taggedPlan)

    assert(TestPlanValidator.resolveValidationContext(finalPlan).contains(expectedContext))
    TestPlanValidator.validatePlan(finalPlan)
  }

  test("different contexts at the same nearest depth are rejected") {
    val left = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), context("left"))
    val right = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), context("right"))
    val finalPlan = PlanValidationBinaryExec(left, right)

    val error = intercept[IllegalArgumentException] {
      TestPlanValidator.resolveValidationContext(finalPlan)
    }
    assert(error.getMessage.contains("ambiguous validation contexts"))
  }

  test("validation uses the tagged configuration instead of mutable SQLConf") {
    val taggedContext = context("snapshot", "MissingFromSnapshot")
    val finalPlan = TestPlanValidator.tagForValidation(
      PlanValidationLeafExec(), taggedContext)
    val sqlConf = SQLConf.get
    val key = RapidsConf.TEST_VALIDATE_EXECS_ONGPU.key
    val previousValue = sqlConf.getConfString(key, null)
    sqlConf.setConfString(key, "PlanValidationLeafExec")
    try {
      val error = intercept[IllegalArgumentException] {
        TestPlanValidator.validatePlan(finalPlan)
      }
      assert(error.getMessage.contains("MissingFromSnapshot"))
    } finally {
      if (previousValue == null) {
        sqlConf.unsetConf(key)
      } else {
        sqlConf.setConfString(key, previousValue)
      }
    }
  }

  test("collects final plans through adaptive and query-stage wrappers") {
    val required = PlanValidationRequiredLeafExec()
    val stage = mock(classOf[QueryStageExec])
    when(stage.plan).thenReturn(required)
    when(stage.children).thenReturn(Seq.empty)
    when(stage.expressions).thenReturn(Seq.empty)

    val finalPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(stage),
      context("adaptive-query-stage", "PlanValidationRequiredLeafExec"))
    val adaptive = mock(classOf[AdaptiveSparkPlanExec])
    when(adaptive.executedPlan).thenReturn(finalPlan)
    when(adaptive.children).thenReturn(Seq.empty)
    when(adaptive.expressions).thenReturn(Seq.empty)
    when(adaptive.getTagValue(
      any[TreeNodeTag[TestPlanValidator.ValidationContext]])).thenReturn(None)

    val planNodes = TestPlanValidator.collectPlanNodes(adaptive)
    assert(planNodes.exists(_ eq adaptive))
    assert(planNodes.exists(_ eq finalPlan))
    assert(planNodes.exists(_ eq stage))
    assert(planNodes.exists(_ eq required))
    TestPlanValidator.validatePlan(adaptive)
  }

  test("collects reused exchange and subquery graphs once by identity") {
    val exchangeLeaf = PlanValidationRequiredLeafExec()
    val exchange = PlanValidationExchangeExec(exchangeLeaf)
    val reusedExchangeLeft = ReusedExchangeExec(exchange.output, exchange)
    val reusedExchangeRight = ReusedExchangeExec(exchange.output, exchange)

    val subqueryLeaf = PlanValidationRequiredLeafExec()
    val subquery = SubqueryExec("validation-subquery", subqueryLeaf)
    val reusedSubqueryLeft = ReusedSubqueryExec(subquery)
    val reusedSubqueryRight = ReusedSubqueryExec(subquery)

    val root = PlanValidationBinaryExec(
      PlanValidationBinaryExec(reusedExchangeLeft, reusedExchangeRight),
      PlanValidationBinaryExec(reusedSubqueryLeft, reusedSubqueryRight))
    val planNodes = TestPlanValidator.collectPlanNodes(root)

    assert(planNodes.count(_ eq exchange) == 1)
    assert(planNodes.count(_ eq exchangeLeaf) == 1)
    assert(planNodes.count(_ eq subquery) == 1)
    assert(planNodes.count(_ eq subqueryLeaf) == 1)
  }

  test("collects subquery plans referenced from expressions") {
    val subqueryLeaf = PlanValidationRequiredLeafExec()
    val subquery = SubqueryExec("validation-expression-subquery", subqueryLeaf)
    val reusedSubquery = ReusedSubqueryExec(subquery)
    val inSubquery = SparkInSubqueryExec(
      Literal(1), reusedSubquery, NamedExpression.newExprId)
    val root = PlanValidationExpressionExec(Seq(inSubquery))

    val planNodes = TestPlanValidator.collectPlanNodes(root)
    assert(planNodes.exists(_ eq reusedSubquery))
    assert(planNodes.exists(_ eq subquery))
    assert(planNodes.exists(_ eq subqueryLeaf))

    TestPlanValidator.tagForValidation(
      root, context("expression-subquery", "PlanValidationRequiredLeafExec"))
    TestPlanValidator.validatePlan(root)
  }

  test("required exec and CPU rejection checks use the complete collected plan") {
    val requiredPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(PlanValidationRequiredLeafExec()),
      context("required", "PlanValidationRequiredLeafExec"))
    TestPlanValidator.validatePlan(requiredPlan)

    val cpuPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(PlanValidationDisallowedExec()), context("cpu-operator"))
    val cpuError = intercept[IllegalArgumentException] {
      TestPlanValidator.validatePlan(cpuPlan)
    }
    assert(cpuError.getMessage.contains("PlanValidationDisallowedExec"))

    val expressionPlan = TestPlanValidator.tagForValidation(
      PlanValidationUnaryExec(PlanValidationGpuExec(Seq(Add(Literal(1), Literal(2))))),
      context("cpu-expression"))
    val expressionError = intercept[IllegalArgumentException] {
      TestPlanValidator.validatePlan(expressionPlan)
    }
    assert(expressionError.getMessage.contains("Add"))
  }

  test("Delta write wrapper is exempt while its child is still validated") {
    val gpuPlan = TestPlanValidator.tagForValidation(
      RapidsDeltaWriteExec(PlanValidationGpuExec(Seq.empty)), context("delta-write-wrapper"))
    TestPlanValidator.validatePlan(gpuPlan)

    val cpuPlan = TestPlanValidator.tagForValidation(
      RapidsDeltaWriteExec(PlanValidationDisallowedExec()), context("delta-write-child"))
    val error = intercept[IllegalArgumentException] {
      TestPlanValidator.validatePlan(cpuPlan)
    }
    assert(error.getMessage.contains("PlanValidationDisallowedExec"))
  }
}
