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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastNestedLoopJoinRetrySuite extends RmmSparkRetrySuiteBase {
  private val x = AttributeReference("x", IntegerType, nullable = false)()
  private val y = AttributeReference("y", IntegerType, nullable = false)()
  private val z = AttributeReference("z", IntegerType, nullable = false)()
  private val buildAttributes = Seq(x, y, z)

  private case class TestLeafExec(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("TestLeafExec is not executable")
  }

  private case class TestBroadcastNestedLoopJoin(
      left: SparkPlan,
      right: SparkPlan,
      postBuildCondition: List[NamedExpression])
      extends GpuBroadcastNestedLoopJoinExecBase(
        left,
        right,
        Inner,
        GpuBuildRight,
        condition = None,
        postBuildCondition,
        targetSizeBytes = 1024L) {
    override lazy val allMetrics: Map[String, GpuMetric] = Map.empty
  }

  override def afterEach(): Unit = {
    RmmSpark.getAndResetNumRetryThrow(/* taskId */ 1)
    RmmSpark.getAndResetNumSplitRetryThrow(/* taskId */ 1)
    super.afterEach()
  }

  private def postBuildExpressions: List[NamedExpression] = {
    val shared = GpuMultiply(
      GpuAdd(x, y, failOnError = false)(), z, failOnError = false)()
    List(
      GpuAlias(GpuSubtract(shared, x, failOnError = false)(), "shared_minus_x")(),
      GpuAlias(GpuSubtract(shared, y, failOnError = false)(), "shared_minus_y")(),
      x,
      y,
      z)
  }

  private def buildBatch(): ColumnarBatch = {
    val table = new Table.TestBuilder()
        .column(1.asInstanceOf[java.lang.Integer], 2, 3)
        .column(4.asInstanceOf[java.lang.Integer], 5, 6)
        .column(2.asInstanceOf[java.lang.Integer], 3, 4)
        .build()
    withResource(table) { tbl =>
      GpuColumnVector.from(tbl, Array(IntegerType, IntegerType, IntegerType))
    }
  }

  private def collectInts(batch: ColumnarBatch, column: Int): Seq[Int] = {
    val gpuColumn = batch.column(column).asInstanceOf[GpuColumnVector]
    withResource(gpuColumn.getBase.copyToHost()) { hostColumn =>
      (0 until batch.numRows()).map(row => hostColumn.getInt(row))
    }
  }

  test("BNLJ build-side shared JIT tier retries GpuRetryOOM") {
    val conf = new SQLConf()
    conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, "true")
    conf.setConfString(RapidsConf.ENABLE_COMBINED_EXPRESSIONS.key, "true")
    conf.setConfString(RapidsConf.ENABLE_PROJECT_AST_JIT.key, "true")
    conf.setConfString(RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.key, "true")
    val expressions = postBuildExpressions
    val buildProject = GpuProjectExec(expressions, TestLeafExec(buildAttributes))
    val join = TestBroadcastNestedLoopJoin(
      TestLeafExec(Seq.empty), buildProject, expressions)
    val taskContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("GpuBroadcastNestedLoopJoinRetrySuite")
        .getOrCreate()

    TrampolineUtil.setTaskContext(taskContext)
    try {
      SQLConf.withExistingConf(conf) {
        val boundProject = GpuBindReferences.bindGpuProjectReferencesTiered(
          expressions, buildAttributes, conf, Map.empty)
        val jitExpressions = boundProject.exprTiers.flatten.flatMap(_.collect {
          case expression: GpuAstJitExpression => expression
        })
        assertResult(2)(boundProject.exprTiers.size)
        assertResult(1)(jitExpressions.size)
        assert(jitExpressions.head.child.isInstanceOf[GpuMultiply])
        assert(jitExpressions.head.child.find(_.isInstanceOf[GpuAdd]).isDefined)

        val projectBuildSide = join.buildSidePostProjection.get
        // Compile before arming the OOM so the retry is exercised by computeColumnJit.
        withResource(projectBuildSide(buildBatch())) { _ => }

        val retryInput = buildBatch()
        RmmSpark.getAndResetNumRetryThrow(/* taskId */ 1)
        RmmSpark.getAndResetNumSplitRetryThrow(/* taskId */ 1)
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        withResource(projectBuildSide(retryInput)) { output =>
          assertResult(Seq(9, 19, 33))(collectInts(output, 0))
          assertResult(Seq(6, 16, 30))(collectInts(output, 1))
        }
        assert(RmmSpark.getAndResetNumRetryThrow(/* taskId */ 1) > 0)
        assertResult(0)(RmmSpark.getAndResetNumSplitRetryThrow(/* taskId */ 1))
      }
    } finally {
      try {
        taskContext.markTaskComplete()
      } finally {
        TrampolineUtil.unsetTaskContext()
        ScalableTaskCompletion.reset()
        spark.stop()
      }
    }
  }
}
