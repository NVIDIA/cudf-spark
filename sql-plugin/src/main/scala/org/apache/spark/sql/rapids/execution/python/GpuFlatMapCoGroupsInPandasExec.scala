/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.FlatMapCoGroupsInPandasExec
import org.apache.spark.sql.rapids.execution.python.BatchGroupUtils._
import org.apache.spark.sql.rapids.execution.python.shims.GpuCoGroupedArrowPythonRunner
import org.apache.spark.sql.rapids.shims.{ArrowUtilsShim, DataTypeUtilsShim}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuFlatMapCoGroupsInPandasExecMeta(
    flatPandas: FlatMapCoGroupsInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[FlatMapCoGroupsInPandasExec](flatPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  override def tagPlanForGpu(): Unit = {
    // Fall back to CPU when the two grouping columns in a pair have different types.
    // e.g.
    //   Left grouping column is (a: Int),
    //   Right grouping columns are (a: String, b: Int)
    // The first pair <a: Int, a:String> has different types. This is a meaningless case,
    // but Spark can run unexpectedly due to no type check in the UnsafeRow, while GPU
    // will throw an exception if Java Assertion is enabled.
    if (flatPandas.leftGroup.zipWithIndex.exists { case (leftGpAttr, at) =>
      flatPandas.rightGroup.lift(at).exists { rightGpAttr =>
        !leftGpAttr.dataType.sameType(rightGpAttr.dataType)
      }
    }) {
      willNotWorkOnGpu("grouping columns from two DataFrames have different types.")
    }
  }

  private val leftGroupingAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.leftGroup.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val rightGroupingAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.rightGroup.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
    flatPandas.func.asInstanceOf[PythonUDF], conf, Some(this))

  private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    leftGroupingAttrs ++ rightGroupingAttrs ++ resultAttrs :+ udf

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    GpuFlatMapCoGroupsInPandasExec(
      leftGroupingAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      rightGroupingAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      udf.convertToGpu(),
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      left,
      right
    )
  }
}

/**
 * GPU version of Spark's `FlatMapCoGroupsInPandasExec`
 *
 * This node aims at accelerating the data transfer between JVM and Python for GPU pipeline, and
 * scheduling GPU resources for its Python processes.
 */
case class GpuFlatMapCoGroupsInPandasExec(
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    udf: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends SparkPlan with ShimBinaryExecNode with GpuPythonExecBase {

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)
  private val pyUDF = udf.asInstanceOf[GpuPythonUDF]
  private val chainedFunc = Seq((ChainedPythonFunctions(Seq(pyUDF.func)), pyUDF.resultId.id))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    val leftDist = if (leftGroup.isEmpty) AllTuples else ClusteredDistribution(leftGroup)
    val rightDist = if (rightGroup.isEmpty) AllTuples else ClusteredDistribution(rightGroup)
    leftDist :: rightDist :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftGroup.map(SortOrder(_, Ascending)) ::
      rightGroup.map(SortOrder(_, Ascending)) :: Nil
  }

  /* Rapids things start */
  // One batch as input to keep the integrity for each group
  override def childrenCoalesceGoal: Seq[CoalesceGoal] =
    Seq(RequireSingleBatch, RequireSingleBatch)

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (numInputRows, numInputBatches, numOutputRows, numOutputBatches) = commonGpuMetrics()
    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    // Python wraps the resulting columns in a single struct column.
    val pythonOutputSchema = StructType(
      StructField("out_struct", DataTypeUtilsShim.fromAttributes(output)) :: Nil)

    // Resolve the argument offsets and related attributes.
    val GroupArgs(leftDedupAttrs, leftArgOffsets, leftGroupingOffsets) =
      resolveArgOffsets(left, leftGroup)
    val GroupArgs(rightDedupAttrs, rightArgOffsets, rightGroupingOffsets) =
      resolveArgOffsets(right, rightGroup)

    left.executeColumnar().zipPartitions(right.executeColumnar())  { (leftIter, rightIter) =>
      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

      // Only execute if partition is not empty
      if (leftIter.isEmpty && rightIter.isEmpty) Iterator.empty else {
        // project and group for left and right
        val leftGroupedIter = projectAndGroup(leftIter, left.output, leftDedupAttrs,
          leftGroupingOffsets, numInputRows, numInputBatches, allMetrics)
        val rightGroupedIter = projectAndGroup(rightIter, right.output, rightDedupAttrs,
          rightGroupingOffsets, numInputRows, numInputBatches, allMetrics)
        // Cogroup the data
        val pyInputIter = new CoGroupedIterator(leftGroupedIter, leftDedupAttrs,
          leftGroupingOffsets, rightGroupedIter, rightDedupAttrs,  rightGroupingOffsets)

        val pyRunner = new GpuCoGroupedArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
          Array(leftArgOffsets ++ rightArgOffsets),
          DataTypeUtilsShim.fromAttributes(leftDedupAttrs),
          DataTypeUtilsShim.fromAttributes(rightDedupAttrs),
          sessionLocalTimeZone,
          pythonRunnerConf,
          // The whole group data should be written in a single call, so here is unlimited
          Int.MaxValue,
          pythonOutputSchema)

        executePython(pyInputIter, output, pyRunner, numOutputRows, numOutputBatches)
      }
    }
  } // end of internalDoExecuteColumnar
}
