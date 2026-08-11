/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.FlatMapGroupsInPandasExec
import org.apache.spark.sql.rapids.execution.python.BatchGroupUtils._
import org.apache.spark.sql.rapids.execution.python.shims.GpuGroupedPythonRunnerFactory
import org.apache.spark.sql.rapids.shims.DataTypeUtilsShim
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuFlatMapGroupsInPandasExecMeta(
    flatPandas: FlatMapGroupsInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[FlatMapGroupsInPandasExec](flatPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  private val groupingAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.groupingAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
    flatPandas.func.asInstanceOf[PythonUDF], conf, Some(this))

  private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    flatPandas.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = groupingAttrs ++ resultAttrs :+ udf

  override def convertToGpu(): GpuExec =
    GpuFlatMapGroupsInPandasExec(
      groupingAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      udf.convertToGpu(),
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      childPlans.head.convertIfNeeded()
    )
}

/**
 * GPU version of Spark's `FlatMapGroupsInPandasExec`
 *
 * Rows in each group are passed to the Python worker as an Arrow record batch.
 * The Python worker turns the record batch to a `pandas.DataFrame`, invoke the
 * user-defined function, and passes the resulting `pandas.DataFrame`
 * as an Arrow record batch. Finally, each record batch is turned to
 * a ColumnarBatch.
 *
 * This node aims at accelerating the data transfer between JVM and Python for GPU pipeline, and
 * scheduling GPU resources for its Python processes.
 */
case class GpuFlatMapGroupsInPandasExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends SparkPlan with ShimUnaryExecNode with GpuPythonExecBase {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  private val udf = func.asInstanceOf[GpuPythonUDF]

  // One batch as input to keep the integrity for each group
  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(RequireSingleBatch)

  // The input batch will be split into multiple batches by grouping expression, and
  // processed by Python executors group by group, so better to coalesce the output batches.
  override def coalesceAfter: Boolean = true

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (mNumInputRows, mNumInputBatches, mNumOutputRows, mNumOutputBatches) = commonGpuMetrics()

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val chainedFunc = Seq((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id))
    val localOutput = output
    val localChildOutput = child.output
    // Python wraps the resulting columns in a single struct column.
    val pythonOutputSchema = StructType(
        StructField("out_struct", DataTypeUtilsShim.fromAttributes(localOutput)) :: Nil)

    // Resolve the argument offsets and related attributes.
    val GroupArgs(dedupAttrs, argOffsets, groupingOffsets) =
        resolveArgOffsets(child, groupingAttributes)

    val runnerFactory = GpuGroupedPythonRunnerFactory(conf, chainedFunc, Array(argOffsets),
        DataTypeUtilsShim.fromAttributes(dedupAttrs), pythonOutputSchema,
        udf.evalType)

    // Start processing. Map grouped batches to ArrowPythonRunner results.
    child.executeColumnar().mapPartitionsInternal { inputIter =>
      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

      // Projects each input batch into the deduplicated schema, and splits
      // into separate group batches to sends them to Python group by group later.
      val pyInputIter = projectAndGroup(inputIter, localChildOutput, dedupAttrs, groupingOffsets,
          mNumInputRows, mNumInputBatches, allMetrics)

      if (pyInputIter.hasNext) {
        // Launch Python workers only when the data is not empty.
        val pyRunner = runnerFactory.getRunner()
        executePython(pyInputIter, localOutput, pyRunner, mNumOutputRows, mNumOutputBatches)
      } else {
        // Empty partition, return it directly
        inputIter
      }
    } // end of mapPartitionsInternal
  }
}
