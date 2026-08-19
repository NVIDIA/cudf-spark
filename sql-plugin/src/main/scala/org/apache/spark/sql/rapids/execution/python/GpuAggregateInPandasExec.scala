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

import ai.rapids.cudf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.shims.{GpuGroupedPythonRunnerFactory, PythonArgumentUtils}
import org.apache.spark.sql.rapids.shims.DataTypeUtilsShim
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch



/**
 * Physical node for aggregation with group aggregate Pandas UDF.
 *
 * This plan works by sending the necessary (projected) input grouped data as Arrow record batches
 * to the Python worker, the Python worker invokes the UDF and sends the results to the executor.
 * Finally the executor evaluates any post-aggregation expressions and join the result with the
 * grouped key.
 *
 * This node aims at accelerating the data transfer between JVM and Python for GPU pipeline, and
 * scheduling GPU resources for its Python processes.
 */
case class GpuAggregateInPandasExec(
    gpuGroupingExpressions: Seq[NamedExpression],
    udfExpressions: Seq[GpuPythonFunction],
    pyOutAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)(
    cpuGroupingExpressions: Seq[NamedExpression])
  extends ShimUnaryExecNode with GpuPythonExecBase {

  override def otherCopyArgs: Seq[AnyRef] = cpuGroupingExpressions :: Nil

  override val output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (cpuGroupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(cpuGroupingExpressions) :: Nil
    }
  }

  private def collectFunctions(udf: GpuPythonFunction):
  ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: GpuPythonFunction) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assertInTests(children.forall(_.find(_.isInstanceOf[GpuPythonFunction]).isEmpty))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(cpuGroupingExpressions.map(SortOrder(_, Ascending)))

  // One batch as input to keep the integrity for each group.
  // (This should be replaced by an iterator that can split batches on key boundaries eventually.
  //  For more details please refer to the following issue:
  //        https://github.com/NVIDIA/spark-rapids/issues/2200 )
  //
  // But there is a special case that the iterator can not work. when 'groupingExpressions' is
  // empty, the input must be a single batch, because the whole data is treated as a single
  // group, and it should be sent to Python at one time.
  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(RequireSingleBatch)

  // When groupingExpressions is not empty, the input batch will be split into multiple
  // batches by the grouping expressions, and processed by Python executors group by group,
  // so better to coalesce the output batches.
  override def coalesceAfter: Boolean = gpuGroupingExpressions.nonEmpty

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (mNumInputRows, mNumInputBatches, mNumOutputRows, mNumOutputBatches) = commonGpuMetrics()

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val childOutput = child.output
    val resultExprs = resultExpressions

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val udfArgs = PythonArgumentUtils.flatten(inputs)

    // Schema of input rows to the python runner
    val aggInputSchema = StructType(udfArgs.flattenedTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    }.toArray)

    // Start processing
    child.executeColumnar().mapPartitionsInternal { inputIter =>
      val context = TaskContext.get()

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      // First projects the input batches to (groupingExpressions + allInputs), which is minimum
      // necessary for the following processes.
      // Doing this can reduce the data size to be split, probably getting a better performance.
      val groupingRefs = GpuBindReferences.bindGpuReferences(gpuGroupingExpressions,
        childOutput, allMetrics)
      val pyInputRefs = GpuBindReferences.bindGpuReferences(udfArgs.flattenedArgs,
        childOutput, allMetrics)
      val miniIter = inputIter.map { batch =>
        mNumInputBatches += 1
        mNumInputRows += batch.numRows()
        withResource(batch) { b =>
          GpuProjectExec.project(b, groupingRefs ++ pyInputRefs)
        }
      }

      // Second splits into separate group batches.
      val miniAttrs =
        (gpuGroupingExpressions ++ udfArgs.flattenedArgs).asInstanceOf[Seq[Attribute]]
      val keyConverter = (groupedBatch: ColumnarBatch) => {
        // No `safeMap` because here does not increase the ref count.
        // (`Seq.indices.map()` is NOT lazy, so it is safe to be used to slice the columns.)
        val keyCudfColumns = groupingRefs.indices.map(
          groupedBatch.column(_).asInstanceOf[GpuColumnVector].getBase)
        if (keyCudfColumns.isEmpty) {
          // No grouping columns, then the whole batch is a group. Returns the dedicated batch
          // as the group key.
          // This batch means there is only one empty row, just like the 'new UnsafeRow()'
          // used in Spark. The row number setting to 1 is because Python returns only one row
          // as the aggregate result for the whole batch, and 'CombiningIterator' requires the
          // the same row number for both the key batch and the result batch to be combined.
          new ColumnarBatch(Array(), 1)
        } else {
          // Uses `cudf.Table.gather` to pick the first row in each group as the group key.
          // Doing this is because
          //   - The Python worker produces only one row as the aggregate result,
          //   - The key rows in a group are equal to each other.
          //
          // (Now this is done group by group, so the performance would not be good when
          //  there are too many small groups.)
          withResource(new cudf.Table(keyCudfColumns: _*)) { table =>
            withResource(cudf.ColumnVector.fromInts(0)) { gatherMap =>
              withResource(table.gather(gatherMap)) { oneRowTable =>
                GpuColumnVector.from(oneRowTable, groupingRefs.map(_.dataType).toArray)
              }
            }
          }
        }
      }

      val batchProducer = new BatchProducer(
        BatchGroupedIterator(miniIter, miniAttrs, groupingRefs.indices), Some(keyConverter))
      val pyInputIter = batchProducer.asIterator.map { batch =>
        withResource(batch) { _ =>
          val pyInputColumns = pyInputRefs.indices.safeMap { idx =>
            batch.column(idx + groupingRefs.size).asInstanceOf[GpuColumnVector].incRefCount()
          }
          new ColumnarBatch(pyInputColumns.toArray, batch.numRows())
        }
      }

      val runnerFactory = GpuGroupedPythonRunnerFactory(conf, pyFuncs, udfArgs.argOffsets,
        aggInputSchema, DataTypeUtilsShim.fromAttributes(pyOutAttributes),
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF, udfArgs.argNames)

      // Third, sends to Python to execute the aggregate and returns the result.
      if (pyInputIter.hasNext) {
        // Launch Python workers only when the data is not empty.
        val pyRunner = runnerFactory.getRunner()
        val pyOutputIterator = pyRunner.compute(pyInputIter, context.partitionId(), context)

        val combinedAttrs = gpuGroupingExpressions.map(_.toAttribute) ++ pyOutAttributes
        val resultRefs = GpuBindReferences.bindGpuReferences(resultExprs, combinedAttrs,
          allMetrics)
        // Gets the combined batch for each group and projects for the output.
        new CombiningIterator(batchProducer.getBatchQueue, pyOutputIterator,
            pyRunner.asInstanceOf[GpuArrowOutput], mNumOutputRows,
            mNumOutputBatches).map { combinedBatch =>
          withResource(combinedBatch) { batch =>
            GpuProjectExec.project(batch, resultRefs)
          }
        }
      } else {
        // Empty partition, returns it directly
        inputIter
      }
    }
  } // end of internalDoExecuteColumnar

}

object GpuAggregateInPandasExec {
  def apply(gpuGroupingExpressions: Seq[NamedExpression],
      udfExpressions: Seq[GpuPythonFunction],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan)(
      cpuGroupingExpressions: Seq[NamedExpression]) = {
    new GpuAggregateInPandasExec(gpuGroupingExpressions, udfExpressions,
      udfExpressions.map(_.resultAttribute), resultExpressions, child)(cpuGroupingExpressions)
  }

  def apply(gpuGroupingExpressions: Seq[NamedExpression],
      udfExpressions: Seq[GpuPythonFunction],
      pyOutAttributes: Seq[Attribute],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan)(
      cpuGroupingExpressions: Seq[NamedExpression]) = {
    new GpuAggregateInPandasExec(gpuGroupingExpressions, udfExpressions,
      pyOutAttributes, resultExpressions, child)(cpuGroupingExpressions)
  }

}
