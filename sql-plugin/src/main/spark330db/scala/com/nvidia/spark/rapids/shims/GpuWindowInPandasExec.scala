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

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.window._

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.{BatchProducer, CombiningIterator, GpuPythonHelper, GpuWindowInPandasExecBase, GroupingIterator}
import org.apache.spark.sql.rapids.execution.python.shims.{GpuArrowPythonRunner, PythonArgumentUtils}
import org.apache.spark.sql.rapids.shims.{ArrowUtilsShim, DataTypeUtilsShim}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This GpuWindowInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for Python processes
 */
case class GpuWindowInPandasExec(
    projectList: Seq[Expression],  // parameter name is different
    gpuPartitionSpec: Seq[Expression],
    cpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression]) extends GpuWindowInPandasExecBase {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: Nil

  override final def pythonModuleKey: String = "databricks"

  // On Databricks, the projectList contains not only the window expression, but may also contains
  // the input attributes. So we need to extract the window expressions from it.
  override def windowExpression: Seq[Expression] = projectList.filter { expr =>
    expr.find(node => node.isInstanceOf[GpuWindowExpression]).isDefined
  }

  // On Databricks, the projectList is expected to be the final output, and it is nondeterministic.
  // It may contain the input attributes or not, or even part of the input attributes. So
  // we need to project the joined batch per this projectList.
  // But for the schema, just return it directly.
  override def output: Seq[Attribute] = projectList
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  override def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch = {
    // Project the data
    withResource(joinedBatch) { joinBatch =>
      GpuProjectExec.project(joinBatch, outReferences)
    }
  }

  // On Databricks, binding the references on driver side will get some invalid expressions
  // (e.g. none#0L, none@1L) in the `projectList`, causing failures in `test_window` test.
  // So need to do the binding for `projectList` lazily, and the binding will actually run
  // on executors now.
  private lazy val outReferences = {
    val allExpressions = windowFramesWithExpressions.map(_._2).flatten
    val references = allExpressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      GpuBoundReference(child.output.size + i, e.dataType,
        e.nullable)(NamedExpression.newExprId, s"gpu_win_$i")
    }
    val unboundToRefMap = allExpressions.zip(references).toMap
    // Bound the project list for GPU
    GpuBindReferences.bindGpuReferences(
      projectList.map(_.transform(unboundToRefMap)), child.output, allMetrics)
  }

  // Override internalDoExecuteColumnar so we use the correct GpuArrowPythonRunner
  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (numInputRows, numInputBatches, numOutputRows, numOutputBatches) = commonGpuMetrics()
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    // 1) Unwrap the expressions and build some info data:
    //    - Map from expression index to frame index
    //    - Helper functions
    //    - isBounded by given a frame index
    val expressionsWithFrameIndex = windowFramesWithExpressions.map(_._2).zipWithIndex.flatMap {
      case (bufferEs, frameIndex) => bufferEs.map(expr => (expr, frameIndex))
    }
    val exprIndex2FrameIndex = expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap
    val (lowerBoundIndex, upperBoundIndex, frameWindowBoundTypes) =
      computeWindowBoundHelpers
    val isBounded = { frameIndex: Int => lowerBoundIndex(frameIndex) >= 0 }

    // 2) Extract window functions, here should be Python (Pandas) UDFs
    val allWindowExpressions = expressionsWithFrameIndex.map(_._1)
    val udfExpressions = PythonUDFShim.getUDFExpressions(allWindowExpressions)
    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == allWindowExpressions.length)

    // 3) Build the python runner configs
    val udfWindowBoundTypesStr = pyFuncs.indices
      .map(expId => frameWindowBoundTypes(exprIndex2FrameIndex(expId)).value)
      .mkString(",")
    val pythonRunnerConf: Map[String, String] = ArrowUtilsShim.getPythonRunnerConfMap(conf) +
      (windowBoundTypeConf -> udfWindowBoundTypesStr)

    // 4) Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs. This is similar to how other Python UDF node
    // handles UDF inputs.
    val udfArgs = PythonArgumentUtils.flatten(inputs)

    // In addition to UDF inputs, we will prepend window bounds for each UDFs.
    // For bounded windows, we prepend lower bound and upper bound. For unbounded windows,
    // we no not add window bounds. (strictly speaking, we only need to lower or upper bound
    // if the window is bounded only on one side, this can be improved in the future)

    // 5) Setting window bounds for each window frames. Each window frame has different bounds so
    // each has its own window bound columns.
    val windowBoundsInput = windowFramesWithExpressions.indices.flatMap { frameIndex =>
      if (isBounded(frameIndex)) {
        Seq(
          BoundReference(lowerBoundIndex(frameIndex), IntegerType, nullable = false),
          BoundReference(upperBoundIndex(frameIndex), IntegerType, nullable = false)
        )
      } else {
        Seq.empty
      }
    }

    // 6) Setting the window bounds argOffset for each UDF. For UDFs with bounded window, argOffset
    // for the UDF is (lowerBoundOffset, upperBoundOffset, inputOffset1, inputOffset2, ...)
    // For UDFs with unbounded window, argOffset is (inputOffset1, inputOffset2, ...)
    pyFuncs.indices.foreach { exprIndex =>
      val frameIndex = exprIndex2FrameIndex(exprIndex)
      if (isBounded(frameIndex)) {
        udfArgs.argOffsets(exprIndex) =
          Array(lowerBoundIndex(frameIndex), upperBoundIndex(frameIndex)) ++
            udfArgs.argOffsets(exprIndex).map(_ + windowBoundsInput.length)
        if (udfArgs.argNames.isDefined) {
          val argNames = udfArgs.argNames.get
          val boundsNames: Array[Option[String]] = Array(None, None)
          argNames(exprIndex) = boundsNames ++ argNames(exprIndex)
        }
      } else {
        udfArgs.argOffsets(exprIndex) =
          udfArgs.argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      }
    }

    // 7) Building the final input and schema
    val allInputs = windowBoundsInput ++ udfArgs.flattenedArgs
    val allInputTypes = allInputs.map(_.dataType)
    val pythonInputSchema = StructType(
      allInputTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }
    )

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf, pythonModuleKey)
    // cache in a local to avoid serializing the plan

    // Build the Python output schema from UDF expressions instead of the 'windowExpression',
    // because the 'windowExpression' does NOT always represent the Python output schema.
    // For example, on Databricks when projecting only one column from a Python UDF output
    // where containing multiple result columns, there will be only one item in the
    // 'windowExpression' for the projecting output, but the output schema for this Python
    // UDF contains multiple columns.
    val pythonOutputSchema = DataTypeUtilsShim.fromAttributes(udfExpressions.map(_.resultAttribute))
    val childOutput = child.output
    val localMetrics = allMetrics

    // 8) Start processing.
    child.executeColumnar().mapPartitions { inputIter =>
      val context = TaskContext.get()

      val boundDataRefs = GpuBindReferences.bindGpuReferences(udfArgs.flattenedArgs, childOutput,
        localMetrics)
      // Re-batching the input data by GroupingIterator
      val boundPartitionRefs = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, childOutput,
        localMetrics)
      val batchProducer = new BatchProducer(
        new GroupingIterator(inputIter, boundPartitionRefs, numInputRows, numInputBatches))
      val pyInputIterator = batchProducer.asIterator.map { batch =>
        withResource(batch) { _ =>
          withResource(GpuProjectExec.project(batch, boundDataRefs)) { projectedCb =>
            // Compute the window bounds and insert to the head of each row for one batch
            insertWindowBounds(projectedCb)
          }
        }
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
          udfArgs.argOffsets,
          pythonInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf,
          /* The whole group data should be written in a single call, so here is unlimited */
          Int.MaxValue,
          pythonOutputSchema,
          udfArgs.argNames)

        val outputIterator = pyRunner.compute(pyInputIterator, context.partitionId(), context)
        new CombiningIterator(batchProducer.getBatchQueue, outputIterator, pyRunner,
            numOutputRows, numOutputBatches).map(projectResult)
      } else {
        // Empty partition, return the input iterator directly
        inputIter
      }

    } // End of mapPartitions
  } // End of internalDoExecuteColumnar

}
