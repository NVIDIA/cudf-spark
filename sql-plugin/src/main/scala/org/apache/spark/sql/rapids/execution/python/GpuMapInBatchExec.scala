/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters.seqAsJavaListConverter

import ai.rapids.cudf
import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.rapids.execution.python.shims._
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * A relation produced by applying a function that takes an iterator of batches
 * such as pandas DataFrame or PyArrow's record batches, and outputs an iterator of them.
 */
trait GpuMapInBatchExec extends ShimUnaryExecNode with GpuPythonExecBase {

  protected val func: Expression
  protected val pythonEvalType: Int

  protected val isBarrier: Boolean

  private val udf = func.asInstanceOf[GpuPythonUDF]

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (numInputRows, numInputBatches, numOutputRows, numOutputBatches) = commonGpuMetrics()

    val pyInputTypes = child.schema
    val chainedFunc = Seq((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)
    val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val localOutput = output
    val localBatchSize = batchSize
    val localEvalType = pythonEvalType

    // Start process
    val func = (inputIter: Iterator[ColumnarBatch]) => {
      val context = TaskContext.get()

      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val pyInputSchema = StructType(StructField("in_struct", pyInputTypes) :: Nil)

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      val pyInputIterator = new RebatchingRoundoffIterator(inputIter, pyInputTypes,
        localBatchSize, numInputRows, numInputBatches).map { batch =>
          // Here we wrap it via another column so that Python sides understand it
          // as a DataFrame.
          withResource(batch) { b =>
            val structColumn = cudf.ColumnVector.makeStruct(GpuColumnVector.extractBases(b): _*)
            withResource(structColumn) { stColumn =>
              val gpuColumn = GpuColumnVector.from(stColumn.incRefCount(), pyInputTypes)
              new ColumnarBatch(Array(gpuColumn), b.numRows())
            }
          }
        }
      val pyRunner = new GpuArrowPythonRunner(
          chainedFunc,
          localEvalType,
          argOffsets,
          pyInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf,
          localBatchSize,
          GpuColumnVector.structFromAttributes(localOutput.asJava)) {
        override def toBatch(table: Table): ColumnarBatch = {
          BatchGroupedIterator.extractChildren(table, localOutput)
        }
      }

      pyRunner.compute(pyInputIterator, context.partitionId(), context)
        .map { cb =>
          numOutputBatches += 1
          numOutputRows += cb.numRows
          cb
        }
    } // end of func

    if (isBarrier) {
      child.executeColumnar().barrier().mapPartitions(func)
    } else {
      child.executeColumnar().mapPartitionsInternal(func)
    }

  } // end of internalDoExecuteColumnar

}
