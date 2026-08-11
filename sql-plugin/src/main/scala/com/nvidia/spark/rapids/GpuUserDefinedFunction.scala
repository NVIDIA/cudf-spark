/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{HostColumnVectorCore, NvtxColor, NvtxRange}
import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UserDefinedExpression}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Common implementation across all RAPIDS accelerated UDF types */
trait GpuUserDefinedFunction extends GpuExpression
    with ShimExpression
    with UserDefinedExpression with Serializable {
  /** name of the UDF function */
  val name: String

  /** User's UDF instance */
  val function: RapidsUDF

  /** True if the UDF is deterministic */
  val udfDeterministic: Boolean

  override def hasSideEffects: Boolean = true

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)
  override val selfNonDeterministic: Boolean = !udfDeterministic

  private[this] val nvtxRangeName = s"UDF: $name"
  private[this] lazy val funcCls = TrampolineUtil.getSimpleName(function.getClass)
  private[this] lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  private[this] lazy val outputType = dataType.catalogString

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val cols = children.safeMap(_.columnarEval(batch))
    withResource(cols) { exprResults =>
      val funcInputs = exprResults.map(_.getBase()).toArray
      withResource(new NvtxRange(nvtxRangeName, NvtxColor.PURPLE)) { _ =>
        try {
          val resultColumn = function.evaluateColumnar(batch.numRows(), funcInputs: _*)
          closeOnExcept(resultColumn) { _ =>
            if (batch.numRows() != resultColumn.getRowCount) {
              throw new IllegalStateException("UDF returned a different row count than the " +
                  s"input, expected ${batch.numRows} found ${resultColumn.getRowCount}")
            }
            GpuColumnVector.fromChecked(resultColumn, dataType)
          }
        } catch {
          case e: Exception =>
            throw new SparkException("Failed to execute user defined function " +
                s"($funcCls: ($inputTypesString) => $outputType)", e)
        }
      }
    }
  }
}

object GpuUserDefinedFunction {
  // UDFs can support all types except UDT which does not have a clear columnar representation.
  val udfTypeSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
      TypeSig.BINARY + TypeSig.CALENDAR + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()

  /** (This will be initialized once per process) */
  lazy val hostColumnAssertionEnabled: Boolean =
    classOf[HostColumnVectorCore].desiredAssertionStatus()
}

/**
 * Execute a row based UDF efficiently by pulling back only the columns the UDF needs to host
 * and do the processing on CPU.
 */
trait GpuRowBasedUserDefinedFunction extends GpuExpression
    with ShimExpression with UserDefinedExpression with Serializable with Logging {
  /** name of the UDF function */
  val name: String

  /** True if the UDF is deterministic */
  val udfDeterministic: Boolean

  /** True if the UDF needs null check when converting input columns to rows */
  val checkNull: Boolean

  /** The row based function of the UDF. */
  protected def evaluateRow(childrenRow: InternalRow): Any

  private[this] lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  private[this] lazy val outputType = dataType.catalogString

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)
  override val selfNonDeterministic: Boolean = !udfDeterministic
  override def hasSideEffects: Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val cpuUDFStart = System.nanoTime
    // These child columns will be closed by `ColumnarToRowIterator`.
    val argCols = children.safeMap(_.columnarEval(batch))
    val prepareArgsEnd = System.nanoTime
    try {
      // 1 Convert the argument columns to row.
      // 2 Evaluate the CPU UDF row by row and cache the result.
      // 3 Build a result column from the cache.
      val retConverter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
      val retType = GpuColumnVector.convertFrom(dataType, nullable)
      val retRow = new GenericInternalRow(size = 1)
      withResource(new RapidsHostColumnBuilder(retType, batch.numRows)) { builder =>
        /**
         * This `nullSafe` is for https://github.com/NVIDIA/spark-rapids/issues/3942.
         * And more details can be found from
         *     https://github.com/NVIDIA/spark-rapids/pull/3997#issuecomment-957650846
         */
        val nullSafe = checkNull && GpuUserDefinedFunction.hostColumnAssertionEnabled
        new ColumnarToRowIterator(
            Iterator.single(new ColumnarBatch(argCols.toArray, batch.numRows())),
            NoopMetric,
            NoopMetric,
            NoopMetric,
            NoopMetric,
            nullSafe,
            // ensure `releaseSemaphore` is false so we don't release the semaphore
            // mid projection.
            releaseSemaphore = false).foreach { row =>
          retRow.update(0, evaluateRow(row))
          retConverter.append(retRow, 0, builder)
        }
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          val cpuRunningTime = System.nanoTime - prepareArgsEnd
          // Use log to record the eclipsed time for the UDF running before
          // figuring out how to support Spark metrics in this expression.
          logDebug(s"It took ${cpuRunningTime} ns to run UDF $name, and " +
            s"${prepareArgsEnd - cpuUDFStart} ns to get the input from children.")
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Failed to execute user defined function: " +
          s"($name: ($inputTypesString) => $outputType)", e)
    }
  } // end of `columnarEval`

}
