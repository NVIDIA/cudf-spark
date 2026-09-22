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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression
import java.util.concurrent.{Callable, CancellationException, Future, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, Expression, UnsafeProjection}
import org.apache.spark.sql.rapids.BridgeHostColumnProjection
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU expression that wraps a CPU expression subtree. This allows individual CPU expressions
 * to run while keeping the overall plan on the GPU.
 *
 * This expression:
 * 1. Takes GPU column data from its GPU expression children  
 * 2. Copies the data to host memory
 * 3. Evaluates the CPU expression tree on the host data
 * 4. Copies the results back to GPU memory
 *
 * @param gpuInputs GPU expressions that provide inputs to the CPU expression tree
 * @param cpuExpression The CPU expression tree to evaluate; returned as the last element of
 *                      children, after gpuInputs.
 * @param outputDataType The output data type of the expression
 * @param outputNullable Whether the output can be null
 */
case class GpuCpuBridgeExpression(
    gpuInputs: Seq[Expression],
    cpuExpression: Expression,
    outputDataType: DataType,
    outputNullable: Boolean) extends GpuExpression with ShimExpression 
    with Logging with GpuBind with GpuMetricsInjectable {

  override def children: Seq[Expression] = gpuInputs ++ Seq(cpuExpression)

  override def dataType: DataType = outputDataType
  override def nullable: Boolean = outputNullable
  // Bridge expressions may contain CPU expressions with unknown side effects
  override def hasSideEffects: Boolean = true

  // Mutable metrics fields - injected after binding
  private var cpuBridgeProcessingTime: Option[GpuMetric] = None
  private var cpuBridgeWaitTime: Option[GpuMetric] = None

  /**
   * Inject metrics into this expression. Called after binding but before execution.
   */
  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    import GpuMetric._
    cpuBridgeProcessingTime = metrics.get(CPU_BRIDGE_PROCESSING_TIME)
    cpuBridgeWaitTime = metrics.get(CPU_BRIDGE_WAIT_TIME)
  }

  override def prettyName: String = "gpu_cpu_bridge"

  override def toString: String = {
    val gpuInputsStr = gpuInputs.map(_.toString).mkString(", ")
    s"GpuCpuBridge(gpuInputs=[${gpuInputsStr}], cpuExpression=${cpuExpression.toString})"
  }

  override def sql: String = {
    val gpuInputsSql = gpuInputs.map(_.sql).mkString(", ")
    s"gpu_cpu_bridge(gpuInputs=[${gpuInputsSql}], cpuExpression=${cpuExpression.sql})"
  }

  override def bind(input: AttributeSeq): GpuExpression = {
    // The CPU expressions are already bound, so we don't need/want to
    // rebind them.

    // Regular GPU binding does not properly handle
    // named lambda variables. It would probably be good to fix this
    // at some point, but this works for now...
    val boundGpuInputs = gpuInputs.map { arg =>
      GpuBindReferences.bindRefInternal[Expression, GpuExpression](arg, input, {
        case lr: GpuNamedLambdaVariable if input.indexOf(lr.exprId) >= 0 =>
          val ordinal = input.indexOf(lr.exprId)
          GpuBoundReference(ordinal, lr.dataType, input(ordinal).nullable)(lr.exprId, lr.name)
      })
    }
    val boundExpression = GpuCpuBridgeExpression(boundGpuInputs, cpuExpression,
      outputDataType, outputNullable)
    // Preserve metrics when binding
    boundExpression.cpuBridgeProcessingTime = this.cpuBridgeProcessingTime
    boundExpression.cpuBridgeWaitTime = this.cpuBridgeWaitTime
    boundExpression
  }

  @transient private lazy val resultType = GpuColumnVector.convertFrom(dataType, nullable)
  
  // Thread-local projection for code generation path. Each thread gets its own projection to
  // avoid internal memory reuse conflicts, and the cached projection is scoped to the current
  // Spark task attempt so long-lived bridge worker threads do not reuse stale projections.
  @transient private lazy val threadLocalProjection:
      ThreadLocal[(Long, BridgeHostColumnProjection)] =
    new ThreadLocal[(Long, BridgeHostColumnProjection)]()

  private def currentTaskAttemptId: Long =
    Option(org.apache.spark.TaskContext.get()).map(_.taskAttemptId()).getOrElse(Long.MinValue)

  private def getThreadLocalProjection(): BridgeHostColumnProjection = {
    val taskAttemptId = currentTaskAttemptId
    val cached = threadLocalProjection.get()
    if (cached == null || cached._1 != taskAttemptId) {
      threadLocalProjection.remove()
      val projection = createCodeGeneratedProjection()
      threadLocalProjection.set((taskAttemptId, projection))
      projection
    } else {
      cached._2
    }
  }

  private def clearThreadLocalProjection(): Unit = threadLocalProjection.remove()
  
  @transient private lazy val evaluationFunction: (Iterator[InternalRow], Int) => GpuColumnVector =
    createEvaluationFunction()

  // One input attribute per gpuInput, carrying its data type and nullability.
  @transient private lazy val inputAttrs: Seq[Attribute] = gpuInputs.zipWithIndex.map {
    case (e, i) => AttributeReference(s"_bridge_in_$i", e.dataType, e.nullable)()
  }

  // Returns a fresh projection that copies each input row into an UnsafeRow.
  private def makeInputToUnsafe(): UnsafeProjection =
    UnsafeProjection.create(inputAttrs, inputAttrs)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      // Handle empty batch case
      return GpuColumnVector.fromNull(numRows, dataType)
    }
    
    // Time the entire CPU bridge operation from the SparkPlan perspective
    GpuMetric.nsOption(cpuBridgeWaitTime) {
      // Nondeterministic expressions are excluded from the bridge, so parallel evaluation is safe.
      if (GpuCpuBridgeThreadPool.shouldParallelize(numRows)) {
        logDebug(s"Using parallel processing for ${numRows} rows " +
          s"(expression: ${cpuExpression.getClass.getSimpleName})")
        evaluateInParallel(batch)
      } else {
        logDebug(s"Using sequential processing for ${numRows} rows " +
          s"(expression: ${cpuExpression.getClass.getSimpleName})")
        evaluateSequentially(batch)
      }
    }
  }
  
  /**
   * Evaluate the expression sequentially (single-threaded) for small batches.
   */
  private def evaluateSequentially(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    // Evaluate GPU input expressions and get GPU columns
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))

    // Time the CPU processing (columnar->row->CPU expr->columnar)
    GpuMetric.nsOption(cpuBridgeProcessingTime) {
      // Convert columnar data to rows for CPU expression evaluation
      // Note: ColumnarBatch takes ownership of the columns, so we don't close them separately
      val inputBatch = new ColumnarBatch(gpuInputColumns.toArray, numRows)
      val rowIterator = closeOnExcept(inputBatch) { _ =>
        new ColumnarToRowIterator(
          Iterator.single(inputBatch),
          NoopMetric,
          NoopMetric,
          NoopMetric,
          NoopMetric,
          nullSafe = false,
          // The bridge remains inside the owning GPU task/operator while rows are materialized
          // on CPU, so semaphore ownership stays with the caller.
          releaseSemaphore = false
        )
      }

      withResource(rowIterator) { rowIterator =>
        evaluationFunction(rowIterator.map(makeInputToUnsafe()), numRows)
      }
    }
  }
  
  /**
   * Evaluate the expression in parallel using sub-batches for large batches.
   */
  private def evaluateInParallel(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    val ranges = GpuCpuBridgeThreadPool.getSubBatchRanges(numRows)

    logDebug(s"Processing $numRows rows in ${ranges.length} parallel sub-batches " +
      s"AVG ${numRows.toDouble / ranges.length}")

    // Retry is still owned by the operator evaluating this expression. The bridge makes the
    // derived GPU input columns spillable before worker submission, but the caller-owned input
    // batch is outside this expression's ownership and cannot be made spillable here. Workers
    // also temporarily hold materialized subVector slices that are not spillable while CPU row
    // conversion consumes them, so a concurrent OOM in that window can fail rather than spill.
    // Retryable worker failures are unwrapped below so the owning task's retry boundary sees the
    // original exception, but that does not guarantee recovery from every OOM.

    // Evaluate GPU input expressions once
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))
    
    // Create spillable wrapper for GPU input columns to ensure they can be spilled
    val spillableInputBatch = closeOnExcept(
      new ColumnarBatch(gpuInputColumns.toArray, numRows)) { inputBatch =>
      SpillableColumnarBatch(inputBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    // Pool threads accumulate their CPU processing time here. The owning task adds the total
    // to the shared metric once after all sub-batches complete, because GpuMetric (SQLMetric)
    // updates are not thread-safe and would race across the pool threads.
    val processingTimeNs = new LongAdder()

    val abortRequested = new AtomicBoolean(false)

    try {
      val subBatchResults = withResource(
        new SharedSpillableInput(spillableInputBatch)) { sharedInput =>
        // Submit sub-batch processing tasks to the thread pool with priority
        val submittedTasks = submitSubBatchTasks(sharedInput, ranges, processingTimeNs,
          abortRequested)

        // Collect results in submission order. safeMap closes the already-collected results if a
        // future fails; drain only the not-yet-consumed tasks, since this one and the earlier
        // ones have either failed or are already closed.
        submittedTasks.zipWithIndex.safeMap { case (submittedTask, i) =>
          Try {
            // The spillable result transfers ownership, so we incRefCount the column we keep.
            withResource(submittedTask.future.get()) { spillableResult =>
              withResource(spillableResult.getColumnarBatch()) { cb =>
                cb.column(0).asInstanceOf[GpuColumnVector].incRefCount()
              }
            }
          } match {
            case Success(col) => col
            case Failure(exception) =>
              cleanupFuturesOnFailure(submittedTasks.drop(i + 1), abortRequested)
              throw unwrapExecutionException(exception)
          }
        }
      }

      // Concatenate all sub-batch results
      concatenateResults(subBatchResults)
    } finally {
      // Flush on both success and failure: a failing sub-batch throws out of the block above, and
      // the time already recorded by completed workers would otherwise be lost.
      cpuBridgeProcessingTime.foreach(_.add(processingTimeNs.sum()))
    }
  }

  private def submitSubBatchTasks(
      sharedInput: SharedSpillableInput,
      ranges: Seq[(Int, Int)],
      processingTimeNs: LongAdder,
      abortRequested: AtomicBoolean): Seq[SubmittedSubBatchTask] = {
    val submittedTasks = scala.collection.mutable.ArrayBuffer[SubmittedSubBatchTask]()
    try {
      ranges.foreach { case (startRow, endRow) =>
        val submittedTask = closeOnExcept(sharedInput.acquire()) { inputRef =>
          val task = new SpillableSubBatchEvaluationTask(inputRef, startRow, endRow,
            processingTimeNs, abortRequested)
          closeOnExcept(task) { task =>
            SubmittedSubBatchTask(task, GpuCpuBridgeThreadPool.submitPrioritizedTask(task))
          }
        }
        submittedTasks += submittedTask
      }
      submittedTasks.toSeq
    } catch {
      case t: Throwable =>
        cleanupFuturesOnFailure(submittedTasks.toSeq, abortRequested)
        throw t
    }
  }

  /**
   * Evaluates a sub-batch of rows on the CPU with spillable input and output.
   */
  private class SpillableSubBatchEvaluationTask(
      spillableInputRef: SharedSpillableInputRef,
      startRow: Int,
      endRow: Int,
      processingTimeNs: LongAdder,
      abortRequested: AtomicBoolean)
      extends Callable[SpillableColumnarBatch] with AutoCloseable {

    override def close(): Unit = spillableInputRef.close()

    override def call(): SpillableColumnarBatch = {
      val subBatchSize = endRow - startRow

      // Retry is owned by the operator evaluating this expression. Do not add a retry boundary
      // inside these workers, because blocking shared pool threads can prevent higher-priority
      // bridge work from starting. Retryable failures must propagate to the owning task thread.
      try {
        throwIfAborted()
        // Create a sub-batch wrapper that represents just the slice we need
        val subBatchInput = withResource(spillableInputRef.getColumnarBatch()) { inputBatch =>
          val subBatchColumns = (0 until inputBatch.numCols()).safeMap { i =>
            val col = inputBatch.column(i).asInstanceOf[GpuColumnVector]
            closeOnExcept(col.getBase.subVector(startRow, endRow)) { sliced =>
              GpuColumnVector.from(sliced, col.dataType())
            }
          }
          new ColumnarBatch(subBatchColumns.toArray, subBatchSize)
        }
        // The iterator takes ownership of the subBatchInput
        val rowIterator = closeOnExcept(subBatchInput) { _ =>
          new ColumnarToRowIterator(
            Iterator.single(subBatchInput),
            NoopMetric,
            NoopMetric,
            NoopMetric,
            NoopMetric,
            nullSafe = false,
            // The bridge remains inside the owning GPU task/operator while rows are materialized
            // on CPU, so semaphore ownership stays with the caller.
            releaseSemaphore = false
          )
        }
        withResource(rowIterator) { rowIterator =>
          withResource(new NvtxRange("evaluateOnCPU", NvtxColor.BLUE)) { _ =>
            // Time the actual CPU processing on this thread
            val processingStartTime = System.nanoTime()
            val result = try {
              evaluationFunction(rowIterator.map(makeInputToUnsafe()), subBatchSize)
            } finally {
              processingTimeNs.add(System.nanoTime() - processingStartTime)
            }

            // Wrap the result column in a spillable batch.
            if (abortRequested.get()) {
              withResource(result) { _ =>
                throw new CancellationException("Sub-batch evaluation aborted")
              }
            }
            val spillableResult = closeOnExcept(result) { result =>
              val resultBatch = new ColumnarBatch(Array(result), subBatchSize)
              SpillableColumnarBatch(resultBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            }
            if (abortRequested.get()) {
              withResource(spillableResult) { _ =>
                throw new CancellationException("Sub-batch evaluation aborted")
              }
            }
            spillableResult
          }
        }
      } finally {
        clearThreadLocalProjection()
        spillableInputRef.close()
      }
    }

    private def throwIfAborted(): Unit = {
      if (abortRequested.get()) {
        throw new CancellationException("Sub-batch evaluation aborted")
      }
    }
  }

  private class SharedSpillableInput(spillableInputBatch: SpillableColumnarBatch)
      extends AutoCloseable {
    private[this] val lock = new Object()

    def acquire(): SharedSpillableInputRef = lock.synchronized {
      spillableInputBatch.incRefCount()
      new SharedSpillableInputRef(spillableInputBatch, lock)
    }

    override def close(): Unit = lock.synchronized {
      spillableInputBatch.close()
    }
  }

  private class SharedSpillableInputRef(
      spillableInputBatch: SpillableColumnarBatch,
      lock: AnyRef) extends AutoCloseable {
    private[this] val closed = new AtomicBoolean(false)

    def getColumnarBatch(): ColumnarBatch = spillableInputBatch.getColumnarBatch()

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        lock.synchronized {
          spillableInputBatch.close()
        }
      }
    }
  }

  private case class SubmittedSubBatchTask(
      task: SpillableSubBatchEvaluationTask,
      future: Future[SpillableColumnarBatch])
  
  // Future.get() wraps failures in ExecutionException; unwrap so retryable OOM types reach
  // the surrounding retry framework.
  private def unwrapExecutionException(t: Throwable): Throwable = t match {
    case e: java.util.concurrent.ExecutionException if e.getCause != null => e.getCause
    case other => other
  }

  // Request abort, cancel tasks that have not started, and wait briefly for running tasks so any
  // returned spillable results can be closed. Running tasks own their input refs and release them
  // in their own finally blocks.
  private def cleanupFuturesOnFailure(
      submittedTasks: Seq[SubmittedSubBatchTask],
      abortRequested: AtomicBoolean): Unit = {
    abortRequested.set(true)

    val runningOrCompletedTasks = submittedTasks.filter { submitted =>
      val cancelledBeforeStart = submitted.future.cancel(false)
      if (cancelledBeforeStart) {
        submitted.task.close()
      }
      !cancelledBeforeStart
    }

    if (runningOrCompletedTasks.nonEmpty) {
      logDebug(s"${runningOrCompletedTasks.length} futures were already running or completed, " +
        "waiting for completion to clean up resources")
    }

    var cleanupFailure: Option[Throwable] = None
    runningOrCompletedTasks.foreach { submitted =>
      try {
        // Wait up to 1 second for the future to complete. If it times out, the worker still owns
        // its input reference and will close it when it exits.
        withResource(submitted.future.get(1, TimeUnit.SECONDS)) { _ => () }
      } catch {
        case _: TimeoutException =>
          logWarning("Timed out waiting for future completion during cleanup. " +
            "The worker owns its input reference and will close it when it exits.")
        case _: CancellationException =>
          // Future was cancelled. No output was returned.
        case t: Throwable =>
          unwrapExecutionException(t) match {
            case _: CancellationException =>
              // The worker observed abortRequested. No output was returned.
            case cleanupException =>
              // Capture the first cleanup exception but don't let it mask the original error
              if (cleanupFailure.isEmpty) {
                cleanupFailure = Some(cleanupException)
              } else {
                logWarning(s"Exception during future cleanup: ${cleanupException.getMessage}")
              }
          }
      }
    }
    
    // Log any cleanup failures but don't throw - we want the original exception to propagate
    cleanupFailure.foreach { t =>
      logError("Failed to clean up some future resources during error handling", t)
    }
  }
  
  /**
   * Concatenate results from multiple sub-batches into a single result column.
   */
  private def concatenateResults(subBatchResults: Seq[GpuColumnVector]): GpuColumnVector = {
    if (subBatchResults.length == 1) {
      subBatchResults.head
    } else {
      withResource(subBatchResults) { ownedResults =>
        // Convert to cuDF columns for concatenation
        val cudfColumns = ownedResults.map(_.getBase)
        val concatenated = ai.rapids.cudf.ColumnVector.concatenate(cudfColumns: _*)
        closeOnExcept(concatenated) { concat =>
          GpuColumnVector.from(concat, dataType)
        }
      }
    }
  }

  /**
   * Creates an evaluation function for the CPU expression.
   * Takes an iterator of rows and the expected row count, produces a complete 
   * GpuColumnVector result.
   * 
   * Uses code generation by default. If codegen fails, Spark's CodeGeneratorWithInterpretedFallback
   * will automatically create an InterpretedBridgeHostColumnProjection instead.
   */
  private def createEvaluationFunction(): (Iterator[InternalRow], Int) => GpuColumnVector = {
    (rowIterator: Iterator[InternalRow], numRows: Int) => {
      withResource(new RapidsHostColumnBuilder(resultType, numRows)) { builder =>
        // Get thread-local projection - each thread has its own to avoid memory conflicts
        // but the projection is reused across batches within the same thread for performance
        val projection = getThreadLocalProjection()
        
        // Stream through the iterator without caching - preserves memory efficiency
        projection.apply(rowIterator, Array(builder))

        // Build the result column and put it on the GPU
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    }
  }
  
  /**
   * Creates a code-generated projection for the CPU expression using a 
   * modified version of Spark's code generation.
   * This provides much better performance than interpreted evaluation.
   */
  private def createCodeGeneratedProjection(): BridgeHostColumnProjection =
    BridgeHostColumnProjection.create(Seq(cpuExpression))
}
