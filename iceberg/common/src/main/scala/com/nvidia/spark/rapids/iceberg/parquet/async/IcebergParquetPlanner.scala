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

package com.nvidia.spark.rapids.iceberg.parquet.async

import java.util.{
  ArrayDeque,
  ArrayList,
  Collections,
  IdentityHashMap,
  Iterator => JIterator,
  Optional
}
import java.util.concurrent.{
  CompletableFuture,
  CompletionException,
  ExecutorService,
  ScheduledFuture,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{
  CachedGpuBatchIterator,
  GpuSemaphore,
  RapidsConf,
  RmmRapidsRetryIterator,
  SingleGpuColumnarBatchIterator
}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuMetric.{
  FILECACHE_DATA_RANGE_HITS,
  FILECACHE_DATA_RANGE_HITS_SIZE,
  FILECACHE_DATA_RANGE_MISSES,
  FILECACHE_DATA_RANGE_MISSES_SIZE,
  FILECACHE_DATA_RANGE_READ_TIME,
  FILTER_TIME,
  ICEBERG_ASYNC_FILE_READ_TIME,
  ICEBERG_ASYNC_REQUEST_COUNT,
  ICEBERG_ASYNC_REQUESTED_BYTES,
  IO_WAIT_TIME
}
import com.nvidia.spark.rapids.iceberg.parquet.{
  GpuAsyncIcebergParquetReader,
  GpuParquetReaderPostProcessor,
  IcebergPartitionedFile,
  MultiThread
}
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.parquet.MakeParquetTableProducer
import com.nvidia.spark.rapids.reader.ReadPlanner

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The concrete asynchronous read planner for Iceberg Parquet.
 *
 * This class deliberately contains the complete Iceberg pipeline: footer filtering, packed data
 * reads, event-driven row-group planning, logical Parquet combination, and GPU decode. The shared
 * [[com.nvidia.spark.rapids.reader.UnifiedReader]] only starts the futures and asks this planner
 * for the next ready decode input.
 *
 * Footer and data completions call synchronized planner methods directly. With combining enabled,
 * files are admitted in data-completion order; otherwise they are admitted in input order. A
 * combine timeout closes any partial group. Emitted results remain in plan-emission order even if
 * their worker futures complete out of order.
 *
 * The planner owns every successful [[FileFragment]] after [[addFile]] returns. Each emitted
 * [[ParquetCombinedResult]] retains the fragments it needs, so closing the planner cannot
 * invalidate an input already handed to the Spark task thread.
 */
final class IcebergParquetPlanner(
    reader: GpuAsyncIcebergParquetReader,
    taskContext: TaskContext,
    executor: ExecutorService,
    closed: AtomicBoolean)
    extends ReadPlanner[
      IcebergPartitionedFile,
      FooterResult,
      FileFragment,
      ParquetCombinedResult] {

  private val conf = reader.conf
  private val rapidsConf = new RapidsConf(SparkEnv.get.conf)
  private val requestSizeBytes = rapidsConf.icebergAsyncReadRequestSize
  private val multiThreadConf = conf.threadConf.asInstanceOf[MultiThread]
  private val combineThreshold = if (multiThreadConf.disableCombining) {
    0L
  } else {
    multiThreadConf.combineConf.combineThresholdSize
  }
  private val combineWaitMs = if (multiThreadConf.disableCombining) {
    0L
  } else {
    Math.max(0L, multiThreadConf.combineConf.combineWaitTime.toLong)
  }
  private val combineEnabled = combineThreshold > 0L
  private val session = new StableGreedyReadPlanner(
    conf.maxBatchSizeRows,
    conf.maxBatchSizeBytes,
    combineThreshold).newSession()
  private val taskAttemptId = if (taskContext == null) -1L else taskContext.taskAttemptId()

  private val files = new ArrayList[FileState]()
  private val dataByFooter = new IdentityHashMap[FooterResult, FileFragment]()
  private val attributedFragments = Collections.newSetFromMap(
    new IdentityHashMap[FileFragment, java.lang.Boolean]())
  private val outputs =
    new ArrayDeque[CompletableFuture[Optional[ParquetCombinedResult]]]()
  private val waiters =
    new ArrayDeque[CompletableFuture[Optional[ParquetCombinedResult]]]()
  private val combinedInputs = new ArrayList[CompletableFuture[ParquetCombinedResult]]()

  private var nextOrderedFile = 0
  private var admittedFiles = 0
  private var registrationComplete = false
  private var planningComplete = false
  private var resourcesClosed = false
  private var failure: Throwable = _
  private var timerGeneration = 0L
  private var timer: ScheduledFuture[_] = _

  override def readFooter(
      file: IcebergPartitionedFile,
      executor: ExecutorService): CompletableFuture[FooterResult] = {
    CompletableFuture.supplyAsync(() => runAsTask {
      checkOpen()
      val start = System.nanoTime()
      try {
        val (parquetInfo, shadedFileReadSchema) =
          reader.filterParquetBlocks(file, conf.expectedSchema)
        val postProcessor = new GpuParquetReaderPostProcessor(
          parquetInfo,
          reader.constantsProvider(file),
          conf.expectedSchema,
          shadedFileReadSchema,
          conf.metrics)
        new FooterResult(
          file,
          parquetInfo.blocks.toList.asJava,
          parquetInfo.schema,
          parquetInfo.readSchema,
          parquetInfo.dateRebaseMode,
          parquetInfo.timestampRebaseMode,
          parquetInfo.hasInt96Timestamps,
          postProcessor)
      } finally {
        conf.metrics.get(FILTER_TIME).foreach(_ += System.nanoTime() - start)
      }
    }, executor)
  }

  override def readData(
      file: IcebergPartitionedFile,
      footer: FooterResult,
      executor: ExecutorService): CompletableFuture[FileFragment] = {
    CompletableFuture.supplyAsync(() => runAsTask {
      checkOpen()
      val input = reader.rapidsFileIO.newInputFile(file.file.getDelegate.location())
      ParquetDataReader.read(footer, input, closed, requestSizeBytes)
    }, executor)
  }

  override def decode(parquetInput: ParquetCombinedResult): JIterator[ColumnarBatch] = {
    val subtask = parquetInput.getPlan
    val stats = parquetInput.getStats
    conf.metrics.get(FILECACHE_DATA_RANGE_HITS).foreach(_ += stats.getCacheHitCount)
    conf.metrics.get(FILECACHE_DATA_RANGE_HITS_SIZE).foreach(_ += stats.getCacheHitBytes)
    conf.metrics.get(FILECACHE_DATA_RANGE_MISSES).foreach(_ += stats.getCacheMissCount)
    conf.metrics.get(FILECACHE_DATA_RANGE_MISSES_SIZE).foreach(_ += stats.getCacheMissBytes)
    conf.metrics.get(FILECACHE_DATA_RANGE_READ_TIME).foreach(_ += stats.getCacheReadNanos)
    conf.metrics.get(IO_WAIT_TIME).foreach(_ += stats.getIoReadWaitNanos)
    conf.metrics.get(ICEBERG_ASYNC_FILE_READ_TIME).foreach(_ += stats.getIoNanos)
    conf.metrics.get(ICEBERG_ASYNC_REQUEST_COUNT).foreach(_ += stats.getIoRequestCount)
    conf.metrics.get(ICEBERG_ASYNC_REQUESTED_BYTES).foreach(_ += stats.getIoRequestedBytes)
    conf.metrics.get("readBufferSize").foreach(_ += subtask.getDataSizeBytes)
    val firstFooter = subtask.getFileSlices.get(0).getFooter
    val postProcessor = firstFooter.getPostProcessor
    val readSchema = firstFooter.getReadSchema
    val clippedSchema = firstFooter.getClippedSchema

    if (clippedSchema.getFieldCount == 0) {
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val emptyInput = new ColumnarBatch(
        Array.empty[org.apache.spark.sql.vectorized.ColumnVector],
        Math.toIntExact(subtask.getRowCount))
      val processed = postProcessor.process(emptyInput)
      new CloseableJavaBatchIterator(new SingleGpuColumnarBatchIterator(processed))
    } else {
      val parseOptions = reader.asyncParquetOptions(readSchema, clippedSchema)
      val splits = subtask.getFileSlices.asScala
        .map(_.getFooter.getFile.sparkPartitionedFile)
        .toArray
      val decoded = RmmRapidsRetryIterator.withRetryNoSplit[Iterator[ColumnarBatch]] {
        val attempt = parquetInput.materialize()
        // A retry receives fresh owning header/footer buffers and fresh owning references to the
        // fragment slices. MakeParquetTableProducer consumes them once invoked; closeOnExcept
        // covers failures before that ownership transfer. CachedGpuBatchIterator eagerly drains
        // the producer, so returned batches no longer depend on asynchronous host storage.
        closeOnExcept(attempt) { hostBuffers =>
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val producer = MakeParquetTableProducer(
            conf.useChunkedReader,
            conf.maxChunkedReaderMemoryUsageSizeBytes,
            conf.conf,
            conf.targetBatchSizeBytes,
            parseOptions,
            hostBuffers,
            conf.metrics,
            firstFooter.getDateRebaseMode,
            firstFooter.getTimestampRebaseMode,
            firstFooter.hasInt96Timestamps(),
            conf.caseSensitive,
            useFieldId = false,
            readSchema,
            clippedSchema,
            splits,
            conf.parquetDebugDumpPrefix,
            conf.parquetDebugDumpAlways)
          CachedGpuBatchIterator(producer, readSchema.fields.map(_.dataType))
        }
      }
      new PostProcessingJavaBatchIterator(decoded, postProcessor)
    }
  }

  override def addFile(
      fileId: Int,
      footer: CompletableFuture[FooterResult],
      data: CompletableFuture[FileFragment]): Unit = synchronized {
    checkAccepting()
    if (fileId != files.size()) {
      throw new IllegalArgumentException("file IDs must be registered contiguously")
    }
    if (footer == null) {
      throw new NullPointerException("footer")
    }
    if (data == null) {
      throw new NullPointerException("data")
    }
    val state = new FileState(footer)
    // Publish the state first because an already-completed future invokes its callback inline.
    files.add(state)
    footer.whenComplete((value: FooterResult, error: Throwable) => onFooter(state, value, error))
    data.whenComplete((value: FileFragment, error: Throwable) => onData(state, value, error))
  }

  override def noMoreFiles(): Unit = synchronized {
    checkAccepting()
    registrationComplete = true
    finishIfPossible()
  }

  override def nextReady(): CompletableFuture[Optional[ParquetCombinedResult]] = synchronized {
    if (!outputs.isEmpty) {
      outputs.removeFirst()
    } else if (failure != null) {
      failedFuture(failure)
    } else if (planningComplete || closed.get()) {
      CompletableFuture.completedFuture(Optional.empty[ParquetCombinedResult]())
    } else {
      val waiter = new CompletableFuture[Optional[ParquetCombinedResult]]()
      waiters.addLast(waiter)
      waiter
    }
  }

  private def onFooter(
      state: FileState,
      footer: FooterResult,
      error: Throwable): Unit = synchronized {
    if (error != null) {
      fail(error)
    } else if (!closed.get()) {
      if (footer == null) {
        fail(new NullPointerException("footer future completed with null"))
      } else {
        state.footerValue = footer
      }
    }
  }

  private def onData(
      state: FileState,
      data: FileFragment,
      error: Throwable): Unit = synchronized {
    if (error != null) {
      fail(error)
    } else if (data == null) {
      fail(new IllegalStateException("data future completed with null"))
    } else if (closed.get() || failure != null) {
      data.close()
    } else {
      state.dataValue = data
      val footerAvailable = if (state.footerValue != null) {
        true
      } else {
        try {
          state.footerValue = state.footer.join()
          true
        } catch {
          case footerError: Throwable =>
            data.close()
            fail(footerError)
            false
        }
      }
      if (footerAvailable) {
        if (combineEnabled) {
          admit(state)
        } else {
          drainInputOrder()
        }
        finishIfPossible()
      }
    }
  }

  private def drainInputOrder(): Unit = {
    var canContinue = true
    while (canContinue && nextOrderedFile < files.size()) {
      val next = files.get(nextOrderedFile)
      if (next.dataValue == null) {
        canContinue = false
      } else {
        nextOrderedFile += 1
        admit(next)
      }
    }
  }

  private def admit(state: FileState): Unit = {
    if (!state.admitted) {
      state.admitted = true
      admittedFiles += 1
      dataByFooter.put(state.footerValue, state.dataValue)
      emitPlans(session.add(state.footerValue))
      if (combineEnabled && session.hasOpenBlocks) {
        scheduleFreshCombineTimeout()
      } else {
        cancelTimer()
      }
    }
  }

  private def scheduleFreshCombineTimeout(): Unit = {
    cancelTimer()
    timerGeneration += 1
    val generation = timerGeneration
    timer = IcebergParquetPlanner.timer.schedule(
      new Runnable {
        override def run(): Unit = onCombineTimeout(generation)
      },
      combineWaitMs,
      TimeUnit.MILLISECONDS)
  }

  private def onCombineTimeout(generation: Long): Unit = synchronized {
    if (!closed.get() && failure == null && !planningComplete &&
        generation == timerGeneration && session.hasOpenBlocks) {
      timer = null
      emitPlans(session.flush())
      finishIfPossible()
    }
  }

  private def finishIfPossible(): Unit = {
    if (!planningComplete && registrationComplete && admittedFiles == files.size()) {
      cancelTimer()
      emitPlans(session.finish())
      planningComplete = true
      while (!waiters.isEmpty) {
        waiters.removeFirst().complete(Optional.empty[ParquetCombinedResult]())
      }
    }
  }

  private def emitPlans(plans: java.util.List[ReadSubtask]): Unit = {
    plans.asScala.foreach { plan =>
      if (failure == null) {
        val fragments = new ArrayList[FileFragment](plan.getFileSlices.size())
        var planIsValid = true
        plan.getFileSlices.asScala.foreach { slice =>
          if (planIsValid) {
            val fragment = dataByFooter.get(slice.getFooter)
            if (fragment == null) {
              fail(new IllegalStateException("plan references data that is not ready"))
              planIsValid = false
            } else {
              fragments.add(fragment)
            }
          }
        }
        if (planIsValid) {
          val combined = combine(plan, fragments)
          combinedInputs.add(combined)
          val output = new CompletableFuture[Optional[ParquetCombinedResult]]()
          combined.whenComplete((input: ParquetCombinedResult, error: Throwable) => {
            if (error != null) {
              IcebergParquetPlanner.this.synchronized {
                fail(error)
              }
              output.completeExceptionally(unwrap(error))
            } else if (closed.get() && input != null) {
              input.close()
              output.complete(Optional.of(input))
            } else {
              output.complete(Optional.of(input))
            }
          })
          if (waiters.isEmpty) {
            outputs.addLast(output)
          } else {
            pipe(output, waiters.removeFirst())
          }
        }
      }
    }
  }

  /** Build a zero-copy logical Parquet input and attribute each fragment's metrics once. */
  private def combine(
      plan: ReadSubtask,
      data: java.util.List[FileFragment]): CompletableFuture[ParquetCombinedResult] = synchronized {
    val fragments = new ArrayList[FileFragment](data)
    val metricFragments = new ArrayList[FileFragment]()
    fragments.asScala.foreach { fragment =>
      if (attributedFragments.add(fragment)) {
        metricFragments.add(fragment)
      }
    }
    CompletableFuture.supplyAsync(() => {
      val start = System.nanoTime()
      val stats = aggregate(metricFragments, System.nanoTime() - start)
      new ParquetCombinedResult(plan, fragments, stats)
    }, executor)
  }

  private def aggregate(
      fragments: java.util.List[FileFragment],
      combineNanos: Long): SubtaskStats = {
    var ioNanos = 0L
    var allocNanos = 0L
    var readWaitNanos = 0L
    var routeNanos = 0L
    var finalizeNanos = 0L
    var requestCount = 0L
    var requestedBytes = 0L
    var hitCount = 0L
    var hitBytes = 0L
    var missCount = 0L
    var missBytes = 0L
    var cacheReadNanos = 0L
    fragments.asScala.foreach { fragment =>
      val stats = fragment.getStats
      ioNanos = Math.addExact(ioNanos, stats.ioNanos)
      allocNanos = Math.addExact(allocNanos, stats.allocNanos)
      readWaitNanos = Math.addExact(readWaitNanos, stats.readWaitNanos)
      routeNanos = Math.addExact(routeNanos, stats.routeNanos)
      finalizeNanos = Math.addExact(finalizeNanos, stats.finalizeNanos)
      requestCount = Math.addExact(requestCount, stats.requestCount)
      requestedBytes = Math.addExact(requestedBytes, stats.requestedBytes)
      hitCount = Math.addExact(hitCount, stats.cacheHitCount)
      hitBytes = Math.addExact(hitBytes, stats.cacheHitBytes)
      missCount = Math.addExact(missCount, stats.cacheMissCount)
      missBytes = Math.addExact(missBytes, stats.cacheMissBytes)
      cacheReadNanos = Math.addExact(cacheReadNanos, stats.cacheReadNanos)
    }
    new SubtaskStats(
      ioNanos,
      allocNanos,
      readWaitNanos,
      routeNanos,
      finalizeNanos,
      requestCount,
      requestedBytes,
      combineNanos,
      hitCount,
      hitBytes,
      missCount,
      missBytes,
      cacheReadNanos)
  }

  private def fail(error: Throwable): Unit = {
    if (failure == null && !closed.get()) {
      failure = unwrap(error)
      cancelTimer()
      while (!waiters.isEmpty) {
        waiters.removeFirst().completeExceptionally(failure)
      }
      outputs.clear()
    }
  }

  private def pipe[T](
      source: CompletableFuture[T],
      destination: CompletableFuture[T]): Unit = {
    source.whenComplete((value: T, error: Throwable) => {
      if (error == null) {
        destination.complete(value)
      } else {
        destination.completeExceptionally(unwrap(error))
      }
    })
  }

  override def close(): Unit = synchronized {
    if (!resourcesClosed) {
      resourcesClosed = true
      closed.set(true)
      cancelTimer()
      while (!waiters.isEmpty) {
        waiters.removeFirst().complete(Optional.empty[ParquetCombinedResult]())
      }
      outputs.clear()
      combinedInputs.asScala.foreach { combined =>
        combined.whenComplete((input: ParquetCombinedResult, _: Throwable) => {
          if (input != null) {
            input.close()
          }
        })
      }
      files.asScala.foreach { state =>
        if (state.dataValue != null && !state.baseReferenceClosed) {
          state.baseReferenceClosed = true
          state.dataValue.close()
        }
      }
    }
  }

  private def cancelTimer(): Unit = {
    timerGeneration += 1
    if (timer != null) {
      timer.cancel(false)
      timer = null
    }
  }

  private def checkAccepting(): Unit = {
    if (registrationComplete) {
      throw new IllegalStateException("all files have already been registered")
    }
    if (closed.get()) {
      throw new IllegalStateException("planner is closed")
    }
    if (failure != null) {
      throw new CompletionException(failure)
    }
  }

  private def checkOpen(): Unit = {
    if (closed.get()) {
      throw new java.util.concurrent.CancellationException("Parquet reader is closed")
    }
  }

  private def runAsTask[T](operation: => T): T = {
    try {
      if (taskContext == null) {
        operation
      } else {
        TrampolineUtil.setTaskContext(taskContext)
        RmmSpark.poolThreadWorkingOnTask(taskAttemptId)
        try {
          operation
        } finally {
          RmmSpark.poolThreadFinishedForTask(taskAttemptId)
          TrampolineUtil.unsetTaskContext()
        }
      }
    } catch {
      case error: CompletionException => throw error
      case error: Throwable => throw new CompletionException(error)
    }
  }

  private def unwrap(error: Throwable): Throwable = {
    var current = error
    while (current.isInstanceOf[CompletionException] && current.getCause != null) {
      current = current.getCause
    }
    current
  }

  private def failedFuture[T](error: Throwable): CompletableFuture[T] = {
    val result = new CompletableFuture[T]()
    result.completeExceptionally(error)
    result
  }

  /** Mutable state for one registered file; all accesses are protected by this planner's lock. */
  private final class FileState(val footer: CompletableFuture[FooterResult]) {
    var footerValue: FooterResult = _
    var dataValue: FileFragment = _
    var admitted = false
    var baseReferenceClosed = false
  }

  /** Java-facing wrapper that preserves the RAPIDS iterator's close semantics. */
  private class CloseableJavaBatchIterator(
      protected val owner: Iterator[ColumnarBatch])
      extends JIterator[ColumnarBatch] with AutoCloseable {
    private var iteratorClosed = false

    override def hasNext: Boolean = {
      val more = owner.hasNext
      if (!more) {
        close()
      }
      more
    }

    override def next(): ColumnarBatch = owner.next()

    override def close(): Unit = {
      if (!iteratorClosed) {
        owner match {
          case resource: AutoCloseable => resource.close()
          case _ =>
        }
        iteratorClosed = true
      }
    }
  }

  /** Applies one task-confined Iceberg post-processor and closes pending GPU batches on failure. */
  private class PostProcessingJavaBatchIterator(
      decoded: Iterator[ColumnarBatch],
      postProcessor: GpuParquetReaderPostProcessor)
      extends CloseableJavaBatchIterator(decoded) {

    override def next(): ColumnarBatch = {
      try {
        postProcessor.process(owner.next())
      } catch {
        case error: Throwable =>
          close()
          throw error
      }
    }
  }
}

private object IcebergParquetPlanner {
  private val timer = TrampolineUtil.newDaemonSingleThreadScheduledExecutor(
    "iceberg-parquet-combine-timer")
}
